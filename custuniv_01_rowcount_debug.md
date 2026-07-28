# custuniv_01 / CIS_DATA_POP_02 — row-count reconciliation

Playbook for diagnosing a row-count mismatch between the generated
`cbs.CIS_DATA_POP_02` and production `CIS_DATA_POP_02`.

## Observed (MAY ME)
| | rows |
|---|---|
| `CIS_DATA_NEW2` (driving table, pre-filter) | 51,530,835 |
| `CIS_DATA_POP_02` (generated)               | 55,536,627 |
| **delta**                                   | **+4,005,792** (~8%) |

The generated output exceeds the driving table's **raw** count. Since the `WHERE`
clause can only *reduce* `CIS_DATA_NEW2`, the real fan-out is even larger than 4M
(filters trim 51.5M, then a join multiplies it back up past 55.5M). Output exceeding
the driving table is proof of a `LEFT JOIN` fan-out on its own -- no distinct-keys
check needed. (An earlier 55.3M-vs-55.5M / +190K note compared the wrong month-end.)

## Invariant
`custuniv_01` is a LEFT-JOIN enrichment **driven by `CIS_DATA_NEW2`**: the output must
be **exactly one row per `CIS_DATA_NEW2` row that survives the `WHERE`** — never more.
So `output rows == filtered CIS_DATA_NEW2 rows`. Prod's 55,346,570 IS that filtered
count. `+190,057` is therefore either a fan-out or a base/filter difference.

Sub in the run's `<M>` (`mth_tm_id`), `<s>` (`stream`), `<rundate>`, `<yyyymm>`.

## 1. Fan-out vs base/filter difference
Distinguish the two causes first:

```sql
SELECT
    COUNT(*)                                         AS gather_rows,       -- 55,536,627?
    COUNT(DISTINCT (a.account, a.cid, a.file_date))  AS distinct_cis_keys  -- true CIS grain
FROM ingestion.CIS_DATA_NEW2 a
WHERE a.file_yr_mth = '<yyyymm>' AND a.RELATION_CODE <> 'POA'
  AND COALESCE(a.PRODUCT, '') <> 'SEA';
```
- `distinct_cis_keys == 55,346,570` -> **fan-out** (a join multiplies rows). Go to (2).
- `distinct_cis_keys == 55,536,627` -> **base/filter difference** (extra CIS rows prod
  drops -- most likely the `PRD_CD NOT IN ('VFB','BLV')` filter, applied in prod via the
  revolving join, or the emulated CIS_DATA_NEW2 simply has extra rows).

The base count *including* the revolving PRD_CD filter (this itself fans out if
BASEL_ACCT_DIM / the revolving snapshot have dup keys):

```sql
-- base population (SAS WHERE clause on CIS_DATA_NEW2 + the revolving PRD_CD filter)
SELECT COUNT(*)
FROM ingestion.CIS_DATA_NEW2 a
LEFT JOIN ingestion.BASEL_ACCT_DIM b ON LPAD(a.account, 23, '0') = b.ACCT_NUM
LEFT JOIN ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT f
       ON b.BASEL_ACCT_ID = f.BASEL_ACCT_ID AND f.MTH_TM_ID = <M>
WHERE a.file_yr_mth = '<yyyymm>'
  AND a.RELATION_CODE <> 'POA'
  AND COALESCE(a.PRODUCT, '') <> 'SEA'
  AND COALESCE(f.PRD_CD, '') NOT IN ('VFB', 'BLV');
-- NOTE: this itself fans out if BASEL_ACCT_DIM / the revolving snapshot have dup keys.
```

## 2. Find which join has duplicate keys
Whichever query returns rows is (a) fan-out source.

```sql
-- b: account dim (joined on ACCT_NUM, NOT month-scoped -> a dup multiplies everything)
SELECT ACCT_NUM, COUNT(*) FROM ingestion.BASEL_ACCT_DIM
GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 5;

-- c/d/e: emulated drvd-vars (key = BASEL_ACCT_ID, MTH_TM_ID, STREAM)
SELECT BASEL_ACCT_ID FROM emulated.BASEL_REVLVNG_CR_BASE_DRVD_VARS
  WHERE MTH_TM_ID = <M> AND STREAM = '<s>' GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 5;
SELECT BASEL_ACCT_ID FROM emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2
  WHERE MTH_TM_ID = <M> AND STREAM = '<s>' GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 5;
SELECT BASEL_ACCT_ID FROM emulated.BASEL_MORT_ACCT_DRVD_VARS
  WHERE MTH_TM_ID = <M> AND STREAM = '<s>' GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 5;

-- f/g/h: ingestion snapshots (key = BASEL_ACCT_ID, MTH_TM_ID)
SELECT BASEL_ACCT_ID FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
  WHERE MTH_TM_ID = <M> GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 5;
SELECT BASEL_ACCT_ID FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT
  WHERE MTH_TM_ID = <M> GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 5;
SELECT BASEL_ACCT_ID FROM ingestion.BASEL_MORT_MTH_SNAPSHOT
  WHERE MTH_TM_ID = <M> GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 5;

-- k/l: mortgage-keyed (key = MORTGAGE_NO, PROCESS_DATE, STREAM)
SELECT MORTGAGE_NO FROM emulated.STATUS_FINAL
  WHERE CAST(PROCESS_DATE AS DATE) = '<rundate>' AND STREAM = '<s>'
  GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 5;
SELECT MORTGAGE_NO FROM emulated.TWELVE_MON_DEF_WINDOW
  WHERE PROCESS_DATE = '<rundate>' AND STREAM = '<s>'
  GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 5;

-- i/j: observation-point tables (key = BASEL_ACCT_ID, OBSVTN_MTH_TM_ID, STREAM)
-- Only relevant once OBSVTN_MTH_TM_ID = <M> rows exist (producer run at process M+12).
SELECT BASEL_ACCT_ID FROM emulated.REVLVNG_CR_OBSVTN_PT_DRVD_VAR
  WHERE OBSVTN_MTH_TM_ID = <M> AND STREAM = '<s>' GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 5;
SELECT BASEL_ACCT_ID FROM emulated.PSNL_LOAN_OBSVTN_PT_DRVD_VAR
  WHERE OBSVTN_MTH_TM_ID = <M> AND STREAM = '<s>' GROUP BY 1 HAVING COUNT(*) > 1 LIMIT 5;
```

## Prime suspects
1. **k / l (mortgage joins)** — join on `TRY_CAST(h.MORT_NUM AS BIGINT) = MORTGAGE_NO`.
   More than one row per `(MORTGAGE_NO, PROCESS_DATE, STREAM)` fans out every mortgage
   account.
2. **c/d/e/f/g/h** — duplicate `(BASEL_ACCT_ID, MTH_TM_ID)` if the source wasn't deduped.
3. **NOT i/j** for a current-month run: `OBSVTN_MTH_TM_ID = M` needs a producer run at
   process `M+12`, which usually hasn't happened, so those joins match nothing. Once the
   table is backfilled that far, both the PDEAD (`OBSVTN = P-12`) and LGD (`OBSVTN = P-24`)
   rows can land at `OBSVTN = M` -> fan-out; restrict with the PD/EAD guard:
   `AND i.PROCESS_MTH_TM_ID = i.OBSVTN_MTH_TM_ID + 12 * 40`.

## Fix patterns
Once the culprit is known, dedup that join's source to one row per key:

```sql
-- inside export_gather, wrap the offending table:
LEFT JOIN (
    SELECT * FROM emulated.STATUS_FINAL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY MORTGAGE_NO, PROCESS_DATE, STREAM
        ORDER BY <tiebreak>   -- e.g. INSRT_PROCESS_TMSTMP DESC
    ) = 1
) k ON ...
```

or fix upstream so the table has no duplicate `(key)` rows.
