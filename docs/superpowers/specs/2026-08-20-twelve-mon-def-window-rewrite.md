# `emulated.TWELVE_MON_DEF_WINDOW` — SAS rewrite, design and reconciliation

**Date:** 2026-08-20
**SAS source:** `sas-develop/.../rrap_iias/RRAP_MOR_MODEL_02_BNS_MOR_PD_G.sas`
(status derivation in `RRAP_MOR_MODEL_01_DEFINE_STATUS_G.sas`)
**Branch:** `fix/emulated-sas-parity`

---

## 1. What the table is

For each observation point, the table answers one question per mortgage:

> given this mortgage was **current** at time `T`, did it **default** within the
> following 12 months?

It looks **forward**, not back. One row per `(MORTGAGE_NO, PROCESS_DATE)`, where
`PROCESS_DATE` is the observation-window start month-end.

```
obs_start (T)                                   T + 12 months
    |                                                 |
    +--+--+--+--+--+--+--+--+--+--+--+--+
    1  2  3  4  5  6  7  8  9 10 11 12 13   <- 13 monthly slots
    |
  _status1 must be 'CUR', or all three outputs are NULL
```

| column | meaning |
|---|---|
| `DEFAULT_IND` | `1` if a CUR→DEF transition occurred in the window, `0` if not, `NULL` if the window did not start `CUR` |
| `DEFAULT_DATE` | month of that transition |
| `DEFAULT_BAL` | balance at that transition |

Each run emits **39** windows per mortgage, obs-starts `P-38 … P`.

### Two numbers that are easy to conflate

- **39** — observation windows per run (`P-38 … P`)
- **13** — monthly slots inside one window (`obs_start … obs_start+12`)

### Why row counts vary per mortgage

A mortgage gets a row for obs-start `M` only if it has at least one observation
inside `[M, M+12]`:

```
rows = min(P, last_obs) - max(P-38, first_obs-12) + 1
```

39 means present throughout. Fewer means born late or ran off. Note the `-12`:
a mortgage first seen at `P-17` still appears in windows starting from `P-29`,
because that window's span *ends* on `P-17`.

### Structurally immature windows

The newest 12 windows cannot show a default. The `obs_start = P` window has one
slot, `P-1` has two, and so on — no CUR→DEF transition is representable. This is
identical in SAS and is not a defect. `custuniv_01` reads only the
`PROCESS_DATE = rundate` window, which is therefore always `0` or `NULL`.

---

## 2. The SAS

| lines | step |
|---|---|
| `:962-966` | `start_period_dt = end_period_dt - 38 months` |
| `:999-1006` | `subset_data` = `mortgage_no, status, current_bal, process_date` from `results.status_final_&pn`, `ORDER BY mortgage_no, process_date` |
| `:1011-1020` | `num_months = intck('month', start, end) + 1` = **39** |
| `:1033-1160` | `create_pd_obs_window` — 13-slot arrays per `(mortgage, obs_start)` |
| `:1170-1218` | `last_new_default` — the CUR→DEF scan |
| `:1517-1537` | union the 8 partitions, `%delete_table`, bulk reload |

The scan:

```sas
if _status1 = "CUR" then do;
   default_ind = 0;  default_bal = 0;  default_date = .;
   %do ii = 1 %to 12;
      %let jj = %eval(&ii.+1);
      if _status&ii = "CUR" and _status&jj = "DEF" then do;
         default_ind  = 1;
         default_date = _process_date&jj.;
         default_bal  = _current_bal&jj.;
      end;
   %end;
end;
```

No `leave`/`else` — the loop runs all 12 iterations, so **the last matching
transition wins**.

### `mortgage_no` in SAS is never derived

```
nzuser.MORTGAGE_HIST                     <- mortgage-keyed natively
  |  partitioned by mod(input(mortgage_no,11.), &parts)
  v
intmed.mortgage_hist_&pn                 (RRAP_MOR_ACCT_02_UNLOAD:1000)
  v  derives new_status, adds model_excl
results.status_final_&pn                 (RRAP_MOR_MODEL_01:997)
  v
  by mortgage_no -> the window scan       (RRAP_MOR_MODEL_02:999)
```

`BASEL_ACCT_ID` appears nowhere. **This is the single most important difference.**

### `new_status` has two branches

```sas
case
   when not missing(b.mort_num) then b.cross_dflt_pit_status   /* PIT override */
   when <residential, not paid off, delq<90, not foreclosed,
         bal<>0, lra<>'Y'> or current_bal < 0        then 'CUR'
   when <residential, (delq>=90 or foreclosed or lra='Y'), bal>0>
     or <residential, foreclosed, paid off, max(bal,-suspense)>0>
                                                     then 'DEF'
end as new_status
```

with the override read from a **precomputed, mortgage-keyed** table:

```sas
left join NZRRAP.PIT_STATUS_PRE_STEP as b
       on b.src_sys_cd = 'MOR'
      and b.cross_dflt_pit_override_f = 'Y'
      and a.mortgage_no = b.mort_num
      and a.process_date = b.mort_process_date;
```

### Two SAS behaviours worth recording

- **`model_excl='N'` filtering is disabled** (`RRAP_MOR_MODEL_01:1092`, commented
  out) despite the header comment claiming otherwise. Nothing is excluded.
- **`mth_end_dt&mm` is retained and not reset per mortgage**
  (`:1078-1095`). A mortgage with no row at its window's start month inherits the
  *previous* mortgage's `process_date`. The rewrite derives it correctly from
  `TM_DIM`, so the rewrite is **more** correct here — do not "fix" this to match.

---

## 3. The rewrite

```
ingestion.MORT_MTH_SNAPSHOT               <- account-keyed (NOT MORTGAGE_HIST)
  v
features.PIT_STATUS_ACCOUNT_ORIG          <- delinquency branch of new_status
  v
features.PIT_STATUS_CROSS_DEFAULT_ORIG    <- override branch (derived, not read)
  +  features.CURRENT_BAL
  v
features.DEFAULT_IND / DEFAULT_DATE / DEFAULT_BAL
     39 obs windows x 13 slots, keyed by BASEL_ACCT_ID,
     batched MOD(HASH(BASEL_ACCT_ID), 6) to bound memory
  v
emulated.TWELVE_MON_DEF_WINDOW            <- thin projection
     + features.MORT_NUM  -> MORTGAGE_NO
     + ingestion.TM_DIM   -> PROCESS_DATE
```

Written one partition per `(OBSN_DT, STREAM)` via delete + insert. SAS drops and
reloads the whole table, so the rewrite **retains history SAS discards** — any
comparison against production must filter to a single `OBSN_DT`.

### Design rationale

| choice | why |
|---|---|
| features rather than `STATUS_FINAL` | `STATUS_FINAL` is itself built from these features and gets its mortgage number the same way; routing through it adds a hop and 4 unused features (`LRA_STATUS`, `PAID_OFF_DATE`, `PD_OFF_F`, `TOTAL_SUSPENSE`) without changing a value. Backfill drops from 7 monthly assets + 39 assembly runs to 3. |
| keyed by `BASEL_ACCT_ID` | the platform's universal grain; `MORT_MTH_SNAPSHOT` is what is ingested |
| batched 6 ways | the 39×13 fan-out OOMs on the full MOR population otherwise |
| partitioned by `OBSN_DT` | keeps monthly history |

### The seam

`TWELVE_MON_DEF_WINDOW.py:56` is the one thing SAS has no equivalent for:

```sql
INNER JOIN ( SELECT BASEL_ACCT_ID, TRY_CAST(MORT_NUM AS BIGINT) AS MORTGAGE_NO
             FROM features.MORT_NUM
             WHERE SRC_SYS_CD = 'MOR' AND TRY_CAST(MORT_NUM AS BIGINT) IS NOT NULL
             QUALIFY ROW_NUMBER() OVER (
                 PARTITION BY BASEL_ACCT_ID ORDER BY OBSN_DT DESC) = 1
) mn ON mn.BASEL_ACCT_ID = ind.BASEL_ACCT_ID
```

Every defect found in this table originated here. It is valid **only because the
account→mortgage mapping is 1:1**, verified: `0` mortgages with more than one
`BASEL_ACCT_ID`.

---

## 4. Changes made

| commit | change |
|---|---|
| `5585368` | `DEFAULT_BAL` takes the **last** transition's balance |
| `18d6892` | removed dead `COALESCE` blocks from all three features |
| `ce100d7` | rekeyed features to `MORTGAGE_NO` — **reverted** |
| `074351f` | reverted the rekey; resolve `MORT_NUM` per window month; dedup the `TM_DIM` join |
| `14a940b` | stripped comments |
| `e2dc045` | skip un-castable `MORT_NUM` (`NOT NULL` violation) |
| `cd0f217` | resolve `MORT_NUM` **per account**, not per window month |

### `DEFAULT_BAL` (`5585368`)

The rewrite used `COALESCE` over the twelve balance expressions, highest slot
first. That is equivalent to SAS *only when the last matching transition has a
non-NULL balance*. `CURRENT_BAL` reaches the scan via a `LEFT JOIN`, so it can be
NULL — and `COALESCE` then skips that slot and returns an **earlier** default's
balance. Now the last matching slot is resolved first (`GREATEST` over slot
numbers), then that slot's balance is taken; `ELSE 0` preserves the SAS
no-transition case.

Verified against all six SAS branches. Note this case **cannot arise in SAS** —
both `DEF` branches require a positive balance, and `features.CURRENT_BAL` is the
same column from the same table, so it only fires where that feature has a
coverage gap.

### The `MORT_NUM` join — three iterations

1. **Original:** `WHERE OBSN_DT = <rundate>`. A mortgage that left the book before
   the run month has no row in that snapshot, so the `INNER JOIN` discarded
   **every window it had** — 579,370 mortgages, 11,040,024 rows.
2. **`074351f`:** joined per window month. Fixed the population, but a blank
   `MORT_NUM` in a given month yields NULL against `MORTGAGE_NO BIGINT NOT NULL`.
3. **`e2dc045` + `cd0f217`:** filter to castable values, and resolve **once per
   account** from the most recent castable month. Tying the label to the window's
   own month discarded whole windows over a single blank month — that over-corrected
   by 5,641,182 rows.

### The rekey that was reverted (`ce100d7` → `074351f`)

Keying the scan by `MORTGAGE_NO` is structurally faithful — SAS scans
`by mortgage_no`. It was reverted after the `0 multi-account mortgages` check
showed the two groupings are **identical** on this data, making the simpler
account-keyed version equivalent and avoiding an invented `DEF`-wins tie-break
that SAS never specifies.

---

## 5. Reconciliation (`P = 2026-07-31`)

```
generated   57,039,726     (current code, at cd0f217)
production  53,508,344
excess      +3,531,382
```

`DEFAULT_IND` breakdown — **measured on an earlier run** (at `ce100d7`, total
57,018,105), so it is 21,621 rows short of the current total. Directions and
magnitudes hold; re-measure before quoting exact figures:

| `DEFAULT_IND` | generated | production | diff |
|---|---:|---:|---:|
| `0` | 43,124,001 | 39,651,989 | +3,472,012 |
| `1` | 145,009 | 147,512 | −2,503 |
| `NULL` | 13,749,095 | 13,708,843 | +40,252 |
| **total** | **57,018,105** | **53,508,344** | **+3,509,761** |

Set comparison on `(MORTGAGE_NO, PROCESS_DATE)`:

```
in_prod_only       = 0            <- production IS a key-subset
in_gen_only        = 3,531,382    <- the entire excess is NEW keys
same_key_diff_ind  = 2,222
```

Distinct `MORTGAGE_NO`: **1,863,041 both sides** — population matches exactly.

**Default rate: 0.3351% generated vs 0.3706% production — understated 9.6%.**
This, not the row count, is the number that matters for a PD table.

Both the set comparison and the distinct-`MORTGAGE_NO` figures are from the
current code; only the `DEFAULT_IND` breakdown predates it.

---

## 6. Known variances

### +3,472,012 extra `DEFAULT_IND = 0` windows — blocked

Run-off mortgages stay visible ~6 months longer in `MORT_MTH_SNAPSHOT` than in
`MORTGAGE_HIST`. Confirmed as `CUR` months, not post-payoff NULLs. Not fixable
without `MORTGAGE_HIST`, which is not populated.

### ~2,222 `DEFAULT_IND` disagreements — open

Prime suspect is the cross-default override. SAS reads a precomputed,
**mortgage-keyed** `PIT_STATUS_PRE_STEP` and takes its status verbatim; the
rewrite **derives** the override **per account** from STEP-plan membership and
only ever flips `CUR → DEF`. `TRIM` has been ruled out as a cause.

Next step — dump the 13-month status sequence for windows where production says
default and the rewrite does not, and classify:

| observation | diagnosis |
|---|---|
| no row for that month | history gap in the feature |
| `CUR`, `override = 'N'` | override not applied |
| `CUR`, `override = 'Y'` | flip did not stick |
| `DEF` present | slot alignment / scan issue |

### The root fix

Populate `emulated.MORTGAGE_HIST` and scan that. It removes the account→mortgage
translation entirely and inherits SAS's exact notion of when a mortgage's history
ends — addressing both variances at source rather than compensating downstream.

---

## 7. Backfill (`P = 2026-07-31` → `2023-05-31 … 2026-07-31`, 39 months)

| asset | months |
|---|---|
| `features.PIT_STATUS_CROSS_DEFAULT_ORIG` (MOR) | 39 |
| `features.CURRENT_BAL` (MOR) | 39 |
| `features.MORT_NUM` (MOR) | 39 — read across all months, no `OBSN_DT` filter |
| `features.PIT_STATUS_ACCOUNT_ORIG` | 39 |
| `ingestion.MORT_MTH_SNAPSHOT` (+ `AIRB_`, `BASEL_MORT_`) | 39 |
| `ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT` (+ `_PRE_SL`, `RLP_TO_SL_ACCT_LIST`) | 39 |
| `ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT` | 39 |
| `ingestion.BASEL_ACCT_DIM`, `ingestion.TNG_ACCT_MO` | 39 (for `MORT_NUM`) |
| `reference.CHRG_OFF_LKP`, `BLOCK_RECL_LKP`, `SRC_PRD_LKP` | load once |
| `ingestion.TM_DIM` | calendar covering the span |

Only the three `reference.*` lookups are true sources; the repo builds the rest.

The SQL also requests `forward_end = P+12`. That data cannot exist, so the scan
stops at `P` and the newest 12 windows stay immature — expected, same as SAS.

---

## 8. Standing checks

```sql
-- 1. the 1:1 assumption this design rests on. MUST stay 0.
SELECT COUNT(*) FROM (
  SELECT MORTGAGE_NO, PROCESS_DATE FROM emulated.TWELVE_MON_DEF_WINDOW
  WHERE OBSN_DT = DATE '<rundate>' AND STREAM = '<stream>'
  GROUP BY 1,2 HAVING COUNT(*) > 1);

-- 2. accounts silently dropped by the MORT_NUM join
SELECT COUNT(*) FROM (
  SELECT BASEL_ACCT_ID FROM features.MORT_NUM WHERE SRC_SYS_CD = 'MOR'
  GROUP BY 1
  HAVING COUNT(*) FILTER (WHERE TRY_CAST(MORT_NUM AS BIGINT) IS NOT NULL) = 0);

-- 3. window coverage: expect exactly 39
SELECT COUNT(DISTINCT PROCESS_DATE) FROM emulated.TWELVE_MON_DEF_WINDOW
WHERE OBSN_DT = DATE '<rundate>' AND STREAM = '<stream>';

-- 4. status padding (would break every '= CUR' comparison)
SELECT DISTINCT PIT_STATUS_CROSS_DEFAULT_ORIG, length(PIT_STATUS_CROSS_DEFAULT_ORIG)
FROM features.PIT_STATUS_CROSS_DEFAULT_ORIG WHERE SRC_SYS_CD = 'MOR';

-- 5. remaining unverified: DEFAULT_DATE / DEFAULT_BAL row-level parity
SELECT COUNT(*) FROM (
  SELECT MORTGAGE_NO, PROCESS_DATE, DEFAULT_IND, DEFAULT_DATE, DEFAULT_BAL
  FROM read_parquet('<prod>')
  EXCEPT
  SELECT MORTGAGE_NO, PROCESS_DATE, DEFAULT_IND, DEFAULT_DATE, DEFAULT_BAL
  FROM emulated.TWELVE_MON_DEF_WINDOW
  WHERE OBSN_DT = DATE '<rundate>' AND STREAM = '<stream>');
-- 2,222 => the DEFAULT_IND diffs are the only row-level differences
-- more    => DATE and/or BAL diverge too (re-check with ROUND(DEFAULT_BAL,2))
```

Comparisons must filter a single `OBSN_DT`: the table accumulates one partition
per run, and consecutive runs overlap on 38 of their 39 obs-start months.
