# The `emulated` layer — what each table does

**Date:** 2026-08-24
**Scope:** `functions/emulated/filteredtable/*.py` → `emulated.*`
**DAG:** `<stream>_emulated`, gated behind `features` via
`[stream_dependencies] emulated = features`

---

## What this layer is

`emulated.*` reproduces the Netezza/SAS tables the CBS chain used to read. Each
module is one table, ported from a specific SAS job.

Almost all of them are **thin assemblies over `features.*`** rather than
reimplementations: the row-level derivations live in the feature layer (computed
once, shared), and the emulated module joins them into the shape the SAS table
had. The exceptions are noted per table below.

### Shared conventions

- **One TaskGroup per `.py`** — every file in the directory becomes a task group,
  no registry. Deleting the file is the only way to remove the step.
- **`duckdb_delete` → `duckdb_load`** — delete this run's partition, then insert.
  The table accumulates one partition per run; SAS replaced its tables wholesale.
  **Any comparison against production must filter a single `OBSN_DT`.**
- **Partitioned by `(OBSN_DT, STREAM)`** for all but three: `CIS_DATA_NEW2`
  (`FILE_YR_MTH`), `MORTGAGE_HIST` (`PROCESS_DATE`), `CBS_MDM_FLAGS`
  (`EFF_DT, STREAM`).
- **`OBSN_DT` = the run month-end**, i.e. `logical_date - 1 month`. A June run
  processes May.
- **Ordering is by asset string matching only** — a module runs after another
  because it lists that module's `DOWNSTREAM_ASSET` in its own `UPSTREAM_ASSET`.
  A typo produces no edge, no error, and silent parallel execution.

---

## The tables

### `CIS_DATA_NEW2`

**SAS:** `J_RRAP_MOR_SRC_0130_BNS_CIS_DATA_NEW_FINAL.sas`
**Grain:** `FILE_YR_MTH` (4-char `YYMM`), one row per customer-account relationship
**Upstreams:** none declared — input is **files**, not tables

The only module whose source is a fixed-width file drop. `cis_parse` reads
`CIS1..CIS8` from the landing directory, applies the SAS column layout
(`CIFKEY 1-17`, `PRODUCT 18-20`, `ACCOUNT 21-33`, `CID 34-49`,
`PRIMARY_FLAG 90`, `RELATION_CODE 91-93`, `CUST_TYPE 94-95`), the 30-rule
`PRIMARY_FLAG` remap, and the per-file `NODUPKEY` on primaries, then writes
Hive-partitioned Parquet which `duckdb_load` reads.

Paths come from Airflow Variables `cis_load_incoming` / `cis_load_out_root` /
`cis_load_workers`. Parse library lives at `dags/util/cis_load.py` — it cannot
live in `functions/` because every `.py` there becomes a task group.

Guarded against concurrent execution: the task group is generated once per
stream, so up to three DAGs may invoke the parse for the same month. It takes an
atomic lock directory and writes a `_SUCCESS` marker; later callers skip.

**Consumed by:** `custuniv_01`

---

### `BASEL_REVLVNG_CR_BASE_DRVD_VARS`

**SAS:** `J_RRII_KS10_2103_BASEL_REVLVNG_CR_BASE_DRVD_VARS.sas`
**Grain:** `(OBSN_DT, STREAM)` + `BASEL_ACCT_ID`, one month per run
**Upstreams:** `PIT_STATUS_CROSS_DEFAULT_ORIG`, `CONSM_PRD_TREATMNT_CD`,
`CONSM_SCORECRD_EXCLSN_F`, `ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT`

Revolving-credit (KS) account attributes for the run month. Feature-join
assembly over the revolving snapshot.

**Consumed by:** `custuniv_01`

---

### `BASEL_PSNL_LOAN_ACCT_DRVD_VARS`

**SAS:** `J_RRII_KS10_2105_BASEL_PSNL_LOAN_ACCT_DRVD_VARS.sas`
**Grain:** `(OBSN_DT, STREAM)` + `MTH_TM_ID`, `BASEL_ACCT_ID`, `BASEL_CUST_ID`
**Upstreams:** `PIT_STAT_VER_1_CD`, `OS_BAL_AMT`, `STEP_FLAG`,
`CONSM_PRD_TREATMNT_CD`, `TRNST_EXCLSN_F`, `ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT`,
`ingestion.BASEL_ACCT_DIM`

Personal-loan (SPL) account attributes, V1 status.

**Consumed by:** nothing in this repo — see *Orphans* below.

---

### `BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2`

**SAS:** `J_RRAP_TL10_2104_BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2.sas`
**Grain:** `(OBSN_DT, STREAM)` + `BASEL_ACCT_ID`, `MTH_TM_ID`
**Upstreams:** `PIT_STATUS_CROSS_DEFAULT_ORIG`, `OS_BAL_AMT_V2`, `PRD_ID`,
`MODEL_EXCL_F_V2`, `COMM_F_V2`, `TREATMENT_F`,
`ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT`

The V2 personal-loan attributes — `PIT_STATUS_V2`, `TREATMNT_F`, `OS_BAL_AMT_V2`.

**Population rule, from SAS `2104:232`:**

```sql
where m.mth_tm_id between <start> and <end>
  and m.RECD_STAT_CD in (4,5,6,7,8)
```

That `RECD_STAT_CD` filter is not an optimisation — it defines which accounts
exist in the table, and several `INNER JOIN`s in `J_RRAP_TL10_2201` depend on it
(including `FILTER_ACCOUNT` at `:5327`, behind `HD_FILTER_ACCOUNT`). Removing it
widens the SPL observation cohort and changes `custuniv_01`.

**Note:** the emulated version writes **one `MTH_TM_ID` per run**
(`a.MTH_TM_ID = <M>`) stamped with `OBSN_DT = rundate`, whereas SAS builds a
**date range**. Any consumer needing a historical window of this table needs that
many run partitions.

**Consumed by:** `custuniv_01`; also referenced in `model/scored/itl_lgdd.py` and
`itl_lgdnd.py`

---

### `BASEL_MORT_ACCT_DRVD_VARS`

**SAS:** `J_RRII_KS10_2107_BASEL_MORT_ACCT_DRVD_VARS.sas`
**Grain:** `(OBSN_DT, STREAM)` + `MTH_TM_ID`, `BASEL_ACCT_ID`
**Upstreams:** `COMM_TP_CD`, `DLQNT_DAY_CNT`, `OS_BAL_AMT`,
`CONSM_PRD_TREATMNT_CD`, `ingestion.MORT_MTH_SNAPSHOT`

Mortgage account attributes for the run month.

**Consumed by:** `custuniv_01`

---

### `MORTGAGE_HIST`

**Grain:** `MORTGAGE_NO`, `PROCESS_DATE`; partitioned by `PROCESS_DATE`
**Upstreams:** `ingestion.AIRB_MORT_MTH_SNAPSHOT`, `reference.GENWORTH_BULKINS`,
`reference.GEN_LKP`

Mortgage servicing history. **This is the analogue of `nzuser.MORTGAGE_HIST`,
which the SAS MOR chain reads as its root source** — it is mortgage-keyed
natively, unlike everything else in this layer.

**Consumed by:** nothing — see *Orphans*. Worth knowing that the MOR chain was
deliberately re-pointed at `ingestion.MORT_MTH_SNAPSHOT` instead, which is the
root cause of two open variances in `TWELVE_MON_DEF_WINDOW`.

---

### `STATUS_FINAL`

**SAS:** `RRAP_MOR_MODEL_01_DEFINE_STATUS_G.sas` + `RRAP_MOR_MODEL_01A_GATHER_LOAD_G.sas`
**Grain:** `(OBSN_DT, STREAM)` + `MORTGAGE_NO`, `PROCESS_DATE`
**Upstreams:** `PIT_STATUS_CROSS_DEFAULT_ORIG`, `CURRENT_BAL`, `MORT_NUM`,
`LRA_STATUS`, `PAID_OFF_DATE`, `PD_OFF_F`, `TOTAL_SUSPENSE`

Per-mortgage monthly status. `STATUS` is a direct passthrough of
`features.PIT_STATUS_CROSS_DEFAULT_ORIG`; `MODEL_EXCL` is derived from
status + balance + `PD_OFF_F`.

Two SAS behaviours reproduced deliberately:

- The `model_excl='N'` filter is **commented out** in SAS (`:1092`), so **nothing
  is excluded** despite the header comment claiming otherwise.
- `MORTGAGE_NO` comes from `features.MORT_NUM` deduped **per account**
  (`PARTITION BY BASEL_ACCT_ID`), so this table is account-grained carrying a
  mortgage label. Valid only while the account→mortgage mapping is 1:1.

**Consumed by:** `custuniv_01`

---

### `TWELVE_MON_DEF_WINDOW`

**SAS:** `RRAP_MOR_MODEL_02_BNS_MOR_PD_G.sas`
**Grain:** `(OBSN_DT, STREAM)` + `MORTGAGE_NO`, `PROCESS_DATE`
**Upstreams:** `DEFAULT_IND`, `DEFAULT_DATE`, `DEFAULT_BAL`, `MORT_NUM`,
`ingestion.TM_DIM`

Mortgage PD observation windows. For each observation point: *given this mortgage
was current at time T, did it default in the following 12 months?*

**39 windows per run** (obs-starts `P-38 … P`), each spanning **13 monthly slots**
(`obs_start … obs_start+12`). The scan looks **forward**.

The windowed CUR→DEF detection lives entirely in the three `DEFAULT_*` features
(batched 6 ways to bound memory); this module is a projection that converts
`BASEL_ACCT_ID → MORTGAGE_NO` via `features.MORT_NUM` and
`OBSVTN_MTH_TM_ID → PROCESS_DATE` via `TM_DIM`.

Full detail, reconciliation and open variances:
[`2026-08-20-twelve-mon-def-window-rewrite.md`](2026-08-20-twelve-mon-def-window-rewrite.md).

**Consumed by:** `custuniv_01` — and only the `PROCESS_DATE = rundate` window.

---

### `PSNL_LOAN_OBSVTN_PT_DRVD_VAR`

**SAS:** `J_RRAP_TL10_2201_PSNL_LOAN_OBSVTN_PT_DRVD_VAR.sas`
**Grain:** `(OBSN_DT, STREAM)` + `BASEL_ACCT_ID`, `OBSVTN_MTH_TM_ID`
**Upstreams:** `LAST_NEW_DFT_DT`, `LAST_NEW_DFT_BAL_AMT`, `MODEL_DFT_F`,
`PIT_STATUS_CROSS_DEFAULT_ORIG`, `TREATMENT_F`,
`ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT`, `ingestion.TM_DIM`

SPL observation points. **Two rows per account per run**, from two disjoint
windows:

| branch | window | `OBSVTN_MTH_TM_ID` | cohort at that point |
|---|---|---|---|
| PDEAD | `[R-12, R]` | `R-12` | `CUR` + `TREATMNT_F='A'`, and a `DEF`/`CHG` in the window |
| LGD | `[R-48, R-24]` | `R-24` | `DEF` + `TREATMNT_F='A'` |

The gap `(R-24, R-12)` is scanned by neither, deliberately.

`MODEL_DFT_F` is `Y`/`N` for PDEAD and **always NULL** for LGD — SAS initialises
it and never sets it on the LGD path (`:2635`).

The PDEAD cohort is restricted by `HD_FILTER_ACCOUNT` (`:5302-5339` + `:5484-5489`),
which requires a real balance: `OS_BAL_AMT_V2 >= 1 AND TOT_CRNT_BAL_AMT > 0`.

**Consumed by:** `custuniv_01`

---

### `REVLVNG_CR_OBSVTN_PT_DRVD_VAR`

**SAS:** `J_RRII_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR.sas`
**Grain:** `(OBSN_DT, STREAM)` + `BASEL_ACCT_ID`, `OBSVTN_MTH_TM_ID`
**Upstreams:** `LAST_NEW_DFT_DT`, `LAST_NEW_DFT_BAL_AMT`, `MODEL_DFT_F`

KS observation points. Same two-window structure as the SPL table, and the same
`OBSVTN_MTH_TM_ID` offsets. A thin projection — the windowed scan lives in the
features.

SAS calls `%rrap_defaulter_model` twice with different `WINDOW_START`/`WINDOW_END`
and appends both results; the rewrite `UNION ALL`s two CTEs.

**Consumed by:** `custuniv_01`

---

### `CBS_MDM_FLAGS`

**Grain:** `(EFF_DT, STREAM)` + `MTH_TM_ID`, `DATE_TYPE`, `PARTY_ID`

Not produced in this directory — comes from `source_ingestion` sq084 (Hive
extract of `crz_cust_scorecard.cbs_mdm_flags`).

**Consumed by:** `custuniv_02` (the MDM join) and `mdmflags_check` (the gate).

---

## Two ways the SPL/KS and MOR sides differ

Worth internalising, because it explains most of the reconciliation work:

| | SPL / KS observation tables | MOR (`TWELVE_MON_DEF_WINDOW`) |
|---|---|---|
| rows per account per run | 2 (two fixed observation points) | 39 (a rolling window panel) |
| window direction | LGD looks **back** from its label; PDEAD looks **forward** | always forward from the label |
| default detection | last-`CUR` / earliest-`DEF`-after (SPL), or `lag()` edge (KS) | 13-slot pivot, last `CUR→DEF` transition |
| native key | `BASEL_ACCT_ID` | `MORTGAGE_NO` in SAS, `BASEL_ACCT_ID` here |

---

## Orphans

Produced but consumed by nothing in this repo:

- **`emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS`** (V1)
- **`emulated.MORTGAGE_HIST`**

Neither is necessarily dead — both may be read by downstream DAGs or by hand —
but nothing declares them upstream, so the generator draws no edge and they run
in parallel with everything else. `MORTGAGE_HIST` is the more interesting of the
two: it is the faithful analogue of the SAS MOR chain's root source, and
populating it would remove the `BASEL_ACCT_ID → MORTGAGE_NO` translation that
sits behind the open `TWELVE_MON_DEF_WINDOW` variances.

An asset produced with no consumer is also how a gate silently fails — see the
`cbs.MDMFLAGS_OK` case, where the check ran in parallel with the chain for months
because no module listed it as an input.

---

## Backfill shape

Most of this layer needs **only the run month** of its upstreams. The exceptions
are the observation tables, whose features scan history:

| table | history needed |
|---|---|
| `TWELVE_MON_DEF_WINDOW` | 39 months (`P-38 … P`) of `PIT_STATUS_CROSS_DEFAULT_ORIG`, `CURRENT_BAL`, `MORT_NUM` (MOR) |
| `PSNL_LOAN_OBSVTN_PT_DRVD_VAR` | `P-48 … P` for the SPL features; `P-12 … P` for `BASEL_PSNL_LOAN_MTH_SNAPSHOT` |
| `REVLVNG_CR_OBSVTN_PT_DRVD_VAR` | `[R-48, R-24]` and `[R-12, R]` — the gap is genuinely skippable |
| everything else | run month only |

Full per-asset list with dates:
`emulated_upstream_backfill_CONSOLIDATED_<P>.csv` at the repo root.

**Caution:** features are stored one `OBSN_DT` per run. A feature that looks fine
at the run month can be entirely absent for a historical window, which surfaces
as NULL rather than as an error.

---

## Standing checks

```sql
-- 1. compare against production on ONE partition; the table accumulates
SELECT OBSN_DT, STREAM, COUNT(*) FROM emulated.<table> GROUP BY 1,2 ORDER BY 1;

-- 2. declared grain actually unique?
SELECT <key cols>, COUNT(*) FROM emulated.<table>
WHERE OBSN_DT = DATE '<rundate>' AND STREAM = '<stream>'
GROUP BY ALL HAVING COUNT(*) > 1;

-- 3. assets produced but consumed by nobody (a gate that isn't gating)
--    resolve the asset graph offline rather than reading it off the DAG UI
```
