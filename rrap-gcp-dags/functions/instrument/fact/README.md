# Instrument fact column loaders

Account-level PD/LGD/EAD and collateral columns for `instruments.BASEL_ANALYTCL_BL_INSTRUMNT_FACT`.

Each file in this directory is one Airflow DAG (`fact__<column>`) that loads a single
`instruments.*` table for the **process month** (`OBSN_DT = rundate`, `STREAM` from DAG context).
Loaders write **one month per run**; they do not stack historical observation windows into the
output table (unlike `emulated.TWELVE_MON_DEF_WINDOW`).

Gather job: `functions/instrument/fact/basel_analytcl_bl_instrmnt_fact.py` left-joins
83 column tables + spine `instruments.PIT_STAT_CD` onto one row per account for the run month
(`export_ks` / `export_mor` / `export_spl` / `export_tng`, same pattern as `amort`).

## Historical data for one month of INSTRUMNT_FACT

The fact table itself holds **one partition** per run. Historical depth is an **upstream
backfill** requirement so rolling features, model scores, and lookback columns are populated
correctly for that single output month.

Example: process month **2026-01-31** → one `OBSN_DT = 2026-01-31` partition in
`instruments.BASEL_ANALYTCL_BL_INSTRUMNT_FACT`.

| Backfill depth | What it enables |
|----------------|-----------------|
| **1 month** | Skeleton run: ingestion snapshots, `PIT_STAT_CD`, point-in-time features, column loaders, gather. PD/LGD/EAD ratio columns stay NULL without model scoring. |
| **6 months** | `*6M` rolling features used in PD scorecards (e.g. `BR147_MAX6M`, `GO04_MAX6M`, `NSF_NUM_MAX6M`, `D2D_BAL_AMT_P_MIN6M`). Window is current month plus prior 5 months (`INTERVAL 5 MONTH` … `rundate`). |
| **24 months** | `*24M` rolling features (`DLQNT_MTH_MAX24M`, …), `features.MONTH_DEF_24M` → `instruments.CONS_DFT_MTH_CNT` for KS/MOR/TNG-MOR. Window is current month plus prior 23 months (`INTERVAL 23 MONTH` … `rundate`); `MONTH_DEF_24M` uses a 24-month PIT status lookback. |
| **~36 months (12 quarters)** | Property/index lookbacks: `PREV_12_QTR_PRPTY_VAL_AMT`, `INDEXED_PRPTY_VAL_AMT`, `PRPTY_VAL_CORR_PCTG` (Teranet / provincial index via `MTH_TM_ID - 40*12*3` ≈ 12 quarters in `TM_DIM` encoding). |

**Recommended backfill for on-prem parity (MOR):** run ingestion + features from **`mth_tm_id - 24`**
through process month; extend to **36 months** when validating property/LTV columns.

**Not required for instrument fact:** ~39 months of `emulated.STATUS_FINAL` / `TWELVE_MON_DEF_WINDOW`.
That path supports the SAS PD **observation-window** chain (MODEL_02 → MODEL_03), not the gather job.
GCP `mor_pd` scoring reads current-month `MORT_MTH_SNAPSHOT` plus rolling **features**, not
`emulated.TWELVE_MON_DEF_WINDOW`.

## Dependency chain (one month out)

```
ingestion (MORT_MTH_SNAPSHOT, BASEL_ACCT_DIM, …)
    ↓  1 mo min; 6–24 mo for rolling features; ~36 mo for property indices
features (PIT_STATUS_*, MONTH_DEF_24M, *6M/*24M, …)
    ↓
models (mor_pd / mor_lgdd / mor_lgdnd / … score + segment)
    ↓  current month; inputs need history above
instruments.*  (this directory — 85 column loaders + gather)
    ↓  each loader: OBSN_DT = rundate
instruments.BASEL_ANALYTCL_BL_INSTRUMNT_FACT  (gather)
```

## Run order (example: MOR, 2026-01-31)

1. **Ingestion** — `MORT_MTH_SNAPSHOT`, `BASEL_ACCT_DIM`, Teranet/index tables for process month
   (plus historical months per table above).
2. **Features** — point-in-time and rolling derivations for process month (requires stacked history
   in feature tables for `*6M` / `*24M`).
3. **Model scoring** — `models.MOR_PD_SCORE`, `models.MOR_LGDD_SCORE`, `models.MOR_LGDND_SCORE`,
   segmentation tables for the process month.
4. **Instrument fact** — all `fact__*` DAGs for `(rundate, stream)`.
5. **Gather** — `fact__basel_analytcl_bl_instrmnt_fact`.

## Loaders with non-trivial upstream history

| Loader | Upstream history driver |
|--------|-------------------------|
| `cons_dft_mth_cnt` | `features.MONTH_DEF_24M` (24 mo PIT status) for KS/MOR/TNG-MOR; `features.MONTH_DEF` for SPL |
| `prev_12_qtr_prpty_val_amt` | ~12 quarters of Teranet / provincial house index + step plan snapshots |
| `indexed_prpty_val_amt`, `crnt_prpty_val_amt`, `crnt_ltv_rto` | Current month + index history as above |
| `pd_acct_score`, `pd_*_rptg_rto`, `lgd_*`, `ead_*` | `models.*_SCORE` / `models.*_SEGMENT` for process month (models need 6–24 mo feature history) |
| `pit_stat_cd` | Current-month PIT status features only |

Most other loaders join **current-month** ingestion or model outputs and inherit history only
through those upstream layers.

## Layout

| Path | Role |
|------|------|
| `functions/instrument/fact/*.py` | Column loader DAG definitions (+ gather in `basel_analytcl_bl_instrmnt_fact.py`) |
| `conf/{non_resl,resl,ifrs9}/instruments/fact/basel_analytcl_bl_instrmnt_fact.export_{ks,mor,spl,tng}.sql` | Gather export SQL (`config_type="instrument"`) |
| `rrap-ducklake-catalog/schemas/instruments/*.sql` | Column + gather table DDL (incl. `BASEL_ANALYTCL_BL_INSTRUMNT_FACT.sql`) |

`instruments.FINAL_RTO` lives under `functions/instrument/` (segment grain) and is **excluded**
from the gather join.

## SAS reference

| On-prem | GCP |
|---------|-----|
| J_RRAP MODEL_60 column derivations | `functions/instrument/fact/*` |
| J_RRAP_2700 / J_PLL consolidate | `BASEL_ANALYTCL_BL_INSTRUMNT_FACT` gather |
