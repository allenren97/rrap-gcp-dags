# Emulated tables: feature-generated vs monolithic — parity & cost analysis

**Date:** 2026-07-12
**Scope:** all 8 `functions/emulated/filteredtable/*.py` tables
**Goal:** for each emulated table, determine whether it should be built by
*assembling `features.*` single-column assets* (feature-generated) or kept as a
*single self-contained SQL block* (monolithic) — judged on **SAS/output parity**
and **performance/cost** (the two axes the owner cares about). Pre-list the
features that would need to be created if we featurize.

> Verification is **static logic analysis** (SAS docstrings + DuckDB SQL read
> side-by-side). No source data is available locally, so no execution parity was
> run. Where a claim depends on runtime data it is marked *(needs runtime check)*.

---

## 1. The two paradigms

**Monolithic** — one `duckdb_load` (or `export_result`) with a CTE chain that
computes every derived column inline from the ingestion snapshot + dims +
lookups, in dependency order. One scan of the snapshot. Reference examples:
`BASEL_MORT_ACCT_DRVD_VARS`, `BASEL_PSNL_LOAN_ACCT_DRVD_VARS`.

**Feature-generated** — read the ingestion snapshot as the spine, then
`LEFT JOIN` N pre-computed `features.<COL>` assets on `BASEL_ACCT_ID + OBSN_DT`.
Each feature is materialized independently by its own DAG task. Reference
example: `BASEL_REVLVNG_CR_BASE_DRVD_VARS_FEATURE_GENERATED` (16 feature joins;
its schema is byte-identical to the monolithic `BASEL_REVLVNG_CR_BASE_DRVD_VARS`,
i.e. it is the parity twin).

---

## 2. Classification framework

For each **output column** we ask two questions.

**(A) Parity — row-context dependence.** Can the column's value be computed from
*only its own account-row's inputs*, or does it need the *filtered / joined /
windowed rowset* of the table?

| Class | Meaning | Featurize? |
|---|---|---|
| **Independent** | value = f(this row's snapshot columns + lookups) | safe to split |
| **Intra-row chain** | depends on *other derived columns of the same row* | safe, but the feature must recompute the chain internally (recomputation) |
| **Cross-row / temporal** | window func, lag, running counter, multi-month history, self-join to prior output | **do not split** — splitting changes semantics |

**(B) Cost.** Featurizing turns one snapshot scan into: 1 spine scan + **N feature
materializations** (each its own scan/filter of the snapshot) + **N left joins**
at assembly. For per-row scalar columns that is pure overhead unless the feature
is *reused* by another table.

Recommendation rule used below:

- **Cross-row / temporal columns present** → monolithic (parity-driven).
- **All independent, and columns are reused across tables** → feature-generated.
- **All independent but single-use** → monolithic is cheaper; featurize only for
  consistency with the established pattern.
- **Mixed** → hybrid: featurize the reused independent inputs, keep the entangled
  / pass-through columns inline.

---

## 3. Cross-cutting findings (read before the per-table sections)

1. **"Feature exists" ≠ "drop-in reusable".** Same-named feature files exist for
   most derived columns, but they are wired to specific source snapshots and
   `SRC_*` filters. Parity requires the feature's **source table + grain + filter**
   to match the emulated table's, per column.

2. **Mortgage source mismatch (the main landmine).** The emulated
   `BASEL_MORT_ACCT_DRVD_VARS` reads **`ingestion.BASEL_MORT_MTH_SNAPSHOT`** (74
   cols). The existing mortgage-family features (`COMM_TP_CD`,
   `LAND_RGSTRN_ACT_STAT_F`, `DLQNT_DAY_CNT`, the MOR branch of `OS_BAL_AMT`,
   `CONSM_PRD_TREATMNT_CD`, …) read **`ingestion.MORT_MTH_SNAPSHOT`** (170 cols) —
   a *different, wider* snapshot. So featurizing the emulated MORT table with the
   current features would **not** be parity-safe without either (a) reconciling
   the two snapshots or (b) building BASEL-snapshot variants of those features.

3. **Personal-loan source matches.** The multi-product features
   (`OS_BAL_AMT`, `CONSM_PRD_TREATMNT_CD`, `TRNST_EXCLSN_F`, `STEP_F`) contain a
   PSNL branch that reads the **same `BASEL_PSNL_LOAN_MTH_SNAPSHOT`** as the
   emulated PSNL table — so PSNL is a materially better reuse candidate than MORT
   *(needs per-column logic diff to confirm the branch matches)*.

4. **`PIT_STAT_VER_1_CD` has no feature** and is defined differently per product
   (mortgage: `COMM_TP_CD`/balance/foreclosure/delinquency logic; personal loan:
   `RECD_STAT_CD` + `DAY_ODUE` bucketing). It would need product-specific feature
   variants if featurized.

---

## 4. Master matrix

| # | Emulated table | SAS source (per docstring/trace) | Nature | Cross-row logic? | Current style | Parity risk if featurized | Recommendation |
|---|---|---|---|---|---|---|---|
| 1 | `BASEL_MORT_ACCT_DRVD_VARS` | RRAP mortgage acct derived vars | per-row scalar | No | monolithic (0 feat) | **High** (MORT vs BASEL_MORT snapshot) | **Monolithic** (or featurize only after building BASEL-snapshot feature variants) |
| 2 | `BASEL_PSNL_LOAN_ACCT_DRVD_VARS` | RRAP PSNL acct derived vars | per-row scalar | No | monolithic (0 feat) | Low–Med | **Hybrid / optional feature-generated** (source matches) |
| 3 | `BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2` | RRAP PSNL derived vars (v2) | temporal | **Yes** (prev-month self-join, recursive `CONS_DFT_MTH_CNT`, LAG/ROW_NUMBER over full history) | partial (3 feat) | **High** | **Monolithic** |
| 4 | `BASEL_REVLVNG_CR_BASE_DRVD_VARS` | RRAP revolving-credit base derived vars | per-row scalar | No | **feature-generated** (16 feat) — reference | n/a (already the parity twin) | **Feature-generated** (keep; it is the pattern exemplar) |
| 5 | `PSNL_LOAN_OBSVTN_PT_DRVD_VAR` | `J_RRAP_TL10_2201_PSNL_LOAN_OBSVTN_PT_DRVD_VAR.sas` | observation-window / temporal | **Yes** (LGDD/LGDND paths, ROW_NUMBER, 48-mo window) | hybrid (7 feat inputs) | **High** for window cols | **Hybrid (keep)** — features for inputs, window derivation inline |
| 6 | `REVLVNG_CR_OBSVTN_PT_DRVD_VAR` | `J_RRII_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR.sas` | recovery-window / temporal | **Yes** (ROW_NUMBER×13, LAG×6, ~38-mo history) | hybrid (8 feat inputs) | **High** for window cols | **Hybrid (keep)** |
| 7 | `STATUS_FINAL` | `RRAP_MOR_MODEL_01_DEFINE_STATUS_G.sas` + `01A_GATHER_LOAD_G.sas` | pass-through + interdependent delq | No cross-row, but ~40 pass-through cols + `delq→months→status→excl` chain | hybrid (2 feat: `MORT_NUM`, `PIT_STATUS_CROSS_DEFAULT_ORIG`) | Med | **Hybrid (keep)** |
| 8 | `TWELVE_MON_DEF_WINDOW` | `RRAP_MOR_MODEL_02_BNS_MOR_PD_G.sas` (create_pd_obs_window + last_new_default) | **window-level** (not account-level) | **Yes** (13-slot pivot, ROW_NUMBER, 39 obs windows, CUR→DEF scan) | monolithic (0 feat) | **N/A — output grain is (mortgage, window), not (account, month)** | **Monolithic** |

**Bottom line:** of the 8, only **#4 is genuinely feature-generated**, and it
should stay so. **#2** is the one clear net-new featurization candidate (source
matches). **#1** is featurizable *in principle* but blocked by the snapshot
mismatch. **#3, #5, #6, #7, #8** should **stay monolithic/hybrid** — their
defining columns are cross-row/temporal or pass-through, where splitting into
per-account single-column features would break parity or is grain-incompatible.

---

## 5. Per-table detail

### 1. `BASEL_MORT_ACCT_DRVD_VARS` — monolithic → **keep monolithic**

Source: `ingestion.BASEL_MORT_MTH_SNAPSHOT` + `BASEL_ACCT_DIM` (`SRC_APP_CD='MO'`)
+ `TM_DIM` + `reference.TRNST_EXCLSN_LKP`. All columns are per-row scalars with
intra-row chains:

- `OS_BAL_AMT = CRNT_BAL_AMT + INTR_ACCR_AMT`
- `COMM_TP_CD` ← `SCRTY_TP_2` → feeds `CONSM_PRD_TREATMNT_CD` and `PIT_STAT_VER_1_CD`
- `DLQNT_DAY_CNT` (uses scalar `last_business_day`) → feeds `DLQNT_MTH_CNT`
- `LAND_RGSTRN_ACT_STAT_F` ← `FUND_CD` → feeds `PIT_STAT_VER_1_CD`
- `PIT_STAT_VER_1_CD` = f(COMM_TP_CD, balance, foreclosure, DLQNT_MTH_CNT, LRA)

Feature availability (name match): `COMM_TP_CD`, `CONSM_PRD_TREATMNT_CD`,
`DLQNT_DAY_CNT`, `DLQNT_MTH_CNT`, `LAND_RGSTRN_ACT_STAT_F`, `OS_BAL_AMT`,
`STEP_F`, `TRNST_EXCLSN_F`, `ACCT_NUM` **exist**; `PIT_STAT_VER_1_CD` **missing**.

**But** every existing mortgage feature reads `MORT_MTH_SNAPSHOT`, not
`BASEL_MORT_MTH_SNAPSHOT` (§3.2). Recommendation: **keep monolithic**. Revisit
only if a decision is made to (a) reconcile the two mortgage snapshots, or (b)
create BASEL-snapshot feature variants — at which point all these columns are
independent and safe to split. There is no correctness benefit to splitting a
single-scan per-row computation into 10 scans + 10 joins otherwise.

*Missing feature if featurized:* `PIT_STAT_VER_1_CD` (mortgage variant) — **plus**
BASEL-snapshot variants of the 9 above.

### 2. `BASEL_PSNL_LOAN_ACCT_DRVD_VARS` — monolithic → **feature-generated candidate (optional)**

Source: `ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT` + `BASEL_ACCT_DIM` +
`TRNST_EXCLSN_LKP`. Columns all per-row scalar:
`OS_BAL_AMT` (= TOT_CRNT_BAL+ADD_ON+ACCR), `CONSM_PRD_TREATMNT_CD` (excl/balance),
`PIT_STAT_VER_1_CD` (`RECD_STAT_CD`+`DAY_ODUE` buckets), `STEP_F`
(`STEP_PLN_SNAPSHOT_ID>0`), `TRNST_EXCLSN_F`.

Unlike MORT, the multi-product features' **PSNL branch reads the same
`BASEL_PSNL_LOAN_MTH_SNAPSHOT`** (§3.3), so `OS_BAL_AMT`,
`CONSM_PRD_TREATMNT_CD`, `TRNST_EXCLSN_F`, `STEP_F` are plausibly reusable
*(needs per-column logic diff)*. `PIT_STAT_VER_1_CD` (PSNL variant) is missing.

Recommendation: **the strongest net-new featurization candidate** — *but reuse
was checked and does not hold*. The table is consumed by 5 downstream jobs
(`cbs/custuniv/custuniv_01`, `BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2`,
`PSNL_LOAN_OBSVTN_PT_DRVD_VAR`, `model/scored/itl_lgdd`, `itl_lgdnd`) — but every
one joins it **as a whole table**, and none consume its derived columns as
`features.*` (`PIT_STAT_VER_1_CD` is used as a feature nowhere). So under
**minimum-cost parity there is no reuse or correctness win**: featurizing adds N
feature scans + N joins for columns consumed as a table anyway. **Keep
monolithic.**

*Missing feature if featurized:* `PIT_STAT_VER_1_CD` (PSNL variant). Others:
confirm PSNL branch parity, else PSNL variants.

### 3. `BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2` — partial → **keep monolithic**

Deeply temporal. Contains: a **prev-month self-join** (`prev_month` reads the
table's own prior `MTH_TM_ID`), a **recursive running counter**
`CONS_DFT_MTH_CNT` (prev value + 1 while in default), and a full-history
`LAG`/`ROW_NUMBER` block computing `INT_AT_DEFAULT` (interest at the default
event) that feeds `OS_BAL_AMT_V2`. These cannot be expressed as independent
per-account single-column features without either duplicating the whole history
scan per column or losing the cross-month dependency. **Monolithic is correct.**
The 3 feature refs it already uses (`PIT_STATUS_CROSS_DEFAULT_ORIG`) are inputs,
which is fine. *No new features recommended.*

### 4. `BASEL_REVLVNG_CR_BASE_DRVD_VARS` — feature-generated → **keep (reference pattern)**

The `_FEATURE_GENERATED` variant joins 16 features onto the revolving-credit
snapshot spine; all columns are per-row independent, and the same features are
consumed by other revolving-credit tables (#6) — i.e. genuinely reused. This is
the case where feature-generated wins on both maintainability and reuse, and its
schema equals the monolithic twin for parity checking. **Keep.** This is the
template the other *independent-column* tables should imitate — but only when
their features share the same source snapshot.

### 5. `PSNL_LOAN_OBSVTN_PT_DRVD_VAR` — hybrid → **keep hybrid**

SAS: `J_RRAP_TL10_2201_...`. Builds LGDD (DEF) / LGDND (CUR) observation-point
rows over a 48-month window. Uses `features.PIT_STATUS_CROSS_DEFAULT_ORIG`,
`TREATMENT_F`, `SUB_PORT_F` as **per-row inputs** (correctly featurized), then
derives `LAST_NEW_DFT_*`, `RCVRY_WINDOW_CUTOFF_*`, `MODEL_DFT_F` via
ROW_NUMBER/window logic that is **cross-row/temporal** and must stay inline.
**Already in the right shape.** *No new features recommended.*

### 6. `REVLVNG_CR_OBSVTN_PT_DRVD_VAR` — hybrid → **keep hybrid**

SAS: `J_RRII_KS10_2510_...`. Recovery-window derivation over ~38 month-ends;
19 window clauses, ROW_NUMBER×13, LAG×6. Consumes 8 features as per-row inputs;
window derivation inline. **Already correct.** *No new features recommended.*

### 7. `STATUS_FINAL` — hybrid → **keep hybrid**

SAS: `RRAP_MOR_MODEL_01_DEFINE_STATUS_G.sas` + `01A_GATHER_LOAD_G.sas`. Two
genuinely-derived columns are already sourced from features (`STATUS` via
`MORT_NUM`+`PIT_STATUS_CROSS_DEFAULT_ORIG`). The rest is (a) ~40 **pass-through**
columns straight from `ingestion.MORTGAGE_HIST` (not features — no value in
wrapping each in a feature asset) and (b) an interdependent
`DELQ_DAYS_2 → DELQ_MONTHS_2 → MODEL_EXCL` chain off history dates. Featurizing
the pass-throughs is pure overhead; the delq chain is cheap inline.
**Keep hybrid.** Optional: `DELQ_DAYS_2`/`DELQ_MONTHS_2` could become MOR-variant
features *only if* reused by another mortgage table (not currently the case).

### 8. `TWELVE_MON_DEF_WINDOW` — monolithic → **keep monolithic**

SAS: `RRAP_MOR_MODEL_02_BNS_MOR_PD_G.sas`. Output grain is
**(MORTGAGE_NO, WINDOW_END_DT)** — a window-level table, *not* account×month — so
it is categorically not a `features.*` (account-month) producer. It pivots 13
monthly status slots per obs-window, ranks by process date, and scans CUR→DEF
transitions across the window. **Monolithic is the only correct form.**

---

## 6. Recommendation summary

| Recommendation | Tables |
|---|---|
| **Keep monolithic** | #1 (blocked by snapshot mismatch), #3, #8 |
| **Keep as-is (already feature-generated / hybrid)** | #4, #5, #6, #7 |
| ~~Net-new featurization candidate~~ → **keep monolithic** (no reuse win, see §5.2) | #2 |

**Final call (objective = minimum-cost parity):** **no table should be
converted.** #4 stays feature-generated; #5–#7 stay hybrid; #1, #2, #3, #8 stay
monolithic. Featurizing #2 — the only structural candidate — yields no reuse or
correctness benefit because its columns are consumed table-wise, not as features.
The feature-generated pattern is only warranted for #4 (proven reuse across
revolving-credit tables).

**"Is monolithic better?"** — Yes for #1, #3, #8, and it is already the right
call for the hybrids #5–#7. Feature-generated is better *only* for #4 (proven)
and *potentially* #2 — the cases where columns are per-row **independent** AND the
features share the emulated table's source snapshot AND are reused elsewhere. The
prevailing failure mode to avoid is featurizing per-row single-use columns, which
adds N scans + N joins for zero parity benefit, or featurizing cross-row/temporal
columns, which breaks parity.

---

## 7. Implementation backlog — features to create (pre-list)

Only the featurization paths that survive the analysis:

**If #2 `BASEL_PSNL_LOAN_ACCT_DRVD_VARS` is featurized (pending Q1):**
- [ ] `features.PIT_STAT_VER_1_CD` — PSNL variant (`RECD_STAT_CD` + `DAY_ODUE`)
- [ ] Verify PSNL branch parity of existing `OS_BAL_AMT`, `CONSM_PRD_TREATMNT_CD`,
      `TRNST_EXCLSN_F`, `STEP_F`; create PSNL variants only if the branch diverges.
- [ ] `emulated.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_FEATURE_GENERATED` twin + parity
      query (`EXCEPT` both directions vs the monolithic twin).

**If #1 `BASEL_MORT_ACCT_DRVD_VARS` is featurized (blocked — needs §3.2 decision first):**
- [ ] Reconcile `BASEL_MORT_MTH_SNAPSHOT` vs `MORT_MTH_SNAPSHOT`, OR create
      BASEL-snapshot variants of: `COMM_TP_CD`, `LAND_RGSTRN_ACT_STAT_F`,
      `DLQNT_DAY_CNT`, `DLQNT_MTH_CNT`, `OS_BAL_AMT`, `CONSM_PRD_TREATMNT_CD`,
      `STEP_F`, `TRNST_EXCLSN_F`, `ACCT_NUM`.
- [ ] `features.PIT_STAT_VER_1_CD` — mortgage variant.

**No new features:** #3, #4, #5, #6, #7, #8.

---

## 8. Open questions for the owner

- **Q1.** ~~For `BASEL_PSNL_LOAN_ACCT_DRVD_VARS` (#2), are its derived columns
  reused?~~ **Answered:** consumed as a whole table by 5 jobs, never as
  `features.*` → no reuse win → keep monolithic.
- **Q2.** For mortgage (#1), is reconciling `BASEL_MORT_MTH_SNAPSHOT` with
  `MORT_MTH_SNAPSHOT` in scope, or should the emulated MORT table stay monolithic
  off the BASEL snapshot?
- **Q3.** Is the objective *pattern uniformity* (make everything feature-generated
  where semantically safe) or *minimum-cost parity* (only featurize where there is
  a reuse or correctness win)? §2's rule currently optimizes the latter.
