# Emulated tables — SAS parity verification

**Date:** 2026-07-12
**Method:** static line-by-line comparison of each `emulated/*.py` DuckDB SQL
against its original SAS job in `sas-develop/.../sas/rrap_iias/`. No data
execution — findings marked **CONFIRMED** (logic definitely differs) or
**PLAUSIBLE** (differs only for specific data that may or may not occur).

Severity: **High** = wrong values for common rows · **Med** = wrong values for
edge-case rows or a dropped guard · **Low** = representational (blank vs NULL)
or cosmetic.

SAS source map:

| Emulated table | SAS job |
|---|---|
| BASEL_MORT_ACCT_DRVD_VARS | `J_RRII_KS10_2107_BASEL_MORT_ACCT_DRVD_VARS.sas` (258 ln) |
| BASEL_PSNL_LOAN_ACCT_DRVD_VARS | `J_RRII_KS10_2105_BASEL_PSNL_LOAN_ACCT_DRVD_VARS.sas` (131 ln) |
| BASEL_REVLVNG_CR_BASE_DRVD_VARS | `J_RRII_KS10_2103_BASEL_REVLVNG_CR_BASE_DRVD_VARS.sas` (550 ln) |
| BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 | (SAS source TBD) |
| PSNL_LOAN_OBSVTN_PT_DRVD_VAR | `J_RRAP_TL10_2201_PSNL_LOAN_OBSVTN_PT_DRVD_VAR.sas` (6396 ln) |
| REVLVNG_CR_OBSVTN_PT_DRVD_VAR | `J_RRII_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR.sas` (156 ln) |
| STATUS_FINAL | `RRAP_MOR_MODEL_01_DEFINE_STATUS_G.sas` (1343) + `01A_GATHER_LOAD_G.sas` (1331) |
| TWELVE_MON_DEF_WINDOW | `RRAP_MOR_MODEL_02_BNS_MOR_PD_G.sas` (1545 ln) |

Progress: ✅ 1–6 verified (line/core level) · ◑ 7 partial · ⊘ 8 blocked (no SAS source).
See the **Summary of findings** table at the bottom.

---

## 1. BASEL_MORT_ACCT_DRVD_VARS — mostly faithful, 1 real divergence

Source join, `OS_BAL_AMT`, `COMM_TP_CD`, `LAND_RGSTRN_ACT_STAT_F`,
`DLQNT_MTH_CNT`, `PIT_STAT_VER_1_CD`, `STEP_F`, `CONSM_PRD_TREATMNT_CD`,
`TRNST_EXCLSN_F` all verified **equivalent** to the SAS. The SQL even correctly
guards the `SCRTY_TP_2` last-3-char substring (`GREATEST(len-2, 1)`) that SAS
`SUBSTR` would take on a short string. Good port overall.

### Finding 1.1 — `DLQNT_DAY_CNT` branch fall-through *(CONFIRMED logic diff, PLAUSIBLE impact, Med)*

SAS (`J_RRII_KS10_2107`, lines 92–115) selects the delinquency-date branch on
`FLOAT_CD` **alone**:

```sas
IF PD_OFF_DT NE . OR STRIP(PD_OFF_F)='Y' THEN DLQNT_DAY_CNT_TEMP=0;
ELSE IF STRIP(FLOAT_CD) IN ('W','B','S')
        THEN DLQNT_DAY_CNT_TEMP = LAST_BUSINESS_DAY1 - WK_FRST_UNPAID_DT;   /* if WK date missing → TEMP = . */
     ELSE DLQNT_DAY_CNT_TEMP = LAST_BUSINESS_DAY1 - FRST_UNPAID_DT;
/* TEMP EQ . → DLQNT_DAY_CNT = . (NULL) */
```

The SQL (`BASEL_MORT_ACCT_DRVD_VARS.py`, `derived` CTE) adds
`AND s.WK_FRST_UNPAID_DT IS NOT NULL` to the W/B/S branch, so when `FLOAT_CD ∈
('W','B','S')` **and** `WK_FRST_UNPAID_DT IS NULL`, it **falls through** to the
`FRST_UNPAID_DT` branch and returns a number — whereas SAS returns NULL.

- **Failure row:** a mortgage with `FLOAT_CD='W'`, `PD_OFF_DT` null,
  `WK_FRST_UNPAID_DT` null, `FRST_UNPAID_DT` non-null → SAS `DLQNT_DAY_CNT = NULL`,
  SQL `DLQNT_DAY_CNT = DATE_DIFF(day, FRST_UNPAID_DT, LAST_BUSINESS_DAY)`.
- Note `DLQNT_MTH_CNT` does **not** inherit this: it re-branches on the same null
  and both sides land on 0, so only `DLQNT_DAY_CNT` diverges.
- **Fix if confirmed against data:** drop the `AND ... IS NOT NULL` guard, or make
  the W/B/S branch return NULL when `WK_FRST_UNPAID_DT IS NULL` instead of falling
  through.

### Finding 1.2 — unset status columns are NULL vs SAS blank *(CONFIRMED, Low)*

When neither CUR nor DEF matches, SAS leaves `PIT_STAT_VER_1_CD` as `''`
(character blank); the SQL yields `NULL`. Same for any char column SAS never
assigns. Harmless unless a downstream compares `= ''` rather than `IS NULL`.

---

## 2. BASEL_PSNL_LOAN_ACCT_DRVD_VARS — faithful, 1 dropped guard

`OS_BAL_AMT` (`ROUND(TOT_CRNT_BAL+ADD_ON+ACCR,3)`), `STEP_F`,
`PIT_STAT_VER_1_CD` (`RECD_STAT_CD`+`DAY_ODUE` buckets), `TRNST_EXCLSN_F`,
`CONSM_PRD_TREATMNT_CD`, `BASEL_CUST_ID`, and the source joins all verified
**equivalent** to `J_RRII_KS10_2105`. No value divergences found.

### Finding 2.1 — missing `ACCT_NUM` abort guard *(CONFIRMED, Med — behavioral)*

SAS (lines 81–85) **aborts the job** if any row has a blank `ACCT_NUM` (i.e. a
`BASEL_ACCT_ID` missing from `BASEL_ACCT_DIM`):

```sas
if strip(ACCT_NUM) eq '' then do;
    MSG='Missing BASEL_ACCT_ID='||BASEL_ACCT_ID||' in BASEL_ACCT_DIM Table';
    put MSG; abort cancel;
end;
```

The SQL has no equivalent; it silently emits the row with a NULL `ACCT_NUM`.
Values for well-formed rows match, but the pipeline's **data-quality
fail-fast** is lost — a missing dimension key would surface as a bad row
downstream instead of a job failure. Consider a post-load assertion task
(`SELECT count(*) ... WHERE ACCT_NUM IS NULL` → fail if > 0) to preserve intent.

### Finding 2.2 — unset `PIT_STAT_VER_1_CD` NULL vs blank *(CONFIRMED, Low)*

Same blank-vs-NULL note as 1.2 (rows with `RECD_STAT_CD` outside `4–8`).

---

## 3. STATUS_FINAL — delq logic faithful; STATUS delegated cross-source

Verified against `RRAP_MOR_MODEL_01_DEFINE_STATUS_G.sas` (lines 1016–1116).

**Faithful:** `temp_delq_days_2`, `delq_days_2`, `temp_delq_months_2`,
`delq_months_2` (incl. the `days>=90 AND months=3 → 4` rule) and `MODEL_EXCL`
(`paid_off_ind='Y' OR status<>'CUR' OR current_bal<=0 → 'Y'`) all match the SAS.

### Finding 3.1 — `STATUS` delegated to a feature built off a *different* source table *(PLAUSIBLE, High)*

SAS computes `new_status` inline from `MORTGAGE_HIST` columns (comm_type, delq,
foreclose_ind, lra_status, current_bal, total_suspense) with a step cross-default
override. The rewrite instead sets `STATUS = features.PIT_STATUS_CROSS_DEFAULT_ORIG`
(MOR) joined via `features.MORT_NUM`.

Good news: the underlying `pit_status_account_orig` (MOR branch) **does**
reproduce the SAS status CASE — including the `OR CRNT_BAL_AMT < 0 → 'CUR'`
precedence quirk and the foreclosed/paid-off `greatest(bal, -suspense) > 0 → 'DEF'`
clause. So the *formula* is preserved.

The risk is **source & coverage**:
- The feature reads `ingestion.MORT_MTH_SNAPSHOT`; STATUS_FINAL processes
  `ingestion.MORTGAGE_HIST`. If the two disagree for a mortgage-month (they are
  different tables), `STATUS` reflects the snapshot while `MODEL_EXCL`,
  `CURRENT_BAL`, etc. reflect the history — an internal inconsistency SAS can't
  produce (it derives all of them from one row).
- **Coverage:** rows in `MORTGAGE_HIST` with no matching `MORT_NUM`/PIT feature
  row get `STATUS = NULL → MODEL_EXCL='Y'` (excluded). SAS would compute a real
  inline status for those same rows. *(needs runtime check: count of
  MORTGAGE_HIST rows unmatched by features.MORT_NUM for the month.)*
- Also confirm `pit_status_account_orig`'s MOR CUR branch includes the
  `delq_days<90 AND delq_months<4` test that SAS requires (not visible in the
  excerpt read).

### Finding 3.2 — `temp_delq_months_2` `GREATEST(...,0)` floor *(CONFIRMED diff, no impact, Low)*

SQL wraps `intck('month',...)+1` in `GREATEST(...,0)`; SAS does not. Benign: the
only path to a negative month count is already gated to 0 by the
`delq_days_2 = 0` branch, so outputs are identical. Cosmetic.

---

## 4. TWELVE_MON_DEF_WINDOW — default detection faithful; 1 likely bug + 1 edge

Verified against `RRAP_MOR_MODEL_02_BNS_MOR_PD_G.sas` macros
`create_pd_obs_window` (l.1033–1160) and `last_new_default` (l.1170–1220).

**Faithful:** the 13-month window bounds (`obs_start … LAST_DAY(obs_start+12mo)`),
the start_period = `month_end(end_period − 38mo)`, slot ranking by process_date,
and the whole `last_new_default` logic — `default_ind/date/bal`, the "last
CUR→DEF transition wins" via `COALESCE(ii=12…1)` mirroring the SAS ascending loop
where the highest match overwrites, and the `_status1='CUR'` gating. All correct.

### Finding 4.1 — `PROCESS_DATE = obs_start` contradicts the spec & SAS *(CONFIRMED, High)*

The `.py` emits `obs_start AS PROCESS_DATE` (the window's **start** month-end).
But the table's own DDL header states:

> "PROCESS_DATE matches the SAS: the process_date of the **last** observation in
> the window (highest filled slot)."

And in SAS, the output `process_date` is the retained value from the triggering
row — the **last** observation accumulated (slot 13 or the final record), not the
window start. So `obs_start` is almost certainly wrong; it should be the max /
highest-slot `process_date` in the window.

- **Failure row:** any multi-observation window → SAS/spec `PROCESS_DATE` = last
  obs date (≈ near `window_end`), SQL `PROCESS_DATE` = `obs_start` (12 months
  earlier). Only coincides for single-observation windows.
- **Fix:** emit the last observation's process_date, e.g.
  `MAX(process_date)` in `obs_window` (or `_process_date` at the highest filled
  slot), not `obs_start`. *(Confirm intended value against a SAS sample.)*

### Finding 4.2 — short windows without the mortgage's final record *(PLAUSIBLE, Med)*

SAS outputs a window only when `counter=13` **or** the current row is
`last.mortgage_no`. A window with `<13` observations whose latest qualifying obs
is **not** the mortgage's global last record is therefore **dropped**. The SQL
outputs every `(mortgage_no, obs_start)` group with `≥1` row. Divergence appears
only for mortgages with a **gap** in their monthly history (data, gap, data):
SAS drops the pre-gap short windows, SQL keeps them. *(needs runtime check:
mortgages with non-contiguous PROCESS_DATE history.)*

### Finding 4.3 — `slot > 13` handling *(CONFIRMED, Low)*

SQL caps at `WHERE slot <= 13`; SAS array size 13 would hit an out-of-bounds
error if a window ever had >13 obs (e.g., duplicate process dates in a month).
SQL silently drops the extras. Benign unless duplicates exist; arguably the SQL
is safer, but note the behavior differs.

---

## 5. REVLVNG_CR_OBSVTN_PT_DRVD_VAR — faithful port ✅

Verified against `J_RRII_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR.sas` + the
`%rrap_defaulter_model` macro. This is the **cleanest port of the set** — the
hard parts all match:

- **TM_ID month arithmetic** `± N*40` (40 units = 1 month): PD/EAD window
  `mth-12*40 … mth`, LGD window `mth-48*40 … mth-24*40`, observation months at
  `mth-12*40` / `mth-24*40`, recovery cutoff `LAST_NEW_DEFAULT_DATE + 24*40` and
  `TM_LVL_END_DT + INTERVAL 24 MONTH` — all match SAS exactly.
- **`_NEW_DEFAULT_FLG`** reproduces every KS/CC sub-condition of the macro
  (lag PIT `CUR → DEF/CHG` gated on `TOT_UNPAID_FNCL_CHRG_AMT` vs `OS_BAL_AMT`,
  HELOC split, the `<=0` / `>0` charge-off branches).
- `LAST_NEW_DEFAULT_DATE = MAX(MTH_TM_ID)` over new-defaults; the `-40` prior-month
  lag join for the charge-off balance adjustment; output filters
  (`PIT='CUR'`/`<>'CUR'`, `SML_BUS_F='N'`, `CONSM_PRD_TREATMNT_CD='A'`); `MODEL_DFT_F`.

Minor notes (not divergences): confirm the balance adjustment uses `GREATEST`
(two-arg SAS `max()`), not the `MAX()` aggregate; and the `SML_BUS_F` de-dup
(`ROW_NUMBER … ORDER BY SML_BUS_F DESC`) assumes 'Y' wins on multi-row features —
reasonable, but absent from SAS (which reads the single base-table value).

---

## 6. BASEL_REVLVNG_CR_BASE_DRVD_VARS (feature-generated) — 1 column-source divergence

Verified the assembly against `J_RRII_KS10_2103` and spot-checked the features it
joins.

- **`REVISED_EXPSR_AMT`** feature = `CASE WHEN CR_LMT_AMT > TOT_NEW_BAL_AMT THEN
  CR_LMT_AMT ELSE TOT_NEW_BAL_AMT END` — **exact match** to SAS (l.86–89). ✅
- **`PIT_STAT_VER_2_CD`** is delegated to `features.PIT_STATUS_CROSS_DEFAULT_ORIG`
  (KS). The underlying `pit_status_account_orig` (KS branch) **does** reproduce the
  SAS temporal logic — `BLOCK_RECL_LKP`, `CHRG_OFF_LKP`, `PREV_SNAP` (prior month),
  `v_PT_STAT_BLCK_RECL_CD_LKP*` — structurally matching l.154–210. *Confirm two
  details:* the current threshold is `BNS_DLQNT_DAY < 210` (not the deprecated
  `<120` `OLD_PIT_STAT_VER_2_CD`), and `PREV_SNAP` keys on `MTH_TM_ID − 40`.

### Finding 6.1 — `HELOC_F` derived from a different source column *(PLAUSIBLE, Med)*

SAS (l.77–81): `HELOC_F = 'Y'` when `SUB_PRD_CD='RS' OR STEP_PLN_SNAPSHOT_ID NOT
IN (-1,-2)`.
Feature `heloc_f.py`: `'Y'` when `SUB_PRD_CD='RS' OR COALESCE(TRIM(
STEP_PLN_AGRMNT_NUM),'') <> ''`.

The second predicate uses **`STEP_PLN_AGRMNT_NUM <> ''`** instead of
**`STEP_PLN_SNAPSHOT_ID NOT IN (-1,-2)`**. These usually agree (an account with a
real step plan has both) but are not guaranteed identical — an account with a
valid snapshot id but a blank agreement number (or vice-versa) flips `HELOC_F`,
which then cascades into `PIT_STAT_VER_2_CD` (HELOC branch) and downstream
default detection. *(needs runtime check: `count(*)` where the two predicates
disagree.)*

### Note 6.2 — assembly fan-out risk *(watch)*

The feature-generated spine `LEFT JOIN`s each feature on `BASEL_ACCT_ID + OBSN_DT`.
Only the PIT join is filtered to `SRC_SYS_CD='KS'`. If any joined feature holds
>1 row per account-month (e.g. multiple `SRC_SYS_CD`), the assembly fans out and
inflates row counts vs the SAS single-pass. Worth a `COUNT(*)` equality check
feature-generated vs the monolithic twin.

---

## 7. PSNL_LOAN_OBSVTN_PT_DRVD_VAR — partial (SAS is 6,396 lines of generated code)

**Coverage caveat:** the SAS (`J_RRAP_TL10_2201_…`) is INFA-generated boilerplate
(hundreds of `attrib/length/label` blocks); isolating the true transformation is
a dedicated effort beyond this pass. Verified **structurally**:

- The py builds the per-account 48-month base via feature joins
  (`PIT_STATUS_CROSS_DEFAULT_ORIG`, `TREATMENT_F`, `SUB_PORT_F`) then derives
  `LAST_NEW_DFT_*`, `RCVRY_WINDOW_CUTOFF_*`, `MODEL_DFT_F` through window logic —
  the same shape as the verified `REVLVNG_CR_OBSVTN` (which shares the
  `%rrap_defaulter_model` lineage). Recovery-cutoff and last-new-default patterns
  look consistent with the SAS output columns (l.1066–1137).

**Verified structurally (2026-07-12 pass):** the `.py` implements the LGDD (DEF,
`-48mo … -24mo`) and LGDND (CUR, `-12mo`) windows, last-new-default
(`max_non_def` latest-CUR → first-DEF-after), recovery cutoffs and `MODEL_DFT_F`
— consistent with the SAS structure (l.872–874 `CONDITION=PIT_STAT_VER_2_CD='DEF'`)
and the verified `REVLVNG_CR_OBSVTN` window convention. Source note: SAS reads
`BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2`; the `.py` reconstructs from snapshot + features
— but it keys off `PIT_STAT_VER_2_CD` (faithful) and does **not** use
`OS_BAL_AMT_V2`, so finding 8.1 does not propagate here.

**Derivation-level verification (2026-07-12, traced the INFA chain `FINAL` ←
`WLGH4H` ← `PreFinal`/`PSNL_MTH_SNAPSHOT`):**

| Derivation | SAS | `.py` | Match |
|---|---|---|---|
| Recovery cutoff | `INTNX('MONTH', LAST_NEW_DEF_DATE, 24, 'e')` | `LAST_DAY(LAST_NEW_DFT_DT + INTERVAL 24 MONTH)` | ✅ exact |
| `RCVRY..._TM_ID` | `-1` → TM_DIM join | `-1` → TM_DIM join | ✅ |
| `RCVRY..._MTH_TM_ID` | `TM_DIM` month lookup on cutoff | same | ✅ |
| Descriptive cols | pass-through from per-account base | pass-through from `spl_acct_feats` | ✅ |
| Last-new-default | CUR→DEF new-default path + non-CUR fallback (`…EXCEPT NEW_DEF_DATE_1`) | `def_after_cur` (first DEF after last CUR) + `never_cur → def_never_cur` | ✅ structurally equivalent |

**Verdict: verified faithful against SAS at the derivation level.** Recovery
window is an exact match; last-new-default = most-recent `CUR→DEF` onset with a
never-CUR fallback, mirroring the SAS. **Residual (not byte-traced through INFA):**
(a) confirm `LAST_NEW_DFT_BAL_AMT` and `MODEL_DFT_F` derive the same as the SAS
per-account base; (b) pathological same-month-transition tie handling in the SAS
`NEW_DEF_DATE_1` step. No divergence found.

---

## 8. BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 — mostly faithful; 1 inverted condition

Source confirmed: **`J_RRAP_TL10_2104_BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2.sas`**
(undated base = current 2024 `RRMSS-2841` version; dated backups are stale).

**Faithful:** `PIT_STATUS` (= `CROSS_DFLT_PIT_STATUS` lookup — matches the RRMSS-2841
migration, so my feature delegation is correct here); `CONS_DFT_MTH_CNT`
(recursion at SAS l.656–662); the 2011 commercial-flag fix; `OS_BAL_AMT`
(`principal+addon+accr`); `SUB_PORTFL` (S01–S08 DIRECT / S09–S15 INDIRECT); all
six exclusion flags (`IND_BAL_EXCL` <100/<1, `IND_HL_EXCL` S11, `IND_OTHIND_EXCL`
S14, `IND_COM_EXCL` flagv2∈{1,2}, `IND_CAB_EXCL` 18192/99432, `IND_24MOS_EXCL`
>24); `MODEL_EXCL_F`; `TREATMNT_F` (`Z` when com/cab excl). Solid port.

### Finding 8.1 — `INT_AT_DEFAULT` / `OS_BAL_AMT_V2` uses an inverted transition *(CONFIRMED condition inversion, PLAUSIBLE impact, High)*

CR27 intent (SAS l.724–731): capture accrued interest at the **beginning** of the
current default spell. SAS (`spl_default_beg_int`, l.834–859) sorts **descending**
and captures at the **`CUR→DEF` onset** boundary:

```sas
* descending process_date; int_def = lag(accr_int)=NEWER month; pit=lag(pit_status)=NEWER
if (pit_status='CUR' and pit='DEF')            -> int_at_default = int_def   /* accr at the DEF month where default STARTED */
   or (last.loan/cab and pit_status='DEF')     -> int_at_default = accr_int  /* already DEF at earliest history */
```

My `.py` (`ranked_hist`/`int_at_default`) uses **ascending** LAG and the **reverse**
boundary — `DEF→CUR` (recovery):

```sql
WHEN LAG_PIT='DEF' AND PIT_STATUS='CUR' THEN LAG_ACCR_INTR   -- captures the LAST DEF month before recovery
WHEN TM_LVL_END_DT = EARLIEST_DT AND PIT_STATUS='DEF' THEN ACCR_INTR   -- earliest-DEF fallback (matches SAS)
```

The SAS captures the **onset** (first DEF month); the `.py` captures the **recovery**
(last DEF month before returning to CUR), and for a *currently-ongoing* default
(no recovery in history) captures nothing → `OS_BAL_AMT_V2` falls back to current
`ACCR_INT` instead of the onset interest.

- **Failure row:** account with spell M1=CUR,M2=DEF,M3=DEF (obs at M3). SAS
  `int_at_default` = M2's accr (onset) → `OS_BAL_AMT_V2 = principal+addon+M2_accr`.
  `.py` finds no `DEF→CUR`, `int_at_default`=NULL → `OS_BAL_AMT_V2` uses M3's accr.
- The earliest-DEF fallback matches; only the transition branch is inverted.
- **Impact is data-dependent** (defaulted accounts with history) and `OS_BAL_AMT_V2`
  is a key LGD input, so worth a targeted data trace. *(needs runtime check.)*

### Finding 8.2 — prev-month join key narrower than SAS *(PLAUSIBLE, Low)*

SAS matches the prior month on `loan_no + cab + basel_acct_id` (l.626–629); the
`.py` `prev_month` join uses `BASEL_ACCT_ID` only. Diverges for accounts whose
`loan_no`/`cab` changed month-over-month — SAS treats it as a new spell (resets
`CONS_DFT_MTH_CNT`), the `.py` continues the count. `BASEL_ACCT_ID`-only is
arguably the more stable key, but it differs from SAS.

---

## Summary of findings

| # | Table | Verdict | Top issue |
|---|---|---|---|
| 1 | BASEL_MORT_ACCT_DRVD_VARS | 🔴 confirmed bug | `DLQNT_DAY_CNT` W/B/S null fall-through (1.1) |
| 2 | BASEL_PSNL_LOAN_ACCT_DRVD_VARS | ⚠️ guard dropped | `ACCT_NUM` abort removed (2.1) |
| 3 | STATUS_FINAL | ✅ good (reclassified) | delegation faithful to current SAS; 3 optional checks (3.1) |
| 4 | TWELVE_MON_DEF_WINDOW | 🔴 confirmed bug | `PROCESS_DATE = obs_start` vs SAS (4.1) |
| 5 | REVLVNG_CR_OBSVTN_PT_DRVD_VAR | ✅ faithful | — |
| 6 | BASEL_REVLVNG_CR_BASE_DRVD_VARS | ⚠️ 1 diff | `HELOC_F` uses `STEP_PLN_AGRMNT_NUM` (6.1) |
| 7 | PSNL_LOAN_OBSVTN_PT_DRVD_VAR | ✅ verified faithful | derivation-level trace; recovery cutoff exact, last-new-default sound (2 minor residuals) |
| 8 | BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 | 🔴 confirmed bug | `OS_BAL_AMT_V2` inverted default transition (8.1) |

**Resolution (2026-07-12):**

- **Fixed** (applied to working tree, branch `fix/emulated-sas-parity`):
  - 4.1 `TWELVE_MON PROCESS_DATE` → emit last-observation date (`MAX(process_date)`).
    NB: HEAD's committed code was already correct (`COALESCE(_process_date…)`);
    the `obs_start` form was an uncommitted working-tree regression. Fix restores
    correct behavior in the working tree.
  - 1.1 `MORT DLQNT_DAY_CNT` → `W/B/S` branch selects on `FLOAT_CD` alone,
    returns NULL when the weekly date is missing (no fall-through). HEAD had the bug.
  - 8.1 `PSNL_2 OS_BAL_AMT_V2` → `INT_AT_DEFAULT` now detects the `CUR→DEF` onset
    (captures the DEF-month accr) and picks the most-recent spell.
- **Accepted as-is (no change, by owner decision):**
  - 6.1 `HELOC_F` — `features.HELOC_F` is canonical; its `STEP_PLN_AGRMNT_NUM`
    logic stands as source of truth.
  - 2.1 `ACCT_NUM` guard — silent NULL accepted; no fail-fast reinstated (row
    values already match SAS).
- **Good as-is:** 3 (STATUS_FINAL — delegation matches current SAS `RRMSS-2842`),
  5 (REVLVNG_CR_OBSVTN).
- **Still open — confirm by data:** 7 (PSNL_OBSVTN, INFA-generated SAS), and the
  data-dependent *impact* of the 1.1 / 8.1 fixes (they are code-correct vs SAS but
  unexecuted here).
- **Not fixed (intentional):** MORTGAGE_HIST prepay/province carry-forward — latent,
  no in-scope consumer reads those columns.

**Baseline note:** verification and fixes target the **working tree** (the current
code that runs), per owner direction — not committed HEAD, which differs for
several files (`REVLVNG_CR_BASE` substantially).
