# SAS job logic vs `features.py` logic — side-by-side discrepancies

**Date:** 2026-07-14
**Purpose:** for each emulated table that is (or will be) assembled from
`features.*`, show the SAS job logic and the current `features/derived/*.py`
logic **side by side, only for columns where they differ**, so parity can be
verified before featurizing. Columns verified as matching are listed but not
expanded.

**Legend:** 🔴 value differs for common rows · 🟠 differs on edge/null rows ·
⚪ missing feature (must build) · ✅ verified matching (not expanded).

SAS source map:

| Table | SAS job |
|---|---|
| BASEL_MORT_ACCT_DRVD_VARS | `J_RRII_KS10_2107_BASEL_MORT_ACCT_DRVD_VARS.sas` |
| BASEL_PSNL_LOAN_ACCT_DRVD_VARS | `J_RRII_KS10_2105_BASEL_PSNL_LOAN_ACCT_DRVD_VARS.sas` |
| BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 | `J_RRAP_TL10_2104_..._DRVD_VARS_2.sas` |
| BASEL_REVLVNG_CR_BASE_DRVD_VARS | `J_RRII_KS10_2103_BASEL_REVLVNG_CR_BASE_DRVD_VARS.sas` |
| PSNL_LOAN_OBSVTN_PT_DRVD_VAR | `J_RRAP_TL10_2201_PSNL_LOAN_OBSVTN_PT_DRVD_VAR.sas` |
| REVLVNG_CR_OBSVTN_PT_DRVD_VAR | `J_RRII_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR.sas` |
| STATUS_FINAL | `RRAP_MOR_MODEL_01_DEFINE_STATUS_G.sas` |
| TWELVE_MON_DEF_WINDOW | `RRAP_MOR_MODEL_02_BNS_MOR_PD_G.sas` |

---

## Table 1 — `BASEL_MORT_ACCT_DRVD_VARS` (MOR branch)

Matching (not expanded): ✅ `ACCT_NUM`, `COMM_TP_CD`, `LAND_RGSTRN_ACT_STAT_F`,
`DLQNT_MTH_CNT`, `CONSM_PRD_TREATMNT_CD`.

### 1.1 `DLQNT_DAY_CNT` 🔴 — W/B/S null fall-through
**SAS** `J_RRII_KS10_2107:92-115`
```sas
IF PD_OFF_DT NE . OR STRIP(PD_OFF_F)='Y' THEN DLQNT_DAY_CNT_TEMP=0;
ELSE DO;
   IF STRIP(FLOAT_CD) IN ('W','B','S')   /*and WK_FRST_UNPAID_DT ne .*/
      THEN DLQNT_DAY_CNT_TEMP = LAST_BUSINESS_DAY1 - WK_FRST_UNPAID_DT;
      ELSE DLQNT_DAY_CNT_TEMP = LAST_BUSINESS_DAY1 - FRST_UNPAID_DT;
END;
IF DLQNT_DAY_CNT_TEMP EQ . THEN DLQNT_DAY_CNT=.;      /* WK date missing -> NULL */
ELSE IF DLQNT_DAY_CNT_TEMP<0 THEN DLQNT_DAY_CNT=0; ELSE DLQNT_DAY_CNT=DLQNT_DAY_CNT_TEMP;
```
**feature** `derived/dlqnt_day_cnt.py:73-86` (export_dlqnt_day_cnt_mor)
```sql
CASE
  WHEN PD_OFF_DT IS NOT NULL OR TRIM(PD_OFF_F)='Y' THEN 0
  WHEN TRIM(FLOAT_CD) IN ('W','B','S') AND WK_FRST_UNPAID_DT IS NOT NULL THEN
       CASE WHEN LAST_BUSINESS_DAY-WK_FRST_UNPAID_DT<0 THEN 0 ELSE LAST_BUSINESS_DAY-WK_FRST_UNPAID_DT END
  WHEN FRST_UNPAID_DT IS NOT NULL THEN                  -- fall-through
       CASE WHEN LAST_BUSINESS_DAY-FRST_UNPAID_DT<0 THEN 0 ELSE LAST_BUSINESS_DAY-FRST_UNPAID_DT END
  ELSE NULL END
```
**Diff:** SAS W/B/S with missing `WK_FRST_UNPAID_DT` → NULL. Feature's
`AND WK_FRST_UNPAID_DT IS NOT NULL` falls through to `FRST_UNPAID_DT` → returns a number.

### 1.2 `STEP_F` (MOR) 🔴 — wrong input table
**SAS** `J_RRII_KS10_2107:189-195`
```sas
IF STEP_PLN_SNAPSHOT_ID IN (-1,-2) THEN STEP_F='N'; ELSE STEP_F='Y';
```
**feature** `derived/step_f.py:52-64` (export_mor)
```sql
SELECT mor.BASEL_ACCT_ID, 'MOR' AS SRC_SYS_CD, base.STEP_FLAG as STEP_F
FROM ingestion.MORT_MTH_SNAPSHOT mor
LEFT JOIN ingestion.BASELAYER_MOR base ON mor.MORT_NUM=base.MORT_NUM AND base.MTH_END_DT='{rundate}'
WHERE mor.MTH_TM_ID={mth_tm_id}
```
**Diff:** SAS uses `STEP_PLN_SNAPSHOT_ID` (in the snapshot); feature reads `BASELAYER_MOR.STEP_FLAG`.

### 1.3 `TRNST_EXCLSN_F` (MOR) 🔴 — wrong input table
**SAS** `J_RRII_KS10_2107:56,205-211`
```sas
/* source */ LEFT JOIN NZRRAP.TRNST_EXCLSN_LKP BR ON a.SERV_BR_TRNST_NUM=BR.EXCLUDED_TRNST_NUM
/* rule   */ if EXCLUDED_TRNST_NUM eq '' then TRNST_EXCLSN_F='N'; else TRNST_EXCLSN_F='Y';
```
**feature** `derived/trnst_exclsn_f.py:84-97` (export_mor)
```sql
SELECT mor.BASEL_ACCT_ID, 'MOR' AS SRC_SYS_CD, base.TRANSIT_EXCLUSION_FLAG as TRNST_EXCLSN_F
FROM ingestion.MORT_MTH_SNAPSHOT mor
LEFT JOIN ingestion.BASELAYER_MOR base ON mor.MORT_NUM=base.MORT_NUM AND base.MTH_END_DT='{rundate}'
WHERE mor.MTH_TM_ID={mth_tm_id}
```
**Diff:** SAS derives from `TRNST_EXCLSN_LKP` on `SERV_BR_TRNST_NUM`; feature reads pre-baked `BASELAYER_MOR.TRANSIT_EXCLUSION_FLAG`.

### 1.4 `OS_BAL_AMT` (MOR) 🟠 — COALESCE vs null-propagation
**SAS** `J_RRII_KS10_2107:141`
```sas
os_bal_amt = CRNT_BAL_AMT + INTR_ACCR_AMT;   /* any component . -> os_bal_amt = . */
```
**feature** `derived/os_bal_amt.py:55-64` (export_mor)
```sql
SELECT BASEL_ACCT_ID, (COALESCE(crnt_bal_amt,0)+COALESCE(intr_accr_amt,0)) AS OS_BAL_AMT, 'MOR' AS SRC_SYS_CD
FROM ingestion.MORT_MTH_SNAPSHOT WHERE mth_tm_id={mth_tm_id}
```
**Diff:** SAS null-propagates → NULL; feature COALESCEs to 0. Only bites if a component is NULL.

### 1.5 `PIT_STAT_VER_1_CD` ⚪ — no feature exists
**SAS** `J_RRII_KS10_2107:168-187`
```sas
IF  COMM_TP_CD='RESIDENTIAL' AND CRNT_BAL_AMT NE 0.000
    AND (PD_OFF_DT EQ . OR STRIP(PD_OFF_DT) EQ '')
    AND (DLQNT_MTH_CNT LE 3 OR DLQNT_MTH_CNT EQ .)
    AND (STRIP(FRCLSR_F) NE 'Y' OR strip(FRCLSR_F) EQ '')
    AND (strip(LAND_RGSTRN_ACT_STAT_F) EQ '' OR strip(LAND_RGSTRN_ACT_STAT_F)='N')
THEN PIT_STAT_VER_1_CD='CUR';
ELSE IF CRNT_BAL_AMT NE 0.000 AND COMM_TP_CD='RESIDENTIAL'
    AND (PD_OFF_DT EQ . OR PD_OFF_DT EQ '')
    AND ((DLQNT_MTH_CNT EQ . OR DLQNT_MTH_CNT GT 3)
      OR (strip(FRCLSR_F) EQ '' OR STRIP(FRCLSR_F)='Y')
      OR (strip(LAND_RGSTRN_ACT_STAT_F) EQ '' OR strip(LAND_RGSTRN_ACT_STAT_F)='Y'))
THEN PIT_STAT_VER_1_CD='DEF';   /* else blank */
```
**feature:** none. (Emulated monolith `BASEL_MORT_ACCT_DRVD_VARS.py:192-210` already implements this CASE — the logic the new MOR branch would carry.)

---

## Table 2 — `BASEL_PSNL_LOAN_ACCT_DRVD_VARS` (SPL branch)

Matching (not expanded): ✅ `ACCT_NUM`, `BASEL_CUST_ID`.

### 2.1 `STEP_F` (SPL) 🔴 — feature is a stub
**SAS** `J_RRII_KS10_2105:62-63`
```sas
if STEP_PLN_SNAPSHOT_ID>0 then STEP_F='Y'; else STEP_F='N';
```
**feature** `derived/step_f.py:38-48` (export_spl)
```sql
SELECT spl.BASEL_ACCT_ID, 'SPL' AS SRC_SYS_CD, Null as STEP_F
FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT spl WHERE spl.MTH_TM_ID={mth_tm_id}
```
**Diff:** SAS computes `STEP_PLN_SNAPSHOT_ID>0`; feature SPL branch returns `Null`.

### 2.2 `OS_BAL_AMT` (SPL) 🟠 — missing ROUND + COALESCE
**SAS** `J_RRII_KS10_2105:43`
```sas
round(TOT_CRNT_BAL_AMT + ADD_ON_BAL_AMT + ACCR_INTR, 3) as OS_BAL_AMT
```
**feature** `derived/os_bal_amt.py:70-83` (export_spl)
```sql
SELECT BASEL_ACCT_ID,
       (COALESCE(tot_crnt_bal_amt,0)+COALESCE(add_on_bal_amt,0)+COALESCE(accr_intr,0)) AS OS_BAL_AMT,
       'SPL' AS SRC_SYS_CD
FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT spl WHERE spl.mth_tm_id={mth_tm_id}
```
**Diff:** SAS `ROUND(...,3)`, no COALESCE (null→null). Feature: no ROUND, COALESCE null→0.

### 2.3 `TRNST_EXCLSN_F` (SPL) 🟠 — blank vs NULL test
**SAS** `J_RRII_KS10_2105:72-73`
```sas
if strip(EXCLUDED_TRNST_NUM) eq '' then TRNST_EXCLSN_F='N'; else TRNST_EXCLSN_F='Y';
```
**feature** `derived/trnst_exclsn_f.py:28-52` (export_spl)
```sql
CASE WHEN TRIM(EXCLUDED_TRNST_NUM) IS NULL THEN 'N' ELSE 'Y' END AS TRNST_EXCLSN_F
-- source: BASEL_PSNL_LOAN_MTH_SNAPSHOT a LEFT JOIN TRNST_EXCLSN_LKP c ON a.CRNT_BR_LOCTN_TRNST=c.EXCLUDED_TRNST_NUM
```
**Diff:** SAS tests `=''`; feature tests `IS NULL`. Equal for a lookup miss (NULL);
differ only if `EXCLUDED_TRNST_NUM` is a non-null blank string.

### 2.4 `CONSM_PRD_TREATMNT_CD` (SPL) 🟠 — null-balance edge
**SAS** `J_RRII_KS10_2105:75-76` (with `OS_BAL_AMT`=round, line 43)
```sas
if TRNST_EXCLSN_F='Y' or OS_BAL_AMT le 0 then CONSM_PRD_TREATMNT_CD='Z'; else 'A';
```
**feature** `derived/consm_prd_treatmnt_cd.py:109-116` (export_spl)
```sql
CASE WHEN (TRIM(EXCLUDED_TRNST_NUM)!='' AND EXCLUDED_TRNST_NUM IS NOT NULL)
       OR ROUND(TOT_CRNT_BAL_AMT+ADD_ON_BAL_AMT+ACCR_INTR,3) <= 0 THEN 'Z' ELSE 'A' END
```
**Diff:** formula matches. In SAS a missing `OS_BAL_AMT` (`.`) satisfies `le 0` → 'Z';
in the feature `ROUND(null)<=0` is NULL/false → 'A'. Only bites on a null balance component.

### 2.5 `PIT_STAT_VER_1_CD` (SPL) ⚪ — no feature exists
**SAS** `J_RRII_KS10_2105:65-70`
```sas
if   strip(RECD_STAT_CD)='4' AND DAY_ODUE<=90 then PIT_STAT_VER_1_CD='CUR';
else if strip(RECD_STAT_CD)='4' AND DAY_ODUE>90  then PIT_STAT_VER_1_CD='DEF';
else if strip(RECD_STAT_CD)='5' then PIT_STAT_VER_1_CD='DEF';
else if strip(RECD_STAT_CD)='6' then PIT_STAT_VER_1_CD='CHG';
else if strip(RECD_STAT_CD)='7' then PIT_STAT_VER_1_CD='CHG';
else if strip(RECD_STAT_CD)='8' then PIT_STAT_VER_1_CD='CHG';   /* else blank */
```
**feature:** none. (Emulated monolith `BASEL_PSNL_LOAN_ACCT_DRVD_VARS.py:63-70` implements this CASE.)

---

## Table 3 — `BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2` (feature input: `PIT_STATUS_CROSS_DEFAULT_ORIG` SPL)

Only one feature is consumed; every other column is temporal and computed inline
(prev-month self-join, `CONS_DFT_MTH_CNT` recursion, `OS_BAL_AMT_V2`) — not
featurizable, so not part of this comparison.

### 3.1 `PIT_STATUS` (= `PIT_STATUS_CROSS_DEFAULT_ORIG` SPL) ✅ — matches
The delegated status resolves to `features.PIT_STATUS_ACCOUNT_ORIG` (SPL branch)
+ the step cross-default override. SPL base status:
```sql
-- derived/pit_status_account_orig.py:334-343 (export_spl)
CASE
  WHEN RECD_STAT_CD IN (0,9,NULL) THEN 'CLO'
  WHEN RECD_STAT_CD IN (6,7,8)    THEN 'CHG'
  WHEN CHRG_OFF_DT IS NOT NULL    THEN 'CHG'
  WHEN DAY_ODUE>=90 OR RECD_STAT_CD=5 THEN 'DEF'
  WHEN DAY_ODUE<90 AND RECD_STAT_CD=4 THEN 'CUR' ELSE NULL END
```
Prior verification (`...sas-parity-verification.md` §8) confirms this equals the
SAS `CROSS_DFLT_PIT_STATUS` lookup (RRMSS-2841). **No discrepancy in the feature.**
(NB this is the cross-default status — a *different* column from Table 2's
`PIT_STAT_VER_1_CD`.)

---

## Table 4 — `BASEL_REVLVNG_CR_BASE_DRVD_VARS` (16 KS features — the feature-generated reference)

### 4.1 `HELOC_F` 🔴 — different source column
**SAS** `J_RRII_KS10_2103:77-81`
```sas
(CASE WHEN SUB_PRD_CD='RS' OR STEP_PLN_SNAPSHOT_ID NOT IN (-1,-2) THEN 'Y' ELSE 'N' END) AS HELOC_F
```
**feature** `derived/heloc_f.py:28-32`
```sql
CASE WHEN TRIM(SUB_PRD_CD)='RS' OR COALESCE(TRIM(STEP_PLN_AGRMNT_NUM),'')!='' THEN 'Y' ELSE 'N' END
```
**Diff:** SAS keys on `STEP_PLN_SNAPSHOT_ID NOT IN (-1,-2)`; feature keys on
`STEP_PLN_AGRMNT_NUM <> ''`. Usually agree, flip on an account with a snapshot id
but blank agreement number (or vice-versa). *(Owner previously accepted the feature
as canonical.)* This same predicate propagates into the KS `PIT_STATUS` (§4.3) and Table 6.

### 4.2 `REVISED_EXPSR_AMT` ✅ — identical
**SAS** `J_RRII_KS10_2103:86-89`  → **feature** `derived/revised_expsr_amt.py:35-38`
```
CASE WHEN CR_LMT_AMT > TOT_NEW_BAL_AMT THEN CR_LMT_AMT ELSE TOT_NEW_BAL_AMT END
```
Byte-identical.

### 4.3 `PIT_STAT_VER_2_CD` (= `PIT_STATUS_CROSS_DEFAULT_ORIG` KS) ⚠️ — structurally matches, 2 caveats
The KS base status reproduces the SAS 90/180 threshold selection:
```
SAS J_RRII_KS10_2103:368-376              |  feature pit_status_account_orig.py:127-243 (KS)
PIT_STAT_VER_2_CD90  = <BNS_DLQNT_DAY<120>|  CASE WHEN BASEL_PRD_CD='CC' AND HELOC_F='N'
PIT_STAT_VER_2_CD180 = <BNS_DLQNT_DAY<210>|       THEN <BNS_DLQNT_DAY<210 CASE>
if BASEL_PRD_CD='CC' AND HELOC_F='N'      |       ELSE <BNS_DLQNT_DAY<120 CASE, full HELOC split> END
   then =180 else =90                     |
```
The `CHRG_OFF`/`BLOCK_RECL`/`PREV_SNAP` sub-conditions match SAS l.154-210.
**Caveats:** (1) inherits the `HELOC_F` predicate diff (§4.1); (2) the step
cross-default override — SAS `modify` from `NZRRAP.PIT_STATUS_PRE_STEP`
(l.450-469, RRMSS-2840) — is **reimplemented inline** in
`pit_status_cross_default_orig.py` (siblings sharing a `STEP_PLN_AGRMNT_NUM` with a
CHG/DEF account flip CUR→DEF) rather than read from the pre-step table. Structurally
equivalent; a value-level trace of the override is a separate effort.

### 4.4 Remaining 12 features — verified matching in the prior line-level pass, not re-expanded
`BASEL_PRD_CD`, `BASEL_PRD_DESC`, `CONSM_SCORECRD_EXCLSN_F`, `CONSM_PRD_TREATMNT_CD`,
`RS_F`, `SML_BUS_F`, `STEP_CD`, `TRNST_EXCLSN_F`, `ACCRL_STAT_F`, `LTV_TP_CD`,
`BNKRPY_F`, `PIT_STAT_VER_2_CD90/180`, `TOTAL_EXPSR_ABOVE_LMT_F`.
Two worth a dedicated future pass (most SAS-complex): `STEP_CD` (SAS l.403-416
SCRD/BILL_CD branches) and `TOTAL_EXPSR_ABOVE_LMT_F` (SAS l.473-499 threshold +
per-customer SUM).

---

## Table 5 — `PSNL_LOAN_OBSVTN_PT_DRVD_VAR` (SPL: `PIT_STATUS_CROSS_DEFAULT_ORIG`, `TREATMENT_F`, `SUB_PORT_F`)

All three consumed as per-row inputs; window columns (`LAST_NEW_DFT_*`,
`RCVRY_WINDOW_CUTOFF_*`, `MODEL_DFT_F`) are temporal/inline.

- **`PIT_STATUS_CROSS_DEFAULT_ORIG` (SPL)** — same as §3.1. ✅
- **`SUB_PORT_F`** `derived/sub_port_f.py:39-55` — `S01–S08→DIRECT`, `S09–S15→INDIRECT`.
  Matches the SAS/emulated `SUB_PORTFL` mapping. ✅
- **`TREATMENT_F` (SPL)** `derived/treatment_f.py:74-88`
  ```sql
  CASE WHEN UPPER(TRIM(CRNT_BR_LOCTN_TRNST)) IN (18192,99432) OR UPPER(TRIM(COMM_LOAN_CD))=1 THEN 'Z' ELSE 'A' END
  ```
  Matches the emulated PSNL_2 CAB (`18192/99432`) + commercial exclusion. ✅
- **Caveat:** the SAS (`J_RRAP_TL10_2201`) is 6,396 lines of INFA-generated code;
  inputs verified faithful, but a full byte-trace of the window derivation is a
  separate effort. **No feature-input discrepancy found.**

---

## Table 6 — `REVLVNG_CR_OBSVTN_PT_DRVD_VAR` (KS: `BASEL_PRD_CD`, `PIT_STATUS_CROSS_DEFAULT_ORIG`, `HELOC_F`, `ACCRL_STAT_F`, `CONSM_PRD_TREATMNT_CD`, `SML_BUS_F`, `TRNST_EXCLSN_F`)

Same KS features as Table 4, so:
- **`HELOC_F` 🔴** — same discrepancy as §4.1 (shared feature).
- **`PIT_STATUS_CROSS_DEFAULT_ORIG` (KS) ⚠️** — same caveats as §4.3.
- Other inputs faithful (prior verification called this the cleanest port).
  Recovery-window columns (`_NEW_DEFAULT_FLG`, `LAST_NEW_DEFAULT_DATE`,
  `MODEL_DFT_F`) are temporal/inline, not features.

---

## Table 7 — `STATUS_FINAL` (MOR: `MORT_NUM`, `PIT_STATUS_CROSS_DEFAULT_ORIG`)

### 7.1 `STATUS` (= `PIT_STATUS_CROSS_DEFAULT_ORIG` MOR) 🟠 — formula matches, source/coverage differs
**feature** `derived/pit_status_account_orig.py:280-314` (export_mor) reproduces the
SAS mortgage-model status CASE — incl. the `OR CRNT_BAL_AMT < 0 → 'CUR'` quirk and
the foreclosed/paid-off `greatest(CRNT_BAL_AMT, -TOT_SUSP_BAL_AMT) > 0 → 'DEF'` clause:
```sql
CASE WHEN COMM_TP='RESIDENTIAL' AND PD_OFF_DT IS NULL
       AND ((DLQNT_DAY<90 AND DLQNT_MTH<4) AND FRCLSR_F<>'Y' AND CRNT_BAL_AMT<>0 AND LRA_STAT<>'Y')
       OR CRNT_BAL_AMT<0 THEN 'CUR'
     WHEN (... DLQNT_DAY>=90 OR DLQNT_MTH>=4 OR FRCLSR_F='Y' OR LRA_STAT='Y' ... AND CRNT_BAL_AMT>0)
       OR (... FRCLSR_F='Y' AND PD_OFF_F='Y' AND greatest(CRNT_BAL_AMT, COALESCE(-TOT_SUSP_BAL_AMT,0))>0) THEN 'DEF' END
```
**Diff (source):** the feature reads `ingestion.MORT_MTH_SNAPSHOT`; `STATUS_FINAL`
processes `ingestion.MORTGAGE_HIST`. `MORTGAGE_HIST` rows with no matching MOR feature
row get `STATUS=NULL → MODEL_EXCL='Y'` (excluded), where SAS would compute an inline
status. Formula is faithful; the risk is coverage. *(needs runtime check: count of
MORTGAGE_HIST rows unmatched by `features.MORT_NUM` for the month.)*

### 7.2 `MORT_NUM` ✅ — pass-through key
`derived/mort_num.py:51-61` (export_mor) = `MORT_NUM` straight from `MORT_MTH_SNAPSHOT`. Matches.

---

## Table 8 — `TWELVE_MON_DEF_WINDOW`

**No features consumed** (reads `emulated.STATUS_FINAL`; output grain is
mortgage×window). Nothing to compare here — see the sas-parity-verification doc §4
for its direct SAS comparison.

---

## Summary of discrepancies (all tables)

| # | Table | 🔴 value differs | 🟠 edge/null/source | ⚪ missing feature |
|---|---|---|---|---|
| 1 | BASEL_MORT_ACCT_DRVD_VARS | DLQNT_DAY_CNT, STEP_F, TRNST_EXCLSN_F | OS_BAL_AMT | PIT_STAT_VER_1_CD (MOR) |
| 2 | BASEL_PSNL_LOAN_ACCT_DRVD_VARS | STEP_F | OS_BAL_AMT, TRNST_EXCLSN_F, CONSM_PRD_TREATMNT_CD | PIT_STAT_VER_1_CD (SPL) |
| 3 | BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 | — | — | — |
| 4 | BASEL_REVLVNG_CR_BASE_DRVD_VARS | HELOC_F | PIT_STAT_VER_2_CD (override reimpl) | — |
| 5 | PSNL_LOAN_OBSVTN_PT_DRVD_VAR | — | — | — |
| 6 | REVLVNG_CR_OBSVTN_PT_DRVD_VAR | HELOC_F | PIT_STAT_VER_2_CD (override reimpl) | — |
| 7 | STATUS_FINAL | — | STATUS (source/coverage) | — |
| 8 | TWELVE_MON_DEF_WINDOW | — | — | — |

**Deferred (separate focused passes):** Table 4 `STEP_CD` / `TOTAL_EXPSR_ABOVE_LMT_F`
line-check; the KS/SPL/MOR PIT cross-default *override* value trace; the
INFA-generated observation-point SAS (Tables 5/6) byte trace.
</content>
