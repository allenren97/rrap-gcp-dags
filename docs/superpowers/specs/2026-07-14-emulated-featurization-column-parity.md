# Emulated tables — per-column feature availability & SAS-parity for join-up

**Date:** 2026-07-14
**Goal:** for each emulated table, map every output column to a `features.*`
asset, state whether that feature's generation logic matches the emulated
table's SAS logic, and list the features that still need to be built.
**Method:** static read of `functions/emulated/filteredtable/*.py`,
`functions/features/derived/*.py`, and the SAS parity notes in
`2026-07-12-emulated-sas-parity-verification.md`.

## How the join works
Feature-generated assembly = read the table's ingestion snapshot as the spine,
then `LEFT JOIN features.<COL>` on `BASEL_ACCT_ID + OBSN_DT` (PIT joins also
filter `SRC_SYS_CD`). Each `features.*` is a **multi-source** table with
`SRC_SYS_CD` branches: **KS** (revolving), **SPL** (personal loan), **MOR/MO**
(mortgage), **TNG-MOR**. Reference exemplar: `BASEL_REVLVNG_CR_BASE_DRVD_VARS`
(16 feature joins).

**Parity requires all three to match, per column:** (1) source snapshot,
(2) filter/grain, (3) column formula. A same-named feature is NOT automatically
a drop-in.

---

## Mortgage source relationship (CORRECTED 2026-07-14)
`MORT_MTH_SNAPSHOT` is **built from** `BASEL_MORT_MTH_SNAPSHOT`, not a separate
lineage. Per `ingestion/cleaned/mort_mth_snapshot.py`:

```
MORT_MTH_SNAPSHOT = BASEL_MORT_MTH_SNAPSHOT  (base, all rows kept)
   LEFT JOIN AIRB_MORT_MTH_SNAPSHOT  (adds ~13 AIRB-only cols)
   ON BASEL.mth_tm_id = AIRB.tm_id AND TRIM(BASEL.mort_num) = AIRB.mort_num
```

Every column the mortgage features consume (`SCRTY_TP_2`, `FUND_CD`, `FLOAT_CD`,
`PD_OFF_F/DT`, `WK/FRST_UNPAID_DT`, `CRNT_BAL_AMT`, `INTR_ACCR_AMT`,
`STEP_PLN_SNAPSHOT_ID`, `SERV_BR_TRNST_NUM`, `FRCLSR_F`, `BASEL_ACCT_ID`) comes
from the **BASEL side and is carried through unchanged**. So for any account-month
in both, the BASEL-sourced feature values are **identical to SAS** (which reads
BASEL_MORT_MTH_SNAPSHOT). The "source mismatch" is therefore **not a value
blocker** for these columns.

**The one real risk = row grain.** If `AIRB_MORT_MTH_SNAPSHOT` is not unique on
`(tm_id, mort_num)`, the LEFT JOIN fans out → `MORT_MTH_SNAPSHOT` gets duplicate
account rows → the feature has >1 row/account-month → assembly join inflates.
**Verify AIRB uniqueness on `(tm_id, mort_num)` (needs runtime check).** BASEL
rows with a non-matching `mort_num` are still kept (NULL AIRB cols) — harmless,
features don't read AIRB columns.

→ Revised: MORT is featurizable **without** BASEL-snapshot feature variants,
provided the AIRB fan-out check passes. Remaining MORT gaps are pure **logic**
(next section), not source: `DLQNT_DAY_CNT` null bug, `STEP_F`/`TRNST_EXCLSN_F`
using `BASELAYER_MOR` instead of the `STEP_PLN_SNAPSHOT_ID`/`SERV_BR_TRNST_NUM`
already present in the snapshot, `OS_BAL_AMT` COALESCE, and missing
`PIT_STAT_VER_1_CD`.

---

## Table 1 — BASEL_MORT_ACCT_DRVD_VARS  (source: BASEL_MORT_MTH_SNAPSHOT)

| Output column | Feature exists? | Feature source (MOR branch) | Logic vs SAS/emulated | Verdict |
|---|---|---|---|---|
| MTH_TM_ID, BASEL_ACCT_ID | n/a (spine) | — | pass-through | keep inline |
| BASEL_CUST_ID | n/a | — | = PRIM_BASEL_CUST_ID pass-through | keep inline |
| ACCT_NUM | ✅ `ACCT_NUM` | MORT_MTH_SNAPSHOT + DIM | same DIM join | **source ✗** |
| OS_BAL_AMT | ✅ `OS_BAL_AMT` | MORT_MTH_SNAPSHOT | `crnt_bal+intr_accr` same; feature COALESCEs to 0 | **source ✗** |
| COMM_TP_CD | ✅ `COMM_TP_CD` | MORT_MTH_SNAPSHOT | feature lacks the `GREATEST(len-2,1)` short-string guard the emulated added → diff on <3-char `SCRTY_TP_2` | **source ✗ + edge logic ✗** |
| LAND_RGSTRN_ACT_STAT_F | ✅ `LAND_RGSTRN_ACT_STAT_F` | MORT_MTH_SNAPSHOT (single-source) | same FUND_CD ranges | **source ✗** |
| DLQNT_DAY_CNT | ✅ `DLQNT_DAY_CNT` | MORT_MTH_SNAPSHOT | feature keeps the **W/B/S fall-through bug** (finding 1.1); emulated was fixed to return NULL | **source ✗ + logic ✗ (feature is stale)** |
| DLQNT_MTH_CNT | ✅ `DLQNT_MTH_CNT` | MORT_MTH_SNAPSHOT + `DLQNT_DAY_CNT` | formula matches | **source ✗** |
| CONSM_PRD_TREATMNT_CD | ✅ `CONSM_PRD_TREATMNT_CD` | MORT_MTH_SNAPSHOT (via COMM_TP_CD) | MOR-branch formula matches | **source ✗** |
| TRNST_EXCLSN_F | ✅ `TRNST_EXCLSN_F` | **BASELAYER_MOR.TRANSIT_EXCLUSION_FLAG** | emulated derives from `reference.TRNST_EXCLSN_LKP` on SERV_BR_TRNST_NUM — **different derivation entirely** | **logic ✗ (different input)** |
| STEP_F | ✅ `STEP_F` | **BASELAYER_MOR.STEP_FLAG** | emulated = `STEP_PLN_SNAPSHOT_ID IN (-1,-2)` → different input | **logic ✗** |
| PIT_STAT_VER_1_CD | ❌ **none** | — | mortgage variant (COMM_TP_CD/balance/foreclosure/DLQNT_MTH/LRA) | **MUST GENERATE** |

**Verdict:** blocked. Needs BASEL-snapshot feature variants + a new mortgage
`PIT_STAT_VER_1_CD`, plus fixes to `DLQNT_DAY_CNT` (stale bug), `COMM_TP_CD`
(short-string guard), `TRNST_EXCLSN_F` and `STEP_F` (different inputs).

---

## Table 2 — BASEL_PSNL_LOAN_ACCT_DRVD_VARS  (source: BASEL_PSNL_LOAN_MTH_SNAPSHOT) — VIABLE

Source **matches** the SPL branch of the multi-product features, so this is the
one clean featurization candidate.

| Output column | Feature exists? | SPL branch source | Logic vs emulated/SAS | Verdict |
|---|---|---|---|---|
| BASEL_ACCT_ID, MTH_TM_ID | n/a (spine) | — | pass-through | keep inline |
| BASEL_CUST_ID | n/a | — | = PRIM_BASEL_CUST_ID | keep inline |
| ACCT_NUM | ✅ `ACCT_NUM` | BASEL_PSNL_LOAN + DIM | same | ✅ **usable** |
| OS_BAL_AMT | ✅ `OS_BAL_AMT` | BASEL_PSNL_LOAN_MTH_SNAPSHOT ✓ | `tot_crnt_bal+add_on+accr` — but feature **omits `ROUND(...,3)`** the emulated applies | ⚠️ **near — add ROUND(,3)** |
| CONSM_PRD_TREATMNT_CD | ✅ `CONSM_PRD_TREATMNT_CD` | BASEL_PSNL_LOAN ✓ | SPL branch matches exactly (incl. ROUND + EXCLUDED_TRNST_NUM) | ✅ **usable** |
| TRNST_EXCLSN_F | ✅ `TRNST_EXCLSN_F` | BASEL_PSNL_LOAN ✓ | feature `TRIM(...) IS NULL→'N'` vs emulated `COALESCE(TRIM,'')=''→'N'`; equivalent for lookup-join nulls, differs only on a non-null blank | ⚠️ **usable (blank-vs-null nuance)** |
| STEP_F | ✅ `STEP_F` | BASEL_PSNL_LOAN ✓ | SPL branch returns **`Null`**; emulated = `STEP_PLN_SNAPSHOT_ID > 0 → 'Y' ELSE 'N'` | 🔴 **BROKEN — feature SPL is a stub** |
| PIT_STAT_VER_1_CD | ❌ **none** | — | PSNL variant (`RECD_STAT_CD` + `DAY_ODUE` buckets: 4→CUR/DEF@90, 5→DEF, 6/7/8→CHG) | 🔴 **MUST GENERATE** |

**Verdict:** featurizable after 2 real fixes (`STEP_F` SPL stub → implement;
new `PIT_STAT_VER_1_CD` SPL variant) + 1 small fix (`OS_BAL_AMT` rounding).
NB prior owner call: derived cols here are consumed table-wise, not as features,
so the reuse win is limited — this is a pattern-uniformity decision, not a
correctness one.

---

## Table 4 — BASEL_REVLVNG_CR_BASE_DRVD_VARS — ALREADY FEATURE-GENERATED (reference)

16 features joined and working: `BASEL_PRD_CD`, `CONSM_SCORECRD_EXCLSN_F`,
`CONSM_PRD_TREATMNT_CD`, `HELOC_F`, `PIT_STATUS_CROSS_DEFAULT_ORIG`(→PIT_STAT_VER_2_CD),
`REVISED_EXPSR_AMT`, `RS_F`, `SML_BUS_F`, `STEP_CD`, `TRNST_EXCLSN_F`,
`ACCRL_STAT_F`, `LTV_TP_CD`, `BNKRPY_F`, `PIT_STAT_VER_2_CD90`,
`PIT_STAT_VER_2_CD180`, `TOTAL_EXPSR_ABOVE_LMT_F`. All KS-branch, same snapshot.
Only open item: `HELOC_F` uses `STEP_PLN_AGRMNT_NUM<>''` instead of SAS
`STEP_PLN_SNAPSHOT_ID NOT IN (-1,-2)` (finding 6.1, **accepted** as canonical).
**No new features needed.**

---

## Tables 3, 5, 6, 7, 8 — temporal/window: features are INPUTS only, cannot featurize the derived columns

These already consume features as per-row inputs (correct) and derive their
signature columns via cross-row/temporal SQL (`ROW_NUMBER`, `LAG`, recursion,
multi-month history) that CANNOT be split into per-account single-column
features without breaking parity. Feature inputs each already exists:

| Table | Feature inputs used (exist ✅) | Derived inline (stay) | New features |
|---|---|---|---|
| 3 `BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2` | `PIT_STATUS_CROSS_DEFAULT_ORIG` (SPL) | CONS_DFT_MTH_CNT recursion, INT_AT_DEFAULT/OS_BAL_AMT_V2 (LAG/ROW_NUMBER), prev-month self-join | none |
| 5 `PSNL_LOAN_OBSVTN_PT_DRVD_VAR` | `PIT_STATUS_CROSS_DEFAULT_ORIG`, `TREATMENT_F`, `SUB_PORT_F` (SPL) | LAST_NEW_DFT_*, RCVRY_WINDOW_CUTOFF_*, MODEL_DFT_F (48-mo window) | none |
| 6 `REVLVNG_CR_OBSVTN_PT_DRVD_VAR` | `PIT_STATUS_CROSS_DEFAULT_ORIG`, `BASEL_PRD_CD`, `HELOC_F`, `ACCRL_STAT_F`, `SML_BUS_F`, `CONSM_PRD_TREATMNT_CD`, `TRNST_EXCLSN_F` (KS) | recovery-window ROW_NUMBER×13, LAG×6 | none |
| 7 `STATUS_FINAL` | `MORT_NUM`, `PIT_STATUS_CROSS_DEFAULT_ORIG` (MOR) | ~40 MORTGAGE_HIST pass-throughs + DELQ_DAYS_2→DELQ_MONTHS_2→MODEL_EXCL chain | none (optional: MOR DELQ_* if ever reused) |
| 8 `TWELVE_MON_DEF_WINDOW` | none (reads emulated.STATUS_FINAL) | grain is (mortgage, window) — not account×month | none — not a features producer |

---

## FEATURE-vs-SAS VERIFICATION (direct against the SAS jobs)

Verified each feature's branch against the authoritative SAS DATA step, not the
emulated port. SAS sources: PSNL = `J_RRII_KS10_2105` (reads
`BASEL_PSNL_LOAN_MTH_SNAPSHOT`), MORT = `J_RRII_KS10_2107` (reads
**`BASEL_MORT_MTH_SNAPSHOT`**, line 55).

### PSNL — feature SPL branch vs SAS 2105
| Feature | SAS 2105 logic | Feature SPL branch | Identical to SAS? |
|---|---|---|---|
| `OS_BAL_AMT` | `round(TOT_CRNT_BAL+ADD_ON+ACCR,3)`, no COALESCE | `COALESCE(..,0)+COALESCE(..,0)+COALESCE(..,0)`, no ROUND | **NO** — missing ROUND; COALESCE changes null→0 (SAS→null) |
| `STEP_F` | `STEP_PLN_SNAPSHOT_ID>0 ? 'Y':'N'` | `Null` stub | **NO** — feature is a stub |
| `PIT_STAT_VER_1_CD` | RECD_STAT_CD/DAY_ODUE buckets | — (no feature) | **N/A — missing** |
| `TRNST_EXCLSN_F` | `strip(EXCLUDED_TRNST_NUM)='' ? 'N':'Y'` | `TRIM(..) IS NULL ? 'N':'Y'` | **~** equal for join-nulls; differs on a non-null blank |
| `CONSM_PRD_TREATMNT_CD` | `TRNST_EXCLSN_F='Y' OR OS_BAL_AMT<=0 → 'Z'` (SAS: missing `<=0` is TRUE) | `EXCLUDED_TRNST_NUM<>'' OR ROUND(..,3)<=0 → 'Z'` | **~** formula matches; null-balance → SAS 'Z' vs feature 'A' |
| `ACCT_NUM` | DIM join, strip | DIM join, TRIM | **YES** |

### MORT — feature MOR branch vs SAS 2107
| Feature | SAS 2107 logic (from BASEL_MORT snapshot) | Feature MOR branch (from MORT_MTH snapshot) | Identical to SAS? |
|---|---|---|---|
| `COMM_TP_CD` | `SUBSTR(..,1,1)='6' OR INPUT(SUBSTR(..,len-2,3))>=5` (raw substr, no guard) | same raw `SUBSTR(..,len-2,3)` | **logic YES**, **source NO** (emulated's GREATEST guard is the one that deviates from SAS) |
| `LAND_RGSTRN_ACT_STAT_F` | FUND_CD ranges 2000-2199/2202-2249/6490-6499 | same ranges | **logic YES**, **source NO** |
| `DLQNT_DAY_CNT` | W/B/S branch: `LBD - WK_FRST_UNPAID_DT`; if WK date `.` → TEMP `.` → **NULL** | `... AND WK_FRST_UNPAID_DT IS NOT NULL` then falls through to FRST branch → **number** | **NO** — feature returns a number where SAS returns NULL |
| `DLQNT_MTH_CNT` | `intck('MONTH',WK/FRST_DT1,tm_st)+1`, floored 0 | `GREATEST(DATE_DIFF('month',WK/FRST,tm_st)+1,0)` | **logic YES**, **source NO** (also inherits buggy DLQNT_DAY_CNT) |
| `CONSM_PRD_TREATMNT_CD` | `COMM_TP_CD≠'RESIDENTIAL' OR PD_OFF_DT≠. OR OS_BAL_AMT<=0 → 'Z'` | same formula | **logic YES**, **source NO**; null-balance 'Z' vs 'A' |
| `TRNST_EXCLSN_F` | `EXCLUDED_TRNST_NUM=''?'N':'Y'` from TRNST_EXCLSN_LKP on SERV_BR_TRNST_NUM | `BASELAYER_MOR.TRANSIT_EXCLUSION_FLAG` on MORT_NUM | **NO** — different input entirely |
| `STEP_F` | `STEP_PLN_SNAPSHOT_ID IN (-1,-2)?'N':'Y'` | `BASELAYER_MOR.STEP_FLAG` | **NO** — different input |
| `OS_BAL_AMT` | `CRNT_BAL+INTR_ACCR`, no COALESCE | `COALESCE(..,0)+COALESCE(..,0)` | **logic ~**, **source NO**; null→0 vs null |
| `PIT_STAT_VER_1_CD` | inline CUR/DEF logic | — (no feature) | **N/A — missing** |
| `ACCT_NUM` | DIM join (SRC_APP_CD='MO'), strip | DIM join, TRIM | **YES (logic)**, source scan differs |

**Net:** vs SAS, only `ACCT_NUM` (both), `COMM_TP_CD`/`LAND_RGSTRN`/`DLQNT_MTH_CNT`/
`CONSM_PRD_TREATMNT_CD` (MORT, logic only) are formula-faithful. All mortgage
features fail on **source snapshot**. `STEP_F` (SPL stub + MOR wrong input),
`DLQNT_DAY_CNT` (MOR null bug), `TRNST_EXCLSN_F` (MOR wrong input), and
`OS_BAL_AMT` (rounding/COALESCE) are **not identical to SAS**.

---

## FINALIZED LIST

### A. Features we HAVE and can use as-is (parity OK for their matching source)
- **PSNL (SPL branch):** `ACCT_NUM`, `CONSM_PRD_TREATMNT_CD`, `TRNST_EXCLSN_F` (blank/null nuance)
- **REVLVNG (KS, all 16):** already wired, parity twin verified
- **Feature inputs for temporal tables 3/5/6/7:** all present and correct

### B. Features we HAVE but logic/source does NOT match — need a fix or a variant
| Feature | Problem | Needed for |
|---|---|---|
| `STEP_F` (SPL) | returns `Null` stub; should be `STEP_PLN_SNAPSHOT_ID>0` | PSNL |
| `OS_BAL_AMT` (SPL) | missing `ROUND(...,3)` | PSNL |
| `STEP_F` (MOR) | uses BASELAYER_MOR.STEP_FLAG, emulated uses STEP_PLN_SNAPSHOT_ID IN(-1,-2) | MORT |
| `TRNST_EXCLSN_F` (MOR) | uses BASELAYER_MOR flag, emulated uses TRNST_EXCLSN_LKP join | MORT |
| `COMM_TP_CD` (MOR) | missing short-string `GREATEST(len-2,1)` guard | MORT |
| `DLQNT_DAY_CNT` (MOR) | stale W/B/S fall-through bug (emulated fixed to NULL) | MORT |
| all MORT features | read `MORT_MTH_SNAPSHOT`, emulated reads `BASEL_MORT_MTH_SNAPSHOT` | MORT (blocker) |

### C. Features we DON'T have — must generate
| New feature | Definition | For |
|---|---|---|
| `PIT_STAT_VER_1_CD` (SPL) | `RECD_STAT_CD`+`DAY_ODUE` buckets (4→CUR/DEF@90; 5→DEF; 6/7/8→CHG) | PSNL |
| `PIT_STAT_VER_1_CD` (MOR) | COMM_TP_CD/CRNT_BAL/FRCLSR_F/DLQNT_MTH_CNT/LRA CUR-vs-DEF logic | MORT |
| BASEL-snapshot variants of the 9 MORT features (or reconcile snapshots) | rebuild off `BASEL_MORT_MTH_SNAPSHOT` | MORT |

**No new features:** tables 3, 4, 5, 6, 7, 8.
</content>
</invoke>
