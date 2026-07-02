
/************************************************************************************/
/*  Business Logic changes                                                        ***/
/*  V3.1      -   Oct24,2015                                                      ***/
/*                First version                                                   ***/
/*  V4.1     -   Oct28,2015                                                       ***/
/*   per discussion with Probe team                                               ***/
/*   1)	Scorecard ID:  63001, 63002, 63003   6-Credit cards, 3-collection         ***/
/*   2)	Exclusion score : -997                                                    ***/
/* Feb 25: - Changed dates logic for selecting month end from Daily Snapshot table***/
/*EDRTLR.RISK_REVLVNG_CR_DLY_SNAPSHOT now we pick the                             ***/
/*latest available date in the current month.                                     ***/
/* Nov 23, 2016: - Added the section for DL CSV file input                        ***/
/*                                                                                ***/
/************************************************************************************/

options mprint;
%put &dldropf &JU_FTP_PTH &JU_FILE_NAME &DB2_svr &pme_dt &pme_yr &pme_mnth &pme_my &TM &dly_TM &Last_Avl_dt_mth;

%macro pre_SCORE();
*------------------------------------------------------------*;
* generateScorepoints_note;
*------------------------------------------------------------*;
SCORECARD_POINTS = 0;

*------------------------------------------------------------*;
* Variable: B11_cashadv_asperof_totalpur;
*------------------------------------------------------------*;
if MISSING(B11_cashadv_asperof_totalpur) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 68;
SCR_B11_cashadv_asperof_totalpur= 68;
end;
else if NOT MISSING(B11_cashadv_asperof_totalpur) AND B11_cashadv_asperof_totalpur < 4 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 68;
SCR_B11_cashadv_asperof_totalpur = 68;
end;
else if NOT MISSING(B11_cashadv_asperof_totalpur) and 4 <= B11_cashadv_asperof_totalpur AND B11_cashadv_asperof_totalpur < 37 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 61;
SCR_B11_cashadv_asperof_totalpur = 61;
end;
else if NOT MISSING(B11_cashadv_asperof_totalpur) and 37 <= B11_cashadv_asperof_totalpur AND B11_cashadv_asperof_totalpur < 999999 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 58;
SCR_B11_cashadv_asperof_totalpur = 58;
end;
else if NOT MISSING(B11_cashadv_asperof_totalpur) and 999999 <= B11_cashadv_asperof_totalpur then do;
SCORECARD_POINTS = SCORECARD_POINTS + 64;
SCR_B11_cashadv_asperof_totalpur = 64;
end;

*------------------------------------------------------------*;
* Variable: B14_pymnts_asperof_mindue;
*------------------------------------------------------------*;
if MISSING(B14_pymnts_asperof_mindue) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 71;
SCR_B14_pymnts_asperof_mindue= 71;
end;
else if NOT MISSING(B14_pymnts_asperof_mindue) AND B14_pymnts_asperof_mindue < 1.55 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 60;
SCR_B14_pymnts_asperof_mindue = 60;
end;
else if NOT MISSING(B14_pymnts_asperof_mindue) and 1.55 <= B14_pymnts_asperof_mindue AND B14_pymnts_asperof_mindue < 4.97 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 63;
SCR_B14_pymnts_asperof_mindue = 63;
end;
else if NOT MISSING(B14_pymnts_asperof_mindue) and 4.97 <= B14_pymnts_asperof_mindue AND B14_pymnts_asperof_mindue < 16.15 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 66;
SCR_B14_pymnts_asperof_mindue = 66;
end;
else if NOT MISSING(B14_pymnts_asperof_mindue) and 16.15 <= B14_pymnts_asperof_mindue AND B14_pymnts_asperof_mindue < 999999 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 71;
SCR_B14_pymnts_asperof_mindue = 71;
end;
else if NOT MISSING(B14_pymnts_asperof_mindue) and 999999 <= B14_pymnts_asperof_mindue then do;
SCORECARD_POINTS = SCORECARD_POINTS + 65;
SCR_B14_pymnts_asperof_mindue = 65;
end;

*------------------------------------------------------------*;
* Variable: B4_lastocc_ge100util;
*------------------------------------------------------------*;
if MISSING(B4_lastocc_ge100util) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 68;
SCR_B4_lastocc_ge100util= 68;
end;
else if NOT MISSING(B4_lastocc_ge100util) AND B4_lastocc_ge100util < 7 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 56;
SCR_B4_lastocc_ge100util = 56;
end;
else if NOT MISSING(B4_lastocc_ge100util) and 7 <= B4_lastocc_ge100util then do;
SCORECARD_POINTS = SCORECARD_POINTS + 68;
SCR_B4_lastocc_ge100util = 68;

end;

*------------------------------------------------------------*;
* Variable: GBL_CRNT_UTILZTN;
*------------------------------------------------------------*;
if MISSING(GBL_CRNT_UTILZTN) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 97;
SCR_GBL_CRNT_UTILZTN= 97;
end;
else if NOT MISSING(GBL_CRNT_UTILZTN) AND GBL_CRNT_UTILZTN < 0.13 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 62;
SCR_GBL_CRNT_UTILZTN = 62;
end;
else if NOT MISSING(GBL_CRNT_UTILZTN) and 0.13 <= GBL_CRNT_UTILZTN AND GBL_CRNT_UTILZTN < 47.03 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 97;
SCR_GBL_CRNT_UTILZTN = 97;
end;
else if NOT MISSING(GBL_CRNT_UTILZTN) and 47.03 <= GBL_CRNT_UTILZTN AND GBL_CRNT_UTILZTN < 86.31 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 78;
SCR_GBL_CRNT_UTILZTN = 78;
end;
else if NOT MISSING(GBL_CRNT_UTILZTN) and 86.31 <= GBL_CRNT_UTILZTN AND GBL_CRNT_UTILZTN < 98.74 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 59;
SCR_GBL_CRNT_UTILZTN = 59;
end;
else if NOT MISSING(GBL_CRNT_UTILZTN) and 98.74 <= GBL_CRNT_UTILZTN then do;
SCORECARD_POINTS = SCORECARD_POINTS + 46;
SCR_GBL_CRNT_UTILZTN = 46;
end;

*------------------------------------------------------------*;
* Variable: STEP_F;
*------------------------------------------------------------*;
length _fmtSTEP_F $200;
length _normSTEP_F $32;
drop _fmtSTEP_F _normSTEP_F;
_fmtSTEP_F=put(STEP_F, $8.0);
%dmnormcp(_fmtSTEP_F, _normSTEP_F);
if MISSING(_normSTEP_F) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 56;
SCR_STEP_F = 56;
end;
else
if _normSTEP_F in ('N'
) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 63;
SCR_STEP_F = 63;
end;
else
if _normSTEP_F in ('Y'
) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 119;
SCR_STEP_F = 119;
end;
else do;
SCORECARD_POINTS = SCORECARD_POINTS + 56;
SCR_STEP_F = 56;
end;
drop _normSTEP_F;

*------------------------------------------------------------*;
* Variable: SUM_INVEST_BAL;
*------------------------------------------------------------*;
if MISSING(SUM_INVEST_BAL) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 58;
SCR_SUM_INVEST_BAL= 58;

end;
else if NOT MISSING(SUM_INVEST_BAL) AND SUM_INVEST_BAL < 0 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 53;
SCR_SUM_INVEST_BAL = 53;
end;
else if NOT MISSING(SUM_INVEST_BAL) and 0 <= SUM_INVEST_BAL AND SUM_INVEST_BAL < 90 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 58;
SCR_SUM_INVEST_BAL = 58;
end;
else if NOT MISSING(SUM_INVEST_BAL) and 90 <= SUM_INVEST_BAL AND SUM_INVEST_BAL < 1661 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 68;
SCR_SUM_INVEST_BAL = 68;
end;
else if NOT MISSING(SUM_INVEST_BAL) and 1661 <= SUM_INVEST_BAL AND SUM_INVEST_BAL < 32027 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 89;
SCR_SUM_INVEST_BAL = 89;
end;
else if NOT MISSING(SUM_INVEST_BAL) and 32027 <= SUM_INVEST_BAL then do;
SCORECARD_POINTS = SCORECARD_POINTS + 109;
SCR_SUM_INVEST_BAL = 109;
end;

*------------------------------------------------------------*;
* Variable: Tot_New_Bal;
*------------------------------------------------------------*;
if MISSING(Tot_New_Bal) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 68;
SCR_Tot_New_Bal= 68;
end;
else if NOT MISSING(Tot_New_Bal) AND Tot_New_Bal < 61.29 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 121;
SCR_Tot_New_Bal = 121;
end;
else if NOT MISSING(Tot_New_Bal) and 61.29 <= Tot_New_Bal AND Tot_New_Bal < 429.91 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 91;
SCR_Tot_New_Bal = 91;
end;
else if NOT MISSING(Tot_New_Bal) and 429.91 <= Tot_New_Bal AND Tot_New_Bal < 1686.34 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 68;
SCR_Tot_New_Bal = 68;
end;
else if NOT MISSING(Tot_New_Bal) and 1686.34 <= Tot_New_Bal AND Tot_New_Bal < 5001.25 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 61;
SCR_Tot_New_Bal = 61;
end;
else if NOT MISSING(Tot_New_Bal) and 5001.25 <= Tot_New_Bal then do;
SCORECARD_POINTS = SCORECARD_POINTS + 50;
SCR_Tot_New_Bal = 50;
end;

*------------------------------------------------------------*;
* Variable: aa_AT36_MTH_MOST_R_DLQN;
*------------------------------------------------------------*;
if MISSING(aa_AT36_MTH_MOST_R_DLQN) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 69;
SCR_aa_AT36_MTH_MOST_R_DLQN= 69;
end;
else if NOT MISSING(aa_AT36_MTH_MOST_R_DLQN) AND aa_AT36_MTH_MOST_R_DLQN < 2 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 69;
SCR_aa_AT36_MTH_MOST_R_DLQN = 69;
end;
else if NOT MISSING(aa_AT36_MTH_MOST_R_DLQN) and 2 <= aa_AT36_MTH_MOST_R_DLQN AND aa_AT36_MTH_MOST_R_DLQN < 3 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 42;
SCR_aa_AT36_MTH_MOST_R_DLQN = 42;
end;
else if NOT MISSING(aa_AT36_MTH_MOST_R_DLQN) and 3 <= aa_AT36_MTH_MOST_R_DLQN AND aa_AT36_MTH_MOST_R_DLQN < 5 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 51;
SCR_aa_AT36_MTH_MOST_R_DLQN = 51;
end;
else if NOT MISSING(aa_AT36_MTH_MOST_R_DLQN) and 5 <= aa_AT36_MTH_MOST_R_DLQN AND aa_AT36_MTH_MOST_R_DLQN < 9 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 63;
SCR_aa_AT36_MTH_MOST_R_DLQN = 63;
end;
else if NOT MISSING(aa_AT36_MTH_MOST_R_DLQN) and 9 <= aa_AT36_MTH_MOST_R_DLQN then do;
SCORECARD_POINTS = SCORECARD_POINTS + 76;
SCR_aa_AT36_MTH_MOST_R_DLQN = 76;
end;

*------------------------------------------------------------*;
* Variable: aa_CUST_FOR_YEARS;
*------------------------------------------------------------*;
if MISSING(aa_CUST_FOR_YEARS) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 83;
SCR_aa_CUST_FOR_YEARS= 83;
end;
else if NOT MISSING(aa_CUST_FOR_YEARS) AND aa_CUST_FOR_YEARS < 2 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 53;
SCR_aa_CUST_FOR_YEARS = 53;
end;
else if NOT MISSING(aa_CUST_FOR_YEARS) and 2 <= aa_CUST_FOR_YEARS AND aa_CUST_FOR_YEARS < 6 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 58;
SCR_aa_CUST_FOR_YEARS = 58;
end;
else if NOT MISSING(aa_CUST_FOR_YEARS) and 6 <= aa_CUST_FOR_YEARS AND aa_CUST_FOR_YEARS < 9 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 64;
SCR_aa_CUST_FOR_YEARS = 64;
end;
else if NOT MISSING(aa_CUST_FOR_YEARS) and 9 <= aa_CUST_FOR_YEARS AND aa_CUST_FOR_YEARS < 18 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 72;
SCR_aa_CUST_FOR_YEARS = 72;
end;
else if NOT MISSING(aa_CUST_FOR_YEARS) and 18 <= aa_CUST_FOR_YEARS then do;
SCORECARD_POINTS = SCORECARD_POINTS + 83;
SCR_aa_CUST_FOR_YEARS = 83;
end;

*------------------------------------------------------------*;
* Variable: aa_RTO_CSH_CR_LMT;
*------------------------------------------------------------*;
if MISSING(aa_RTO_CSH_CR_LMT) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 72;
SCR_aa_RTO_CSH_CR_LMT= 72;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) AND aa_RTO_CSH_CR_LMT < 0 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 62;
SCR_aa_RTO_CSH_CR_LMT = 62;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) and 0 <= aa_RTO_CSH_CR_LMT AND aa_RTO_CSH_CR_LMT < 0.09 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 72;
SCR_aa_RTO_CSH_CR_LMT = 72;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) and 0.09 <= aa_RTO_CSH_CR_LMT AND aa_RTO_CSH_CR_LMT < 6.88 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 65;
SCR_aa_RTO_CSH_CR_LMT = 65;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) and 6.88 <= aa_RTO_CSH_CR_LMT AND aa_RTO_CSH_CR_LMT < 43.57 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 59;
SCR_aa_RTO_CSH_CR_LMT = 59;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) and 43.57 <= aa_RTO_CSH_CR_LMT then do;
SCORECARD_POINTS = SCORECARD_POINTS + 55;
SCR_aa_RTO_CSH_CR_LMT = 55;
end;



%mend;

%macro x_SCORE();
*------------------------------------------------------------*;
* generateScorepoints_note;
*------------------------------------------------------------*;
SCORECARD_POINTS = 0;

*------------------------------------------------------------*;
* Variable: B13_max_mindueasper_oflim;
*------------------------------------------------------------*;
if MISSING(B13_max_mindueasper_oflim) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 47;
SCR_B13_max_mindueasper_oflim= 47;
end;
else if NOT MISSING(B13_max_mindueasper_oflim) AND B13_max_mindueasper_oflim < 1 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 71;
SCR_B13_max_mindueasper_oflim = 71;
end;
else if NOT MISSING(B13_max_mindueasper_oflim) and 1 <= B13_max_mindueasper_oflim AND B13_max_mindueasper_oflim < 2 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 59;
SCR_B13_max_mindueasper_oflim = 59;
end;
else if NOT MISSING(B13_max_mindueasper_oflim) and 2 <= B13_max_mindueasper_oflim AND B13_max_mindueasper_oflim < 3 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 52;
SCR_B13_max_mindueasper_oflim = 52;
end;
else if NOT MISSING(B13_max_mindueasper_oflim) and 3 <= B13_max_mindueasper_oflim AND B13_max_mindueasper_oflim < 6 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 47;
SCR_B13_max_mindueasper_oflim = 47;
end;
else if NOT MISSING(B13_max_mindueasper_oflim) and 6 <= B13_max_mindueasper_oflim then do;
SCORECARD_POINTS = SCORECARD_POINTS + 44;
SCR_B13_max_mindueasper_oflim = 44;
end;

*------------------------------------------------------------*;
* Variable: B21_maxconsecmths_ge100util;
*------------------------------------------------------------*;
if MISSING(B21_maxconsecmths_ge100util) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 62;
SCR_B21_maxconsecmths_ge100util= 62;
end;
else if NOT MISSING(B21_maxconsecmths_ge100util) AND B21_maxconsecmths_ge100util < 1 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 62;
SCR_B21_maxconsecmths_ge100util = 62;
end;
else if NOT MISSING(B21_maxconsecmths_ge100util) and 1 <= B21_maxconsecmths_ge100util AND B21_maxconsecmths_ge100util < 2 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 45;
SCR_B21_maxconsecmths_ge100util = 45;
end;
else if NOT MISSING(B21_maxconsecmths_ge100util) and 2 <= B21_maxconsecmths_ge100util then do;
SCORECARD_POINTS = SCORECARD_POINTS + 35;
SCR_B21_maxconsecmths_ge100util = 35;
end;

*------------------------------------------------------------*;
* Variable: GO05_INQ_EX_COLL;
*------------------------------------------------------------*;
if MISSING(GO05_INQ_EX_COLL) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 52;
SCR_GO05_INQ_EX_COLL= 52;
end;
else if NOT MISSING(GO05_INQ_EX_COLL) AND GO05_INQ_EX_COLL < 1 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 50;
SCR_GO05_INQ_EX_COLL = 50;
end;
else if NOT MISSING(GO05_INQ_EX_COLL) and 1 <= GO05_INQ_EX_COLL AND GO05_INQ_EX_COLL < 4 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 55;
SCR_GO05_INQ_EX_COLL = 55;
end;
else if NOT MISSING(GO05_INQ_EX_COLL) and 4 <= GO05_INQ_EX_COLL AND GO05_INQ_EX_COLL < 7 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 52;
SCR_GO05_INQ_EX_COLL = 52;
end;
else if NOT MISSING(GO05_INQ_EX_COLL) and 7 <= GO05_INQ_EX_COLL AND GO05_INQ_EX_COLL < 11 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 43;
SCR_GO05_INQ_EX_COLL = 43;
end;
else if NOT MISSING(GO05_INQ_EX_COLL) and 11 <= GO05_INQ_EX_COLL then do;
SCORECARD_POINTS = SCORECARD_POINTS + 36;
SCR_GO05_INQ_EX_COLL = 36;
end;

*------------------------------------------------------------*;
* Variable: SUM_INVEST_BAL;
*------------------------------------------------------------*;
if MISSING(SUM_INVEST_BAL) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 46;
SCR_SUM_INVEST_BAL= 46;
end;
else if NOT MISSING(SUM_INVEST_BAL) AND SUM_INVEST_BAL < 0 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 40;
SCR_SUM_INVEST_BAL = 40;
end;
else if NOT MISSING(SUM_INVEST_BAL) and 0 <= SUM_INVEST_BAL AND SUM_INVEST_BAL < 192 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 46;
SCR_SUM_INVEST_BAL = 46;
end;
else if NOT MISSING(SUM_INVEST_BAL) and 192 <= SUM_INVEST_BAL AND SUM_INVEST_BAL < 1123 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 59;
SCR_SUM_INVEST_BAL = 59;
end;
else if NOT MISSING(SUM_INVEST_BAL) and 1123 <= SUM_INVEST_BAL AND SUM_INVEST_BAL < 4729 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 67;
SCR_SUM_INVEST_BAL = 67;
end;
else if NOT MISSING(SUM_INVEST_BAL) and 4729 <= SUM_INVEST_BAL then do;
SCORECARD_POINTS = SCORECARD_POINTS + 80;
SCR_SUM_INVEST_BAL = 80;
end;

*------------------------------------------------------------*;
* Variable: Tot_New_Bal;
*------------------------------------------------------------*;
if MISSING(Tot_New_Bal) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 54;
SCR_Tot_New_Bal= 54;
end;
else if NOT MISSING(Tot_New_Bal) AND Tot_New_Bal < 97.15 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 136;
SCR_Tot_New_Bal = 136;
end;
else if NOT MISSING(Tot_New_Bal) and 97.15 <= Tot_New_Bal AND Tot_New_Bal < 436.91 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 75;
SCR_Tot_New_Bal = 75;
end;
else if NOT MISSING(Tot_New_Bal) and 436.91 <= Tot_New_Bal AND Tot_New_Bal < 4869.9 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 54;
SCR_Tot_New_Bal = 54;
end;
else if NOT MISSING(Tot_New_Bal) and 4869.9 <= Tot_New_Bal AND Tot_New_Bal < 8763.6 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 40;
SCR_Tot_New_Bal = 40;
end;
else if NOT MISSING(Tot_New_Bal) and 8763.6 <= Tot_New_Bal then do;
SCORECARD_POINTS = SCORECARD_POINTS + 34;
SCR_Tot_New_Bal = 34;
end;

*------------------------------------------------------------*;
* Variable: aa_AT34_TOT_UTILZTN;
*------------------------------------------------------------*;
if MISSING(aa_AT34_TOT_UTILZTN) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 48;
SCR_aa_AT34_TOT_UTILZTN= 48;
end;
else if NOT MISSING(aa_AT34_TOT_UTILZTN) AND aa_AT34_TOT_UTILZTN < 2 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 51;
SCR_aa_AT34_TOT_UTILZTN = 51;
end;
else if NOT MISSING(aa_AT34_TOT_UTILZTN) and 2 <= aa_AT34_TOT_UTILZTN AND aa_AT34_TOT_UTILZTN < 46 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 60;
SCR_aa_AT34_TOT_UTILZTN = 60;
end;
else if NOT MISSING(aa_AT34_TOT_UTILZTN) and 46 <= aa_AT34_TOT_UTILZTN AND aa_AT34_TOT_UTILZTN < 65 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 54;
SCR_aa_AT34_TOT_UTILZTN = 54;
end;
else if NOT MISSING(aa_AT34_TOT_UTILZTN) and 65 <= aa_AT34_TOT_UTILZTN AND aa_AT34_TOT_UTILZTN < 96 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 48;
SCR_aa_AT34_TOT_UTILZTN = 48;
end;
else if NOT MISSING(aa_AT34_TOT_UTILZTN) and 96 <= aa_AT34_TOT_UTILZTN then do;
SCORECARD_POINTS = SCORECARD_POINTS + 43;
SCR_aa_AT34_TOT_UTILZTN = 43;
end;

*------------------------------------------------------------*;
* Variable: aa_AT36_MTH_MOST_R_DLQN;
*------------------------------------------------------------*;
if MISSING(aa_AT36_MTH_MOST_R_DLQN) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 58;
SCR_aa_AT36_MTH_MOST_R_DLQN= 58;
end;
else if NOT MISSING(aa_AT36_MTH_MOST_R_DLQN) AND aa_AT36_MTH_MOST_R_DLQN < 2 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 50;
SCR_aa_AT36_MTH_MOST_R_DLQN = 50;
end;
else if NOT MISSING(aa_AT36_MTH_MOST_R_DLQN) and 2 <= aa_AT36_MTH_MOST_R_DLQN AND aa_AT36_MTH_MOST_R_DLQN < 3 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 39;
SCR_aa_AT36_MTH_MOST_R_DLQN = 39;
end;

else if NOT MISSING(aa_AT36_MTH_MOST_R_DLQN) and 3 <= aa_AT36_MTH_MOST_R_DLQN AND aa_AT36_MTH_MOST_R_DLQN < 4 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 45;
SCR_aa_AT36_MTH_MOST_R_DLQN = 45;
end;
else if NOT MISSING(aa_AT36_MTH_MOST_R_DLQN) and 4 <= aa_AT36_MTH_MOST_R_DLQN AND aa_AT36_MTH_MOST_R_DLQN < 7 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 51;
SCR_aa_AT36_MTH_MOST_R_DLQN = 51;
end;
else if NOT MISSING(aa_AT36_MTH_MOST_R_DLQN) and 7 <= aa_AT36_MTH_MOST_R_DLQN then do;
SCORECARD_POINTS = SCORECARD_POINTS + 58;
SCR_aa_AT36_MTH_MOST_R_DLQN = 58;
end;

*------------------------------------------------------------*;
* Variable: aa_CUST_FOR_YEARS;
*------------------------------------------------------------*;
if MISSING(aa_CUST_FOR_YEARS) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 54;
SCR_aa_CUST_FOR_YEARS= 54;
end;
else if NOT MISSING(aa_CUST_FOR_YEARS) AND aa_CUST_FOR_YEARS < 2 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 44;
SCR_aa_CUST_FOR_YEARS = 44;
end;
else if NOT MISSING(aa_CUST_FOR_YEARS) and 2 <= aa_CUST_FOR_YEARS AND aa_CUST_FOR_YEARS < 4 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 46;
SCR_aa_CUST_FOR_YEARS = 46;
end;
else if NOT MISSING(aa_CUST_FOR_YEARS) and 4 <= aa_CUST_FOR_YEARS AND aa_CUST_FOR_YEARS < 8 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 50;
SCR_aa_CUST_FOR_YEARS = 50;
end;
else if NOT MISSING(aa_CUST_FOR_YEARS) and 8 <= aa_CUST_FOR_YEARS AND aa_CUST_FOR_YEARS < 20 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 54;
SCR_aa_CUST_FOR_YEARS = 54;
end;
else if NOT MISSING(aa_CUST_FOR_YEARS) and 20 <= aa_CUST_FOR_YEARS then do;
SCORECARD_POINTS = SCORECARD_POINTS + 60;
SCR_aa_CUST_FOR_YEARS = 60;
end;

*------------------------------------------------------------*;
* Variable: aa_RTO_CSH_CR_LMT;
*------------------------------------------------------------*;
if MISSING(aa_RTO_CSH_CR_LMT) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 56;
SCR_aa_RTO_CSH_CR_LMT= 56;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) AND aa_RTO_CSH_CR_LMT < 0 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 46;
SCR_aa_RTO_CSH_CR_LMT = 46;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) and 0 <= aa_RTO_CSH_CR_LMT AND aa_RTO_CSH_CR_LMT < 0.47 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 56;
SCR_aa_RTO_CSH_CR_LMT = 56;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) and 0.47 <= aa_RTO_CSH_CR_LMT AND aa_RTO_CSH_CR_LMT < 14.29 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 50;
SCR_aa_RTO_CSH_CR_LMT = 50;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) and 14.29 <= aa_RTO_CSH_CR_LMT AND aa_RTO_CSH_CR_LMT < 60.72 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 47;
SCR_aa_RTO_CSH_CR_LMT = 47;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) and 60.72 <= aa_RTO_CSH_CR_LMT then do;
SCORECARD_POINTS = SCORECARD_POINTS + 44;
SCR_aa_RTO_CSH_CR_LMT = 44;
end;

*------------------------------------------------------------*;
* Variable: aa_WORST_DLQNT_ALL;
*------------------------------------------------------------*;
if MISSING(aa_WORST_DLQNT_ALL) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 58;
SCR_aa_WORST_DLQNT_ALL= 58;
end;
else if NOT MISSING(aa_WORST_DLQNT_ALL) AND aa_WORST_DLQNT_ALL < 30 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 42;
SCR_aa_WORST_DLQNT_ALL = 42;
end;
else if NOT MISSING(aa_WORST_DLQNT_ALL) and 30 <= aa_WORST_DLQNT_ALL AND aa_WORST_DLQNT_ALL < 41 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 58;
SCR_aa_WORST_DLQNT_ALL = 58;
end;
else if NOT MISSING(aa_WORST_DLQNT_ALL) and 41 <= aa_WORST_DLQNT_ALL AND aa_WORST_DLQNT_ALL < 51 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 50;
SCR_aa_WORST_DLQNT_ALL = 50;
end;
else if NOT MISSING(aa_WORST_DLQNT_ALL) and 51 <= aa_WORST_DLQNT_ALL AND aa_WORST_DLQNT_ALL < 57 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 46;
SCR_aa_WORST_DLQNT_ALL = 46;
end;
else if NOT MISSING(aa_WORST_DLQNT_ALL) and 57 <= aa_WORST_DLQNT_ALL then do;
SCORECARD_POINTS = SCORECARD_POINTS + 38;
SCR_aa_WORST_DLQNT_ALL = 38;
end;

*------------------------------------------------------------*;
* Variable: GO03_CR_BUREAU_WORST_RT;
*------------------------------------------------------------*;
length _fmtGO03_CR_BUREAU_WORST_RT $200;
length _normGO03_CR_BUREAU_WORST_RT $32;
drop _fmtGO03_CR_BUREAU_WORST_RT _normGO03_CR_BUREAU_WORST_RT;
_fmtGO03_CR_BUREAU_WORST_RT=put(GO03_CR_BUREAU_WORST_RT, $1.0);
%dmnormcp(_fmtGO03_CR_BUREAU_WORST_RT, _normGO03_CR_BUREAU_WORST_RT);
if MISSING(_normGO03_CR_BUREAU_WORST_RT) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 47;
SCR_GO03_CR_BUREAU_WORST_RT = 47;
end;
else
if _normGO03_CR_BUREAU_WORST_RT in ('0'
, '3'
, '4'
, '5'
, '7'
, '8'
, '9'
) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 45;
SCR_GO03_CR_BUREAU_WORST_RT = 45;
end;
else
if _normGO03_CR_BUREAU_WORST_RT in ('2'
) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 47;
SCR_GO03_CR_BUREAU_WORST_RT = 47;
end;
else
if _normGO03_CR_BUREAU_WORST_RT in ('1'
) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 52;
SCR_GO03_CR_BUREAU_WORST_RT = 52;
end;
else do;
SCORECARD_POINTS = SCORECARD_POINTS + 47;
SCR_GO03_CR_BUREAU_WORST_RT = 47;
end;
drop _normGO03_CR_BUREAU_WORST_RT;

*------------------------------------------------------------*;
* Variable: STEP_F;
*------------------------------------------------------------*;
length _fmtSTEP_F $200;
length _normSTEP_F $32;
drop _fmtSTEP_F _normSTEP_F;
_fmtSTEP_F=put(STEP_F, $8.0);
%dmnormcp(_fmtSTEP_F, _normSTEP_F);
if MISSING(_normSTEP_F) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 45;
SCR_STEP_F = 45;
end;
else
if _normSTEP_F in ('N'
) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 50;
SCR_STEP_F = 50;
end;
else
if _normSTEP_F in ('Y'
) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 69;
SCR_STEP_F = 69;
end;
else do;
SCORECARD_POINTS = SCORECARD_POINTS + 45;
SCR_STEP_F = 45;
end;
drop _normSTEP_F;

%mend;
%macro m30_SCORE();
*------------------------------------------------------------*;
* generateScorepoints_note;
*------------------------------------------------------------*;
SCORECARD_POINTS = 0;

*------------------------------------------------------------*;
* Variable: B141_pymnts_asperof_mindue;
*------------------------------------------------------------*;
if MISSING(B141_pymnts_asperof_mindue) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 46;
SCR_B141_pymnts_asperof_mindue= 46;
end;
else if NOT MISSING(B141_pymnts_asperof_mindue) AND B141_pymnts_asperof_mindue < 1.36 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 44;
SCR_B141_pymnts_asperof_mindue = 44;
end;
else if NOT MISSING(B141_pymnts_asperof_mindue) and 1.36 <= B141_pymnts_asperof_mindue AND B141_pymnts_asperof_mindue < 6.48 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 46;
SCR_B141_pymnts_asperof_mindue = 46;
end;
else if NOT MISSING(B141_pymnts_asperof_mindue) and 6.48 <= B141_pymnts_asperof_mindue AND B141_pymnts_asperof_mindue < 15.65 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 49;
SCR_B141_pymnts_asperof_mindue = 49;
end;
else if NOT MISSING(B141_pymnts_asperof_mindue) and 15.65 <= B141_pymnts_asperof_mindue then do;
SCORECARD_POINTS = SCORECARD_POINTS + 52;
SCR_B141_pymnts_asperof_mindue = 52;
end;

*------------------------------------------------------------*;
* Variable: B21_maxconsecmths_ge100util;
*------------------------------------------------------------*;
if MISSING(B21_maxconsecmths_ge100util) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 55;
SCR_B21_maxconsecmths_ge100util= 55;
end;
else if NOT MISSING(B21_maxconsecmths_ge100util) AND B21_maxconsecmths_ge100util < 1 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 55;
SCR_B21_maxconsecmths_ge100util = 55;
end;
else if NOT MISSING(B21_maxconsecmths_ge100util) and 1 <= B21_maxconsecmths_ge100util AND B21_maxconsecmths_ge100util < 2 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 46;
SCR_B21_maxconsecmths_ge100util = 46;
end;
else if NOT MISSING(B21_maxconsecmths_ge100util) and 2 <= B21_maxconsecmths_ge100util then do;
SCORECARD_POINTS = SCORECARD_POINTS + 39;
SCR_B21_maxconsecmths_ge100util = 39;
end;

*------------------------------------------------------------*;
* Variable: GO05_INQ_EX_COLL;
*------------------------------------------------------------*;
if MISSING(GO05_INQ_EX_COLL) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 48;
SCR_GO05_INQ_EX_COLL= 48;
end;
else if NOT MISSING(GO05_INQ_EX_COLL) AND GO05_INQ_EX_COLL < 1 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 44;
SCR_GO05_INQ_EX_COLL = 44;
end;
else if NOT MISSING(GO05_INQ_EX_COLL) and 1 <= GO05_INQ_EX_COLL AND GO05_INQ_EX_COLL < 3 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 52;
SCR_GO05_INQ_EX_COLL = 52;
end;
else if NOT MISSING(GO05_INQ_EX_COLL) and 3 <= GO05_INQ_EX_COLL AND GO05_INQ_EX_COLL < 7 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 48;
SCR_GO05_INQ_EX_COLL = 48;
end;
else if NOT MISSING(GO05_INQ_EX_COLL) and 7 <= GO05_INQ_EX_COLL AND GO05_INQ_EX_COLL < 12 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 40;
SCR_GO05_INQ_EX_COLL = 40;
end;
else if NOT MISSING(GO05_INQ_EX_COLL) and 12 <= GO05_INQ_EX_COLL then do;
SCORECARD_POINTS = SCORECARD_POINTS + 35;
SCR_GO05_INQ_EX_COLL = 35;
end;

*------------------------------------------------------------*;
* Variable: MTH_SINCE_MOST_RECNT_DLQNT;
*------------------------------------------------------------*;
if MISSING(MTH_SINCE_MOST_RECNT_DLQNT) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 55;
SCR_MTH_SINCE_MOST_RECNT_DLQNT= 55;
end;
else if NOT MISSING(MTH_SINCE_MOST_RECNT_DLQNT) AND MTH_SINCE_MOST_RECNT_DLQNT < 2 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 44;
SCR_MTH_SINCE_MOST_RECNT_DLQNT = 44;
end;
else if NOT MISSING(MTH_SINCE_MOST_RECNT_DLQNT) and 2 <= MTH_SINCE_MOST_RECNT_DLQNT AND MTH_SINCE_MOST_RECNT_DLQNT < 3 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 36;
SCR_MTH_SINCE_MOST_RECNT_DLQNT = 36;
end;
else if NOT MISSING(MTH_SINCE_MOST_RECNT_DLQNT) and 3 <= MTH_SINCE_MOST_RECNT_DLQNT AND MTH_SINCE_MOST_RECNT_DLQNT < 5 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 48;
SCR_MTH_SINCE_MOST_RECNT_DLQNT = 48;
end;
else if NOT MISSING(MTH_SINCE_MOST_RECNT_DLQNT) and 5 <= MTH_SINCE_MOST_RECNT_DLQNT AND MTH_SINCE_MOST_RECNT_DLQNT < 999 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 55;
SCR_MTH_SINCE_MOST_RECNT_DLQNT = 55;
end;
else if NOT MISSING(MTH_SINCE_MOST_RECNT_DLQNT) and 999 <= MTH_SINCE_MOST_RECNT_DLQNT then do;
SCORECARD_POINTS = SCORECARD_POINTS + 44;
SCR_MTH_SINCE_MOST_RECNT_DLQNT = 44;
end;

*------------------------------------------------------------*;
* Variable: NUM_OF_CRNT_DLQNT_ACCT;
*------------------------------------------------------------*;
if MISSING(NUM_OF_CRNT_DLQNT_ACCT) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 51;
SCR_NUM_OF_CRNT_DLQNT_ACCT= 51;
end;
else if NOT MISSING(NUM_OF_CRNT_DLQNT_ACCT) AND NUM_OF_CRNT_DLQNT_ACCT < 1 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 44;
SCR_NUM_OF_CRNT_DLQNT_ACCT = 44;
end;
else if NOT MISSING(NUM_OF_CRNT_DLQNT_ACCT) and 1 <= NUM_OF_CRNT_DLQNT_ACCT AND NUM_OF_CRNT_DLQNT_ACCT < 2 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 51;
SCR_NUM_OF_CRNT_DLQNT_ACCT = 51;
end;
else if NOT MISSING(NUM_OF_CRNT_DLQNT_ACCT) and 2 <= NUM_OF_CRNT_DLQNT_ACCT then do;
SCORECARD_POINTS = SCORECARD_POINTS + 31;
SCR_NUM_OF_CRNT_DLQNT_ACCT = 31;
end;

*------------------------------------------------------------*;
* Variable: SUM_DEP_BAL;
*------------------------------------------------------------*;
if MISSING(SUM_DEP_BAL) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 39;
SCR_SUM_DEP_BAL= 39;
end;
else if NOT MISSING(SUM_DEP_BAL) AND SUM_DEP_BAL < -1103 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 45;
SCR_SUM_DEP_BAL = 45;
end;
else if NOT MISSING(SUM_DEP_BAL) and -1103 <= SUM_DEP_BAL AND SUM_DEP_BAL < 9 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 39;
SCR_SUM_DEP_BAL = 39;
end;
else if NOT MISSING(SUM_DEP_BAL) and 9 <= SUM_DEP_BAL AND SUM_DEP_BAL < 254 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 51;
SCR_SUM_DEP_BAL = 51;
end;
else if NOT MISSING(SUM_DEP_BAL) and 254 <= SUM_DEP_BAL AND SUM_DEP_BAL < 1455 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 68;
SCR_SUM_DEP_BAL = 68;
end;
else if NOT MISSING(SUM_DEP_BAL) and 1455 <= SUM_DEP_BAL then do;
SCORECARD_POINTS = SCORECARD_POINTS + 101;
SCR_SUM_DEP_BAL = 101;
end;

*------------------------------------------------------------*;
* Variable: TOT_REL;
*------------------------------------------------------------*;
if MISSING(TOT_REL) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 46;
SCR_TOT_REL= 46;
end;
else if NOT MISSING(TOT_REL) AND TOT_REL < 0 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 46;
SCR_TOT_REL = 46;
end;
else if NOT MISSING(TOT_REL) and 0 <= TOT_REL AND TOT_REL < 3 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 42;
SCR_TOT_REL = 42;
end;
else if NOT MISSING(TOT_REL) and 3 <= TOT_REL AND TOT_REL < 5 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 47;
SCR_TOT_REL = 47;
end;

else if NOT MISSING(TOT_REL) and 5 <= TOT_REL AND TOT_REL < 7 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 55;
SCR_TOT_REL = 55;
end;
else if NOT MISSING(TOT_REL) and 7 <= TOT_REL then do;
SCORECARD_POINTS = SCORECARD_POINTS + 68;
SCR_TOT_REL = 68;
end;

*------------------------------------------------------------*;
* Variable: Tot_New_Bal;
*------------------------------------------------------------*;
if MISSING(Tot_New_Bal) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 47;
SCR_Tot_New_Bal= 47;
end;
else if NOT MISSING(Tot_New_Bal) AND Tot_New_Bal < 73.09 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 140;
SCR_Tot_New_Bal = 140;
end;
else if NOT MISSING(Tot_New_Bal) and 73.09 <= Tot_New_Bal AND Tot_New_Bal < 430.69 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 69;
SCR_Tot_New_Bal = 69;
end;
else if NOT MISSING(Tot_New_Bal) and 430.69 <= Tot_New_Bal AND Tot_New_Bal < 1018.24 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 53;
SCR_Tot_New_Bal = 53;
end;
else if NOT MISSING(Tot_New_Bal) and 1018.24 <= Tot_New_Bal AND Tot_New_Bal < 5191.43 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 47;
SCR_Tot_New_Bal = 47;
end;
else if NOT MISSING(Tot_New_Bal) and 5191.43 <= Tot_New_Bal then do;
SCORECARD_POINTS = SCORECARD_POINTS + 36;
SCR_Tot_New_Bal = 36;
end;

*------------------------------------------------------------*;
* Variable: aa_AT34_TOT_UTILZTN;
*------------------------------------------------------------*;
if MISSING(aa_AT34_TOT_UTILZTN) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 44;
SCR_aa_AT34_TOT_UTILZTN= 44;
end;
else if NOT MISSING(aa_AT34_TOT_UTILZTN) AND aa_AT34_TOT_UTILZTN < 5 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 48;
SCR_aa_AT34_TOT_UTILZTN = 48;
end;
else if NOT MISSING(aa_AT34_TOT_UTILZTN) and 5 <= aa_AT34_TOT_UTILZTN AND aa_AT34_TOT_UTILZTN < 34 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 60;
SCR_aa_AT34_TOT_UTILZTN = 60;
end;
else if NOT MISSING(aa_AT34_TOT_UTILZTN) and 34 <= aa_AT34_TOT_UTILZTN AND aa_AT34_TOT_UTILZTN < 73 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 50;
SCR_aa_AT34_TOT_UTILZTN = 50;
end;
else if NOT MISSING(aa_AT34_TOT_UTILZTN) and 73 <= aa_AT34_TOT_UTILZTN AND aa_AT34_TOT_UTILZTN < 104 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 44;
SCR_aa_AT34_TOT_UTILZTN = 44;
end;
else if NOT MISSING(aa_AT34_TOT_UTILZTN) and 104 <= aa_AT34_TOT_UTILZTN then do;
SCORECARD_POINTS = SCORECARD_POINTS + 40;
SCR_aa_AT34_TOT_UTILZTN = 40;
end;

*------------------------------------------------------------*;
* Variable: aa_RTO_CSH_CR_LMT;
*------------------------------------------------------------*;
if MISSING(aa_RTO_CSH_CR_LMT) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 54;
SCR_aa_RTO_CSH_CR_LMT= 54;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) AND aa_RTO_CSH_CR_LMT < 0 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 46;
SCR_aa_RTO_CSH_CR_LMT = 46;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) and 0 <= aa_RTO_CSH_CR_LMT AND aa_RTO_CSH_CR_LMT < 2.33 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 54;
SCR_aa_RTO_CSH_CR_LMT = 54;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) and 2.33 <= aa_RTO_CSH_CR_LMT AND aa_RTO_CSH_CR_LMT < 13.83 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 47;
SCR_aa_RTO_CSH_CR_LMT = 47;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) and 13.83 <= aa_RTO_CSH_CR_LMT AND aa_RTO_CSH_CR_LMT < 91.34 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 41;
SCR_aa_RTO_CSH_CR_LMT = 41;
end;
else if NOT MISSING(aa_RTO_CSH_CR_LMT) and 91.34 <= aa_RTO_CSH_CR_LMT then do;
SCORECARD_POINTS = SCORECARD_POINTS + 33;
SCR_aa_RTO_CSH_CR_LMT = 33;
end;

*------------------------------------------------------------*;
* Variable: GO03_CR_BUREAU_WORST_RT;
*------------------------------------------------------------*;
length _fmtGO03_CR_BUREAU_WORST_RT $200;
length _normGO03_CR_BUREAU_WORST_RT $32;
drop _fmtGO03_CR_BUREAU_WORST_RT _normGO03_CR_BUREAU_WORST_RT;
_fmtGO03_CR_BUREAU_WORST_RT=put(GO03_CR_BUREAU_WORST_RT, $1.0);
%dmnormcp(_fmtGO03_CR_BUREAU_WORST_RT, _normGO03_CR_BUREAU_WORST_RT);
if MISSING(_normGO03_CR_BUREAU_WORST_RT) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 42;
SCR_GO03_CR_BUREAU_WORST_RT = 42;
end;
else
if _normGO03_CR_BUREAU_WORST_RT in ('0'
, '3'
, '4'
, '5'
, '7'
, '8'
, '9'
) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 42;
SCR_GO03_CR_BUREAU_WORST_RT = 42;
end;
else
if _normGO03_CR_BUREAU_WORST_RT in ('2'
) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 47;
SCR_GO03_CR_BUREAU_WORST_RT = 47;
end;
else
if _normGO03_CR_BUREAU_WORST_RT in ('1'
) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 50;
SCR_GO03_CR_BUREAU_WORST_RT = 50;
end;
else do;
SCORECARD_POINTS = SCORECARD_POINTS + 42;
SCR_GO03_CR_BUREAU_WORST_RT = 42;
end;
drop _normGO03_CR_BUREAU_WORST_RT;

*------------------------------------------------------------*;
* Variable: STEP_F;
*------------------------------------------------------------*;
length _fmtSTEP_F $200;
length _normSTEP_F $32;
drop _fmtSTEP_F _normSTEP_F;
_fmtSTEP_F=put(STEP_F, $8.0);
%dmnormcp(_fmtSTEP_F, _normSTEP_F);
if MISSING(_normSTEP_F) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 45;
SCR_STEP_F = 45;
end;
else
if _normSTEP_F in ('Y'
) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 87;
SCR_STEP_F = 87;
end;
else
if _normSTEP_F in ('N'
) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 45;
SCR_STEP_F = 45;
end;
else do;
SCORECARD_POINTS = SCORECARD_POINTS + 45;
SCR_STEP_F = 45;
end;
drop _normSTEP_F;

%mend;

/* since the below code will be reused in many queries, therefore storing it in a marco*/

%macro Risk_Acct_Qry(score);

%IF &score = zero %THEN %DO;
%let value= %bquote(score=0);
%END;
%ELSE %DO;
%let value=  %bquote(score <> 0);
%END;


   				select * from (
		    		select 
					RISK_ACCT_ID
					,corptn_num
					,EFF_TM_ID
					,BNS_DLQNT_DAY
					,CR_LMT_AMT 
					,TOT_NEW_BAL_AMT as Tot_New_Bal
					,ACCT_STAT_CD
					,BLOCK_RECL_CD
					,CHRG_OFF_CD
					,CORP_RTL_F
					,PRD_CD
					,SUB_PRD_CD
					,TRNST_NUM
					,ACCT_NUM
					,case 
				    when  SUB_PRD_CD  in ('ST')  or  SUB_PRD_CD  in ('ST')  then 1 
					when  CORP_RTL_F in ('C') then 2 
					when  ACCT_STAT_CD        not in ('1', '5','6') then 3
					when  TRNST_NUM  IN (18192, 99432) then  4
					when   BLOCK_RECL_CD        IN ('B4')  then 5 
					/* B4: deceased */
					when  BLOCK_RECL_CD   IN ( 'B5', 'D',  'SF', 'XS','XV', 'SS','S', '2')  then 6  
				    /*
				    B5	Collection- Bankrupt
				    D 	Written Out of Records
				    FX	Fixed Payment
				    SF	Fraud
				    XS	Temporarily Suspend Charge Privileges
				    SS	Lost/Stolen
				    */
				    when  (  SUBSTR(BLOCK_RECL_CD,1,1) IN ('V','P','S') AND TOT_NEW_BAL_AMT<=0 ) then 7  
				   /*CLOSED, FROZEN WITH BAL<=0*/
				    when (BLOCK_RECL_CD )  IN ('S')   	then 8  
					/*no need for COMPRESS(BLOCK_RECL_CD ) as no blanks found */
				   /* S	Temporarily Lost*/
					when  CHRG_OFF_CD IN ('1','2','C','N', 'P','Q')  then 9 
					/*
				    Charge-off Codes	
				    0	Current
				    1	Charged off
				    N	non-accural
				    2	Current, pending charge-off
				    P	Current, pending non-accrual
				    C	non-accural pending current
				    Q	non-accrual pending bad debt
				    */
					when  CR_LMT_AMT  <=0 then 10
					when  PRD_CD  ='VIC' then 11
					else 0 end as score
			from    EDRTLR.RISK_REVLVNG_CR_DLY_SNAPSHOT b 
			where   
            EFF_TM_ID =  &Last_Avl_dt_mth. 

			and  substr(PRD_CD, 1, 1) in ('A', 'V') 
		    and  BNS_DLQNT_DAY > 0  /* only keep 22-89 for cc scoring */
		    and  BNS_DLQNT_DAY <=119 /*Derrick EDIT to 119 original 89*/
			) Final 
			where   &value.
%mend;

%macro get_val(p_val=);
	 max (Case when MTH_TM_ID=&tm.            then &p_val. END) as &p_val.1,
	 max (Case when MTH_TM_ID=%eval(&tm.-40)  then &p_val. END) as &p_val.2,
	 max (Case when MTH_TM_ID=%eval(&tm.-80)  then &p_val. END) as &p_val.3,
	 max (Case when MTH_TM_ID=%eval(&tm.-120)  then &p_val. END) as &p_val.4,
	 max (Case when MTH_TM_ID=%eval(&tm.-160)  then &p_val. END) as &p_val.5,
	 max (Case when MTH_TM_ID=%eval(&tm.-200)  then &p_val. END) as &p_val.6
%mend;

proc sql;
   connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
   create table work.RV_MONTHLY_&pme_my. as
   select *
   from connection to DB2( 

 select  
     a.RISK_ACCT_ID,
	 %get_val(p_val=TOT_MIN_PYMT_AMT),
	 %get_val(p_val=TOT_PD_AMT),


	 %get_val(p_val=CSH_ADVNC_CRNT_CYCL_BAL_AMT),
	 %get_val(p_val=PRCH_CRNT_CYCL_BAL_AMT),

     %get_val(p_val=TOT_NEW_BAL_AMT),
     %get_val(p_val=CR_LMT_AMT),
     %get_val(p_val=LAST_PYMT_AMT)

		from (
		select RISK_ACCT_ID from (
				%Risk_Acct_Qry(zero) )
			) a
	left outer join 
	EDRTLR.RISK_REVLVNG_CR_MTH_SNAPSHOT     as b
	on a.RISK_ACCT_ID = b.RISK_ACCT_ID
	and MTH_TM_ID in (
     &tm.,           
     %eval(&tm.-40),
     %eval(&tm.-80), 
     %eval(&tm.-120),
     %eval(&tm.-160),
     %eval(&tm.-200),
     %eval(&tm.-240)
    )
group by a.RISK_ACCT_ID

);
disconnect from db2;
quit;

%macro checkNo();
%let DSNId = %sysfunc(open(work.RV_MONTHLY_&pme_my.));
%let DSObs = %sysfunc(attrn(&DSNId,nobs));
%let rc = %sysfunc(close(&DSNId.));
%let NumObs = &DSObs.;

%if  &DSObs. = 0 %then %do; 
%put work.RV_MONTHLY_&pme_my. have 0 records.;
%abort cancel;
%end;

/*check if EDRTLR.RISK_REVLVNG_CR_MTH_SNAPSHOT has data for 
&tm., %eval(&tm.-40), %eval(&tm.-80), %eval(&tm.-120), %eval(&tm.-160),%eval(&tm.-200),%eval(&tm.-240)
*/

%put &tm.;
%do z=0 %to 6; 
proc sql;
connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
create table work.tm_&z. as select * from connection to DB2( 
select count(*) as count from EDRTLR.RISK_REVLVNG_CR_MTH_SNAPSHOT where MTH_TM_ID in (%eval(&tm-&z*40))
); disconnect from db2;
quit;
data _null_; set work.tm_&z.; call symput('cnt',count); run;
%put &z &cnt;
%if &cnt=0 %then %do;
%put EDRTLR.RISK_REVLVNG_CR_MTH_SNAPSHOT does not have &z month data;
%abort cancel;
%end;
%end;

/*check if EDRTLR.RISK_REVLVNG_CR_DLY_SNAPSHOT has data for &Last_Avl_dt_mth. */
proc sql;
connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
create table work.tm_d as select * from connection to DB2( 
select count(*) as count from EDRTLR.RISK_REVLVNG_CR_DLY_SNAPSHOT where EFF_TM_ID =  &Last_Avl_dt_mth.;
); disconnect from db2;
quit;
data _null_; set work.tm_d; call symput('cnt_d',count); run;
%put &cnt_d;
%if &cnt_d=0 %then %do;
%put EDRTLR.RISK_REVLVNG_CR_DLY_SNAPSHOT does not have &Last_Avl_dt_mth. data;
%abort cancel;
%end;

/*check if EDRTLR.RISK_CUST_ACCT_RLTNP_SNAPSHOT for &tm */
proc sql;
connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
create table work.tm_r as select * from connection to DB2( 
select count(*) as count from EDRTLR.RISK_CUST_ACCT_RLTNP_SNAPSHOT where MTH_TM_ID= &TM.;
); disconnect from db2;
quit;
data _null_; set work.tm_r; call symput('cnt_r',count); run;
%put &cnt_r;
%if &cnt_r=0 %then %do;
%put EDRTLR.RISK_CUST_ACCT_RLTNP_SNAPSHOT does not have &TM. data;
%abort cancel;
%end;

/*check if RRAP_R.BASEL_ANALYTCL_BL_INSTRMNT_FACT for &tm */
proc sql;
connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
create table work.tm_i as 
select count(*) as count from RRAP_R.BASEL_ANALYTCL_BL_INSTRMNT_FACT where MTH_TM_ID= &TM.;
disconnect from db2;
quit;
data _null_; set work.tm_i; call symput('cnt_i',count); run;
%put &cnt_i;
%if &cnt_i=0 %then %do;
%put RRAP_R.BASEL_ANALYTCL_BL_INSTRMNT_FACT does not have &TM. data;
%abort cancel;
%end;

%mend;
%checkNo();


data work.RV_MONTHLY_&pme_my.;
set  work.RV_MONTHLY_&pme_my. ;
rename   
TOT_NEW_BAL_AMT1=TOT_NEW_BAL1
TOT_NEW_BAL_AMT2=TOT_NEW_BAL2
TOT_NEW_BAL_AMT3=TOT_NEW_BAL3
TOT_NEW_BAL_AMT4=TOT_NEW_BAL4
TOT_NEW_BAL_AMT5=TOT_NEW_BAL5
TOT_NEW_BAL_AMT6=TOT_NEW_BAL6
;
run;

data work.sRV_MONTHLY_&pme_my._2;
set  work.RV_MONTHLY_&pme_my.;

if TOT_NEW_BAL1 > CR_LMT_AMT1 then totamtdue1=TOT_PD_AMT1-CR_LMT_AMT1+TOT_NEW_BAL1;
else totamtdue1=TOT_PD_AMT1;
if TOT_NEW_BAL2 > CR_LMT_AMT2 then totamtdue2=TOT_PD_AMT2-CR_LMT_AMT2+TOT_NEW_BAL2;
else totamtdue2=TOT_PD_AMT2;
if TOT_NEW_BAL3 > CR_LMT_AMT3 then totamtdue3=TOT_PD_AMT3-CR_LMT_AMT3+TOT_NEW_BAL3;
else totamtdue3=TOT_PD_AMT3;
if TOT_NEW_BAL4 > CR_LMT_AMT4 then totamtdue4=TOT_PD_AMT4-CR_LMT_AMT4+TOT_NEW_BAL4;
else totamtdue4=TOT_PD_AMT4;
if TOT_NEW_BAL5 > CR_LMT_AMT5 then totamtdue5=TOT_PD_AMT5-CR_LMT_AMT5+TOT_NEW_BAL5;
else totamtdue5=TOT_PD_AMT5;
if TOT_NEW_BAL6 > CR_LMT_AMT6 then totamtdue6=TOT_PD_AMT6-CR_LMT_AMT6+TOT_NEW_BAL6;
else totamtdue6=TOT_PD_AMT6;

array BAL   {6} TOT_NEW_BAL1 - TOT_NEW_BAL6;
array CRLIM {6} CR_LMT_AMT1 - CR_LMT_AMT6;

B4_lastocc_ge100util=7;
do i = 6 to 1 by -1;
        if BAL{i} > CRLIM{i} then B4_lastocc_ge100util = 7-i;
end;


array TOTPURCSH_HIS {6} totpurcsh_his_01 - totpurcsh_his_06;
array PUR_HIS       {6} PRCH_CRNT_CYCL_BAL_AMT1 - PRCH_CRNT_CYCL_BAL_AMT6;
array CSHADV_HIS    {6} CSH_ADVNC_CRNT_CYCL_BAL_AMT1 - CSH_ADVNC_CRNT_CYCL_BAL_AMT6;

do i = 1 to 6;
        TOTPURCSH_HIS {i} = PUR_HIS {i} + CSHADV_HIS {i};
end;

flag_char11_divbyzero = (sum (of TOTPURCSH_HIS {*}) = 0);
if flag_char11_divbyzero = 0 then
B11_cashadv_asperof_totalpur =
int(sum (of CSHADV_HIS{*}) / sum (of TOTPURCSH_HIS{*})*100);
        else B11_cashadv_asperof_totalpur = 999999;


array PAY_HIS    {6} LAST_PYMT_AMT1 - LAST_PYMT_AMT6;
array MINDUE_HIS {6} TOT_MIN_PYMT_AMT1 - TOT_MIN_PYMT_AMT6;
array past_due {6} TOT_PD_AMT1-TOT_PD_AMT6;


array CRLIM_ZERO_OR_MISSING {6}
crlim_zero_or_missing_01 - crlim_zero_or_missing_06;
array MINDUE_HIS_PEROFLIM {6}
mindue_his_peroflim_01 - mindue_his_peroflim_06;

do i = 1 to 6;
	if CRLIM{i} <= 0 then CRLIM_ZERO_OR_MISSING {i} = 1;
else CRLIM_ZERO_OR_MISSING{i} = 0;
end;
flag_char13_divbyzero = (sum (of CRLIM_ZERO_OR_MISSING{*}) >0);

if flag_char13_divbyzero = 0 then do;
	do i = 1 to 6;
    	MINDUE_HIS_PEROFLIM {i} =
int(((MINDUE_HIS{i} + past_due{i})/ CRLIM {i})*100);
	end;
end;
else B13_max_mindueasper_oflim = 999999;

B13_max_mindueasper_oflim =
max (of mindue_his_peroflim_01 - mindue_his_peroflim_06);
B13_max_mindueasper_oflim =
min (B13_max_mindueasper_oflim,6);




flag_char14_divbyzero =
((sum (of TOT_MIN_PYMT_AMT2 - TOT_MIN_PYMT_AMT6)
+sum(of TOT_PD_AMT2-TOT_PD_AMT6)) = 0);

if flag_char14_divbyzero = 0 then
B14_pymnts_asperof_mindue =
(sum (of LAST_PYMT_AMT1 - LAST_PYMT_AMT5) /
(sum(of TOT_MIN_PYMT_AMT2 - TOT_MIN_PYMT_AMT6)
+ sum(of TOT_PD_AMT2-TOT_PD_AMT6)));
        else B14_pymnts_asperof_mindue = 999999;

flag_char141_divbyzero =
((sum (of TOT_MIN_PYMT_AMT2 - TOT_MIN_PYMT_AMT6)) = 0);

if flag_char141_divbyzero = 0 then
B141_pymnts_asperof_mindue =
(sum (of LAST_PYMT_AMT1 - LAST_PYMT_AMT5) /
(sum(of TOT_MIN_PYMT_AMT2 - TOT_MIN_PYMT_AMT6)));
        else B141_pymnts_asperof_mindue = 999999;

starting = 0;
ending = 0;
B21_maxconsecmths_ge100util = 0;
do i = 1 to 6;
if BAL{i} => CRLIM{i} and CRLIM{i} ne 0 then ending = i;
if BAL{i} < CRLIM{i} or i=6 or CRLIM{i}=0 then
        do;
                if starting < ending then
                        do;
                  run_ofconsecmths = ending - starting;
                    if B21_maxconsecmths_ge100util
< run_ofconsecmths then
   B21_maxconsecmths_ge100util = run_ofconsecmths;
                        end;
                starting = i;
                ending = i;
        end;
end;
run;

PROC SQL;
  connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
  create table work.dly_coll_cust_&pme_my.  as select *
  from connection to DB2 (
     select  
     b.*
	 from 	(	%Risk_Acct_Qry(zero)  )  as a
			left outer join 
			EDRTLR.RISK_CUST_ACCT_RLTNP_SNAPSHOT     as b
			on a.RISK_ACCT_ID = b.RISK_ACCT_ID
            and b.MTH_TM_ID= &TM.     
where  PRIM_CUST_F='Y'
  );
 disconnect from db2;
quit;

proc sql noprint;
  connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
   create table work.DELI_&pme_my. as
   select *
   from connection to DB2(
 
		select 
               c.*,
		       d.INQRY_CNT             as GO04_NUM_OF_INQRS,
               d.COLCTN_INQR_CNT       as GO22_NUM_OF_COLCTN_INQRS,
               d.CR_BUREAU_WORST_RT_F  as GO03_CR_BUREAU_WORST_RT,   
               d.TOT_UTLTN_AMT         as AT34_TOT_UTILZTN,      
               d.MTH_SINCE_MOST_RECNT_DLQNT_CNT  as AT36_MTH_MOST_R_DLQN 
		from 
			(
					select distinct RISK_CUST_ID 
					from(
					     select  
						 b.*
						 from (	%Risk_Acct_Qry(zero)  )as a
								left outer join 
								EDRTLR.RISK_CUST_ACCT_RLTNP_SNAPSHOT     as b
								on a.RISK_ACCT_ID = b.RISK_ACCT_ID
					            and b.MTH_TM_ID= &TM.     
							where b.PRIM_CUST_F='Y' 
						) fnl

			)c
		   inner join 
		      EDRTLR.RISK_CR_BUREAU_DELI_MTH_SNAPSHOT                  d
    	on c.RISK_CUST_ID=d.RISK_CUST_ID and 
              d.MTH_TM_ID= &TM.      

);
quit;

PROC SORT DATA= work.dly_coll_cust_&pme_my.  ; by  RISK_CUST_ID;  run;
PROC SORT DATA= work.DELI_&pme_my.         ; by  RISK_CUST_ID;  run;

data    work.sDELI_&pme_my._FINAL;
merge   work.dly_coll_cust_&pme_my. (IN=a) work.DELI_&pme_my. (IN=b);
BY      RISK_CUST_ID;
RUN;

proc sql;
   connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
   create table work.PROBE_&pme_my._new as
   select *
   from connection to DB2( 
   select * from
   			(
		select 
               b.ACCT_ID,
               b.ACCT_NUM,
               b.RISK_ACCT_ID,
               b.RISK_CUST_ID,
               b.GBL_CRNT_UTILZTN,           
               b.SUM_INVEST_BAL,          
               b.STEP_F,         
               b.CUST_FOR_YEARS,       
               b.WORST_DLQNT_ALL,      
               b.SUM_DEP_BAL,     
               b.TOT_REL,     
               b.MTH_SINCE_MOST_RECNT_DLQNT, 
               b.NUM_OF_CRNT_DLQNT_ACCT,   
               b.RTO_CSH_CR_LMT,
			 ROW_NUMBER() OVER (PARTITION BY b.RISK_ACCT_ID ORDER BY b.RISK_ACCT_ID ASC) AS ROWNUM1
 
		from  (   %Risk_Acct_Qry(zero)
					)a
					inner join
		      EDRTLR.PROBE_CUST_RSLT          b
    	on a.RISK_ACCT_ID=b.RISK_ACCT_ID 
		where  
              year(eff_tmstmp)=&pme_yr. AND 
              month(eff_tmstmp)= &pme_mnth.
			  ) fnl
			  where ROWNUM1=1
		order by RISK_ACCT_ID
);
quit;

PROC SORT DATA=work.sRV_MONTHLY_&pme_my._2 ; BY RISK_ACCT_ID;  RUN;
PROC SORT DATA=work.sDELI_&pme_my._FINAL ;    BY RISK_ACCT_ID;  RUN;


/* include corp no below to fix the logic for scorecard output*/
proc sql;
   connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
   create table work.dly_coll_&pme_my. as
   select *
   from connection to DB2( 
(%Risk_Acct_Qry(zero)
order by RISK_ACCT_ID)
);
disconnect from db2;
quit;

/* added following step to include corporation number */
proc sql;
create table work.dly_coll_&pme_my._v2 as
select 
					RISK_ACCT_ID
					,EFF_TM_ID
					,BNS_DLQNT_DAY
					,CR_LMT_AMT 
					,Tot_New_Bal
					,ACCT_STAT_CD
					,BLOCK_RECL_CD
					,CHRG_OFF_CD
					,CORP_RTL_F
					,PRD_CD
					,SUB_PRD_CD
					,TRNST_NUM
/*					,ACCT_NUM*/
					,score
					,CATS(0,A.corptn_num,substr(A.acct_num,verify(A.acct_num,'0'))) as ACCT_NUM 
				
from 
work.dly_coll_&pme_my. a
order by RISK_ACCT_ID
;
quit;

data   work.sdata_merged_&pme_my.;
merge  work.dly_coll_&pme_my._V2      (in=a)   
       work.sRV_MONTHLY_&pme_my._2  (in=b)  
       work.PROBE_&pme_my._new     (in=c)   
       work.sDELI_&pme_my._FINAL    (iN=d) 
      ;
by RISK_ACCT_ID;
if a;


array x _numeric_;
do over x; if x=. then x=-999999; end;

rename 
GO22_NUM_OF_COLCTN_INQRS=aa_GO22_NUM_OF_COLCTN_INQRS
AT34_TOT_UTILZTN=aa_AT34_TOT_UTILZTN
AT36_MTH_MOST_R_DLQN=aa_AT36_MTH_MOST_R_DLQN
WORST_DLQNT_ALL=aa_WORST_DLQNT_ALL
CUST_FOR_YEARS=aa_CUST_FOR_YEARS
RTO_CSH_CR_LMT=aa_RTO_CSH_CR_LMT
;
drop ROWNUM1 ;
RUN;

%macro def_len();
length 
char1_description
char2_description
char3_description
char4_description
char5_description
char6_description
char7_description
char8_description
char9_description
char10_description
char11_description
char12_description   
$80.;
%mend;

data  work.stest_seg1;
set   work.sdata_merged_&pme_my.;
%def_len;

where 22 <=BNS_DLQNT_DAY<=29; 
CMSRD_ID=63001; %pre_score;  

char1_description="B11_cashadv_asperof_totalpur" ;
char2_description="B14_pymnts_asperof_mindue "   ;
char3_description="B4_lastocc_ge100util  "     ;  
char4_description="GBL_CRNT_UTILZTN      "    ;   
char5_description="SUM_INVEST_BAL        "    ; 
char6_description="TOT_NEW_BAL          "    ;    
char7_description="AT36_MTH_MOST_R_DLQN "    ; 
char8_description="CUST_FOR_YEARS      "     ; 
char9_description="RTO_CSH_CR_LMT      "    ;  
char10_description="STEP_F   "           ;

rename 
B11_cashadv_asperof_totalpur   =char1_value
B14_pymnts_asperof_mindue      =char2_value
B4_lastocc_ge100util           =char3_value
GBL_CRNT_UTILZTN               =char4_value
SUM_INVEST_BAL                 =char5_value 
TOT_NEW_BAL                    =char6_value
aa_AT36_MTH_MOST_R_DLQN        =char7_value
aa_CUST_FOR_YEARS              =char8_value
aa_RTO_CSH_CR_LMT              =char9_value
STEP_F                         =char10_value


SCR_B11_cashadv_asperof_totalpur   =char1_weight
SCR_B14_pymnts_asperof_mindue      =char2_weight
SCR_B4_lastocc_ge100util           =char3_weight
SCR_GBL_CRNT_UTILZTN               =char4_weight
SCR_SUM_INVEST_BAL                 =char5_weight
SCR_TOT_NEW_BAL                    =char6_weight
SCR_aa_AT36_MTH_MOST_R_DLQN        =char7_weight
SCR_aa_CUST_FOR_YEARS              =char8_weight
SCR_aa_RTO_CSH_CR_LMT              =char9_weight
SCR_STEP_F                         =char10_weight

;
run;



data  work.stest_seg2;
set   work.sdata_merged_&pme_my.;
%def_len;
where 30 <=BNS_DLQNT_DAY<=59; 

CMSRD_ID=63002; 
if GO04_NUM_OF_INQRS not in (-999999)
and aa_GO22_NUM_OF_COLCTN_INQRS not in (-999999)
then GO05_INQ_EX_COLL=GO04_NUM_OF_INQRS - aa_GO22_NUM_OF_COLCTN_INQRS;
else  GO05_INQ_EX_COLL =-999999;

%X_SCORE;    
aa_WORST_DLQNT_ALL_s=put(aa_WORST_DLQNT_ALL, 8.); 


char1_description= "B13_max_mindueasper_oflim   ";
char2_description= "B21_maxconsecmths_ge100util ";
char3_description= "GO05_INQ_EX_COLL            ";
char4_description= "SUM_INVEST_BAL              ";
char5_description= "TOT_NEW_BAL                 ";
char6_description= "aa_AT34_TOT_UTILZTN         ";
char7_description= "aa_AT36_MTH_MOST_R_DLQN     ";
char8_description= "aa_CUST_FOR_YEARS           ";
char9_description= "aa_RTO_CSH_CR_LMT           ";
char10_description="aa_WORST_DLQNT_ALL_s        ";
char11_description="GO03_CR_BUREAU_WORST_RT     ";
char12_description="STEP_F                      ";



rename 
B13_max_mindueasper_oflim    =char1_value
B21_maxconsecmths_ge100util  =char2_value
GO05_INQ_EX_COLL             =char3_value
SUM_INVEST_BAL               =char4_value
TOT_NEW_BAL                  =char5_value
aa_AT34_TOT_UTILZTN          =char6_value
aa_AT36_MTH_MOST_R_DLQN      =char7_value
aa_CUST_FOR_YEARS            =char8_value
aa_RTO_CSH_CR_LMT            =char9_value
aa_WORST_DLQNT_ALL_s         =char10_value
GO03_CR_BUREAU_WORST_RT      =char11_value
STEP_F                       =char12_value

SCR_B13_max_mindueasper_oflim    =char1_weight
SCR_B21_maxconsecmths_ge100util  =char2_weight
SCR_GO05_INQ_EX_COLL             =char3_weight
SCR_SUM_INVEST_BAL               =char4_weight
SCR_TOT_NEW_BAL                  =char5_weight
SCR_aa_AT34_TOT_UTILZTN          =char6_weight
SCR_aa_AT36_MTH_MOST_R_DLQN      =char7_weight
SCR_aa_CUST_FOR_YEARS            =char8_weight
SCR_aa_RTO_CSH_CR_LMT            =char9_weight
SCR_aa_WORST_DLQNT_ALL           =char10_weight
SCR_GO03_CR_BUREAU_WORST_RT      =char11_weight
SCR_STEP_F                       =char12_weight
;
run; 

data  work.stest_seg3;
set   work.sdata_merged_&pme_my.;
%def_len;

where  60 <=BNS_DLQNT_DAY<=89  ; 
if GO04_NUM_OF_INQRS not in (-999999)
and aa_GO22_NUM_OF_COLCTN_INQRS not in (-999999)
then  GO05_INQ_EX_COLL=GO04_NUM_OF_INQRS - aa_GO22_NUM_OF_COLCTN_INQRS;
else  GO05_INQ_EX_COLL =-999999;

CMSRD_ID=63003; %m30_SCORE; 

aa_RTO_CSH_CR_LMT_s=put(aa_RTO_CSH_CR_LMT, 8.2); 

char1_description= "B141_pymnts_asperof_mindue  ";
char2_description= "B21_maxconsecmths_ge100util ";
char3_description= "GO05_INQ_EX_COLL            ";
char4_description= "MTH_SINCE_MOST_RECNT_DLQNT  ";
char5_description= "NUM_OF_CRNT_DLQNT_ACCT      ";
char6_description= "SUM_DEP_BAL                 ";
char7_description= "TOT_REL                     ";
char8_description= "TOT_NEW_BAL                 ";
char9_description= "aa_AT34_TOT_UTILZTN         ";
char10_description="aa_RTO_CSH_CR_LMT_s         ";
char11_description="GO03_CR_BUREAU_WORST_RT     ";
char12_description="STEP_F                      ";

rename 
B141_pymnts_asperof_mindue   =char1_value
B21_maxconsecmths_ge100util  =char2_value
GO05_INQ_EX_COLL             =char3_value
MTH_SINCE_MOST_RECNT_DLQNT   =char4_value
NUM_OF_CRNT_DLQNT_ACCT       =char5_value
SUM_DEP_BAL                  =char6_value
TOT_REL                      =char7_value
TOT_NEW_BAL                  =char8_value
aa_AT34_TOT_UTILZTN          =char9_value
aa_RTO_CSH_CR_LMT_s          =char10_value
GO03_CR_BUREAU_WORST_RT      =char11_value
STEP_F                       =char12_value


SCR_B141_pymnts_asperof_mindue   =char1_weight
SCR_B21_maxconsecmths_ge100util  =char2_weight
SCR_GO05_INQ_EX_COLL             =char3_weight
SCR_MTH_SINCE_MOST_RECNT_DLQNT   =char4_weight
SCR_NUM_OF_CRNT_DLQNT_ACCT       =char5_weight
SCR_SUM_DEP_BAL                  =char6_weight
SCR_TOT_REL                      =char7_weight
SCR_TOT_NEW_BAL                  =char8_weight
SCR_aa_AT34_TOT_UTILZTN          =char9_weight
SCR_aa_RTO_CSH_CR_LMT            =char10_weight
SCR_GO03_CR_BUREAU_WORST_RT      =char11_weight
SCR_STEP_F                       =char12_weight
;
run;


data work.SCORE_DETAILED_TABLE_&pme_my.;
set 
work.stest_seg1
work.stest_seg2
work.stest_seg3
;
SCORE_DES='CC COLLECTION SCORE';
SCRING_Date=put(datetime(),E8601DT20.);

keep 
ACCT_NUM
RISK_ACCT_ID
PRD_CD
SUB_PRD_CD
BNS_DLQNT_DAY
SCRING_Date
CMSRD_ID
SCORECARD_POINTS
char:;

rename  
SCORECARD_POINTS =CM_SCORE; 

run;

/* EDIT: (N DONMEZ) Inserting the DL score here  */
DATA WORK.dl_scored_table_&pme_my.;
    LENGTH        
        EFF_TM_ID          8
		RISK_ACCT_ID       8
        SCORE_DL           8
		BNS_DLQNT_DAY      8 ;
    INFILE "&dldropf.dl_scores_&pme_my..csv"
        LRECL=250
        ENCODING="LATIN1"
        TERMSTR=LF
        DLM=','
		firstobs=2
        MISSOVER
        DSD ;
    INPUT        
        EFF_TM_ID        : ?? BEST5.        
		RISK_ACCT_ID     : ?? BEST9.
        SCORE_DL         : ?? BEST3.
		BNS_DLQNT_DAY    : ?? BEST2. ;
RUN;

/* EDIT: (N DONMEZ) Inserting the DL treatment score here  */
DATA WORK.dl_treatment_raw_&pme_my.;
    LENGTH                
		RISK_ACCT_ID  8        
		DAILY_BNS_DLQNT_DAY  8 
		DAILY_TOT_NEW_BAL_AMT  8 
		R_COND_T_0_PRED  8 
		R_COND_T_1_PRED  8 ;
    INFILE "&dldropf.dl_treatment_&curr_my..csv"
        LRECL=250
        ENCODING="LATIN1"
        TERMSTR=LF
        DLM=','
		firstobs=2
        MISSOVER
        DSD ;
    INPUT        
        RISK_ACCT_ID  : ?? BEST9.        		
		DAILY_BNS_DLQNT_DAY  : ?? BEST2.         
		DAILY_TOT_NEW_BAL_AMT  : ?? BEST9. 
		R_COND_T_0_PRED  : ?? BEST4.
		R_COND_T_1_PRED  : ?? BEST4. ;
RUN;

data work.dl_treatment_v1_&pme_my. (drop= 
		R_COND_T_0_PRED 
		R_COND_T_1_PRED 
		DAILY_TOT_NEW_BAL_AMT 
		DAILY_BNS_DLQNT_DAY);
	set work.dl_treatment_raw_&pme_my.;
	length TR_SCORE1 8;
	length TR_SCORE2 8;
	length TR_SCORE3 8;
	length TR_DELTA1 8;
	length TR_DELTA2 8;
	length TR_BALANCE1 8;
	length TR_BALANCE2 8;	
	TR_SCORE1 = round(R_COND_T_0_PRED * 1000, 1);
	TR_SCORE2 = round(R_COND_T_1_PRED * 1000, 1);
	TR_SCORE3 = 0;
	TR_DELTA1 = TR_SCORE1 - TR_SCORE2;
	TR_DELTA2 = 0;
	TR_BALANCE1 = round((R_COND_T_1_PRED - R_COND_T_0_PRED) * DAILY_TOT_NEW_BAL_AMT, 1);	
	TR_BALANCE2 = 0;
	if TR_DELTA1 < 0 then TR_DELTA1 = 0;
	if TR_BALANCE1 < 0 then TR_BALANCE1 = 0;
	if TR_BALANCE1 > 9999999 then TR_BALANCE1 = 9999999;
	*if DAILY_BNS_DLQNT_DAY < 90;  /* ---- otherwise skip the record ----- */
run;

proc sql;
	create table work.dl_treatment_v2_&pme_my. as
	select
	coalesce(a.RISK_ACCT_ID,b.RISK_ACCT_ID) as RISK_ACCT_ID,
	b.TR_SCORE1,
	b.TR_SCORE2,
	b.TR_SCORE3,
	b.TR_DELTA1,
	b.TR_DELTA2,
	b.TR_BALANCE1,
	b.TR_BALANCE2
	from work.SCORE_DETAILED_TABLE_&pme_my. as a 
	full join work.dl_treatment_v1_&pme_my. as b
	on a.RISK_ACCT_ID=b.RISK_ACCT_ID;
quit;

/*Derrick EDIT - fix for missing acct num and nbr delq day values from right table of left join*/
proc sql;
	create table work.SCORE_TABLE_v2_&pme_my. (drop=x y) as
	select
		a.RISK_ACCT_ID as temp_risk_acct_id,
		d.ACCT_NUM,
		d.BNS_DLQNT_DAY,
		b.*,		
		a.TR_SCORE1,
		a.TR_SCORE2,
		a.TR_SCORE3,
		a.TR_DELTA1,
		a.TR_DELTA2,
		a.TR_BALANCE1,
		a.TR_BALANCE2,		
		c.SCORE_DL
	from work.dl_treatment_v2_&pme_my. as a
	left join work.SCORE_DETAILED_TABLE_&pme_my. (rename=(acct_num=x bns_dlqnt_day=y)) as b
	on a.RISK_ACCT_ID = b.RISK_ACCT_ID
	left join work.dl_scored_table_&pme_my. as c
	on a.RISK_ACCT_ID = C.RISK_ACCT_ID	
	left join SDATA_MERGED_&pme_my. as d
	on a.RISK_ACCT_ID = d.RISK_ACCT_ID;
quit;
/*Derrick edit end*/

/* ----- amend missing scores ------ */
data work.score_table_v3_&pme_my. (rename=(temp_risk_acct_id=RISK_ACCT_ID));
	set work.score_table_v2_&pme_my. (drop=RISK_ACCT_ID);
	if TR_SCORE1 = . then TR_SCORE1 = 0;
	if TR_SCORE2 = . then TR_SCORE2 = 0;
	if TR_SCORE3 = . then TR_SCORE3 = 0;
	if TR_DELTA1 = . then TR_DELTA1 = 0;
	if TR_DELTA2 = . then TR_DELTA2 = 0;
	if TR_BALANCE1 = . then TR_BALANCE1 = 0;
	if TR_BALANCE2 = . then TR_BALANCE2 = 0;
run;

data work.SCORE_DETAILED_TABLE_&pme_my.;
	set work.SCORE_TABLE_v3_&pme_my.;
run;

/* ------------ END OF EDIT -------------------- */


proc sql;
create table work.get_pd_RTO as
select

A.*,
b.basel_model_id as PD_BASEL_MODEL_ID,
c.basel_model_id as EAD_BASEL_MODEL_ID,
d.basel_model_id as LGD_BASEL_MODEL_ID,
e.acct_num,
substr(acct_num,11,13) as account_num  length=13  format=$13.,
e.cis_prd_cd
from 
(
SELECT 
MTH_TM_ID, BASEL_ACCT_ID AS RISK_ACCT_ID ,
PD_ACCT_SCORE ,
EAD_ACCT_SCORE ,
LGD_ACCT_SCORE,

PD_MODEL_RTO,
EAD_MODEL_RTO,
LGD_MODEL_RTO,

SRC_SYS_CD,
PD_BASEL_SEG_ID ,
EAD_BASEL_SEG_ID,
LGD_BASEL_SEG_ID

FROM RRAP_R.BASEL_ANALYTCL_BL_INSTRMNT_FACT  
where SRC_SYS_CD='KS' AND  mth_tm_id in (&TM. ) 
)  as a
left join RRAP_NZ.BASEL_SEG_V      as b on a.PD_BASEL_SEG_ID=b.basel_seg_id
left join RRAP_NZ.BASEL_SEG_V      as c on a.EAD_BASEL_SEG_ID=c.basel_seg_id
left join RRAP_NZ.BASEL_SEG_V      as d on a.LGD_BASEL_SEG_ID=d.basel_seg_id
left join RRAP_NZ.BASEL_ACCT_DIM_V as e on a.RISK_ACCT_ID=e.basel_acct_id
order by A.RISK_ACCT_ID;
;
quit;


proc sql;
   connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
   create table work.dly_coll&pme_my._all as
   select *
   from connection to DB2( 

select
RISK_ACCT_ID 
,PRD_CD  
,SUB_PRD_CD    
,BNS_DLQNT_DAY
,CR_LMT_AMT   as Credit_Lim
,TOT_NEW_BAL_AMT   as Tot_new_bal
,CORP_RTL_F 
,ACCT_STAT_CD  
,TRNST_NUM  
,BLOCK_RECL_CD        
,CHRG_OFF_CD
,corptn_num
,acct_num
/*,CATS(corptn_num,substr(acct_num,verify(acct_num,'0'))) as ACCT_NUM*/
from    EDRTLR.RISK_REVLVNG_CR_DLY_SNAPSHOT b
			where   
            EFF_TM_ID =  &Last_Avl_dt_mth. 
			and substr(PRD_CD, 1, 1) in ('A', 'V')
	       and coalesce(PRD_CD,'') <> 'VIC' 
		   /*PRD_CD='VIC' then delete;*/
		   );
disconnect from db2;
quit;


/* include corp no column below to fix output for Ju extract */
proc sql;
   connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
   create table work.dly_coll_&pme_my._EX as
   select *
   from connection to DB2 (
%Risk_Acct_Qry(notzero)
);
disconnect from db2;
quit;



proc sort data=work.dly_coll&pme_my._all;            by RISK_ACCT_ID; run;
proc sort data=work.get_pd_RTO ;       by RISK_ACCT_ID; run;
proc sort data=work.SCORE_DETAILED_TABLE_&pme_my. ;   by RISK_ACCT_ID; run;
proc sort data=work.dly_coll_&pme_my._EX;   by RISK_ACCT_ID; run;

data  work.JU_FEED_1_&pme_my.;
merge work.dly_coll&pme_my._all  (in=a) 
      work.get_pd_RTO 
      work.SCORE_DETAILED_TABLE_&pme_my. 
                           (in=c  
                           keep=RISK_ACCT_ID CMSRD_ID CM_SCORE SCORE_DL TR_SCORE: TR_DELTA: TR_BALANCE: char:) 
	 work.dly_coll_&pme_my._EX (in=d  keep=RISK_ACCT_ID );	
     
by   RISK_ACCT_ID ;
if a;

if d then CM_SCORE =-997;
if d then SCORE_DL =-997;
if d then TR_SCORE1 =-997;
if d then TR_SCORE2 =-997;
if d then TR_SCORE3 =-997;
if d then TR_DELTA1 =-997;
if d then TR_DELTA2 =-997;
if d then TR_BALANCE1 =0;
if d then TR_BALANCE2 =0;

if (0<=    BNS_DLQNT_DAY <=119)
then 
EL_UNAD  = PD_MODEL_RTO*LGD_MODEL_RTO*EAD_MODEL_RTO*MAX(Credit_Lim,   Tot_new_bal);


else if    BNS_DLQNT_DAY >=120
then  EL_UNAD  = LGD_MODEL_RTO*Tot_new_bal;

RENAME 
CMSRD_ID          =SRD_ID
CM_SCORE          =score
PD_BASEL_MODEL_ID =PD_MDL_ID
EAD_BASEL_MODEL_ID=EAD_MDL_ID

PD_ACCT_SCORE   = PD_SCORE
EAD_ACCT_SCORE  = EAD_SCORE
PRD_CD          = product_cd
SUB_PRD_CD      = SUB_PROD_CD
;
run;


data work.JU_FINAL_&pme_my.; 
set  work.JU_FEED_1_&pme_my.; 
length  SCRING_Date $20.;
SCRING_Date=put(datetime(),E8601DT20.);

if (0<=BNS_DLQNT_DAY<=119)
then do;
LGD_ND_MDL_ID=LGD_BASEL_MODEL_ID;
LGD_ND_SCORE =LGD_ACCT_SCORE;
end;

else if  BNS_DLQNT_DAY>=120
then do;
LGD_D_MDL_ID=LGD_BASEL_MODEL_ID;
LGD_D_SCORE=LGD_ACCT_SCORE;
end;

drop 
LGD_BASEL_MODEL_ID
LGD_ACCT_SCORE
;
keep 
acct_num
corptn_num
product_cd
SUB_PROD_CD
SCRING_Date

SRD_ID
SCORE
SCORE_DL
TR_SCORE1
TR_SCORE2
TR_SCORE3
TR_DELTA1
TR_DELTA2
TR_BALANCE1
TR_BALANCE2

EL_UNAD
PD: 
EAD:
LGD_D_:
LGD_ND:;
run;

/* JU FLAT FILE GENERATION CODE */
/*the following code will load data in COLL_JU_EXRCT DB2 table */

proc sql;
create table work.EIMMDSM_&pme_my. as

select 
input('2',$1.) as Record_type
,&dly_TM as EFF_TM_ID 
,input(CATS(corptn_num,substr(acct_num,verify(acct_num,'0'))),21.) as CORPTN_AND_ACCT_NUM format z21.
, PRODUCT_CD as PRD_CD 
, SUB_PROD_CD   as   SUB_PRD_CD
, put(TRANWRD(SCRING_DATE,"T"," "),$20. -l) as SCORING_DT_TEXT /*removing T and left align */
, case when SCORE=-997 then SCORE else  SRD_ID  end as MODEL_SCORECRD_ID
, SCORE  as MODEL_SCORE
, SCORE_DL
, TR_SCORE1
, TR_SCORE2
, TR_SCORE3
, TR_DELTA1
, TR_DELTA2
, TR_BALANCE1
, TR_BALANCE2
, LGD_ND_SCORE 
, LGD_ND_MDL_ID as LGD_ND_MODEL_ID
, LGD_D_SCORE 
, LGD_D_MDL_ID as LGD_D_MODEL_ID  
, EL_UNAD
, PD_SCORE 
, PD_MDL_ID  as PD_MODEL_ID
, EAD_SCORE 
, EAD_MDL_ID as EAD_MODEL_ID
,put(datetime(),datetime20.) as  INSRT_PROCESS_TMSTMP
, input(' ',$98.) as Filler 
 from work.ju_final_&pme_my. as A
;
quit;


proc sql noprint;
select count(*) into :NBR_RECORDS  from work.EIMMDSM_&pme_my. ;
quit;

%macro checkNBR_RECORDS();
%if  &NBR_RECORDS. = 0 %then %do; 
%abort cancel;
%end;
%mend;
%checkNBR_RECORDS();

/* formatting values as per the JU spec file */
proc format;
picture EL_FMT
low -< 0 = '0999999999.9999' (prefix='-') 
0 - high = '0999999999.9999' (prefix='+')
.='               '; 
run;

proc format;
picture rep_miss_5spc
.= '     '
other = '99999'; 
run;

proc format;
picture scrfmt
.= '     '
-997= '0999 ' (prefix='-') /*format with blank at the end */
other = '99999'; 
run;

/* EDIT: (N DONMEZ) adding new formats for treatment scores-fields */
proc format;
picture treat1fmt
.= '    '
-997= '0999' (prefix='-')
other = '9999'; 
run;

proc format;
picture treat2fmt
.= '       '
other = '9999999'; 
run;
/* ------ end of edit --------------------*/

filename out "&JU_FTP_PTH.&JU_FILE_NAME.";

data _null_;
set work.EIMMDSM_&pme_my. end=eof;
file out recfm=v lrecl=250;

 format HEADER $250.;     
     HEADER = "1"||"EIMMDSM "||"&Start_date_code."||PUT(" ",$233.);

if _n_=1 then  
put 
@1 HEADER $250.; 

put
@1  Record_Type $1.
@2  CORPTN_AND_ACCT_NUM z21.
@23 PRD_CD $3.
@26 SUB_PRD_CD $3.
@29 SCORING_DT_TEXT $20.
@49 MODEL_SCORECRD_ID scrfmt. 
@54 MODEL_SCORE scrfmt. 
@59 LGD_ND_SCORE rep_miss_5spc. 
@64 LGD_ND_MODEL_ID rep_miss_5spc. 
@69 LGD_D_SCORE rep_miss_5spc. 
@74 LGD_D_MODEL_ID rep_miss_5spc. 
@79 EL_UNAD EL_FMT. 
@94 PD_SCORE rep_miss_5spc. 
@99 PD_MODEL_ID rep_miss_5spc. 
@104 EAD_SCORE rep_miss_5spc. 
@109 EAD_MODEL_ID rep_miss_5spc. 
@114 SCORE_DL scrfmt.
@119 TR_SCORE1 treat1fmt.
@123 TR_SCORE2 treat1fmt.
@127 TR_SCORE3 treat1fmt.
@131 TR_DELTA1 treat1fmt.
@135 TR_DELTA2 treat1fmt.
@139 TR_BALANCE1 treat2fmt.
@146 TR_BALANCE2 treat2fmt.
@153 Filler $98.;

format FOOTER $250.;     
 FOOTER = "9"||put(&NBR_RECORDS.,z9.)||PUT(" ",$250.);

if eof then put @1 FOOTER $250.;
run; 

/*check if output file is empty*/
%macro checkfile();
%let rc=%sysfunc(filename(filrf,"&JU_FTP_PTH.&JU_FILE_NAME."));
%let fid=%sysfunc(fopen(&filrf));
%if &fid > 0 %then %do;
%let rc=%sysfunc(fread(&fid));
%let rc=%sysfunc(fget(&fid,mystring));
%if &rc NE 0 %then %do;
%abort cancel;
%end;
%end;
%mend;
%checkfile();

/*start of code to update DB2 tables with JU and scorecard output */
	
proc sql;
create table work.EIMMDSM3_&pme_my. as
select 
 EFF_TM_ID 
,cats('0',input(put(CORPTN_AND_ACCT_NUM,23.),$23.))   as CORPTN_AND_ACCT_NUM length=23
,PRD_CD length=3 format $3.
,SUB_PRD_CD
,SCORING_DT_TEXT 
,MODEL_SCORECRD_ID
,MODEL_SCORE
,LGD_ND_SCORE 
,LGD_ND_MODEL_ID
,LGD_D_SCORE 
,LGD_D_MODEL_ID  
,EL_UNAD
,PD_SCORE 
,PD_MODEL_ID
,EAD_SCORE 
,EAD_MODEL_ID
,datetime() as INSRT_PROCESS_TMSTMP
,SCORE_DL
,TR_SCORE1
,TR_SCORE2
,TR_SCORE3
,TR_DELTA1
,TR_DELTA2
,TR_BALANCE1
,TR_BALANCE2
from work.EIMMDSM_&pme_my.;
quit;


PROC SQL ;
connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
EXECUTE(DELETE FROM EDRTLRT.COLCTN_JU_EXTR where EFF_TM_ID =&dly_TM) BY db2;
QUIT;

/*some options (dbcommit=1000 BULKLOAD=YES  BL_METHOD=CLILOAD) force*/
proc append data=work.EIMMDSM3_&pme_my.  base=OUTTAB.COLCTN_JU_EXTR( BULKLOAD=YES  BL_METHOD=CLILOAD) ; 
run;


proc sql noprint;
create table work.SCORE_DETAILED_TABLE_&pme_my.db2 as
select 
&dly_TM as EFF_TM_ID
,ACCT_NUM as CORPTN_AND_ACCT_NUM length=23 format $23.
,BNS_DLQNT_DAY 
,CMSRD_ID as MODEL_SCORECRD_ID
,CM_SCORE as MODEL_SCORE
,PRD_CD
,input(put(RISK_ACCT_ID,23.),$23.) as CUST_CID 
,put(TRANWRD(SCRING_DATE,"T"," "),$20. -l) as SCORING_DT_TEXT
,SUB_PRD_CD
,char1_description 		as CHAR1_DESC
,char1_value 			as CHAR1_VAL
,char1_weight 			as CHAR1_WGHT
,char2_description 		as CHAR2_DESC
,char2_value 			as CHAR2_VAL
,char2_weight 			as CHAR2_WGHT
,char3_description 		as CHAR3_DESC
,char3_value 			as CHAR3_VAL
,char3_weight 			as CHAR3_WGHT
,char4_description 		as CHAR4_DESC
,char4_value 			as CHAR4_VAL
,char4_weight 			as CHAR4_WGHT
,char5_description 		as CHAR5_DESC
,char5_value 			as CHAR5_VAL
,char5_weight 			as CHAR5_WGHT
,char6_description 		as CHAR6_DESC
,char6_value 			as CHAR6_VAL
,char6_weight 			as CHAR6_WGHT
,char7_description 		as CHAR7_DESC
,char7_value 			as CHAR7_VAL
,char7_weight 			as CHAR7_WGHT
,char8_description		as CHAR8_DESC
,char8_value 			as CHAR8_VAL
,char8_weight 			as CHAR8_WGHT
,char9_description		as CHAR9_DESC
,char9_value 			as CHAR9_VAL
,char9_weight 			as CHAR9_WGHT
,char10_description 	as CHAR10_DESC
,strip(char10_value) 	as CHAR10_VAL
,char10_weight 			as CHAR10_WGHT
,char11_description 	as CHAR11_DESC
,char11_value 			as CHAR11_VAL
,char11_weight 			as CHAR11_WGHT
,char12_description 	as CHAR12_DESC
,char12_value 			as CHAR12_VAL
,char12_weight 			as CHAR12_WGHT
,datetime() 			as INSRT_PROCESS_TMSTMP
,SCORE_DL
,TR_SCORE1
,TR_SCORE2
,TR_SCORE3
,TR_DELTA1
,TR_DELTA2
,TR_BALANCE1
,TR_BALANCE2
from work.SCORE_DETAILED_TABLE_&pme_my.
;
quit;


PROC SQL ;
connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)") ;
EXECUTE(DELETE FROM EDRTLRT.COLCTN_SCORING_DTL_MODEL_OUTPUT where EFF_TM_ID =&dly_TM) BY db2;
QUIT;

proc append data=work.SCORE_DETAILED_TABLE_&pme_my.db2  base=OUTTAB.COLCTN_SCORING_DTL_MODEL_OUTPUT( BULKLOAD=YES BL_METHOD=CLILOAD )  ; 
run;


