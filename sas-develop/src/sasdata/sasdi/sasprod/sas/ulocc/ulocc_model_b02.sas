*------------------------------------------------------------*;
* Variable: AT36;
*------------------------------------------------------------*;
LABEL WOE_AT36 =
"Weight of Evidence: AT36_MTH_SINCE_MOST_RECNT_DLQN";

if MISSING(AT36) then do;
WOE_AT36 = 0.1280554232;
end;
else if NOT MISSING(AT36) then do;
if AT36 < 3 then do;
WOE_AT36 = -0.769721083;
end;
else
if 3 <= AT36 AND AT36 < 4 then do;
WOE_AT36 = 0.1280554232;
end;
else
if 4 <= AT36 then do;
WOE_AT36 = 0.6052677697;
end;
end;

*------------------------------------------------------------*;
* Variable: BC33min3m;
*------------------------------------------------------------*;
LABEL WOE_BC33min3m = "Weight of Evidence: BC33min3m";

if MISSING(BC33min3m) then do;
WOE_BC33min3m = -0.409711225;
end;
else if NOT MISSING(BC33min3m) then do;
if BC33min3m < 4490 then do;
WOE_BC33min3m = 0.4437414825;
end;
else
if 4490 <= BC33min3m AND BC33min3m < 9910 then do;
WOE_BC33min3m = 0.0169430423;
end;
else
if 9910 <= BC33min3m AND BC33min3m < 25404 then do;
WOE_BC33min3m = -0.409711225;
end;
else
if 25404 <= BC33min3m then do;
WOE_BC33min3m = -1.124073093;
end;
end;

*------------------------------------------------------------*;
* Variable: BNS_DAY_DLQNT;
*------------------------------------------------------------*;
LABEL WOE_BNS_DAY_DLQNT =
"Weight of Evidence: BNS_DAY_DLQNT";

if MISSING(BNS_DAY_DLQNT) then do;
WOE_BNS_DAY_DLQNT = 0.4944915955;
end;
else if NOT MISSING(BNS_DAY_DLQNT) then do;
if BNS_DAY_DLQNT < 77 then do;
WOE_BNS_DAY_DLQNT = 0.4944915955;
end;
else
if 77 <= BNS_DAY_DLQNT AND BNS_DAY_DLQNT < 99 then do;
WOE_BNS_DAY_DLQNT = -0.040323693;
end;
else
if 99 <= BNS_DAY_DLQNT then do;
WOE_BNS_DAY_DLQNT = -0.789890421;
end;
end;

*------------------------------------------------------------*;
* Variable: DEP_AMT;
*------------------------------------------------------------*;
LABEL WOE_DEP_AMT =
"Weight of Evidence: DEP_AMT";

if MISSING(DEP_AMT) then do;
WOE_DEP_AMT = -0.329261992;
end;
else if NOT MISSING(DEP_AMT) then do;
if DEP_AMT < 85.53 then do;
WOE_DEP_AMT = -0.754872955;
end;
else
if 85.53 <= DEP_AMT AND DEP_AMT < 942.75 then do;
WOE_DEP_AMT = -0.329261992;
end;
else
if 942.75 <= DEP_AMT AND DEP_AMT < 6596.24 then do;
WOE_DEP_AMT = 0.3647347083;
end;
else
if 6596.24 <= DEP_AMT then do;
WOE_DEP_AMT = 1.0067877757;
end;
end;

*------------------------------------------------------------*;
* Variable: GO04_GO22;
*------------------------------------------------------------*;
LABEL WOE_GO04_GO22 = "Weight of Evidence: GO04_GO22";

if MISSING(GO04_GO22) then do;
WOE_GO04_GO22 = -0.056418005;
end;
else if NOT MISSING(GO04_GO22) then do;
if GO04_GO22 < 5 then do;
WOE_GO04_GO22 =  0.229734044;
end;
else
if 5 <= GO04_GO22 AND GO04_GO22 < 13 then do;
WOE_GO04_GO22 = -0.056418005;
end;
else
if 13 <= GO04_GO22 then do;
WOE_GO04_GO22 = -0.872768744;
end;
end;

*------------------------------------------------------------*;
* Variable: GO11slp6m;
*------------------------------------------------------------*;
LABEL WOE_GO11slp6m = "Weight of Evidence: GO11slp6m";

if MISSING(GO11slp6m) then do;
WOE_GO11slp6m = -0.166246233;
end;
else if NOT MISSING(GO11slp6m) then do;
if GO11slp6m < -2.8 then do;
WOE_GO11slp6m = -1.048534296;
end;
else
if -2.8 <= GO11slp6m AND GO11slp6m < -1.4 then do;
WOE_GO11slp6m = -0.664253505;
end;
else
if -1.4 <= GO11slp6m AND GO11slp6m < 0 then do;
WOE_GO11slp6m = -0.166246233;
end;
else
if 0 <= GO11slp6m then do;
WOE_GO11slp6m = 0.3294994735;
end;
end;

*------------------------------------------------------------*;
* Variable: OSBAL;
*------------------------------------------------------------*;
LABEL WOE_OSBAL =
"Weight of Evidence: OSBAL";

if MISSING(OSBAL) then do;
WOE_OSBAL = 2.2011658874;
end;
else if NOT MISSING(OSBAL) then do;
if OSBAL < 1350.55 then do;
WOE_OSBAL = 2.2011658874;
end;
else
if 1350.55 <= OSBAL AND OSBAL < 3019.23 then do;
WOE_OSBAL = 1.1727057491;
end;
else
if 3019.23 <= OSBAL AND OSBAL < 10188.11 then do;
WOE_OSBAL = 0.1867619487;
end;
else
if 10188.11 <= OSBAL then do;
WOE_OSBAL = -0.449827655;
end;
end;

*------------------------------------------------------------*;
* Variable: num_cur_accts_cust;
*------------------------------------------------------------*;
LABEL WOE_num_cur_accts_cust = "Weight of Evidence: num_cur_accts_cust";

if MISSING(num_cur_accts_cust) then do;
WOE_num_cur_accts_cust = -0.253940303;
end;
else if NOT MISSING(num_cur_accts_cust) then do;
if num_cur_accts_cust < 2 then do;
WOE_num_cur_accts_cust = -0.253940303;
end;
else
if 2 <= num_cur_accts_cust AND num_cur_accts_cust < 3 then do;
WOE_num_cur_accts_cust = 0.6899415166;
end;
else
if 3 <= num_cur_accts_cust then do;
WOE_num_cur_accts_cust = 1.3727950602;
end;
end;

*------------------------------------------------------------*;
* Variable: tot_mo_bal_custslp3m;
*------------------------------------------------------------*;
LABEL WOE_tot_mo_bal_custslp3m = "Weight of Evidence: tot_mo_bal_custslp3m";

if MISSING(tot_mo_bal_custslp3m) then do;
WOE_tot_mo_bal_custslp3m = -0.179731909;
end;
else if NOT MISSING(tot_mo_bal_custslp3m) then do;
if tot_mo_bal_custslp3m < -683.89 then do;
WOE_tot_mo_bal_custslp3m =  1.120943249;
end;
else
if -683.89 <= tot_mo_bal_custslp3m AND tot_mo_bal_custslp3m < -198.23 then do;
WOE_tot_mo_bal_custslp3m = 0.4584801764;
end;
else
if -198.23 <= tot_mo_bal_custslp3m AND tot_mo_bal_custslp3m < -68.37 then do;
WOE_tot_mo_bal_custslp3m = -0.179731909;
end;
else
if -68.37 <= tot_mo_bal_custslp3m then do;
WOE_tot_mo_bal_custslp3m = -0.548704211;
end;
end;

*------------------------------------------------------------*;
* Variable: total_d2dbal;
*------------------------------------------------------------*;
LABEL WOE_total_d2dbal = "Weight of Evidence: total_d2dbal";

if MISSING(total_d2dbal) then do;
WOE_total_d2dbal = -0.423909743;
end;
else if NOT MISSING(total_d2dbal) then do;
if total_d2dbal < 45.45 then do;
WOE_total_d2dbal = -0.423909743;
end;
else
if 45.45 <= total_d2dbal AND total_d2dbal < 250.03 then do;
WOE_total_d2dbal = 0.1102920036;
end;
else
if 250.03 <= total_d2dbal AND total_d2dbal < 1348.96 then do;
WOE_total_d2dbal = 0.5178491132;
end;
else
if 1348.96 <= total_d2dbal AND total_d2dbal < 2961.33 then do;
WOE_total_d2dbal = 0.9763388058;
end;
else
if 2961.33 <= total_d2dbal then do;
WOE_total_d2dbal = 2.0725591804;
end;
end;

*------------------------------------------------------------*;
* Variable: util;
*------------------------------------------------------------*;
LABEL WOE_util = "Weight of Evidence: util";

if MISSING(util) then do;
WOE_util = -0.386258854;
end;
else if NOT MISSING(util) then do;
if util < 0.22 then do;
WOE_util = 2.8317527868;
end;
else
if 0.22 <= util AND util < 0.83 then do;
WOE_util = 1.5680780197;
end;
else
if 0.83 <= util AND util < 0.98 then do;
WOE_util =  0.458417937;
end;
else
if 0.98 <= util then do;
WOE_util = -0.386258854;
end;
end;

*------------------------------------------------------------*;
* Variable: worst_Dlqdays_ks;
*------------------------------------------------------------*;
LABEL WOE_worst_Dlqdays_ks = "Weight of Evidence: worst_Dlqdays_ks";

if MISSING(worst_Dlqdays_ks) then do;
WOE_worst_Dlqdays_ks = 0.6785301123;
end;
else if NOT MISSING(worst_Dlqdays_ks) then do;
if worst_Dlqdays_ks < 75 then do;
WOE_worst_Dlqdays_ks = 0.6785301123;
end;
else
if 75 <= worst_Dlqdays_ks AND worst_Dlqdays_ks < 96 then do;
WOE_worst_Dlqdays_ks = 0.1481810025;
end;
else
if 96 <= worst_Dlqdays_ks AND worst_Dlqdays_ks < 105 then do;
WOE_worst_Dlqdays_ks = -0.480003889;
end;
else
if 105 <= worst_Dlqdays_ks AND worst_Dlqdays_ks < 127 then do;
WOE_worst_Dlqdays_ks = -0.791802873;
end;
else
if 127 <= worst_Dlqdays_ks then do;
WOE_worst_Dlqdays_ks = -1.315170135;
end;
end;

*------------------------------------------------------------*;
* generateScorepoints_note;
*------------------------------------------------------------*;
SCORECARD_POINTS =          542;

*------------------------------------------------------------*;
* Variable: AT36;
*------------------------------------------------------------*;
if MISSING(AT36) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 9;
SCR_AT36= 9;
end;
else if NOT MISSING(AT36) AND AT36 < 3 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_AT36 = 0;
end;
else if NOT MISSING(AT36) and 3 <= AT36 AND AT36 < 4 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 9;
SCR_AT36 = 9;
end;
else if NOT MISSING(AT36) and 4 <= AT36 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 13;
SCR_AT36 = 13;
end;

*------------------------------------------------------------*;
* Variable: BC33min3m;
*------------------------------------------------------------*;
if MISSING(BC33min3m) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 9;
SCR_BC33min3m= 9;
end;
else if NOT MISSING(BC33min3m) AND BC33min3m < 4490 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 20;
SCR_BC33min3m = 20;
end;
else if NOT MISSING(BC33min3m) and 4490 <= BC33min3m AND BC33min3m < 9910 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 15;
SCR_BC33min3m = 15;
end;
else if NOT MISSING(BC33min3m) and 9910 <= BC33min3m AND BC33min3m < 25404 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 9;
SCR_BC33min3m = 9;
end;
else if NOT MISSING(BC33min3m) and 25404 <= BC33min3m then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_BC33min3m = 0;
end;

*------------------------------------------------------------*;
* Variable: BNS_DAY_DLQNT;
*------------------------------------------------------------*;
if MISSING(BNS_DAY_DLQNT) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 8;
SCR_BNS_DAY_DLQNT= 8;
end;
else if NOT MISSING(BNS_DAY_DLQNT) AND BNS_DAY_DLQNT < 77 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 8;
SCR_BNS_DAY_DLQNT = 8;
end;
else if NOT MISSING(BNS_DAY_DLQNT) and 77 <= BNS_DAY_DLQNT AND BNS_DAY_DLQNT < 99 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 5;
SCR_BNS_DAY_DLQNT = 5;
end;
else if NOT MISSING(BNS_DAY_DLQNT) and 99 <= BNS_DAY_DLQNT then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_BNS_DAY_DLQNT = 0;
end;

*------------------------------------------------------------*;
* Variable: DEP_AMT;
*------------------------------------------------------------*;
if MISSING(DEP_AMT) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 7;
SCR_DEP_AMT= 7;
end;
else if NOT MISSING(DEP_AMT) AND DEP_AMT < 85.53 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_DEP_AMT = 0;
end;
else if NOT MISSING(DEP_AMT) and 85.53 <= DEP_AMT AND DEP_AMT < 942.75 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 7;
SCR_DEP_AMT = 7;
end;
else if NOT MISSING(DEP_AMT) and 942.75 <= DEP_AMT AND DEP_AMT < 6596.24 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 18;
SCR_DEP_AMT = 18;
end;
else if NOT MISSING(DEP_AMT) and 6596.24 <= DEP_AMT then do;
SCORECARD_POINTS = SCORECARD_POINTS + 29;
SCR_DEP_AMT = 29;
end;

*------------------------------------------------------------*;
* Variable: GO04_GO22;
*------------------------------------------------------------*;
if MISSING(GO04_GO22) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 8;
SCR_GO04_GO22= 8;
end;
else if NOT MISSING(GO04_GO22) AND GO04_GO22 < 5 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 11;
SCR_GO04_GO22 = 11;
end;
else if NOT MISSING(GO04_GO22) and 5 <= GO04_GO22 AND GO04_GO22 < 13 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 8;
SCR_GO04_GO22 = 8;
end;
else if NOT MISSING(GO04_GO22) and 13 <= GO04_GO22 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_GO04_GO22 = 0;
end;

*------------------------------------------------------------*;
* Variable: GO11slp6m;
*------------------------------------------------------------*;
if MISSING(GO11slp6m) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 11;
SCR_GO11slp6m= 11;
end;
else if NOT MISSING(GO11slp6m) AND GO11slp6m < -2.8 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_GO11slp6m = 0;
end;
else if NOT MISSING(GO11slp6m) and -2.8 <= GO11slp6m AND GO11slp6m < -1.4 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 5;
SCR_GO11slp6m = 5;
end;
else if NOT MISSING(GO11slp6m) and -1.4 <= GO11slp6m AND GO11slp6m < 0 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 11;
SCR_GO11slp6m = 11;
end;
else if NOT MISSING(GO11slp6m) and 0 <= GO11slp6m then do;
SCORECARD_POINTS = SCORECARD_POINTS + 17;
SCR_GO11slp6m = 17;
end;

*------------------------------------------------------------*;
* Variable: OSBAL;
*------------------------------------------------------------*;
if MISSING(OSBAL) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 46;
SCR_OSBAL= 46;
end;
else if NOT MISSING(OSBAL) AND OSBAL < 1350.55 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 46;
SCR_OSBAL = 46;
end;
else if NOT MISSING(OSBAL) and 1350.55 <= OSBAL AND OSBAL < 3019.23 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 28;
SCR_OSBAL = 28;
end;
else if NOT MISSING(OSBAL) and 3019.23 <= OSBAL AND OSBAL < 10188.11 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 11;
SCR_OSBAL = 11;
end;
else if NOT MISSING(OSBAL) and 10188.11 <= OSBAL then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_OSBAL = 0;
end;

*------------------------------------------------------------*;
* Variable: num_cur_accts_cust;
*------------------------------------------------------------*;
if MISSING(num_cur_accts_cust) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_num_cur_accts_cust= 0;
end;
else if NOT MISSING(num_cur_accts_cust) AND num_cur_accts_cust < 2 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_num_cur_accts_cust = 0;
end;
else if NOT MISSING(num_cur_accts_cust) and 2 <= num_cur_accts_cust AND num_cur_accts_cust < 3 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 10;
SCR_num_cur_accts_cust = 10;
end;
else if NOT MISSING(num_cur_accts_cust) and 3 <= num_cur_accts_cust then do;
SCORECARD_POINTS = SCORECARD_POINTS + 17;
SCR_num_cur_accts_cust = 17;
end;

*------------------------------------------------------------*;
* Variable: tot_mo_bal_custslp3m;
*------------------------------------------------------------*;
if MISSING(tot_mo_bal_custslp3m) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 6;
SCR_tot_mo_bal_custslp3m= 6;
end;
else if NOT MISSING(tot_mo_bal_custslp3m) AND tot_mo_bal_custslp3m < -683.89 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 26;
SCR_tot_mo_bal_custslp3m = 26;
end;
else if NOT MISSING(tot_mo_bal_custslp3m) and -683.89 <= tot_mo_bal_custslp3m AND tot_mo_bal_custslp3m < -198.23 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 16;
SCR_tot_mo_bal_custslp3m = 16;
end;
else if NOT MISSING(tot_mo_bal_custslp3m) and -198.23 <= tot_mo_bal_custslp3m AND tot_mo_bal_custslp3m < -68.37 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 6;
SCR_tot_mo_bal_custslp3m = 6;
end;
else if NOT MISSING(tot_mo_bal_custslp3m) and -68.37 <= tot_mo_bal_custslp3m then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_tot_mo_bal_custslp3m = 0;
end;

*------------------------------------------------------------*;
* Variable: total_d2dbal;
*------------------------------------------------------------*;
if MISSING(total_d2dbal) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_total_d2dbal= 0;
end;
else if NOT MISSING(total_d2dbal) AND total_d2dbal < 45.45 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_total_d2dbal = 0;
end;
else if NOT MISSING(total_d2dbal) and 45.45 <= total_d2dbal AND total_d2dbal < 250.03 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 8;
SCR_total_d2dbal = 8;
end;
else if NOT MISSING(total_d2dbal) and 250.03 <= total_d2dbal AND total_d2dbal < 1348.96 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 14;
SCR_total_d2dbal = 14;
end;
else if NOT MISSING(total_d2dbal) and 1348.96 <= total_d2dbal AND total_d2dbal < 2961.33 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 21;
SCR_total_d2dbal = 21;
end;
else if NOT MISSING(total_d2dbal) and 2961.33 <= total_d2dbal then do;
SCORECARD_POINTS = SCORECARD_POINTS + 37;
SCR_total_d2dbal = 37;
end;

*------------------------------------------------------------*;
* Variable: util;
*------------------------------------------------------------*;
if MISSING(util) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_util= 0;
end;
else if NOT MISSING(util) AND util < 0.22 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 47;
SCR_util = 47;
end;
else if NOT MISSING(util) and 0.22 <= util AND util < 0.83 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 28;
SCR_util = 28;
end;
else if NOT MISSING(util) and 0.83 <= util AND util < 0.98 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 12;
SCR_util = 12;
end;
else if NOT MISSING(util) and 0.98 <= util then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_util = 0;
end;

*------------------------------------------------------------*;
* Variable: worst_Dlqdays_ks;
*------------------------------------------------------------*;
if MISSING(worst_Dlqdays_ks) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 30;
SCR_worst_Dlqdays_ks= 30;
end;
else if NOT MISSING(worst_Dlqdays_ks) AND worst_Dlqdays_ks < 75 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 30;
SCR_worst_Dlqdays_ks = 30;
end;
else if NOT MISSING(worst_Dlqdays_ks) and 75 <= worst_Dlqdays_ks AND worst_Dlqdays_ks < 96 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 22;
SCR_worst_Dlqdays_ks = 22;
end;
else if NOT MISSING(worst_Dlqdays_ks) and 96 <= worst_Dlqdays_ks AND worst_Dlqdays_ks < 105 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 13;
SCR_worst_Dlqdays_ks = 13;
end;
else if NOT MISSING(worst_Dlqdays_ks) and 105 <= worst_Dlqdays_ks AND worst_Dlqdays_ks < 127 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 8;
SCR_worst_Dlqdays_ks = 8;
end;
else if NOT MISSING(worst_Dlqdays_ks) and 127 <= worst_Dlqdays_ks then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_worst_Dlqdays_ks = 0;
end;


collect_score=SCORECARD_POINTS;
score_desc="Behaviour model";
model="ULOC_B02";
