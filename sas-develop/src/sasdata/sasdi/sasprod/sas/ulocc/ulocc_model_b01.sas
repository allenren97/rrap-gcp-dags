*CYLE 1 scorecard;


/*   START OF NODE: WOE and score code   */


*------------------------------------------------------------*;
* Variable: AT34slp6m;
*------------------------------------------------------------*;
LABEL GRP_AT34slp6m = "Grouped: AT34slp6m";
LABEL WOE_AT34slp6m = "Weight of Evidence: AT34slp6m";

if MISSING(AT34slp6m) then do;
GRP_AT34slp6m = 3;
WOE_AT34slp6m = -1.029110333;
end;
else if NOT MISSING(AT34slp6m) then do;
if AT34slp6m < 1.6 then do;
GRP_AT34slp6m = 1;
WOE_AT34slp6m = 0.1835386944;
end;
else
if 1.6 <= AT34slp6m AND AT34slp6m < 6.4 then do;
GRP_AT34slp6m = 2;
WOE_AT34slp6m =  -0.16173147;
end;
else
if 6.4 <= AT34slp6m then do;
GRP_AT34slp6m = 3;
WOE_AT34slp6m = -1.029110333;
end;
end;


*------------------------------------------------------------*;
* Variable: AT36;
*------------------------------------------------------------*;
LABEL GRP_AT36 =
"Grouped: AT36_MTH_SINCE_MOST_RECNT_DLQN";
LABEL WOE_AT36 =
"Weight of Evidence: AT36_MTH_SINCE_MOST_RECNT_DLQN";

if MISSING(AT36) then do;
GRP_AT36 = 4;
WOE_AT36 =  0.555651633;
end;
else if NOT MISSING(AT36) then do;
if AT36 < 3 then do;
GRP_AT36 = 1;
WOE_AT36 = -1.255421094;
end;
else
if 3 <= AT36 AND AT36 < 4 then do;
GRP_AT36 = 2;
WOE_AT36 = -0.613306698;
end;
else
if 4 <= AT36 AND AT36 < 8 then do;
GRP_AT36 = 3;
WOE_AT36 = 0.0324495574;
end;
else
if 8 <= AT36 then do;
GRP_AT36 = 4;
WOE_AT36 =  0.555651633;
end;
end;

*------------------------------------------------------------*;
* Variable: BC33min3m;
*------------------------------------------------------------*;
LABEL GRP_BC33min3m = "Grouped: BC33min3m";
LABEL WOE_BC33min3m = "Weight of Evidence: BC33min3m";

if MISSING(BC33min3m) then do;
GRP_BC33min3m = 2;
WOE_BC33min3m = 0.0570717648;
end;
else if NOT MISSING(BC33min3m) then do;
if BC33min3m < 3042 then do;
GRP_BC33min3m = 1;
WOE_BC33min3m = 0.5708389847;
end;
else
if 3042 <= BC33min3m AND BC33min3m < 7704 then do;
GRP_BC33min3m = 2;
WOE_BC33min3m = 0.0570717648;
end;
else
if 7704 <= BC33min3m AND BC33min3m < 20573 then do;
GRP_BC33min3m = 3;
WOE_BC33min3m = -0.385704592;
end;
else
if 20573 <= BC33min3m then do;
GRP_BC33min3m = 4;
WOE_BC33min3m = -0.961787353;
end;
end;

*------------------------------------------------------------*;
* Variable: BNS_DAY_DLQNT;
*------------------------------------------------------------*;
LABEL GRP_BNS_DAY_DLQNT =
"Grouped: BNS_DAY_DLQNT";
LABEL WOE_BNS_DAY_DLQNT =
"Weight of Evidence: BNS_DAY_DLQNT";

if MISSING(BNS_DAY_DLQNT) then do;
GRP_BNS_DAY_DLQNT = 1;
WOE_BNS_DAY_DLQNT =   0.48882406;
end;
else if NOT MISSING(BNS_DAY_DLQNT) then do;
if BNS_DAY_DLQNT < 38 then do;
GRP_BNS_DAY_DLQNT = 1;
WOE_BNS_DAY_DLQNT =   0.48882406;
end;
else
if 38 <= BNS_DAY_DLQNT AND BNS_DAY_DLQNT < 44 then do;
GRP_BNS_DAY_DLQNT = 2;
WOE_BNS_DAY_DLQNT = 0.1861167832;
end;
else
if 44 <= BNS_DAY_DLQNT AND BNS_DAY_DLQNT < 49 then do;
GRP_BNS_DAY_DLQNT = 3;
WOE_BNS_DAY_DLQNT = -0.060059059;
end;
else
if 49 <= BNS_DAY_DLQNT AND BNS_DAY_DLQNT < 54 then do;
GRP_BNS_DAY_DLQNT = 4;
WOE_BNS_DAY_DLQNT = -0.335845847;
end;
else
if 54 <= BNS_DAY_DLQNT then do;
GRP_BNS_DAY_DLQNT = 5;
WOE_BNS_DAY_DLQNT = -0.658143803;
end;
end;

* Variable: DEP_AMTmin3m;
*------------------------------------------------------------*;
LABEL GRP_DEP_AMTmin3m = "Grouped: DEP_AMTmin3m";
LABEL WOE_DEP_AMTmin3m = "Weight of Evidence: DEP_AMTmin3m";

if MISSING(DEP_AMTmin3m) then do;
GRP_DEP_AMTmin3m = 1;
WOE_DEP_AMTmin3m = -0.370064872;
end;
else if NOT MISSING(DEP_AMTmin3m) then do;
if DEP_AMTmin3m < 1410.09 then do;
GRP_DEP_AMTmin3m = 1;
WOE_DEP_AMTmin3m = -0.370064872;
end;
else
if 1410.09 <= DEP_AMTmin3m AND DEP_AMTmin3m < 3100 then do;
GRP_DEP_AMTmin3m = 2;
WOE_DEP_AMTmin3m = 0.0370064747;
end;
else
if 3100 <= DEP_AMTmin3m then do;
GRP_DEP_AMTmin3m = 3;
WOE_DEP_AMTmin3m = 0.7227371675;
end;
end;

*------------------------------------------------------------*;
* Variable: GO04_GO22;
*------------------------------------------------------------*;
LABEL GRP_GO04_GO22 = "Grouped: GO04_GO22";
LABEL WOE_GO04_GO22 = "Weight of Evidence: GO04_GO22";

if MISSING(GO04_GO22) then do;
GRP_GO04_GO22 = 2;
WOE_GO04_GO22 = 0.0194259699;
end;
else if NOT MISSING(GO04_GO22) then do;
if GO04_GO22 < 5 then do;
GRP_GO04_GO22 = 1;
WOE_GO04_GO22 = 0.3175408514;
end;
else
if 5 <= GO04_GO22 AND GO04_GO22 < 8 then do;
GRP_GO04_GO22 = 2;
WOE_GO04_GO22 = 0.0194259699;
end;
else
if 8 <= GO04_GO22 then do;
GRP_GO04_GO22 = 3;
WOE_GO04_GO22 = -0.780801106;
end;
end;

*------------------------------------------------------------*;
* Variable: GO11slp6m;
*------------------------------------------------------------*;
LABEL GRP_GO11slp6m = "Grouped: GO11slp6m";
LABEL WOE_GO11slp6m = "Weight of Evidence: GO11slp6m";

if MISSING(GO11slp6m) then do;
GRP_GO11slp6m = 1;
WOE_GO11slp6m = -0.664871039;
end;
else if NOT MISSING(GO11slp6m) then do;
if GO11slp6m < 0 then do;
GRP_GO11slp6m = 1;
WOE_GO11slp6m = -0.664871039;
end;
else
if 0 <= GO11slp6m then do;
GRP_GO11slp6m = 2;
WOE_GO11slp6m = 0.2551179675;
end;
end;


*------------------------------------------------------------*;
* Variable: NSF_NUMavg12m;
*------------------------------------------------------------*;
LABEL GRP_NSF_NUMavg12m = "Grouped: NSF_NUMavg12m";
LABEL WOE_NSF_NUMavg12m = "Weight of Evidence: NSF_NUMavg12m";

if MISSING(NSF_NUMavg12m) then do;
GRP_NSF_NUMavg12m = 2;
WOE_NSF_NUMavg12m = -0.325849292;
end;
else if NOT MISSING(NSF_NUMavg12m) then do;
if NSF_NUMavg12m < 0.08 then do;
GRP_NSF_NUMavg12m = 1;
WOE_NSF_NUMavg12m = 0.5068875301;
end;
else
if 0.08 <= NSF_NUMavg12m AND NSF_NUMavg12m < 0.17 then do;
GRP_NSF_NUMavg12m = 2;
WOE_NSF_NUMavg12m = -0.325849292;
end;
else
if 0.17 <= NSF_NUMavg12m then do;
GRP_NSF_NUMavg12m = 3;
WOE_NSF_NUMavg12m = -1.083767955;
end;
end;


*------------------------------------------------------------*;
* Variable: OSBALavg3m;
*------------------------------------------------------------*;
LABEL GRP_OSBALavg3m = "Grouped: OSBALavg3m";
LABEL WOE_OSBALavg3m = "Weight of Evidence: OSBALavg3m";

if MISSING(OSBALavg3m) then do;
GRP_OSBALavg3m = 2;
WOE_OSBALavg3m =  0.145691776;
end;
else if NOT MISSING(OSBALavg3m) then do;
if OSBALavg3m < 3656.9 then do;
GRP_OSBALavg3m = 1;
WOE_OSBALavg3m = 1.5226268418;
end;
else
if 3656.9 <= OSBALavg3m AND OSBALavg3m < 9724.68 then do;
GRP_OSBALavg3m = 2;
WOE_OSBALavg3m =  0.145691776;
end;
else
if 9724.68 <= OSBALavg3m then do;
GRP_OSBALavg3m = 3;
WOE_OSBALavg3m = -0.355176512;
end;
end;

*------------------------------------------------------------*;
* Variable: num_delq_accts_custmax6m;
*------------------------------------------------------------*;
LABEL GRP_num_delq_accts_custmax6m = "Grouped: num_delq_accts_custmax6m";
LABEL WOE_num_delq_accts_custmax6m = "Weight of Evidence: num_delq_accts_custmax6m";

if MISSING(num_delq_accts_custmax6m) then do;
GRP_num_delq_accts_custmax6m = 1;
WOE_num_delq_accts_custmax6m = 0.3694561347;
end;
else if NOT MISSING(num_delq_accts_custmax6m) then do;
if num_delq_accts_custmax6m < 2 then do;
GRP_num_delq_accts_custmax6m = 1;
WOE_num_delq_accts_custmax6m = 0.3694561347;
end;
else
if 2 <= num_delq_accts_custmax6m AND num_delq_accts_custmax6m < 3 then do;
GRP_num_delq_accts_custmax6m = 2;
WOE_num_delq_accts_custmax6m = -0.517208951;
end;
else
if 3 <= num_delq_accts_custmax6m then do;
GRP_num_delq_accts_custmax6m = 3;
WOE_num_delq_accts_custmax6m = -1.094713766;
end;
end;

*------------------------------------------------------------*;
* Variable: tot_mo_bal_custslp3m;
*------------------------------------------------------------*;
LABEL GRP_tot_mo_bal_custslp3m = "Grouped: tot_mo_bal_custslp3m";
LABEL WOE_tot_mo_bal_custslp3m = "Weight of Evidence: tot_mo_bal_custslp3m";

if MISSING(tot_mo_bal_custslp3m) then do;
GRP_tot_mo_bal_custslp3m = 3;
WOE_tot_mo_bal_custslp3m = -0.219258866;
end;
else if NOT MISSING(tot_mo_bal_custslp3m) then do;
if tot_mo_bal_custslp3m < -426.95 then do;
GRP_tot_mo_bal_custslp3m = 1;
WOE_tot_mo_bal_custslp3m = 0.9326218914;
end;
else
if -426.95 <= tot_mo_bal_custslp3m AND tot_mo_bal_custslp3m < 0 then do;
GRP_tot_mo_bal_custslp3m = 2;
WOE_tot_mo_bal_custslp3m = 0.0522583342;
end;
else
if 0 <= tot_mo_bal_custslp3m then do;
GRP_tot_mo_bal_custslp3m = 3;
WOE_tot_mo_bal_custslp3m = -0.219258866;
end;
end;


*------------------------------------------------------------*;
* Variable: total_d2dbal;
*------------------------------------------------------------*;
LABEL GRP_total_d2dbal = "Grouped: total_d2dbal";
LABEL WOE_total_d2dbal = "Weight of Evidence: total_d2dbal";

if MISSING(total_d2dbal) then do;
GRP_total_d2dbal = 1;
WOE_total_d2dbal = -0.498599067;
end;
else if NOT MISSING(total_d2dbal) then do;
if total_d2dbal < 56.03 then do;
GRP_total_d2dbal = 1;
WOE_total_d2dbal = -0.498599067;
end;
else
if 56.03 <= total_d2dbal AND total_d2dbal < 1041.85 then do;
GRP_total_d2dbal = 2;
WOE_total_d2dbal = 0.1227336192;
end;
else
if 1041.85 <= total_d2dbal AND total_d2dbal < 3139.78 then do;
GRP_total_d2dbal = 3;
WOE_total_d2dbal = 0.9205410001;
end;
else
if 3139.78 <= total_d2dbal then do;
GRP_total_d2dbal = 4;
WOE_total_d2dbal = 2.0070436588;
end;
end;


*------------------------------------------------------------*;
* Variable: util;
*------------------------------------------------------------*;
LABEL GRP_util = "Grouped: util";
LABEL WOE_util = "Weight of Evidence: util";

if MISSING(util) then do;
GRP_util = 4;
WOE_util = -0.815228309;
end;
else if NOT MISSING(util) then do;
if util < 0.37 then do;
GRP_util = 1;
WOE_util = 2.5846202715;
end;
else
if 0.37 <= util AND util < 0.89 then do;
GRP_util = 2;
WOE_util = 1.3596295617;
end;
else
if 0.89 <= util AND util < 0.99 then do;
GRP_util = 3;
WOE_util = 0.2463389999;
end;
else
if 0.99 <= util then do;
GRP_util = 4;
WOE_util = -0.815228309;
end;
end;

*------------------------------------------------------------*;
* generateScorepoints_note;
*------------------------------------------------------------*;
SCORECARD_POINTS =          577;

*------------------------------------------------------------*;
* Variable: AT34slp6m;
*------------------------------------------------------------*;
if MISSING(AT34slp6m) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_AT34slp6m= 0;
end;
else if NOT MISSING(AT34slp6m) AND AT34slp6m < 1.6 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 17;
SCR_AT34slp6m = 17;
end;
else if NOT MISSING(AT34slp6m) and 1.6 <= AT34slp6m AND AT34slp6m < 6.4 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 12;
SCR_AT34slp6m = 12;
end;
else if NOT MISSING(AT34slp6m) and 6.4 <= AT34slp6m then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_AT34slp6m = 0;
end;

*------------------------------------------------------------*;
* Variable: AT36;
*------------------------------------------------------------*;
if MISSING(AT36) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 20;
SCR_AT36= 20;
end;
else if NOT MISSING(AT36) AND AT36 < 3 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_AT36 = 0;
end;
else if NOT MISSING(AT36) and 3 <= AT36 AND AT36 < 4 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 7;
SCR_AT36 = 7;
end;
else if NOT MISSING(AT36) and 4 <= AT36 AND AT36 < 8 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 14;
SCR_AT36 = 14;
end;
else if NOT MISSING(AT36) and 8 <= AT36 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 20;
SCR_AT36 = 20;
end;

*------------------------------------------------------------*;
* Variable: BC33min3m;
*------------------------------------------------------------*;
if MISSING(BC33min3m) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 13;
SCR_BC33min3m= 13;
end;
else if NOT MISSING(BC33min3m) AND BC33min3m < 3042 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 20;
SCR_BC33min3m = 20;
end;
else if NOT MISSING(BC33min3m) and 3042 <= BC33min3m AND BC33min3m < 7704 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 13;
SCR_BC33min3m = 13;
end;
else if NOT MISSING(BC33min3m) and 7704 <= BC33min3m AND BC33min3m < 20573 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 7;
SCR_BC33min3m = 7;
end;
else if NOT MISSING(BC33min3m) and 20573 <= BC33min3m then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_BC33min3m = 0;
end;

*------------------------------------------------------------*;
* Variable: BNS_DAY_DLQNT;
*------------------------------------------------------------*;
if MISSING(BNS_DAY_DLQNT) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 23;
SCR_BNS_DAY_DLQNT= 23;
end;
else if NOT MISSING(BNS_DAY_DLQNT) AND BNS_DAY_DLQNT < 38 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 23;
SCR_BNS_DAY_DLQNT = 23;
end;
else if NOT MISSING(BNS_DAY_DLQNT) and 38 <= BNS_DAY_DLQNT AND BNS_DAY_DLQNT < 44 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 17;
SCR_BNS_DAY_DLQNT = 17;
end;
else if NOT MISSING(BNS_DAY_DLQNT) and 44 <= BNS_DAY_DLQNT AND BNS_DAY_DLQNT < 49 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 12;
SCR_BNS_DAY_DLQNT = 12;
end;
else if NOT MISSING(BNS_DAY_DLQNT) and 49 <= BNS_DAY_DLQNT AND BNS_DAY_DLQNT < 54 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 6;
SCR_BNS_DAY_DLQNT = 6;
end;
else if NOT MISSING(BNS_DAY_DLQNT) and 54 <= BNS_DAY_DLQNT then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_BNS_DAY_DLQNT = 0;
end;

*------------------------------------------------------------*;
* Variable: DEP_AMTmin3m;
*------------------------------------------------------------*;
if MISSING(DEP_AMTmin3m) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_DEP_AMTmin3m= 0;
end;
else if NOT MISSING(DEP_AMTmin3m) AND DEP_AMTmin3m < 1410.09 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_DEP_AMTmin3m = 0;
end;
else if NOT MISSING(DEP_AMTmin3m) and 1410.09 <= DEP_AMTmin3m AND DEP_AMTmin3m < 3100 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 7;
SCR_DEP_AMTmin3m = 7;
end;
else if NOT MISSING(DEP_AMTmin3m) and 3100 <= DEP_AMTmin3m then do;
SCORECARD_POINTS = SCORECARD_POINTS + 18;
SCR_DEP_AMTmin3m = 18;
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
else if NOT MISSING(GO04_GO22) and 5 <= GO04_GO22 AND GO04_GO22 < 8 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 8;
SCR_GO04_GO22 = 8;
end;
else if NOT MISSING(GO04_GO22) and 8 <= GO04_GO22 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_GO04_GO22 = 0;
end;

*------------------------------------------------------------*;
* Variable: GO11slp6m;
*------------------------------------------------------------*;
if MISSING(GO11slp6m) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_GO11slp6m= 0;
end;
else if NOT MISSING(GO11slp6m) AND GO11slp6m < 0 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_GO11slp6m = 0;
end;
else if NOT MISSING(GO11slp6m) and 0 <= GO11slp6m then do;
SCORECARD_POINTS = SCORECARD_POINTS + 6;
SCR_GO11slp6m = 6;
end;

*------------------------------------------------------------*;
* Variable: NSF_NUMavg12m;
*------------------------------------------------------------*;
if MISSING(NSF_NUMavg12m) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 6;
SCR_NSF_NUMavg12m= 6;
end;
else if NOT MISSING(NSF_NUMavg12m) AND NSF_NUMavg12m < 0.08 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 13;
SCR_NSF_NUMavg12m = 13;
end;
else if NOT MISSING(NSF_NUMavg12m) and 0.08 <= NSF_NUMavg12m AND NSF_NUMavg12m < 0.17 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 6;
SCR_NSF_NUMavg12m = 6;
end;
else if NOT MISSING(NSF_NUMavg12m) and 0.17 <= NSF_NUMavg12m then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_NSF_NUMavg12m = 0;
end;

*------------------------------------------------------------*;
* Variable: OSBALavg3m;
*------------------------------------------------------------*;
if MISSING(OSBALavg3m) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 6;
SCR_OSBALavg3m= 6;
end;
else if NOT MISSING(OSBALavg3m) AND OSBALavg3m < 3656.9 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 23;
SCR_OSBALavg3m = 23;
end;
else if NOT MISSING(OSBALavg3m) and 3656.9 <= OSBALavg3m AND OSBALavg3m < 9724.68 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 6;
SCR_OSBALavg3m = 6;
end;
else if NOT MISSING(OSBALavg3m) and 9724.68 <= OSBALavg3m then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_OSBALavg3m = 0;
end;

*------------------------------------------------------------*;
* Variable: num_delq_accts_custmax6m;
*------------------------------------------------------------*;
if MISSING(num_delq_accts_custmax6m) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 27;
SCR_num_delq_accts_custmax6m= 27;
end;
else if NOT MISSING(num_delq_accts_custmax6m) AND num_delq_accts_custmax6m < 2 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 27;
SCR_num_delq_accts_custmax6m = 27;
end;
else if NOT MISSING(num_delq_accts_custmax6m) and 2 <= num_delq_accts_custmax6m AND num_delq_accts_custmax6m < 3 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 11;
SCR_num_delq_accts_custmax6m = 11;
end;
else if NOT MISSING(num_delq_accts_custmax6m) and 3 <= num_delq_accts_custmax6m then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_num_delq_accts_custmax6m = 0;
end;

*------------------------------------------------------------*;
* Variable: tot_mo_bal_custslp3m;
*------------------------------------------------------------*;
if MISSING(tot_mo_bal_custslp3m) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_tot_mo_bal_custslp3m= 0;
end;
else if NOT MISSING(tot_mo_bal_custslp3m) AND tot_mo_bal_custslp3m < -426.95 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 32;
SCR_tot_mo_bal_custslp3m = 32;
end;
else if NOT MISSING(tot_mo_bal_custslp3m) and -426.95 <= tot_mo_bal_custslp3m AND tot_mo_bal_custslp3m < 0 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 7;
SCR_tot_mo_bal_custslp3m = 7;
end;
else if NOT MISSING(tot_mo_bal_custslp3m) and 0 <= tot_mo_bal_custslp3m then do;
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
else if NOT MISSING(total_d2dbal) AND total_d2dbal < 56.03 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_total_d2dbal = 0;
end;
else if NOT MISSING(total_d2dbal) and 56.03 <= total_d2dbal AND total_d2dbal < 1041.85 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 8;
SCR_total_d2dbal = 8;
end;
else if NOT MISSING(total_d2dbal) and 1041.85 <= total_d2dbal AND total_d2dbal < 3139.78 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 17;
SCR_total_d2dbal = 17;
end;
else if NOT MISSING(total_d2dbal) and 3139.78 <= total_d2dbal then do;
SCORECARD_POINTS = SCORECARD_POINTS + 31;
SCR_total_d2dbal = 31;
end;

*------------------------------------------------------------*;
* Variable: util;
*------------------------------------------------------------*;
if MISSING(util) then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_util= 0;
end;
else if NOT MISSING(util) AND util < 0.37 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 62;
SCR_util = 62;
end;
else if NOT MISSING(util) and 0.37 <= util AND util < 0.89 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 40;
SCR_util = 40;
end;
else if NOT MISSING(util) and 0.89 <= util AND util < 0.99 then do;
SCORECARD_POINTS = SCORECARD_POINTS + 19;
SCR_util = 19;
end;
else if NOT MISSING(util) and 0.99 <= util then do;
SCORECARD_POINTS = SCORECARD_POINTS + 0;
SCR_util = 0;
end;
*;
* Assign SCORECARD_BIN values;
*;
if SCORECARD_POINTS < 589 then SCORECARD_BIN = 1;
else if SCORECARD_POINTS < 601 then SCORECARD_BIN = 2;
else if SCORECARD_POINTS < 613 then SCORECARD_BIN = 3;
else if SCORECARD_POINTS < 625 then SCORECARD_BIN = 4;
else if SCORECARD_POINTS < 637 then SCORECARD_BIN = 5;
else if SCORECARD_POINTS < 649 then SCORECARD_BIN = 6;
else if SCORECARD_POINTS < 661 then SCORECARD_BIN = 7;
else if SCORECARD_POINTS < 673 then SCORECARD_BIN = 8;
else if SCORECARD_POINTS < 685 then SCORECARD_BIN = 9;
else if SCORECARD_POINTS < 697 then SCORECARD_BIN = 10;
else if SCORECARD_POINTS < 709 then SCORECARD_BIN = 11;
else if SCORECARD_POINTS < 721 then SCORECARD_BIN = 12;
else if SCORECARD_POINTS < 733 then SCORECARD_BIN = 13;
else if SCORECARD_POINTS < 745 then SCORECARD_BIN = 14;
else if SCORECARD_POINTS < 757 then SCORECARD_BIN = 15;
else if SCORECARD_POINTS < 769 then SCORECARD_BIN = 16;
else if SCORECARD_POINTS < 781 then SCORECARD_BIN = 17;
else if SCORECARD_POINTS < 793 then SCORECARD_BIN = 18;
else if SCORECARD_POINTS < 805 then SCORECARD_BIN = 19;
else if SCORECARD_POINTS < 817 then SCORECARD_BIN = 20;
else if SCORECARD_POINTS < 829 then SCORECARD_BIN = 21;
else if SCORECARD_POINTS < 841 then SCORECARD_BIN = 22;
else if SCORECARD_POINTS < 853 then SCORECARD_BIN = 23;
else if SCORECARD_POINTS < 865 then SCORECARD_BIN = 24;
else SCORECARD_BIN = 25;


collect_score=SCORECARD_POINTS;
score_desc="Behaviour model";
model="ULOC_B01";
