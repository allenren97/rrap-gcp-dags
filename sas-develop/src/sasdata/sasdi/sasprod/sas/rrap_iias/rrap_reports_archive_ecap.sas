%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

/*ARCHIVE OLDER REPORTS*/
X mkdir &OUTPATH/mv/outgoing/history/&YEARMONTH;
X mv &OUTPATH/mv/outgoing/mv_edwout_ecap_dr_exposures_f_mthly*.* &OUTPATH/mv/outgoing/history/&YEARMONTH;
X mv &OUTPATH/mv/outgoing/mv_edwout_ecap_dr_exp_ncr_f_qtrly*.* &OUTPATH/mv/outgoing/history/&YEARMONTH;
