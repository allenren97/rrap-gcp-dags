%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

/*ARCHIVE OLDER CCAR_ACAP REPORTS*/
X mkdir &OUTPATH/cmf/outgoing/CCAR_ACAP/history/&YEARMONTH;
X mv &OUTPATH/cmf/outgoing/CCAR_ACAP/DR*.* &OUTPATH/cmf/outgoing/CCAR_ACAP/history/&YEARMONTH;
