%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

/*ARCHIVE OLDER REPORTS*/
X mkdir &OUTPATH/cmf/outgoing/history/&YEARMONTH;
X mv &OUTPATH/cmf/outgoing/DR*.* &OUTPATH/cmf/outgoing/history/&YEARMONTH;
