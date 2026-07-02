***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  POST_ECL_PROVISIONS
*  
*  Purpose: Load POST_ECL_PROVISIONS
*
*  Frequency: Quarter End runs
*
*  Notes:  
*  		  
*
*	Change Log:
*	2021-09-21: Hadi Dimashkieh - Initial Development
*   2022-01-20: Hadi Dimashkieh - Changed format of source file
*
*
***************************************************************************************************************************;


%rrap_dt4_autoexec();

proc sql noprint;
	select catx('_',a.FNCL_YR,(catx('','Q',a.FNCL_QTR_CD))) into :qtrend
	from nzrrap.tm_dim a
	where tm_lvl='Month' and tm_id = &mth_tm_id.
	order by 1;
quit;

%let qtrend=&qtrend.;
%put QTREND=&qtrend.;
%let sourcefile=&owftp./Provisions_Post_ECL_RRAP_&qtrend..csv;
%put sourcefile=&sourcefile.;

%let TARGET=POST_ECL_PROVISIONS;

%let key_fields = Products Security_Type_Desc;
%let digest_fields =;



PROC IMPORT DATAFILE="&sourcefile."
OUT=&TARGET.(rename=('Security Type Desc'n=Security_Type_Desc 'ECL Drawn 1 Adj'n=ECL_Drawn_1_Adj 'ECL Drawn 2 Adj'n=ECL_Drawn_2_Adj 'ECL Drawn 3 Adj'n=ECL_Drawn_3_Adj 'ECL Undrawn 1 Adj'n=ECL_Undrawn_1_Adj 'ECL Undrawn 2 Adj'n=ECL_Undrawn_2_Adj 'ECL Undrawn 3 Adj'n=ECL_Undrawn_3_Adj ))
DBMS=CSV
REPLACE;
GETNAMES=YES;
guessingrows=max;
RUN;


data &TARGET.;
retain mth_tm_id;
	set &TARGET.;
	if missing(Products) and missing(Security_Type_Desc) then delete;
	mth_tm_id = &mth_tm_id.;
	format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.6;
	INSRT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
	UPDT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
run;

proc sort data=&TARGET.; by &key_fields.; run;

proc sql;
connect using nzrrap as nzcon;
execute(delete from &RRAP_DB..&TARGET. where mth_tm_id = &mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
quit;

proc append base=nzrrap.&TARGET.(BULKLOAD=YES BL_METHOD=CLILOAD) data=&TARGET.; run;




