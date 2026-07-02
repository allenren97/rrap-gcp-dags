
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);



%GLOBAL YEARMONTH;
%GLOBAL PRELOAD_DATA_COUNT;
%GLOBAL SRCCOUNT00;
%GLOBAL TGTCOUNT00;
%LET INPUT00=EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT;
%LET TGT00=EDRRAPT.BASEL_RPTG_REVL_CR_SCRD_OT_MORT;
proc sql;
delete from EDRRAPT.BASEL_RPTG_REVL_CR_SCRD_OT_MORT where MTH_TM_ID=&MTH_TM_ID;
quit;


/*Fetch the Year and Month from TM_DIM*/
%macro rrap_db2yearmonth_initialize;
proc sql noprint;
	CREATE table _temp_yearmonth as 
	select clndr_yr, tm_yr_seq_num, '00' as charmonth, '000000' as yearmonth from EDRTLRT.TM_DIM where TM_ID = &MTH_TM_ID;

quit;

/*Convert the integer values to char values as needed*/
data _temp_yearmonth;
set _temp_yearmonth;
if tm_yr_seq_num < 10 then do;
	charmonth='0'||put(tm_yr_seq_num,1.);
end;
else do;
charmonth=tm_yr_seq_num;
end;
yearmonth=clndr_yr||charmonth;
	format yearmonth $char6.;
run;

/*Save the string YYYYMM for the latest MTH_TM_ID fetched from TM_DIM into a variable*/
proc sql noprint;
select yearmonth into :yearmonth from _temp_yearmonth;
quit;
%mend rrap_db2yearmonth_initialize;


/*This transformation accepts the target table to be loaded as an input parameter
and checks existence of data for the processing MTH_TM_ID */
%MACRO preload_data_check;

/*FETCH MONTH END DATE BASED ON MTH_TM_ID*/
PROC SQL NOPRINT;
SELECT TM_LVL_END_DT INTO :MTH_END_DT FROM EDRTLRT.TM_DIM WHERE TM_ID=&MTH_TM_ID;
QUIT;

PROC SQL NOPRINT;
SELECT COUNT(*) INTO :PRELOAD_DATA_COUNT FROM &TGT00. WHERE MTH_TM_ID=&MTH_TM_ID;
QUIT;

%IF &PRELOAD_DATA_COUNT > 0 %THEN %DO;
%PUT TARGET TABLE &TGT00. ALREADY HAS &PRELOAD_DATA_COUNT RECORDS FOR CURRENT PROCESSING MONTH ID &MTH_TM_ID;

%END;
%MEND preload_data_check;

/*This transformation generates the input and output table counts*/
%MACRO datalog;
PROC SQL NOPRINT;
SELECT COUNT(*) INTO :SRCCOUNT00 FROM &INPUT00. WHERE MTH_TM_ID=&MTH_TM_ID.;
SELECT COUNT(*) INTO :TGTCOUNT00 FROM &TGT00. WHERE MTH_TM_ID=&MTH_TM_ID.;
QUIT;
%MEND;


/*CLEANUP MACRO*/
/*THIS MACRO DELETES ALL TEMPORARY SAS DATASETS LISTED BY THE USER AT THE SPECIFIED PATH ON AIX PLATFORM*/
/*LIBREF SHOULD BE THE LIBREF DECLARED FOR A UNIX PATH IN THE PROGRAM*/
/*DATASETS SHOULD BE THE LIST OF DATASETS SEPARATED BY A SINGLE SPACE*/
%MACRO SAS_DATASET_CLEANUP (LIBREF=, DATASETS=);

/*DELETE TEMPORARY DATASETS*/
PROC DATASETS LIBRARY=&LIBREF MEMTYPE=ALL;
DELETE &DATASETS;
RUN;
QUIT;

%MEND;

/*Macro for fetching the yearmonth in an integer format*/
%rrap_db2yearmonth_initialize;

%PUT YEARMONTH IS &YEARMONTH;

/*MACRO TO FETCH THE RECORD COUNT FROM THE TARGET TABLE FOR THE PROCESSING MONTH*/
%preload_data_check;

%PUT &PRELOAD_DATA_COUNT;


/*==========================================================================* 
 * Step:            Conditional Start                     A57ZQ4A4.BO0002TF * 
 * Transform:       Conditional Start                                       * 
 * Description:     If Preload count > 0 dont load the target               * 
 *==========================================================================*/ 

%macro etls_conditionW2VGVPRI;
   %local etls_condition;
   %let etls_conditionTrue = %eval(&PRELOAD_DATA_COUNT = 0);
   %if (&etls_conditionTrue=0) %then
   %do;
      %put ETLS_DIAG: Condition flow did NOT execute, condition was &PRELOAD_DATA_COUNT = 0;
      %goto exitetls_conditionW2VGVPRI;
   %end;
   %else
   %do;
      %put ETLS_DIAG: Condition flow did execute, condition was &PRELOAD_DATA_COUNT = 0;
   %end;

proc sql noprint;
   create table WORK._temp_revl_cr_ot_mort as
      select
         A.MTH_TM_ID,
		 A.SCRTY_TP_CD,
		 A.PRD_ID,
		 A.PRD_CD AS REVLVNG_CR_PRD_CD,
         A.SUB_PRD_CD AS REVLVNG_CR_SUB_PRD_CD,
		 COALESCE(B.SCRTY_DESC,,'') AS SCRTY_DESC,
		 A.SRC_PRD_DESC AS REVLVNG_CR_PRD_DESC,
         (COUNT(A.BASEL_ACCT_ID)) as ACCT_CNT length = 8
               format = 11.
               informat = 11.,
         (SUM(A.ADJUSTED_OS_BAL_AMT)) as ADJUSTED_OS_BAL_AMT length = 8
               format = 22.8
               informat = 22.8,
		 (&SESSIONTIME) as INSRT_PROCESS_TMSTMP length = 8
               format = DATETIME25.6
               informat = DATETIME25.6,
         (&SESSIONTIME) as UPDT_PROCESS_TMSTMP length = 8
               format = DATETIME25.6
               informat = DATETIME25.6
    from edrrapt.BASEL_ANALYTCL_BL_INSTRMNT_FACT A
	left join &INPATH..BASEL_SCRTY_TP_LKP B
         on
         (
            A.SCRTY_TP_CD = B.SCRTY_TP_CD
            and INPUT(B.EFF_FROM_YR_MTH,6.) <= &YEARMONTH
            and INPUT(B.EFF_TO_YR_MTH ,6.) >= &YEARMONTH
         )
	where A.SML_BUS_F='N'  & A.CONSM_PRD_TREATMNT_CD='A'  & A.PIT_STAT_CD in ('CUR','DEF') & A.TRNST_EXCLSN_F='N'  
         & A.SCRTY_TP_DESC <> 'Unsecured' 
         & A.SRC_SYS_CD = 'KS' & A.MTH_TM_ID = &MTH_TM_ID
	GROUP BY 
	A.MTH_TM_ID, A.SCRTY_TP_CD, A.PRD_ID, A.PRD_CD, A.SUB_PRD_CD, B.SCRTY_DESC, A.SRC_PRD_DESC;
quit;
   
proc append base = EDRRAPT.BASEL_RPTG_REVL_CR_SCRD_OT_MORT (BULKLOAD=YES BL_METHOD=CLILOAD)
     data = work._temp_revl_cr_ot_mort force ; 
run; 

proc sql;
connect using edrrapt as db1con;
execute(
UPDATE edrrapt.BASEL_RPTG_REVL_CR_SCRD_OT_MORT SET SCRTY_DESC='' WHERE SCRTY_DESC IS NULL
)by db1con;
disconnect from db1con;
quit;

%datalog;
%PUT 
PROCESSING MONTH IS &MTH_TM_ID 
YEARMONTH IS &YEARMONTH 
PRELOAD DATA COUNT IN TARGET FOR &MTH_TM_ID IS &PRELOAD_DATA_COUNT. 
&INPUT00. COUNT IS &SRCCOUNT00 
&TGT00. COUNT IS &TGTCOUNT00 ;

%exitetls_conditionW2VGVPRI:
%mend etls_conditionW2VGVPRI;

%etls_conditionW2VGVPRI;

%let etls_endTime = %sysfunc(datetime(),datetime.);

