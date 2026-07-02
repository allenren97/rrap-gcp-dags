
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);


%GLOBAL YEARMONTH;
%GLOBAL PRELOAD_DATA_COUNT;
%GLOBAL SRCCOUNT00;
%GLOBAL TGTCOUNT00;
%LET INPUT00=EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT;
%LET TGT00=EDRRAPT.BASEL_ECONMC_CAPTL_NCR_EXTR;

/*Fetch the Year and Month from TM_DIM*/
%macro rrap_db2yearmonth_initialize;
proc sql noprint;
create table _temp_yearmonth as 
select clndr_yr, tm_yr_seq_num, '00' as charmonth, '000000' as yearmonth from 
EDRTLRT.TM_DIM where TM_ID = &MTH_TM_ID;
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
/*Save the string YYYYMM for the latest MTH_TM_ID fetched from TM_DIM into a 
variable*/
proc sql noprint;
select yearmonth into :yearmonth from _temp_yearmonth;
quit;
%mend rrap_db2yearmonth_initialize;

/*This transformation accepts the target table to be loaded as an input 
parameter
and checks existence of data for the processing MTH_TM_ID */
%MACRO preload_data_check;

PROC SQL NOPRINT;
SELECT COUNT(*) INTO :PRELOAD_DATA_COUNT FROM &TGT00. WHERE MTH_END_DT=(SELECT 
TM_LVL_END_DT FROM EDRTLRT.TM_DIM WHERE TM_ID=&MTH_TM_ID);
QUIT;

%IF &PRELOAD_DATA_COUNT > 0 %THEN %DO;
%PUT TARGET TABLE &TGT00. ALREADY HAS &PRELOAD_DATA_COUNT RECORDS FOR CURRENT 
PROCESSING MONTH ID &MTH_TM_ID;
%END;
%MEND preload_data_check;

/*This transformation generates the input and output table counts*/
%MACRO datalog;
PROC SQL NOPRINT;
SELECT COUNT(*) INTO :SRCCOUNT00 FROM &INPUT00. WHERE MTH_TM_ID=&MTH_TM_ID.;
SELECT COUNT(*) INTO :TGTCOUNT00 FROM &TGT00. WHERE MTH_END_DT=(SELECT 
TM_LVL_END_DT FROM EDRTLRT.TM_DIM WHERE TM_ID=&MTH_TM_ID);
QUIT;
%MEND;

/*CLEANUP MACRO*/
/*THIS MACRO DELETES ALL TEMPORARY SAS DATASETS LISTED BY THE USER AT THE 
SPECIFIED PATH ON AIX PLATFORM*/
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

/*MACRO TO FETCH THE RECORD COUNT FROM THE TARGET TABLE FOR THE PROCESSING 
MONTH*/
%preload_data_check;

/*==========================================================================* 
 * Step:            Conditional Start                     A57ZQ4A4.BO0002TX * 
 * Transform:       Conditional Start                                       * 
 * Description:     If Preload count > 0 dont load the target               * 
 *==========================================================================*/

PROC SQL;
DELETE FROM DDB2CON.BASEL_ECONMC_CAPTL_NCR_EXTR WHERE MTH_END_DT="&MTH_END_DT"d;
QUIT;

proc sql;
create table INPATH._temp_rrap_eco_ncr_captl_extr as
select
("&MTH_END_DT"d) as MTH_END_DT length = 8
   format = date.
   informat = date.
   label = 'MTH_END_DT',
PRD_ID,
PIT_STAT_CD,
NCR_EXPSR_CL_KEY_VAL,
ASST_CL_NUM,
ROUND((SUM(
   CASE 
   	WHEN SRC_SYS_CD in ('KS' ,'MOR', 'TNG-MOR') THEN ADJUSTED_OS_BAL_AMT
	WHEN SRC_SYS_CD='SPL' THEN (ADJUSTED_OS_BAL_AMT /*+ UNADJUSTED_ADD_ON_BAL_AMT*/)
   END
   )),0.01) as ADJUSTED_OS_BAL_AMT length = 8
   label = 'ADJUSTED_OS_BAL_AMT',
(DATETIME()) as INSRT_PROCESS_TMSTMP length = 8
   format = DATETIME25.6
   informat = DATETIME25.6
   label = 'INSRT_PROCESS_TMSTMP',
(DATETIME()) as UPDT_PROCESS_TMSTMP length = 8
   format = DATETIME25.6
   informat = DATETIME25.6
   label = 'UPDT_PROCESS_TMSTMP'
from &INPATH..BASEL_ANALYTCL_BL_INSTRMNT_FACT
where SML_BUS_F='N' & CONSM_PRD_TREATMNT_CD='A' & PIT_STAT_CD in ('CUR','DEF') 
& TRNST_EXCLSN_F='N' & MTH_TM_ID=&MTH_TM_ID
group by
   MTH_END_DT,
   PRD_ID,
   PIT_STAT_CD,
   NCR_EXPSR_CL_KEY_VAL,
   ASST_CL_NUM
;
quit;


proc sql;
create view &INPATH.._temp_rrap_eco_ncr_captl_extr_v as
select
MTH_END_DT
format = DATE9.
informat = DATE9.,
PRD_ID,
PIT_STAT_CD as PT_IN_TM_STAT_CD,
NCR_EXPSR_CL_KEY_VAL,
ASST_CL_NUM,
ADJUSTED_OS_BAL_AMT
format = 22.2
informat = 22.2,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP
from &INPATH.._temp_rrap_eco_ncr_captl_extr
;
quit;



proc append base = EDRRAPT.BASEL_ECONMC_CAPTL_NCR_EXTR (BULKLOAD=YES DBCOMMIT=10000 BL_METHOD=CLILOAD) 
   data = &INPATH.._temp_rrap_eco_ncr_captl_extr_v  force ; 
run; 


%PUT 
PROCESSING MONTH IS &MTH_TM_ID 
YEARMONTH IS &YEARMONTH 
MONTH END DATE IS "&MTH_END_DT"d 
PRELOAD DATA COUNT IN TARGET FOR &MTH_TM_ID IS &PRELOAD_DATA_COUNT. 
&INPUT00. COUNT IS &SRCCOUNT00 
&TGT00. COUNT IS &TGTCOUNT00 ;


%PUT etls_endTime = %sysfunc(datetime(),datetime.);



