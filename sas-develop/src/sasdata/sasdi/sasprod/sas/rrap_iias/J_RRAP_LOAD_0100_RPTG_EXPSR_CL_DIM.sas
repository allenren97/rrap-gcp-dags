***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  RPTG_EXPSR_CL_DIM
*  
*  Purpose: Load RPTG_EXPSR_CL_DIM 
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  		  
*
*	Change Log:
*	2021-09-21: Hadi Dimashkieh - Initial Development
*
*
*
***************************************************************************************************************************;

%rrap_dt4_autoexec();


%let sourcefile=&owftp./rrm/rrm_ncr_exposure_class_f_adhoc.del;



%let TARGET=RPTG_EXPSR_CL_DIM;

%let key_fields = NCR_EXPSR_CL_KEY_VAL ;
%let digest_fields = NCR_EXPSR_CL_DESC, FRS_CD, BCAR_SCHED_NUM, CCAR_EXPSR_CL_NM;

%let surrogate_key_flag = Y;
%let surrogate_key = EXPSR_CL_ID;
%let initial_surrogate_key_value = 10000;


DATA &TARGET./*(where=(input(DT4_EXPSR_CL_KEY_VAL,4.) GE 5501 or input(DT4_EXPSR_CL_KEY_VAL,4.) EQ 599))*/;
    LENGTH
        NCR_EXPSR_CL_KEY_VAL $ 4
        NCR_EXPSR_CL_DESC $ 255
        CCAR_EXPSR_CL_NM $ 20
        BCAR_SCHED_NUM   $ 10
        FRS_CD           $ 10 ;
    DROP
        'BCAR Exposure class Name'n ;
    FORMAT
        NCR_EXPSR_CL_KEY_VAL $4.
        NCR_EXPSR_CL_DESC $255.
        CCAR_EXPSR_CL_NM $20.
        BCAR_SCHED_NUM   $10.
        FRS_CD           $10. ;
    INFORMAT
        NCR_EXPSR_CL_KEY_VAL $4.
        NCR_EXPSR_CL_DESC $255.
        CCAR_EXPSR_CL_NM $20.
        BCAR_SCHED_NUM   $10.
        FRS_CD           $10. ;
    INFILE "&sourcefile."
        LRECL=32767
        FIRSTOBS=2
        ENCODING="LATIN1"
        DLM='2c'x
        MISSOVER
        DSD ;
    INPUT
        NCR_EXPSR_CL_KEY_VAL : $4.
        NCR_EXPSR_CL_DESC : $255.
        CCAR_EXPSR_CL_NM : $20.
        BCAR_SCHED_NUM   : $10.
        'BCAR Exposure class Name'n : $1.
        FRS_CD           : $10. ;

RUN;

proc sort data=&target. dupout=duplicates nodupkey; by &key_fields.; run;



proc sql noprint;
	select count(1) into :count_dups from duplicates;
quit;

%macro abort_process;
	%if &count_dups. NE 0 %then %do;
		%PUT;
		%PUT DUPLICATES IN SOURCE FILE. PLEASE FIX BEFORE PROCEEDING.;
		%PUT;
		%abort;
	%end;
%mend abort_process;
%abort_process;

data varnames;
length vars $ 20;
input vars $;
datalines;
NCR_EXPSR_CL_DESC
FRS_CD
BCAR_SCHED_NUM
CCAR_EXPSR_CL_NM
EFF_FROM_YR_MTH
EFF_TO_YR_MTH
; 
run;

/********************************************************************************************************************/
/********************************************************************************************************************/

%DT4_ETL_SCD2_LOAD();

/********************************************************************************************************************/
/********************************************************************************************************************/



