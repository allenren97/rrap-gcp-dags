***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  RPTG_RISK_RT_SYS_DIM
*  
*  Purpose: Load RPTG_RISK_RT_SYS_DIM 
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  		  
*
*	Change Log:
*	2021-10-26: Hadi Dimashkieh - Initial Development
*
*
*
***************************************************************************************************************************;

%rrap_dt4_autoexec();


%let sourcefile=&owftp./rrap/lookup/rrm_edwext_ncr_risk_rt_sys_dim_lookup_f_adhoc.csv;
/*%let sourcefile=/u/s2809211/projects/rrap/dimensions/opsfiles/rrm_edwext_ncr_risk_rt_sys_dim_lookup_f_adhoc.csv;*/



%let TARGET=RPTG_RISK_RT_SYS_DIM;

%let key_fields = NCR_RISK_RT_KEY_VAL NCR_RISK_RT_DESC;
%let digest_fields = 1,1; *NCR_RISK_RT_KEY_VAL, NCR_RISK_RT_DESC;

%let surrogate_key_flag = Y;
%let surrogate_key = NCR_RISK_RT_SYS_ID;
%let initial_surrogate_key_value = 5000;


DATA &TARGET./*(where=(input(DT4_EXPSR_CL_KEY_VAL,4.) GE 5501 or input(DT4_EXPSR_CL_KEY_VAL,4.) EQ 599))*/;
    LENGTH
        NCR_RISK_RT_KEY_VAL $ 4
        NCR_RISK_RT_DESC $ 255;
    FORMAT
        NCR_RISK_RT_KEY_VAL $4.
        NCR_RISK_RT_DESC $255. ;
    INFORMAT
        NCR_RISK_RT_KEY_VAL $4.
        NCR_RISK_RT_DESC $255. ;
    INFILE "&sourcefile."
        LRECL=32767
        FIRSTOBS=2
        ENCODING="LATIN1"
        DLM='2c'x
        MISSOVER
        DSD ;
    INPUT
        NCR_RISK_RT_KEY_VAL : $4.
        NCR_RISK_RT_DESC : $255.;

RUN;

/*data &target.;*/
/*	set &target.;*/
/*	where input(NCR_RISK_RT_KEY_VAL,4.) not between 208 and 215 ;*/
/*run;*/

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
EFF_FROM_YR_MTH
EFF_TO_YR_MTH
; 
run;

/********************************************************************************************************************/
/********************************************************************************************************************/

%DT4_ETL_SCD2_LOAD();

/********************************************************************************************************************/
/********************************************************************************************************************/



/*
DROP TABLE EDRTLRD3D.RPTG_RISK_RT_SYS_DIM IF EXISTS; COMMIT;

CREATE TABLE EDRTLRD3D.RPTG_RISK_RT_SYS_DIM  (
		NCR_RISK_RT_SYS_ID INTEGER NOT NULL, 
		NCR_RISK_RT_KEY_VAL CHAR(4 OCTETS), 
		NCR_RISK_RT_DESC VARCHAR(255 OCTETS), 
		EFF_TO_YR_MTH CHAR(6 OCTETS), 
		EFF_FROM_YR_MTH CHAR(6 OCTETS), 
		CRNT_F CHAR(1 OCTETS) NOT NULL, 
		INSRT_PROCESS_TMSTMP TIMESTAMP NOT NULL, 
		UPDT_PROCESS_TMSTMP TIMESTAMP
	)
	ORGANIZE BY COLUMN
	DATA CAPTURE NONE 
	DISTRIBUTE BY HASH (NCR_RISK_RT_SYS_ID); COMMIT;
*/
