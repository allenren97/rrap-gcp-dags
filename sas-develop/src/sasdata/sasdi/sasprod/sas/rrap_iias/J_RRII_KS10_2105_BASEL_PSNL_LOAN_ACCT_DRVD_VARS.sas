/**************************************************************************************** 
* INFA Job Name			:wf_DM_RRAP_Load_BASEL_PSNL_LOAN_ACCT_DRVD_VARS					*
* SAS Job Name			:J_RRII_KS10_2105_BASEL_PSNL_LOAN_ACCT_DRVD_VARS				* 
* Description			: INFA to SAS Conversion - Initial version reviewed and updated	* 
*           			   address mismatch in column length between PROD and SAS output*
* Source Database/Schema: BLUDBPRD / EDRTLRP1D                                 			* 
* Source Table Name		: BASEL_PSNL_LOAN_MTH_SNAPSHOT BASEL_ACCT_DIM TRNST_EXCLSN_LKP	*
* Target Database/Schema: BLUDBPRD / EDRTLRP1D											*
* Target Table Name 	: BASEL_PSNL_LOAN_ACCT_DRVD_VARS								*
* SAS Code Location		: /sasdata/sasdi/sasprod/sas/rrap_iias							* 
* Created on			: Thursday, March 03, 2021 2:36:46 PM EDT             			* 
* Created by			: Khalid                                              			* 
* Updated on			: Wednesday, March 03, 2022										*
* Updated by			: Vijay Kadiyala												*
* Version				: SAS Enterprise Guide 7.1										*	
****************************************************************************************/ 

%put WORK LOCATION: %sysfunc(getoption(work));
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);


DATA _NULL_;
	CALL SYMPUTX('PROCESSSTARTTIME',PUT(DATETIME(),DATETIME25.0));
RUN;

%PUT NOTE: ******* PROCESS START TIME: &PROCESSSTARTTIME. *******;

%let TM_ID=&MTH_TM_ID;


%Put Info: Curent TMID: &TM_ID;
%Put Info: PROCESSSTARTTIME : 	&PROCESSSTARTTIME.;

        	
PROC SQL noprint;
         CONNECT USING NZRRAP AS NZCON;
     create table  S_BASEL_PSNL_LOAN_MTH_SNAPSHOT as
     select *
     from connection to NZCON
     ( 	SELECT 	a.*,
         	b.acct_num,
         	c.EXCLUDED_TRNST_NUM,
         	round(TOT_CRNT_BAL_AMT + ADD_ON_BAL_AMT + ACCR_INTR,3) as OS_BAL_AMT
     FROM  		&net_db..BASEL_PSNL_LOAN_MTH_SNAPSHOT a
     left join 	&net_db..BASEL_ACCT_DIM b on a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
     left join 	&net_db..TRNST_EXCLSN_LKP c on CRNT_BR_LOCTN_TRNST=EXCLUDED_TRNST_NUM
     WHERE MTH_TM_ID=&TM_ID.
     )
     ;

     disconnect from NZCON;
 quit;


data LD_BASEL_PSNL_LOAN_DRV_VARS (keep= 
	MTH_TM_ID BASEL_ACCT_ID BASEL_CUST_ID ACCT_NUM CONSM_PRD_TREATMNT_CD OS_BAL_AMT PIT_STAT_VER_1_CD STEP_F TRNST_EXCLSN_F INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP);
/*	retain MTH_TM_ID BASEL_ACCT_ID	BASEL_CUST_ID ACCT_NUM CONSM_PRD_TREATMNT_CD OS_BAL_AMT PIT_STAT_VER_1_CD STEP_F TRNST_EXCLSN_F	INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP;*/
	length CONSM_PRD_TREATMNT_CD $10. PIT_STAT_VER_1_CD $10.;
	set S_BASEL_PSNL_LOAN_MTH_SNAPSHOT;
	format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.6 MSG $100.;

	if STEP_PLN_SNAPSHOT_ID>0 then do; STEP_F='Y'; end;
	else do; STEP_F ='N'; end;

	if   	strip(RECD_STAT_CD)='4' AND DAY_ODUE<=90 then do; PIT_STAT_VER_1_CD= 'CUR'; end;
	else if	strip(RECD_STAT_CD)='4' AND DAY_ODUE>90  then do; PIT_STAT_VER_1_CD= 'DEF'; end;
	else if	strip(RECD_STAT_CD)='5' then do; PIT_STAT_VER_1_CD= 'DEF'; end;
	else if strip(RECD_STAT_CD)='6' then do; PIT_STAT_VER_1_CD= 'CHG'; end;
	else if strip(RECD_STAT_CD)='7' then do; PIT_STAT_VER_1_CD= 'CHG'; end;
	else if strip(RECD_STAT_CD)='8' then do; PIT_STAT_VER_1_CD= 'CHG'; end; 

	if strip(EXCLUDED_TRNST_NUM) eq '' then do; TRNST_EXCLSN_F='N'; end; 
	else do; TRNST_EXCLSN_F='Y'; end;

	if TRNST_EXCLSN_F='Y' or OS_BAL_AMT le 0 then do; CONSM_PRD_TREATMNT_CD=    'Z'; end; 
	else do;  CONSM_PRD_TREATMNT_CD= 'A'; end;
	INSRT_PROCESS_TMSTMP="&SYSDATE9.:&SYSTIME."dt   ;
	UPDT_PROCESS_TMSTMP ="&SYSDATE9.:&SYSTIME."dt ;
	BASEL_CUST_ID=PRIM_BASEL_CUST_ID;

	if strip(ACCT_NUM) eq '' then do;
		MSG= 'Missing BASEL_ACCT_ID='||BASEL_ACCT_ID|| ' in BASEL_ACCT_DIM Table';
		put MSG;
		abort cancel;
	end;
run;


 PROC SQL NOPRINT;
 		CONNECT USING NZRRAP AS NZCON;
        EXECUTE(DELETE FROM &net_db..BASEL_PSNL_LOAN_ACCT_DRVD_VARS WHERE MTH_TM_ID=&tm_id.) BY NZCON;
 QUIT;

 proc append base=NZRRAP.BASEL_PSNL_LOAN_ACCT_DRVD_VARS data=LD_BASEL_PSNL_LOAN_DRV_VARS force; run;

DATA _NULL_;
	CALL SYMPUTX('PROCESSENDTIME',PUT(DATETIME(),DATETIME25.0));
	CALL SYMPUTX('PROCESSRUNTIME',PUT((DATETIME()-"&PROCESSSTARTTIME"dt),TIME.));
RUN;

data _null_;
	if 0 then set S_BASEL_PSNL_LOAN_MTH_SNAPSHOT Nobs=Number_of_Obs; 
	call symputx('SourceRec',Number_of_Obs); 
	stop; 
run;
%put Source=&SourceRec.;

data _null_;
	if 0 then set LD_BASEL_PSNL_LOAN_DRV_VARS Nobs=Number_of_Obs; 
	call symputx('TargetRec',Number_of_Obs); 
	stop; 
run;

DATA _NULL_;
	CALL SYMPUTX('PROCESSENDTIME',PUT(DATETIME(),DATETIME25.0));
	CALL SYMPUTX('PROCESSRUNTIME',PUT((DATETIME()-"&PROCESSSTARTTIME"dt),TIME.));
RUN;

PROC SQL NOPRINT;
	    insert into NZRRAP.AUDIT_JOB_TIMER_CHECK (Job_name, MTH_TM_ID, START_Time, End_Time, Source_count, Target_Count)
		Values( 'J_RRII_KS10_2105_BASEL_PSNL_LOAN_ACCT_DRVD_VARS',
			&TM_ID.,
			"&PROCESSSTARTTIME"dt,
			"&PROCESSENDTIME"dt,
			&SourceRec.,
			&TargetRec.
		);
QUIT;

/**************************** END ******************************/
	
