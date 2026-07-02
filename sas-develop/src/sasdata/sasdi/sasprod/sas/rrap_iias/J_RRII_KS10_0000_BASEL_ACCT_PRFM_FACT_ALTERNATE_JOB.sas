***************************************************************************************************************************;
*  Job Name : J_RRII_KS10_0000_BASEL_ACCT_PRFM_FACT.sas
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  BASEL_ACCT_PRFM_FACT  
*  
*  Purpose: Load the Basel Account Performance Fact table
*
*  Frequency: Monthly
*
*  Change Log:  2022-10-20: N Grewal - Initial Development
*				2022-11-15: Replaced loan_amt_at_blk_ins with loan_amt_at_insured_date
*				2022-12-12: Add mth_tm_id, basel_acct_id, updt_process_tmstmp (RRMSS-1663)
*				2022-12-22: Ganesh P: To remove Duplicate Accounts from BASEL_ACCT_PRFM_FACT (RRMSS-1857)
*				2023-01-10: Ganesh P: To add Column CRNT_PRPTY_VAL_AMT, crnt_ltv_rto, clp_flag (RRMSS-1891 AND RRMSS-1553)
*				2023-01-11: Ganesh P: Added logic to send Duplicate Acct_Num notification to team, 
*									  code will abort upon duplicate acct_num
*
*
***************************************************************************************************************************;

%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);
%LET CONNECTION_STRING=%STR(DATABASE=BLUDBPRD authdomain="IIAS_Auth");
%PUT &=CONNECTION_STRING;


/* Get current month end date*/
proc sql noprint;
select cats("'",put(tm_lvl_end_dt,yymmdd10.),"'") into :MTH_END_DT from NZRRAP.TM_DIM WHERE TM_ID = &MTH_TM_ID. and tm_lvl='Month';
quit;

%put &MTH_TM_ID.;
%put &MTH_END_DT.;

DATA basel_prfm_fact_edl;
	LENGTH
		mth_tm_id        $ 11
		'mth_end_dt'n    8
		src_sys_cd       $ 30
		basel_acct_id     8
		acct_num           $ 80
		proc_transit      $ 5
		serv_transit      $ 5
		prd_cd           $ 6
		currency_cd      $ 3
		heloc_ind        $ 1
		ltv_bckt_cd      $ 30
		ltv_rto            8
		loan_amt_at_insured_date   8
		rntl_incm_dpndcy_f $ 1
		trnsctr_ind      $ 1
		currency_mismatch_f $ 1
		tot_expsr_above_1500k_lmt_f $ 1
		rntl_incm_amt      8
		grs_incm_amt       8
		prch_prc_amt       8
		aprsd_val_amt      8
		prpty_val_amt      8
		undrawn_amt        8
		crnt_auth_lmt_amt   8
		occupancy_type_cd $ 1
		os_bal_amt         8
		crnt_ltv_rto       8
		clp_flag         $ 1
		crnt_prpty_val_amt   8
		insrt_process_tmstmp   8
		updt_process_tmstmp   8;
	FORMAT
		mth_tm_id        $CHAR11.
		'mth_end_dt'n  YYMMDD10.
		src_sys_cd       $CHAR30.
		basel_acct_id    BEST20.
		acct_num         $CHAR80.
		proc_transit     $CHAR5.
		serv_transit     $CHAR5.
		prd_cd           $CHAR6.
		currency_cd      $CHAR3.
		heloc_ind        $CHAR1.
		ltv_bckt_cd      $CHAR30.
		ltv_rto          BEST15.
		loan_amt_at_insured_date BEST22.
		rntl_incm_dpndcy_f $CHAR1.
		trnsctr_ind      $CHAR1.
		currency_mismatch_f $CHAR1.
		tot_expsr_above_1500k_lmt_f $CHAR1.
		rntl_incm_amt    BEST22.
		grs_incm_amt     BEST22.
		prch_prc_amt     BEST22.
		aprsd_val_amt    BEST22.
		prpty_val_amt    BEST22.
		undrawn_amt      BEST22.
		crnt_auth_lmt_amt BEST22.
		occupancy_type_cd $CHAR1.
		os_bal_amt       BEST22.
		crnt_ltv_rto     BEST15.
		clp_flag         $CHAR1.
		crnt_prpty_val_amt BEST22.
		insrt_process_tmstmp DATETIME18.
		updt_process_tmstmp DATETIME18.;
	INFORMAT
		mth_tm_id        $CHAR11.
		'mth_end_dt'n  YYMMDD10.
		src_sys_cd       $CHAR30.
		basel_acct_id    BEST20.
		acct_num         $CHAR80.
		proc_transit     $CHAR5.
		serv_transit     $CHAR5.
		prd_cd           $CHAR6.
		currency_cd      $CHAR3.
		heloc_ind        $CHAR1.
		ltv_bckt_cd      $CHAR30.
		ltv_rto          BEST15.
		loan_amt_at_insured_date BEST22.
		rntl_incm_dpndcy_f $CHAR1.
		trnsctr_ind      $CHAR1.
		currency_mismatch_f $CHAR1.
		tot_expsr_above_1500k_lmt_f $CHAR1.
		rntl_incm_amt    BEST22.
		grs_incm_amt     BEST22.
		prch_prc_amt     BEST22.
		aprsd_val_amt    BEST22.
		prpty_val_amt    BEST22.
		undrawn_amt      BEST22.
		crnt_auth_lmt_amt BEST22.
		occupancy_type_cd $CHAR1.
		os_bal_amt       BEST22.
		crnt_ltv_rto     BEST15.
		clp_flag         $CHAR1.
		crnt_prpty_val_amt BEST22.
		insrt_process_tmstmp DATETIME18.
		updt_process_tmstmp DATETIME18.;
	INFILE "/owpftp/apf_edl_&YEARMONTH..csv"
		firstobs=2
		MISSOVER
		DSD;
	INPUT
		mth_tm_id        : $CHAR11.
		'mth_end_dt'n  : ?? YYMMDD10.
		src_sys_cd       : $CHAR30.
		basel_acct_id    : BEST20.
		acct_num         : ?? $CHAR80.
		proc_transit     : ?? $CHAR5.
		serv_transit     : ?? $CHAR5.
		prd_cd           : $CHAR6.
		currency_cd      : $CHAR3.
		heloc_ind        : $CHAR1.
		ltv_bckt_cd      : $CHAR30.
		ltv_rto          : ?? COMMA15.
		loan_amt_at_insured_date : ?? COMMA22.
		rntl_incm_dpndcy_f : $CHAR1.
		trnsctr_ind      : $CHAR1.
		currency_mismatch_f : $CHAR1.
		tot_expsr_above_1500k_lmt_f : $CHAR1.
		rntl_incm_amt    : ?? BEST22.
		grs_incm_amt     : ?? COMMA22.
		prch_prc_amt     : ?? COMMA22.
		aprsd_val_amt    : ?? BEST22.
		prpty_val_amt    : ?? COMMA22.
		undrawn_amt      : ?? COMMA22.
		crnt_auth_lmt_amt : ?? COMMA22.
		occupancy_type_cd : $CHAR1.
		os_bal_amt       : ?? COMMA22.
		crnt_ltv_rto     : ?? COMMA15.
		clp_flag         : $CHAR1.
		crnt_prpty_val_amt : ?? COMMA22.
		insrt_process_tmstmp : ?? ANYDTDTM23.
		updt_process_tmstmp : ?? ANYDTDTM23.;
RUN;

/* Clean up */
proc sql;
	connect using NZRRAP as nzcon;
	EXECUTE(delete from &net_db..BASEL_ACCT_PRFM_FACT where mth_end_Dt=&mth_end_dt) by nzcon;
	disconnect from nzcon;
quit;


/* Append the current month to database*/
proc append base=NZRRAP.BASEL_ACCT_PRFM_FACT (bulkload=yes BL_METHOD=CLILOAD) 
data=basel_prfm_fact_edl force; run;




/*RRMSS-1857: To remove PURE Duplicate Accounts from BASEL_ACCT_PRFM_FACT*/
%LET DUPCOUNT1=;
	proc sql;
			CONNECT TO DB2 AS IIASCON(&CONNECTION_STRING);
				SELECT ACCT_NUM into :DUPCOUNT1 separated by ', '  FROM CONNECTION TO IIASCON 
				(SELECT ACCT_NUM,count(*) FROM &net_db..BASEL_ACCT_PRFM_FACT 
				where mth_end_Dt=&mth_end_dt. GROUP BY MTH_TM_ID,MTH_END_DT,SRC_SYS_CD,BASEL_ACCT_ID,ACCT_NUM,PROC_TRANSIT,SERV_TRANSIT,PRD_CD,CURRENCY_CD,HELOC_IND,LTV_BCKT_CD,LTV_RTO,
											LOAN_AMT_AT_INSURED_DATE,RNTL_INCM_DPNDCY_F,TRNSCTR_IND,CURRENCY_MISMATCH_F,TOT_EXPSR_ABOVE_1500K_LMT_F,RNTL_INCM_AMT,GRS_INCM_AMT,PRCH_PRC_AMT,
										    APRSD_VAL_AMT,PRPTY_VAL_AMT,UNDRAWN_AMT,CRNT_AUTH_LMT_AMT,OCCUPANCY_TYPE_CD,OS_BAL_AMT,CRNT_LTV_RTO,CLP_FLAG,CRNT_PRPTY_VAL_AMT
							HAVING count(*)>1);

	quit;
%PUT &DUPCOUNT1;
%macro Check_Dup1;

%if %length(&DUPCOUNT1)>0  %then %do;

options emailsys=smtp;
filename myemail email
	to=("grt-model-support@scotiabank.com" "edwsupport@scotiabank.com" ) 
	subject="DELETING PURE DUPLICATE ACCT_NUM in BASEL_ACCT_PRFM_FACT : &mth_end_dt."
	content_type="text/html";
ods html path= "&OUTPATH/rrap/" body=myemail style=htmlblue rs=none;
title;
Title j=left bold "Hi,";
Title2 j=left bold "DELETED PURE DUPLICATE ACCT_NUM in &net_db..BASEL_ACCT_PRFM_FACT";
Title3 j=center bold color=Red "The Month Time ID for this month is &MTH_TM_ID";
Footnote j=left bold "Please note: This is a system generated email and responses aren't monitored.";
ODS HTML TEXT= "&DUPCOUNT1 Duplicate ACCT_NUM Deleted &net_db..BASEL_ACCT_PRFM_FACT ";
ods html close;
filename myemail clear;


%put "Deleted duplicates ACCT_NUM &DUPCOUNT";
%end;
%mend;
%Check_Dup1

proc sql;
	connect using NZRRAP as nzcon;
	EXECUTE(
			DELETE FROM
			    (SELECT ROWNUMBER() OVER (PARTITION BY MTH_TM_ID,MTH_END_DT,SRC_SYS_CD,BASEL_ACCT_ID,ACCT_NUM,PROC_TRANSIT,SERV_TRANSIT,PRD_CD,CURRENCY_CD,HELOC_IND,LTV_BCKT_CD,LTV_RTO,
											LOAN_AMT_AT_INSURED_DATE,RNTL_INCM_DPNDCY_F,TRNSCTR_IND,CURRENCY_MISMATCH_F,TOT_EXPSR_ABOVE_1500K_LMT_F,RNTL_INCM_AMT,GRS_INCM_AMT,PRCH_PRC_AMT,
										    APRSD_VAL_AMT,PRPTY_VAL_AMT,UNDRAWN_AMT,CRNT_AUTH_LMT_AMT,OCCUPANCY_TYPE_CD,OS_BAL_AMT,CRNT_LTV_RTO,CLP_FLAG,CRNT_PRPTY_VAL_AMT
										  ) AS RN
			     FROM &net_db..BASEL_ACCT_PRFM_FACT WHERE mth_end_Dt=&mth_end_dt) AS A
			WHERE RN > 1;
			) by nzcon;
	disconnect from nzcon;
quit;

/* Code to find duplicates in BASEL_ACCT_PRFM_FACT Based on ACCT_NUM */
/* BELOW SECTION WILL ABORT THE CODE, INCASE THERE ARE DUPLICATE ACCT_NUM */
%LET DUPCOUNT=;
	proc sql;
			CONNECT TO DB2 AS IIASCON(&CONNECTION_STRING);
				SELECT ACCT_NUM into :DUPCOUNT separated by ', '  FROM CONNECTION TO IIASCON 
				(SELECT ACCT_NUM,count(*) FROM &net_db..BASEL_ACCT_PRFM_FACT 
				where mth_end_Dt=&mth_end_dt. GROUP BY ACCT_NUM HAVING count(*)>1);

	quit;
%PUT &DUPCOUNT;
%macro Check_Dup;

%if %length(&DUPCOUNT)>0  %then %do;

options emailsys=smtp;
filename myemail email
	to=("grt-model-support@scotiabank.com" "edwsupport@scotiabank.com" ) 
	subject="ABORTING JOB: Duplicate accounts in &net_db..BASEL_ACCT_PRFM_FACT : &mth_end_dt."
	content_type="text/html";
ods html path= "&OUTPATH/rrap/" body=myemail style=htmlblue rs=none;
title;
Title j=left bold "Hi,";
Title2 j=left bold "ABORTED JOB: Duplicate accounts in &net_db..BASEL_ACCT_PRFM_FACT";
Title3 j=center bold color=Red "The Month Time ID for this month is &MTH_TM_ID";
Footnote j=left bold "Please note: This is a system generated email and responses aren't monitored.";
ODS HTML TEXT= "&DUPCOUNT Duplicate ACCT_NUM under &net_db..BASEL_ACCT_PRFM_FACT ";
ods html close;
filename myemail clear;


%put "Aborting the job due to duplicates ACCT_NUM";
	%abort abend 255;
%end;
%mend;
%Check_Dup

/* derive mth_tm_id */
proc sql;
	connect using NZRRAP as nzcon;
	EXECUTE(
		UPDATE &net_db..BASEL_ACCT_PRFM_FACT a 
		SET a.mth_tm_id = (SELECT tm_id FROM &net_db..tm_dim WHERE TM_LVL_END_DT = a.mth_end_dt AND tm_lvl = 'Month')  
		WHERE a.mth_end_dt = &MTH_END_DT. 
			) by nzcon;
	disconnect from nzcon;
quit;

/* derive basel_acct_id for MOR */
proc sql;
	connect using NZRRAP as nzcon;
	EXECUTE(
		UPDATE &net_db..BASEL_ACCT_PRFM_FACT a 
		SET a.basel_acct_id = (SELECT gz.basel_acct_id FROM &net_db..BASEL_MORT_MTH_SNAPSHOT gz 
			WHERE gz.mth_tm_id = a.mth_tm_id AND TRIM(gz.mort_num) = a.acct_num) 
		WHERE a.mth_end_dt = &MTH_END_DT AND a.src_sys_cd = 'GZ'; 
			) by nzcon;
	disconnect from nzcon;
quit;

/* derive basel_acct_id for TNG */
proc sql;
	connect using NZRRAP as nzcon;
	EXECUTE(
		UPDATE &net_db..BASEL_ACCT_PRFM_FACT a 
		SET a.basel_acct_id = (SELECT tng.basel_acct_id FROM &net_db..BASEL_ACCT_DIM tng   
			WHERE TRIM(tng.src_app_cd) = 'TNG-MOR' AND tng.src_sys_del_f = 'N' AND tng.src_app_id = a.acct_num)
		WHERE a.mth_end_dt=&MTH_END_DT AND a.src_sys_cd in ('TNG_MTG','TNG_MCAP'); 
			) by nzcon;
	disconnect from nzcon;
quit;

/* derive basel_acct_id for SPL */
proc sql;
	connect using NZRRAP as nzcon;
	EXECUTE(
		UPDATE &net_db..BASEL_ACCT_PRFM_FACT E
		SET E.BASEL_ACCT_ID=D.ORIG_BASEL_ACCT_ID
		FROM
			(
				SELECT DISTINCT A.RLP_LOAN_NO AS ACCT_NUM ,A.ORIG_BASEL_ACCT_ID AS ORIG_BASEL_ACCT_ID
				FROM &net_db..RLP_TO_SL_ACCT_LIST a
			) D WHERE TRIM(L '0' FROM (E.ACCT_NUM))=TRIM(L '0' FROM D.ACCT_NUM)
			AND E.MTH_TM_ID=&MTH_TM_ID) BY nzcon;
	disconnect from nzcon;
quit;

proc sql;
	connect using NZRRAP as nzcon;
	EXECUTE(
		UPDATE &net_db..BASEL_ACCT_PRFM_FACT a 
		SET a.basel_acct_id = (SELECT sl.basel_acct_id FROM &net_db..BASEL_PSNL_LOAN_MTH_SNAPSHOT sl  
								WHERE sl.mth_tm_id = a.mth_tm_id 
								AND CONCAT(LPAD(sl.crnt_br_loctn_trnst,5,'0'), LPAD(TRIM(sl.loan_num),7,'0')) = a.acct_num)
		WHERE a.mth_end_dt=&MTH_END_DT 
		AND a.src_sys_cd ='SL' and 
		(a.basel_acct_id is null or 
			a.BASEL_ACCT_ID NOT IN (SELECT DISTINCT BASEL_ACCT_ID FROM &net_db..BASEL_PSNL_LOAN_MTH_SNAPSHOT WHERE MTH_TM_ID=&MTH_TM_ID)); 
		) by nzcon;
	disconnect from nzcon;
quit;

/* derive basel_acct_id for KS */
proc sql;
	connect using NZRRAP as nzcon;
	EXECUTE(
		UPDATE &net_db..BASEL_ACCT_PRFM_FACT a 
		SET a.basel_acct_id = (SELECT kq.basel_acct_id FROM &net_db..BASEL_REVLVNG_CR_MTH_SNAPSHOT kq 
			WHERE kq.mth_tm_id = a.mth_tm_id AND TRIM(L '0' FROM kq.acct_num) = a.acct_num)
		WHERE a.mth_end_dt = &MTH_END_DT AND a.src_sys_cd = 'KQ'; 
			) by nzcon;
	disconnect from nzcon;
quit;


/* Create table for KS Role Indicator */
PROC SQL NOPRINT;
       CONNECT USING NZRRAP AS NZCON;
        EXECUTE (DELETE FROM &net_db..BASEL_KS_ACCT_TRANSACTOR_ROLE WHERE MTH_TM_ID=&MTH_TM_ID) BY NZCON;
QUIT;


PROC SQL ;
connect using NZRRAP as nzcon;	
execute(
INSERT INTO &net_db..BASEL_KS_ACCT_TRANSACTOR_ROLE
SELECT DISTINCT
       SNAP.MTH_TM_ID,     
       SNAP.BASEL_ACCT_ID,   

	   /* logic for Transactor/Revolver/Del */
	   case when max(0, SNAP.BNS_DLQNT_DAY-30) > 0 then 'D'
	   		when FACT.TRNSCTR_IND = 'T' then 'T'
			when FACT.TRNSCTR_IND = 'N' then 'R'
		else '' end AS ROLE_IND,  
		SYSDATE,
	    SYSDATE
FROM 
	&net_db..BASEL_REVLVNG_CR_MTH_SNAPSHOT SNAP	
	LEFT JOIN 
		(SELECT distinct tm_id as mth_tm_id,
					LPAD(a.ACCT_NUM,23,'0') AS acct_num, 				
					TRNSCTR_IND 					
			FROM &net_db..BASEL_ACCT_PRFM_FACT a, &net_db..tm_dim b 
			WHERE a.mth_end_dt=b.TM_LVL_END_DT AND tm_lvl='Month'
			AND tm_id=&MTH_TM_ID. AND src_sys_cd='KQ' ) FACT
	ON SNAP.MTH_TM_ID = fact.MTH_TM_ID
	AND SNAP.ACCT_NUM = FACT.ACCT_NUM
	WHERE SNAP.MTH_TM_ID=&mth_tm_id 	

) BY NZCON;
QUIT;
