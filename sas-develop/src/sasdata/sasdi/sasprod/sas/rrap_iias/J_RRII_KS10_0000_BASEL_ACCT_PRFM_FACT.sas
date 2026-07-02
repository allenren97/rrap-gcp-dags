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
*				2024-06-03: Add alternate EDL servers & failover logic for APF extraction (RRMSS-2775)
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


options set=SAS_HADOOP_CONFIG_PATH="/sashome/hadoop/";
option set=SAS_HADOOP_JAR_PATH="/sashome/hadoop/lib";
options sastrace=',,,d' sastraceloc=saslog nostsuffix;

%macro edl_sql;
data _null_;
  x = sleep(15,1);
run;

proc sql;
	connect using edl_rcrr as hdpcon;
	create table basel_prfm_fact_edl as
	select * from connection to hdpcon (
        select
        cast(null as decimal(11,0)) mth_tm_id,
        a.mth_end_dt,
        cast(case when a.src_sys_cd = 'KQ_TSYS' THEN 'KQ' ELSE a.src_sys_cd end as varchar(30)) src_sys_cd,
        cast(null as decimal(20,0)) basel_acct_id,
        cast(COALESCE(c.bcm_acct_num, a.acct_num) as varchar(80)) acct_num,
        cast(a.proc_transit as varchar(5)) proc_transit,
        cast(a.serv_transit as varchar(5)) serv_transit,
        cast(a.prd_cd as varchar(6)) prd_cd,
        cast(a.currency_cd as varchar(3)) currency_cd,
        cast(a.heloc_ind as varchar(1)) heloc_ind,
        cast(a.bsl_ltv_bckt_cd as varchar(30)) ltv_bckt_cd ,
        cast(a.bsl_ltv_rto as decimal(15,8)) ltv_rto,
        cast(a.loan_amt_at_insured_date as decimal(22,2))  loan_amt_at_insured_date ,
        cast(a.bsl_rntl_incm_dpndcy_f as varchar(1)) rntl_incm_dpndcy_f,
        cast(a.bsl_trnsctr_ind as varchar(1)) trnsctr_ind,
        cast(a.bsl_currency_mismatch_f as varchar(1)) currency_mismatch_f,
        cast(a.bsl_tot_expsr_above_1500k_lmt_f as varchar(1)) tot_expsr_above_1500k_lmt_f,
        cast(a.bsl_rntl_incm_amt as decimal(22,2)) rntl_incm_amt,
        cast(a.bsl_grs_incm_amt as decimal(22,2)) grs_incm_amt,
        cast(a.bsl_prch_prc_amt as decimal(22,2)) prch_prc_amt,
        cast(a.bsl_aprsd_val_amt as decimal(22,2)) aprsd_val_amt,
        cast(a.bsl_prpty_val_amt as decimal(22,2)) prpty_val_amt,
        cast(a.bsl_undrawn_amt as decimal(22,2)) undrawn_amt,
        cast(a.crnt_auth_lmt_amt as decimal(22,2)) crnt_auth_lmt_amt,
        cast(a.occupancy_type_cd as varchar(1)) occupancy_type_cd,
        cast(a.os_bal_amt as decimal(22,2)) os_bal_amt,
        cast(a.crnt_ltv_rto as decimal(15,8) ) crnt_ltv_rto,
        cast(a.clp_flag as char(1)) clp_flag,
        cast(b.CRNT_PRPTY_VAL_AMT as decimal(22,2))  CRNT_PRPTY_VAL_AMT,
        cast(b.lnk_to_step as char(1)) LNK_TO_STEP,
        &SESSIONTIME AS INSRT_PROCESS_TMSTMP,
        &SESSIONTIME AS UPDT_PROCESS_TMSTMP
        FROM prod_rcrr1.BASEL_ACCT_PRFM_FACT a
        left join prod_rcrr1.acct_prfm_fact b on a.mth_end_dt=b.mth_end_dt and a.src_sys_cd=b.src_sys_cd and a.acct_num=b.acct_num
        left join (
                select distinct bcm_acct_num, tsys_acct_id
                from tsz.kq_tkq_ks_tsys_xref
                where businesseffectivedate = (select max(businesseffectivedate) from tsz.kq_tkq_ks_tsys_xref where businesseffectivedate <= '&mth_end_dt.')
                and end_of_chain_indicator = 'Y'
                ) as c on c.tsys_acct_id = a.acct_num
        where
        a.src_sys_cd in ('KQ', 'GZ', 'SL', 'TNG_MTG', 'TNG_MCAP', 'KQ_TSYS')
        and a.MTH_END_DT= '&mth_end_dt.');
	DISCONNECT FROM hdpcon;
quit;
%mend edl_sql;

%macro edl_extract;
libname edl_rcrr hadoop server="sdpsvrwm0163.scglobal.ad.scotiacapital.com" schema=prod_rcrr1 authdomain="EDL_Auth" 
dbmax_text=300 
uri="jdbc:hive2://sdpsvrwm0163.scglobal.ad.scotiacapital.com:8443/prod_rcrr1;ssl=true;sslTrustStore=/sashome/hadoop/gateway.jks;trustStorePassword=Test123?hive.server2.transport.mode=http;hive.server2.thrift.http.path=gateway/default/hive" 
;
/* initialize return code to non-zero */
%let sqlxrc=0163;
%if &syslibrc. = 0 %then %do; 
  %edl_sql;
%end;

%if &sqlxrc. ne 0 %then %do;
  libname edl_rcrr hadoop server="sdpsvrwm0217.scglobal.ad.scotiacapital.com" schema=prod_rcrr1 authdomain="EDL_Auth" 
  dbmax_text=300 
  uri="jdbc:hive2://sdpsvrwm0217.scglobal.ad.scotiacapital.com:8443/prod_rcrr1;ssl=true;sslTrustStore=/sashome/hadoop/gateway.jks;trustStorePassword=Test123?hive.server2.transport.mode=http;hive.server2.thrift.http.path=gateway/default/hive" 
  ;
  %let sqlxrc=0217;
  %if &syslibrc. = 0 %then %do; 
    %edl_sql;
  %end;

  %if &sqlxrc. ne 0 %then %do;
    libname edl_rcrr hadoop server="sdpsvrwm0291.scglobal.ad.scotiacapital.com" schema=prod_rcrr1 authdomain="EDL_Auth" 
    dbmax_text=300 login_timeout=0 
    uri="jdbc:hive2://sdpsvrwm0291.scglobal.ad.scotiacapital.com:8443/prod_rcrr1;ssl=true;sslTrustStore=/sashome/hadoop/gateway.jks;trustStorePassword=Test123?hive.server2.transport.mode=http;hive.server2.thrift.http.path=gateway/default/hive" 
    ;
    %let sqlxrc=0291;
    %if &syslibrc. = 0 %then %do; 
      %edl_sql;
    %end;
  %end; 
%end; 

%if &sqlxrc. ne 0 %then %do;
  %PUT;
  %PUT >>> Job aborting. APF extraction failed with above EDL servers. Contact RRAP team.;
  %PUT;
  %abort abend;
%end;
%else %do;
  %PUT;
  %PUT >>> APF extraction completed successfully.;
  %PUT;
%end;
%mend edl_extract;
%edl_extract;
options sastrace=off;


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