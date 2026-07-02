/************************************************************************************/
/*  V0.1 - Oct02,2017 (last edit by N. DONMEZ)                                    ***/
/*                                                                                ***/
/*  Name: RSK_DL_PRT3_MTHLY.sas                                                   ***/
/*  Description: Extraction code for Credit Card Treatment model.                 ***/
/*                                                                                ***/
/*  The following macro variables need to be set to run this code:                ***/
/*  &dldropf &db2_svr                                                             ***/
/************************************************************************************/   

%let oot_dt = %sysfunc(intnx(month,%sysfunc(today())-&daysago.,-3,E));
%let oot_curr = %sysfunc(intnx(month,%sysfunc(today())-&daysago.,-2,E));
%let oot_curr_my = %sysfunc(putn(&oot_curr,yymmn6.));

data _null_;
set CRDMC.TM_DIM;
call symput("oot_dly_tm",TM_ID);
where TM_LVL='Day' 
and TM_LVL_END_DT = &oot_dt;
run;

data _null_;
set CRDMC.TM_DIM;
call symput("oot_mth_tm",TM_ID);
where TM_LVL='Month' 
and TM_LVL_END_DT = &oot_dt;
run;

proc sql;
	connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
	create table work.roll_&oot_curr_my. as
	select * from connection to db2(
		select a.MODEL_SCORE, 
			a.SCORE_DL, 
			b.ACCOUNT_NUM, 
			b.bns_dlqnt_day1, 			
			b.block_recl_cd1,
			b.chrg_off_cd1,
			c.bns_dlqnt_day2, 			
			c.block_recl_cd2,
			c.chrg_off_cd2			
		from (
			select CORPTN_AND_ACCT_NUM, 
				substr(CORPTN_AND_ACCT_NUM, 3, 13) as ACCT_NUM,
				EFF_TM_ID, 
				SCORING_DT_TEXT, 
				BNS_DLQNT_DAY, 
				MODEL_SCORE, 
				SCORE_DL
			from EDRTLRT.COLCTN_SCORING_DTL_MODEL_OUTPUT
			where EFF_TM_ID = &oot_dly_tm.) as a
		left join (
			select risk_acct_id as account_num,
				substr(acct_num, 11, 13) as acct_num1, 						
				bns_dlqnt_day as bns_dlqnt_day1,
				block_recl_cd as block_recl_cd1,
				chrg_off_cd as chrg_off_cd1				
			from EDRTLR.risk_revlvng_cr_mth_snapshot
			where mth_tm_id = &oot_mth_tm. + 40) as b
		on a.ACCT_NUM = b.ACCT_NUM1
		left join (
			select substr(acct_num, 11, 13) as acct_num2, 				
				bns_dlqnt_day as bns_dlqnt_day2,
				block_recl_cd as block_recl_cd2,
				chrg_off_cd as chrg_off_cd2				
			from EDRTLR.risk_revlvng_cr_mth_snapshot
			where mth_tm_id = &oot_mth_tm. + 80) as c
		on a.ACCT_NUM = c.ACCT_NUM2
	);
	disconnect from db2;
quit;


*---------- Write the files in destination -------------------;
libname dropf "&dldropf.treatment";

data dropf.roll_&oot_curr_my.;
	set work.roll_&oot_curr_my.;
run;

data _null_;
	file "&dldropf.startdl2.txt";	
	put "roll_&oot_curr_my..sas7bdat";
run;
