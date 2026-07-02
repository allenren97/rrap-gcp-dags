/************************************************************************************/
/*  V0.1 - Oct02,2017 (last edit by N. DONMEZ)                                    ***/
/*                                                                                ***/
/*  Name: RSK_DL_PRT1B_MTHLY.sas                                                  ***/
/*  Description: Extraction code for Credit Card Treatment model.                 ***/
/*  This file is modified from RSK_DL_PRT1_MTHLY.sas.                             ***/
/*                                                                                ***/
/*  The following macro variables need to be set to run this code:                ***/
/*  &dldropf &db2_svr &Last_Avl_dt_mth &curr_mnth &curr_yr &curr_my               ***/
/*  &pme_mnth &pme_yr &pme_my &tm                                                 ***/
/************************************************************************************/      


%put &dldropf &db2_svr &Last_Avl_dt_mth &curr_mnth &curr_yr &curr_my &pme_mnth &pme_yr &pme_my &tm;

%macro Risk_Acct_Qry(score);
%IF &score = zero %THEN %DO;
%let value = %bquote(score=0);
%END;
%ELSE %DO;
%let value = %bquote(score <> 0);
%END;
   				select * from (
		    		select 
					RISK_ACCT_ID
					,corptn_num
					,EFF_TM_ID
					,BNS_DLQNT_DAY
					,CR_LMT_AMT 
					,TOT_NEW_BAL_AMT
					,ACCT_STAT_CD
					,BLOCK_RECL_CD
					,CHRG_OFF_CD
					,CORP_RTL_F
					,PRD_CD
					,SUB_PRD_CD
					,TRNST_NUM
					,ACCT_NUM
					,case 
				    when  SUB_PRD_CD  in ('ST') then 1 
					when  CORP_RTL_F in ('C') then 2 
					when  ACCT_STAT_CD not in ('1','5','6') then 3
					when  TRNST_NUM  IN (18192, 99432) then  4
					when   BLOCK_RECL_CD IN ('B4')  then 5 
					/* B4: deceased */
					when  BLOCK_RECL_CD IN ('B5', 'D', 'SF', 'XS','XV', 'SS', 'S', '2')  then 6  
				    /*
				    B5	Collection- Bankrupt
				    D 	Written Out of Records
				    FX	Fixed Payment
				    SF	Fraud
				    XS	Temporarily Suspend Charge Privileges
				    SS	Lost/Stolen
				    */
				    when  (  SUBSTR(BLOCK_RECL_CD,1,1) IN ('V','P','S') AND TOT_NEW_BAL_AMT<=0 ) then 7  
				   /*CLOSED, FROZEN WITH BAL<=0*/
				    when (BLOCK_RECL_CD )  IN ('S')   	then 8  
					/*no need for COMPRESS(BLOCK_RECL_CD ) as no blanks found */
				   /* S	Temporarily Lost*/
					when  CHRG_OFF_CD IN ('1','2','C','N', 'P','Q')  then 9 
					/*
				    Charge-off Codes	
				    0	Current
				    1	Charged off
				    N	non-accural
				    2	Current, pending charge-off
				    P	Current, pending non-accrual
				    C	non-accural pending current
				    Q	non-accrual pending bad debt
				    */
					when  CR_LMT_AMT  <=0 then 10
					when  PRD_CD  ='VIC' then 11
					else 0 end as score

			from    EDRTLR.RISK_REVLVNG_CR_DLY_SNAPSHOT b 
			where   
            EFF_TM_ID =&Last_Avl_dt_mth.
			and  substr(PRD_CD, 1, 1) in ('A', 'V') 
		    and  BNS_DLQNT_DAY > 0  /* only keep 22-119 for cc scoring */
		    and  BNS_DLQNT_DAY <=119 
			) Final 
			where   &value.			     
%mend;

%macro getms(m); 
			RISK_ACCT_ID                   as   &m._risk_acct_id,
			mth_tm_id                      as   &m._mth_tm_id,
			BAL_HIST_CSH_ADV_AMT           as   &m._BAL_HIST_CSH_ADV_AMT,
			BAL_HIST_PRCHS_AMT             as   &m._BAL_HIST_PRCHS_AMT,
			BAL_HIST_PYMTS_AMT             as   &m._BAL_HIST_PYMTS_AMT,
			BNS_DLQNT_DAY                  as   &m._BNS_DLQNT_DAY,
			CR_LMT_AMT                     as   &m._CR_LMT_AMT,
			CSH_ADVNC_1_CYCL_AGO_BAL_AMT   as   &m._CSH_ADVNC_1_CYCL_AGO_BAL_AMT,
			CSH_ADVNC_2_CYCL_AGO_BAL_AMT   as   &m._CSH_ADVNC_2_CYCL_AGO_BAL_AMT,
			CSH_ADVNC_CRNT_CYCL_BAL_AMT    as   &m._CSH_ADVNC_CRNT_CYCL_BAL_AMT,
			CSH_ADVNC_RCVRY_INTR_AMT       as   &m._CSH_ADVNC_RCVRY_INTR_AMT,			
			LAST_PYMT_AMT                  as   &m._LAST_PYMT_AMT,
			LAST_YR_CR_INTR_PD_AMT         as   &m._LAST_YR_CR_INTR_PD_AMT,
			MTH_DLQNT_CNT                  as   &m._MTH_DLQNT_CNT,
			PRCH_1_CYCL_AGO_BAL_AMT        as   &m._PRCH_1_CYCL_AGO_BAL_AMT,
			PRCH_2_CYCL_AGO_BAL_AMT        as   &m._PRCH_2_CYCL_AGO_BAL_AMT,
			PRCH_CRNT_CYCL_BAL_AMT         as   &m._PRCH_CRNT_CYCL_BAL_AMT,
			PRCH_RCVRY_INTR_AMT            as   &m._PRCH_RCVRY_INTR_AMT,
			REQST_CR_LMT_AMT               as   &m._REQST_CR_LMT_AMT,
			RISK_BAL_RTO                   as   &m._RISK_BAL_RTO,
			TOT_MIN_PYMT_AMT               as   &m._TOT_MIN_PYMT_AMT,
			TOT_NEW_BAL_AMT                as   &m._TOT_NEW_BAL_AMT,
			TOT_PD_AMT                     as   &m._TOT_PD_AMT,
			TOT_UNPAID_FNCL_CHRG_AMT       as   &m._TOT_UNPAID_FNCL_CHRG_AMT
%mend;

*--------------- Extract historical KS data -----------------;
proc sql;
	connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
	create table work.kshist_&curr_my. as
	select * 
	from connection to db2(
	select a.RISK_ACCT_ID as ACCOUNT_NUM, a.EFF_TM_ID, c.*, d.*, e.*, f.*, g.*, h.*
		from (
			select distinct RISK_ACCT_ID, EFF_TM_ID from (
					%Risk_Acct_Qry(zero) )) as a
		left join (select %getms(P1) 
			from EDRTLR.risk_revlvng_cr_mth_snapshot 
			where mth_tm_id = (&tm. - 40)) as c
		on a.RISK_ACCT_ID = c.P1_RISK_ACCT_ID
		left join (select %getms(P2) 
			from EDRTLR.risk_revlvng_cr_mth_snapshot 
			where mth_tm_id = (&tm. - 80)) as d
		on a.RISK_ACCT_ID = d.P2_RISK_ACCT_ID
		left join (select %getms(P3) 
			from EDRTLR.risk_revlvng_cr_mth_snapshot 
			where mth_tm_id = (&tm. - 120)) as e
		on a.RISK_ACCT_ID = e.P3_RISK_ACCT_ID
		left join (select %getms(P4) 
			from EDRTLR.risk_revlvng_cr_mth_snapshot 
			where mth_tm_id = (&tm. - 160)) as f
		on a.RISK_ACCT_ID = f.P4_RISK_ACCT_ID
		left join (select %getms(P5) 
			from EDRTLR.risk_revlvng_cr_mth_snapshot 
			where mth_tm_id = (&tm. - 200)) as g
		on a.RISK_ACCT_ID = g.P5_RISK_ACCT_ID
		left join (select %getms(P6) 
			from EDRTLR.risk_revlvng_cr_mth_snapshot 
			where mth_tm_id = (&tm. - 240)) as h
		on a.RISK_ACCT_ID = h.P6_RISK_ACCT_ID;
	);
	disconnect from db2;
quit; 

*--------------- Extract KS data -------------------------------;
proc sql;
	connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
	create table work.ks_monthly_&curr_my. as
	select * 
	from connection to db2(
		select a.RISK_ACCT_ID as ACCOUNT_NUM, 
			a.EFF_TM_ID, 
			a.DAILY_BNS_DLQNT_DAY, 
			a.DAILY_TOT_NEW_BAL_AMT, 
			b.*
		from (select distinct RISK_ACCT_ID, 
						EFF_TM_ID, 
						BNS_DLQNT_DAY as DAILY_BNS_DLQNT_DAY, 
						TOT_NEW_BAL_AMT as DAILY_TOT_NEW_BAL_AMT					
					from ( %Risk_Acct_Qry(zero) ) ) as a
		left join (select * from EDRTLR.risk_revlvng_cr_mth_snapshot where mth_tm_id = &tm.) as b
		on a.RISK_ACCT_ID = b.RISK_ACCT_ID;
	);
	disconnect from db2;
quit; 

*---------------------- Extract Bureau data -----------------------;
proc sql;
	connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
	create table work.bureau_monthly_&curr_my. as
	select *
	from connection to DB2(
		select 
			c.RISK_ACCT_ID as ACCOUNT_NUM,
			d.*
		from (
			select distinct RISK_CUST_ID, RISK_ACCT_ID
			from(
				select a.RISK_ACCT_ID, b.RISK_CUST_ID
				from ( %Risk_Acct_Qry(zero) ) as a
				left join EDRTLR.RISK_CUST_ACCT_RLTNP_SNAPSHOT as b
				on a.RISK_ACCT_ID = b.RISK_ACCT_ID and b.MTH_TM_ID = &TM.     
				where b.PRIM_CUST_F = 'Y' 
			) as fnl
		) as c
		inner join EDRTLR.RISK_CR_BUREAU_DELI_MTH_SNAPSHOT as d
		on c.RISK_CUST_ID = d.RISK_CUST_ID and d.MTH_TM_ID = &tm.      
	);
	disconnect from db2;
quit;

*------------- Combine and clean up KS and Bureau data ------;
proc sql;
	create table work.ksbr_&curr_my. as
	select a.ACCOUNT_NUM		
		,a.ACCT_CLS_RSN_CD
		,a.BAL_HIST_CSH_ADV_AMT
		,a.BAL_HIST_PRCHS_AMT
		,a.BAL_HIST_PYMTS_AMT
		,a.BNS_DLQNT_DAY
		,a.CR_LMT_AMT
		,a.CSH_ADVNC_1_CYCL_AGO_BAL_AMT
		,a.CSH_ADVNC_2_CYCL_AGO_BAL_AMT
		,a.CSH_ADVNC_CRNT_CYCL_BAL_AMT
		,a.CSH_ADVNC_RCVRY_INTR_AMT
		,a.DAILY_BNS_DLQNT_DAY
		,a.DAILY_TOT_NEW_BAL_AMT		
		,a.DLQNT_HIST_1_12
		,a.DLQNT_HIST_13_24
		,a.EFF_TM_ID
		,a.LAST_PYMT_AMT
		,a.LAST_PYMT_DT
		,a.LAST_YR_CR_INTR_PD_AMT
		,a.MTH_DLQNT_CNT
		,a.PRCH_1_CYCL_AGO_BAL_AMT
		,a.PRCH_2_CYCL_AGO_BAL_AMT
		,a.PRCH_CRNT_CYCL_BAL_AMT
		,a.PRCH_RCVRY_INTR_AMT
		,a.PRD_CD
		,a.REQST_CR_LMT_AMT
		,a.RISK_BAL_RTO
		,a.SUB_PRD_CD
		,a.TOT_MIN_PYMT_AMT
		,a.TOT_NEW_BAL_AMT
		,a.TOT_PD_AMT
		,a.TOT_UNPAID_FNCL_CHRG_AMT
		,b.ACTV_INSTLMNT_TRDS_BAL_GT_ZERO_C
		,b.ACTV_TRADE_30_DAY_RT_CNT
		,b.ACTV_TRADE_60_DAY_RT_CNT
		,b.ACTV_TRADE_OV_90_RT_CNT
		,b.ACTV_TRADE_WITH_AMT_PD_CNT
		,b.ACTV_TRDS_CNT
		,b.BNKRPY_CNT
		,b.BNKRPY_NARRTV_CD
		,b.COLCTN_CNT
		,b.COLCTN_INQR_CNT
		,b.CR_BUREAU_WORST_RT_F
		,b.DERGTRY_PUB_RECD_CNT
		,b.HIGHST_ACTV_HCCL_AMT
		,b.HIGHST_ACTV_UTLTN
		,b.HIGHST_BAL_OS_AMT
		,b.INQRY_CNT
		,b.INQRY_PAST_6_MTH_CNT
		,b.LEGAL_CNT
		,b.MAX_REVLVNG_CR_CRNT_UTLTN_AMT
		,b.MOST_RECNT_BANKRUPCY_DISCHARGE_D
		,b.MOST_RECNT_BANKRUPCY_RPTD_DT
		,b.MTH_SINCE_CR_BUREAU_WORST_RT_CNT
		,b.MTH_SINCE_LAST_30_DAY_DLQNT_CNT
		,b.MTH_SINCE_LAST_60_DAY_DLQNT_CNT
		,b.MTH_SINCE_LAST_90_DAY_DLQNT_CNT
		,b.MTH_SINCE_LAST_ACTY_CNT
		,b.MTH_SINCE_MOST_RECNT_DLQNT_CNT
		,b.MTH_SINCE_MOST_RECNT_TRADE_OPND_
		,b.MTH_SINCE_OLDST_TRADE_OPND_CNT
		,b.NOT_SUFFICENT_FUNDS_CHQ_CNT
		,b.OCC_60_DAY_PD_WITHIN_PAST_12_MTH
		,b.OLDST_OPN_TRADE_AGE_LINE_MTH_CNT
		,b.OPN_TRADE_CNT
		,b.ORIG_COLCTN_AMT
		,b.SATFCTRY_TRADE_CNT
		,b.SATFCTRY_TRADE_GT_3_MTH_CNT
		,b.TM_30_DAY_PD_LAST_12_MTH_CNT
		,b.TM_60_DAY_PD_EVER_CNT
		,b.TM_60_DAY_PD_LAST_12_MTH_CNT
		,b.TM_90_DAY_PD_EVER_CNT
		,b.TM_OV_90_DAY_PD_LAST_12_MTH_CNT
		,b.TOT_AVL_CR_NOT_UTILIZED_AMT
		,b.TOT_BAL_ALL_TRADE_AMT
		,b.TOT_BAL_ALL_TRADE_EXCLD_MORT_AMT
		,b.TOT_BAL_BNK_INSTLMNT_AMT
		,b.TOT_BAL_BNK_REVLVNG_AMT
		,b.TOT_BAL_BNK_REVLVNG_LINES_AMT
		,b.TOT_BAL_FNCL_INSTLMNT_AMT
		,b.TOT_BAL_FNCL_REVLVNG_AMT
		,b.TOT_BAL_INSTLMNT_AMT
		,b.TOT_BAL_MORT_TRADE_AMT
		,b.TOT_BAL_REVLVNG_AMT
		,b.TOT_BAL_RTL_REVLVNG_AMT
		,b.TOT_BAL_TP_BANKCARD_AMT
		,b.TOT_COLCTN_BAL_AMT
		,b.TOT_COMPTV_CRD_RTO
		,b.TOT_COMPTV_LINE_RTO
		,b.TOT_COMPTV_RTO
		,b.TOT_HCCL_ALL_TRADE_AMT
		,b.TOT_HCCL_BNK_INSTLMNT_AMT
		,b.TOT_HCCL_BNK_REVLVNG_AMT
		,b.TOT_HCCL_FNCL_INSTLMNT_AMT
		,b.TOT_HCCL_FNCL_REVLVNG_AMT
		,b.TOT_HCCL_INSTLMNT_AMT
		,b.TOT_HCCL_REVLVNG_AMT
		,b.TOT_HCCL_TP_BANKCARD_AMT
		,b.TOT_MTH_PYMT_AMT
		,b.TOT_ORIG_LEGAL_AMT
		,b.TOT_PD_AMT as BR_TOT_PD_AMT
		,b.TOT_TRADE_CNT
		,b.TOT_UTLTN_AMT
		,b.TOT_UTLTN_BNK_REVLVNG_CRD_AMT
		,b.TOT_UTLTN_BNK_REVLVNG_LINE_AMT
		,b.TRADE_90_DPD_LAST_24_MTH_CNT
		,b.TRADE_BNK_INSTLMNT_CNT
		,b.TRADE_BNK_REVLVNG_CNT
		,b.TRADE_FNCL_REVLVNG_CNT
		,b.TRADE_INSTLMNT_CNT
		,b.TRADE_NEVER_DLQNT_PC
		,b.TRADE_OPND_IN_PAST_6_MTH_CNT
		,b.TRADE_OPND_PAST_12_MTH_WITH_BAL_
		,b.TRADE_OPND_PAST_3_MTH_CNT
		,b.TRADE_RTL_REVLVNG_CNT
		,b.TRADE_TP_BANKCARD_CNT
		,b.TRADE_WORST_EVER_30_DAY_PD_CNT
		,b.TRADE_WORST_EVER_60_DAY_PD_CNT
		,b.TRADE_WORST_EVER_90_DAY_DLQNT_CN
		,b.TRADE_WORST_EVER_90_DAY_PD_CNT
		,b.TRADE_ZERO_BAL_CNT
		,b.TRDS_FNCL_INSTLMNT_CNT
		,b.TRDS_REVLVNG_CNT
	from work.ks_monthly_&curr_my. as a
	left join work.bureau_monthly_&curr_my. as b
	on a.account_num = b.account_num and a.mth_tm_id = b.mth_tm_id;
quit;


* ------ Extract last two months of CACS data ------------------;
proc sql;
	connect to db2(database=DM1P1D user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");	
	create table work.hist_&pme_my. as
	select * from connection to db2(
		select a.RISK_ACCT_ID as ACCOUNT_NUM,
			b.COLCTN_ACTY_CD,
			b.EFF_DT,
			b.PYMT_AMT,
			b.PROMSE_1_AMT,
			b.PROMSE_2_AMT,
			b.PROMSE_1_DT,
			b.PROMSE_2_DT,
			b.PARTY_CNTC_CD,
			b.ROUTE_TO_STATE_CD		
		from (select * 
			from EDDMGR.cacs_hist_dtl 
			where year(eff_dt) = &pme_yr. and month(eff_dt) = &pme_mnth. and loctn_cd = '051020') as b				
		left join (select distinct RISK_ACCT_ID, substr(acct_num, 11, 13) as ACCT_NUM2
			from EDRTLR.RISK_REVLVNG_CR_DLY_SNAPSHOT
			where EFF_TM_ID > (&tm. - 240) and EFF_TM_ID < (&tm. + 80)) as a
		on a.acct_num2 = b.acct_num
	);
	disconnect from db2;
quit;

proc sql;
	connect to db2(database=DM1P1D user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");	
	create table work.hist_&curr_my. as
	select * from connection to db2(
		select a.RISK_ACCT_ID as ACCOUNT_NUM,
			b.COLCTN_ACTY_CD,
			b.EFF_DT,
			b.PYMT_AMT,
			b.PROMSE_1_AMT,
			b.PROMSE_2_AMT,
			b.PROMSE_1_DT,
			b.PROMSE_2_DT,
			b.PARTY_CNTC_CD,
			b.ROUTE_TO_STATE_CD
		from (select * 
			from EDDMGR.cacs_hist_dtl 
			where year(eff_dt) = &curr_yr. and month(eff_dt) = &curr_mnth. and loctn_cd = '051020') as b
		left join (select distinct RISK_ACCT_ID, substr(acct_num, 11, 13) as ACCT_NUM2
			from EDRTLR.RISK_REVLVNG_CR_DLY_SNAPSHOT
			where EFF_TM_ID > (&tm. - 240) and EFF_TM_ID < (&tm. + 80)) as a
		on a.acct_num2 = b.acct_num		
	);
	disconnect from db2;
quit;

*---------- Write the files in destination -------------------;
libname dropf "&dldropf.treatment";

data dropf.hist_&pme_my.;
	set work.hist_&pme_my.;
run;

data dropf.hist_&curr_my.;
	set work.hist_&curr_my.;
run;

data dropf.kshist_&curr_my.;
	set work.kshist_&curr_my.;
run;

data dropf.ksbr_&curr_my.;
	set work.ksbr_&curr_my.;
run;

data _null_;
	file "&dldropf.startdl1.txt";
	put "hist_&pme_my..sas7bdat";
	put "hist_&curr_my..sas7bdat";
	put "kshist_&curr_my..sas7bdat";
	put "ksbr_&curr_my..sas7bdat";
run;


