/************************************************************************************/
/*  V0.1 - Nov21,2016 (last edit by N. DONMEZ)                                    ***/
/*                                                                                ***/
/*  Name: RSK_DL_PRT1_MTHLY.sas                                                   ***/
/*  Description: Extraction code for Credit Card Collection model (DL model is    ***/
/*  run separately). This file is modified from RSK_SCRD_KS_MTHLY.sas.            ***/
/*                                                                                ***/
/*  The following macro variables need to be set to run this code:                ***/
/*  &dldropf &db2_svr &Last_Avl_dt_mth &pme_mnth &pme_yr &pme_my &tm              ***/
/************************************************************************************/          

%put &dldropf &db2_svr &Last_Avl_dt_mth &pme_mnth &pme_yr &pme_my &tm;

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
				    when  SUB_PRD_CD  in ('ST')  or  SUB_PRD_CD  in ('ST')  then 1 
					when  CORP_RTL_F in ('C') then 2 
					when  ACCT_STAT_CD        not in ('1', '5','6') then 3
					when  TRNST_NUM  IN (18192, 99432) then  4
					when   BLOCK_RECL_CD        IN ('B4')  then 5 
					/* B4: deceased */
					when  BLOCK_RECL_CD   IN ( 'B5', 'D',  'SF', 'XS','XV', 'SS','S', '2')  then 6  
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
		    and  BNS_DLQNT_DAY > 0  /* only keep 22-89 for cc scoring */
		    and  BNS_DLQNT_DAY <=89 
			) Final 
			where   &value.			     
%mend;

%macro get_val(p_val=);
	 max (Case when MTH_TM_ID=&tm.            then &p_val. END) as &p_val.1,
	 max (Case when MTH_TM_ID=%eval(&tm.-40)  then &p_val. END) as &p_val.2,
	 max (Case when MTH_TM_ID=%eval(&tm.-80)  then &p_val. END) as &p_val.3,
	 max (Case when MTH_TM_ID=%eval(&tm.-120)  then &p_val. END) as &p_val.4,
	 max (Case when MTH_TM_ID=%eval(&tm.-160)  then &p_val. END) as &p_val.5,
	 max (Case when MTH_TM_ID=%eval(&tm.-200)  then &p_val. END) as &p_val.6
%mend;

proc sql;
	connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
	create table work.RV_MONTHLY_&pme_my. as
	select *
	from connection to DB2(
		select  
		a.RISK_ACCT_ID,

		%get_val(p_val=MTH_DLQNT_CNT),
		%get_val(p_val=PRCH_1_CYCL_AGO_BAL_AMT),
		%get_val(p_val=PRCH_2_CYCL_AGO_BAL_AMT),

		%get_val(p_val=TOT_MIN_PYMT_AMT),
		%get_val(p_val=TOT_PD_AMT),
		%get_val(p_val=CSH_ADVNC_CRNT_CYCL_BAL_AMT),
		%get_val(p_val=PRCH_CRNT_CYCL_BAL_AMT),
		%get_val(p_val=TOT_NEW_BAL_AMT),
		%get_val(p_val=CR_LMT_AMT),
		%get_val(p_val=LAST_PYMT_AMT),
		%get_val(p_val=TOT_CYCL_TO_DT_FNCL_CHRG_AMT),
		%get_val(p_val=TOT_UNPAID_FNCL_CHRG_AMT)

		from (
		select RISK_ACCT_ID from (
				%Risk_Acct_Qry(zero) )
			) a
		left outer join 
		EDRTLR.RISK_REVLVNG_CR_MTH_SNAPSHOT as b
		on a.RISK_ACCT_ID = b.RISK_ACCT_ID
		and MTH_TM_ID in (
			&tm.,           
			%eval(&tm.-40),
			%eval(&tm.-80), 
			%eval(&tm.-120),
			%eval(&tm.-160),
			%eval(&tm.-200)
			)
		group by a.RISK_ACCT_ID
		WITH UR
	);
	disconnect from db2;	
quit;

/* EDIT (N DONMEZ): Changed the following with the CRDM names */
data work.sRV_MONTHLY_&pme_my._2;
	set  work.RV_MONTHLY_&pme_my.;

	if TOT_NEW_BAL_AMT1 > CR_LMT_AMT1 then totamtdue1=TOT_PD_AMT1-CR_LMT_AMT1+TOT_NEW_BAL_AMT1;
	else totamtdue1=TOT_PD_AMT1;
	if TOT_NEW_BAL_AMT2 > CR_LMT_AMT2 then totamtdue2=TOT_PD_AMT2-CR_LMT_AMT2+TOT_NEW_BAL_AMT2;
	else totamtdue2=TOT_PD_AMT2;
	if TOT_NEW_BAL_AMT3 > CR_LMT_AMT3 then totamtdue3=TOT_PD_AMT3-CR_LMT_AMT3+TOT_NEW_BAL_AMT3;
	else totamtdue3=TOT_PD_AMT3;
	if TOT_NEW_BAL_AMT4 > CR_LMT_AMT4 then totamtdue4=TOT_PD_AMT4-CR_LMT_AMT4+TOT_NEW_BAL_AMT4;
	else totamtdue4=TOT_PD_AMT4;
	if TOT_NEW_BAL_AMT5 > CR_LMT_AMT5 then totamtdue5=TOT_PD_AMT5-CR_LMT_AMT5+TOT_NEW_BAL_AMT5;
	else totamtdue5=TOT_PD_AMT5;
	if TOT_NEW_BAL_AMT6 > CR_LMT_AMT6 then totamtdue6=TOT_PD_AMT6-CR_LMT_AMT6+TOT_NEW_BAL_AMT6;
	else totamtdue6=TOT_PD_AMT6;

	array BAL   {6} TOT_NEW_BAL_AMT1 - TOT_NEW_BAL_AMT6;
	array CRLIM {6} CR_LMT_AMT1 - CR_LMT_AMT6;

	/* ---------------- start of edit (N DONMEZ) ---------------------- */
	array utilarray {6} util1 - util6; 

	do i = 6 to 1 by -1;
		if CRLIM{i} ^= 0 then utilarray{i} = BAL{i}/CRLIM{i}; else utilarray{i} = 0;
	end;

	avg_util1_3 = mean (of util1-util3);
	avg_util1_6 = mean (of util1-util6);

	if mean (of util1-util3) > 0 then util1_avg_util1_3 = util1/mean (of util1-util3);
	if mean (of util1-util6) > 0 then util1_avg_util1_6 = util1/mean (of util1-util6);

	max_util1_3 = max (of util1-util3);
	max_util1_6 = max (of util1-util6);

	if (max (of PRCH_CRNT_CYCL_BAL_AMT1-PRCH_CRNT_CYCL_BAL_AMT6)) > 0 then
	pur1_avg6=PRCH_CRNT_CYCL_BAL_AMT1/(mean (of PRCH_CRNT_CYCL_BAL_AMT1-PRCH_CRNT_CYCL_BAL_AMT6));

	if (mean (of TOT_NEW_BAL_AMT1-TOT_NEW_BAL_AMT6))>0 then 
	bal1_avg6=TOT_NEW_BAL_AMT1/(mean (of TOT_NEW_BAL_AMT1-TOT_NEW_BAL_AMT6));

	if (max (of TOT_NEW_BAL_AMT1-TOT_NEW_BAL_AMT3))>0   then 
	bal1_max3=TOT_NEW_BAL_AMT1/(max (of TOT_NEW_BAL_AMT1-TOT_NEW_BAL_AMT3));

	if (max (of TOT_NEW_BAL_AMT1-TOT_NEW_BAL_AMT6))>0   then 
	bal1_max6=TOT_NEW_BAL_AMT1/(max (of TOT_NEW_BAL_AMT1-TOT_NEW_BAL_AMT6));

	if TOT_NEW_BAL_AMT2>0 then
	pay_bal=LAST_PYMT_AMT1/TOT_NEW_BAL_AMT2;

	if sum( of TOT_NEW_BAL_AMT2-TOT_NEW_BAL_AMT4)>0 then
	pay_bal1_3=sum(of LAST_PYMT_AMT1-LAST_PYMT_AMT3)/sum( of TOT_NEW_BAL_AMT2-TOT_NEW_BAL_AMT4);

	if sum( of TOT_NEW_BAL_AMT1-TOT_NEW_BAL_AMT6)>0 then
	Cash_Adv_Curr_Cyc_Bal1_6 = 
	sum(of CSH_ADVNC_CRNT_CYCL_BAL_AMT1-CSH_ADVNC_CRNT_CYCL_BAL_AMT6)/sum( of TOT_NEW_BAL_AMT1-TOT_NEW_BAL_AMT6);

	/* ------------------- end of edit (N DONMEZ) ---------------------- */
	B4_lastocc_ge100util=7;
	do i = 6 to 1 by -1;
		if BAL{i} > CRLIM{i} then B4_lastocc_ge100util = 7-i;
	end;

	array TOTPURCSH_HIS {6} totpurcsh_his_01 - totpurcsh_his_06;
	array PUR_HIS       {6} PRCH_CRNT_CYCL_BAL_AMT1 - PRCH_CRNT_CYCL_BAL_AMT6;
	array CSHADV_HIS    {6} CSH_ADVNC_CRNT_CYCL_BAL_AMT1 - CSH_ADVNC_CRNT_CYCL_BAL_AMT6;

	do i = 1 to 6;
		TOTPURCSH_HIS {i} = PUR_HIS {i} + CSHADV_HIS {i};
	end;

	flag_char11_divbyzero = (sum (of TOTPURCSH_HIS {*}) = 0);

	if flag_char11_divbyzero = 0 then B11_cashadv_asperof_totalpur = int(sum (of CSHADV_HIS{*}) / sum (of TOTPURCSH_HIS{*})*100);
	else B11_cashadv_asperof_totalpur = 999999;

	array PAY_HIS    {6} LAST_PYMT_AMT1 - LAST_PYMT_AMT6;
	array MINDUE_HIS {6} TOT_MIN_PYMT_AMT1 - TOT_MIN_PYMT_AMT6;
	array past_due   {6} TOT_PD_AMT1-TOT_PD_AMT6;

	array CRLIM_ZERO_OR_MISSING {6} crlim_zero_or_missing_01 - crlim_zero_or_missing_06;
	array MINDUE_HIS_PEROFLIM   {6} mindue_his_peroflim_01 - mindue_his_peroflim_06;

	do i = 1 to 6;
		if CRLIM{i} <= 0 then CRLIM_ZERO_OR_MISSING {i} = 1;
		else CRLIM_ZERO_OR_MISSING{i} = 0;
	end;

	flag_char13_divbyzero = (sum (of CRLIM_ZERO_OR_MISSING{*}) >0);

	if flag_char13_divbyzero = 0 then do;
		do i = 1 to 6;
	    	MINDUE_HIS_PEROFLIM {i} = int(((MINDUE_HIS{i} + past_due{i})/ CRLIM {i})*100);
		end;
	end;
	else B13_max_mindueasper_oflim = 999999;

	B13_max_mindueasper_oflim = max (of mindue_his_peroflim_01 - mindue_his_peroflim_06);
	B13_max_mindueasper_oflim = min (B13_max_mindueasper_oflim,6);

	flag_char14_divbyzero = ((sum (of TOT_MIN_PYMT_AMT2 - TOT_MIN_PYMT_AMT6) + sum(of TOT_PD_AMT2-TOT_PD_AMT6)) = 0);

	if flag_char14_divbyzero = 0 then
		B14_pymnts_asperof_mindue =
		(sum (of LAST_PYMT_AMT1 - LAST_PYMT_AMT5) /
		(sum(of TOT_MIN_PYMT_AMT2 - TOT_MIN_PYMT_AMT6)
		+ sum(of TOT_PD_AMT2-TOT_PD_AMT6)));
	else B14_pymnts_asperof_mindue = 999999;

	flag_char141_divbyzero = ((sum (of TOT_MIN_PYMT_AMT2 - TOT_MIN_PYMT_AMT6)) = 0);

	if flag_char141_divbyzero = 0 then
		B141_pymnts_asperof_mindue =
		(sum (of LAST_PYMT_AMT1 - LAST_PYMT_AMT5) /
		(sum(of TOT_MIN_PYMT_AMT2 - TOT_MIN_PYMT_AMT6)));
	else B141_pymnts_asperof_mindue = 999999;

	starting = 0;
	ending = 0;
	B21_maxconsecmths_ge100util = 0;
	do i = 1 to 6;
	if BAL{i} => CRLIM{i} and CRLIM{i} ne 0 then ending = i;

	if BAL{i} < CRLIM{i} or i=6 or CRLIM{i}=0 then do;
		if starting < ending then do;
	    	run_ofconsecmths = ending - starting;
	        if B21_maxconsecmths_ge100util < run_ofconsecmths then
	   			B21_maxconsecmths_ge100util = run_ofconsecmths;
			end;
		starting = i;
		ending = i;
		end;
	end;

	/* Edit (N DONMEZ): Dropping temporary/unneeded variables */

	MTH_DLQNT_CNT = MTH_DLQNT_CNT1;
	PRCH_1_CYCL_AGO_BAL_AMT = PRCH_1_CYCL_AGO_BAL_AMT1;
	PRCH_2_CYCL_AGO_BAL_AMT = PRCH_2_CYCL_AGO_BAL_AMT1;

	drop
	i
	starting 
	ending 
	run_ofconsecmths
	flag_char11_divbyzero
	flag_char13_divbyzero
	flag_char14_divbyzero
	flag_char141_divbyzero;

	drop mindue_his_peroflim_01 - mindue_his_peroflim_06;
	drop MTH_DLQNT_CNT1 - MTH_DLQNT_CNT6;
	drop PRCH_1_CYCL_AGO_BAL_AMT1 - PRCH_1_CYCL_AGO_BAL_AMT6;
	drop PRCH_2_CYCL_AGO_BAL_AMT1 - PRCH_2_CYCL_AGO_BAL_AMT6;
		
run;

proc sql;
	connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
	create table work.dly_coll_cust_&pme_my. as select *
	from connection to DB2 (
		select  
		b.*
		from (	%Risk_Acct_Qry(zero)  )  as a
		left outer join 
		EDRTLR.RISK_CUST_ACCT_RLTNP_SNAPSHOT as b
		on a.RISK_ACCT_ID = b.RISK_ACCT_ID
		and b.MTH_TM_ID= &TM.     
		where PRIM_CUST_F='Y'
	);
	disconnect from db2;
quit;

proc sql noprint;
	connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
	create table work.DELI_&pme_my. as
	select *
	from connection to DB2(
		select 
			c.*,			   
			d.TOT_TRADE_CNT,
			d.ACTV_TRDS_CNT,
			d.SATFCTRY_TRADE_CNT,
			d.TRADE_OPND_IN_PAST_6_MTH_CNT,
			d.MTH_SINCE_LAST_ACTY_CNT,
			d.HIGHST_ACTV_UTLTN,
			d.TRADE_WORST_EVER_30_DAY_PD_CNT,
			d.TRADE_WORST_EVER_60_DAY_PD_CNT,
			d.TRADE_WORST_EVER_90_DAY_PD_CNT,
			d.OCC_60_DAY_PD_WITHIN_PAST_12_MTH_CNT,
			d.MTH_SINCE_OLDST_TRADE_OPND_CNT,
			d.MTH_SINCE_MOST_RECNT_TRADE_OPND_CNT,
			d.TRADE_OPND_PAST_12_MTH_WITH_BAL_GT_ZERO_CNT,
			d.SATFCTRY_TRADE_GT_3_MTH_CNT,
			d.TOT_HCCL_ALL_TRADE_AMT,
			d.ACTV_INSTLMNT_TRDS_BAL_GT_ZERO_CNT,
			d.TRADE_ZERO_BAL_CNT,
			d.TOT_BAL_ALL_TRADE_AMT,
			d.TOT_UTLTN_AMT,
			d.MTH_SINCE_MOST_RECNT_DLQNT_CNT,
			d.TM_30_DAY_PD_LAST_12_MTH_CNT,
			d.TM_60_DAY_PD_LAST_12_MTH_CNT,
			d.TM_OV_90_DAY_PD_LAST_12_MTH_CNT,
			d.TM_60_DAY_PD_EVER_CNT,
			d.TM_90_DAY_PD_EVER_CNT,
			d.TOT_MTH_PYMT_AMT,
			d.HIGHST_BAL_OS_AMT,
			d.HIGHST_ACTV_HCCL_AMT,
			d.ACTV_TRADE_30_DAY_RT_CNT,
			d.ACTV_TRADE_60_DAY_RT_CNT,
			d.ACTV_TRADE_OV_90_RT_CNT,
			d.TRADE_WORST_EVER_90_DAY_DLQNT_CNT,
			d.TRADE_90_DPD_LAST_24_MTH_CNT,
			d.TOT_PD_AMT as bureau_TOT_PD_AMT,
			d.MTH_SINCE_LAST_30_DAY_DLQNT_CNT,
			d.MTH_SINCE_LAST_60_DAY_DLQNT_CNT,
			d.OPN_TRADE_CNT,
			d.OLDST_OPN_TRADE_AGE_LINE_MTH_CNT,
			d.TOT_AVL_CR_NOT_UTILIZED_AMT,
			d.TOT_BAL_MORT_TRADE_AMT,
			d.TOT_BAL_ALL_TRADE_EXCLD_MORT_AMT,
			d.TRADE_TP_BANKCARD_CNT,
			d.TOT_HCCL_TP_BANKCARD_AMT,
			d.TOT_BAL_TP_BANKCARD_AMT,
			d.TRADE_BNK_INSTLMNT_CNT,
			d.TOT_HCCL_BNK_INSTLMNT_AMT,
			d.TOT_BAL_BNK_INSTLMNT_AMT,
			d.TRADE_BNK_REVLVNG_CNT,
			d.MAX_REVLVNG_CR_CRNT_UTLTN_AMT,
			d.TOT_HCCL_BNK_REVLVNG_AMT,
			d.TOT_BAL_BNK_REVLVNG_AMT,
			d.TOT_UTLTN_BNK_REVLVNG_CRD_AMT,
			d.TOT_BAL_BNK_REVLVNG_LINES_AMT,
			d.TOT_UTLTN_BNK_REVLVNG_LINE_AMT,
			d.TOT_COMPTV_RTO as bureau_TOT_COMPTV_RTO,
			d.TOT_COMPTV_CRD_RTO,
			d.TOT_COMPTV_LINE_RTO,
			d.TRDS_FNCL_INSTLMNT_CNT,
			d.TOT_HCCL_FNCL_INSTLMNT_AMT,
			d.TOT_BAL_FNCL_INSTLMNT_AMT,
			d.TRADE_FNCL_REVLVNG_CNT,
			d.TOT_HCCL_FNCL_REVLVNG_AMT,
			d.TOT_BAL_FNCL_REVLVNG_AMT,
			d.INQRY_CNT,
			d.INQRY_PAST_6_MTH_CNT,
			d.TRADE_NEVER_DLQNT_PC,
			d.COLCTN_INQR_CNT,
			d.MTH_SINCE_CR_BUREAU_WORST_RT_CNT,
			d.TRADE_INSTLMNT_CNT,
			d.TOT_HCCL_INSTLMNT_AMT,
			d.TOT_BAL_INSTLMNT_AMT,
			d.BNKRPY_CNT,
			d.COLCTN_CNT,
			d.ORIG_COLCTN_AMT,
			d.TOT_COLCTN_BAL_AMT,
			d.NOT_SUFFICENT_FUNDS_CHQ_CNT,
			d.DERGTRY_PUB_RECD_CNT,
			d.TOT_ORIG_LEGAL_AMT,
			d.LEGAL_CNT,
			d.TRDS_REVLVNG_CNT,
			d.TOT_HCCL_REVLVNG_AMT,
			d.TOT_BAL_REVLVNG_AMT,
			d.TRADE_RTL_REVLVNG_CNT,
			d.TOT_BAL_RTL_REVLVNG_AMT
		from 
		(
			select distinct RISK_CUST_ID 
			from(
				select  
				b.*
				from ( %Risk_Acct_Qry(zero) ) as a
				left outer join 
				EDRTLR.RISK_CUST_ACCT_RLTNP_SNAPSHOT as b
				on a.RISK_ACCT_ID = b.RISK_ACCT_ID
				and b.MTH_TM_ID = &TM.     
				where b.PRIM_CUST_F = 'Y' 
			) as fnl
		) as c
		inner join EDRTLR.RISK_CR_BUREAU_DELI_MTH_SNAPSHOT as d
		on c.RISK_CUST_ID = d.RISK_CUST_ID and d.MTH_TM_ID = &TM.      
	);
quit;

PROC SORT DATA= work.dly_coll_cust_&pme_my.; by RISK_CUST_ID;  run;
PROC SORT DATA= work.DELI_&pme_my. ; by RISK_CUST_ID;  run;

data work.sDELI_&pme_my._FINAL;
	merge work.dly_coll_cust_&pme_my. (IN=a) work.DELI_&pme_my. (IN=b);
	by RISK_CUST_ID;
run;

/* EDIT (N DONMEZ): Added other necessary variables from Probe */
proc sql;
	connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
	create table work.PROBE_&pme_my._new as
	select *
	from connection to DB2( 
		select * from
		(
			select 
			b.ACCT_ID,
			b.ACCT_NUM,
			b.RISK_ACCT_ID,
			b.RISK_CUST_ID,
			b.avg_bal_3_mth,
			b.avg_cr_sav_bal_6_mth,
			b.avg_dly_bal,
			b.avg_dr_sav_bal_6_mth,
			b.avg_fncl_chrg_last_3_mth,
			b.avg_tot_revlvng_fx_rev_3_mth,
			b.avl_cr_not_utilized,
			b.csh_bal,
			b.cust_for_years,
			b.day_dlqnt,
			b.gbl_3_mth_avg_bal,
			b.gbl_3_mth_avg_opn_to_buy,
			b.gbl_3_mth_avg_utilztn,
			b.gbl_crnt_bal,
			b.gbl_crnt_bal_less_mort,
			b.gbl_crnt_opn_to_buy,
			b.gbl_crnt_utilztn,
			b.gbl_revlvng_bal,
			b.hi_revlvng_util_3_mth,
			b.hi_revlvng_utilztn_crnt,
			b.highst_utilztn,
			b.invstmnt_prd_f,
			b.last_dep_amt,
			b.mth_since_cr_bureau_worst_rt,
			b.mth_since_last_90,
			b.mth_since_last_dep,
			b.mth_since_most_recnt_dlqnt,
			b.mth_since_negative_actn,
			b.mth_till_crd_exp,
			b.non_csh_bal,
			b.num_mth_fncl_txn_in_last_3_mth,
			b.num_mth_last_rvw,
			b.num_mth_since_last_acty,
			b.num_mth_since_ofr_expires,
			b.num_mth_since_stmt_zero_bal,
			b.num_mth_until_ofr_expires,
			b.num_mth_zero_bal_3_mth,
			b.num_non_suffcnt_funds,
			b.num_of_actv_trds_pd,
			b.num_of_colctn_inqrs,
			b.num_of_crnt_dlqnt_acct,
			b.num_of_dep_prds,
			b.num_of_mth_lmt_incr,
			b.num_of_mth_since_last_nsf_trig,
			b.num_satfctry_trds,
			b.num_trds,
			b.num_trds_6_mth,
			b.rto_bal_to_lmt_3_mth,
			b.rto_csh_cr_lmt,
			b.rto_last_2_mth_bal_to_lmt,
			b.rto_loans_to_sav,
			b.rto_recnt_pymt_to_bal_3_mth,
			b.rto_revlvng_pymt_bal,
			b.sav_acct_avg_dly_bal_crnt_mth,
			b.sav_acct_avg_dly_bal_fncl_yr,
			b.sav_acct_day_last_acty,
			b.sav_acct_day_last_dep,
			b.sav_acct_mth_end_bal,
			b.sav_acct_num_nsf_p1_p2,
			b.sav_acct_ov_draft_prtctn_lmt,
			b.sav_acct_ovdr_day,
			b.sav_acct_prev_mth_agg_cr_bal,
			b.sav_acct_prev_mth_agg_cr_day,
			b.sav_acct_prev_mth_avg_bal,
			b.sav_acct_years_opn,
			b.sav_cust_avg_dly_bal_cur_mo,
			b.sav_cust_avg_dly_bal_fncl_yr,
			b.sav_cust_day_last_acty,
			b.sav_cust_day_last_dep,
			b.sav_cust_mth_end_bal,
			b.sav_cust_num_of_dep_acct,
			b.sav_cust_ov_draft_prtctn_lmt,
			b.sav_cust_ovdr_day,
			b.sav_cust_prev_mth_agg_cr_bal,
			b.sav_cust_prev_mth_agg_cr_day,
			b.sav_cust_prev_mth_avg_bal,
			b.sav_cust_years_opn,
			b.spl_acct_age,
			b.spl_fncl_chrg,
			b.spl_os_bal,
			b.spl_udt_bal,
			b.spl_udt_day_dlqnt,
			b.step_f,
			b.sum_dep_bal,
			b.sum_invest_bal,
			b.term_mth,
			b.tot_amort_mth,
			b.tot_amt_dlqnt_1,
			b.tot_amt_dlqnt_2,
			b.tot_amt_pd,
			b.tot_bal_due_colctns,
			b.tot_comptv_rto,
			b.tot_comptv_rto_crd,
			b.tot_comptv_rto_lines,
			b.tot_crd_bal,
			b.tot_day_dlqnt,
			b.tot_lines_bal,
			b.tot_mort_bal,
			b.tot_mort_f,
			b.tot_mth_pymts,
			b.tot_orig_aprsd_amt,
			b.tot_rel,
			b.tot_revlvng_fncl_chrg_rev_lmt,
			b.tot_term_mth,
			b.tot_utilztn,
			b.tot_utl_bnk_crd,
			b.tot_utl_bnk_revlvng_lines,
			b.trds_worst_ever_90,
			b.uns_gbl_3_mth_avg_bal,
			b.uns_gbl_3_mth_avg_utilztn,
			b.uns_gbl_3mths_avg_revlvng_bal,
			b.uns_gbl_crnt_bal,
			b.uns_gbl_crnt_lmt,
			b.uns_gbl_crnt_opn_to_buy,
			b.uns_gbl_crnt_utilztn,
			b.uns_gbl_revlvng_bal,
			b.worst_acc_pi_all_sl_mr_dp,
			b.worst_acct_pi_all_lc,
			b.worst_acct_pi_all_vs,
			b.worst_acct_pi_cld_cd,
			b.worst_acct_pi_lc,
			b.worst_acct_pi_sl_mr_dp,
			b.worst_acct_pi_vs,
			b.worst_acct_stat_all,
			b.worst_dlqnt,
			b.worst_dlqnt_all,
			ROW_NUMBER() OVER (PARTITION BY b.RISK_ACCT_ID ORDER BY b.RISK_ACCT_ID ASC) AS ROWNUM1

			from ( %Risk_Acct_Qry(zero) ) as a
			inner join EDRTLR.PROBE_CUST_RSLT as b
			on a.RISK_ACCT_ID=b.RISK_ACCT_ID 
			where  
			year(eff_tmstmp)=&pme_yr. AND 
			month(eff_tmstmp)= &pme_mnth.

		) fnl
		where ROWNUM1=1
		order by RISK_ACCT_ID
		WITH UR
	);
	disconnect from db2;
quit;

PROC SORT DATA=work.sRV_MONTHLY_&pme_my._2 ; BY RISK_ACCT_ID;  RUN;
PROC SORT DATA=work.sDELI_&pme_my._FINAL ;    BY RISK_ACCT_ID;  RUN;

/* include corp no below to fix the logic for scorecard output*/
proc sql;
	connect to db2(database=&db2_svr. user="%sysget(DB2_USER)" password="%sysget(DB2_USER_PASS)");
	create table work.dly_coll_&pme_my. as
	select *
	from connection to DB2( 
		(%Risk_Acct_Qry(zero)
		order by RISK_ACCT_ID)
	);
	disconnect from db2;
quit;

/* added following step to include corporation number */
proc sql;
	create table work.dly_coll_&pme_my._v2 as
	select 
		RISK_ACCT_ID
		,EFF_TM_ID
		,BNS_DLQNT_DAY
		,CR_LMT_AMT 
		,TOT_NEW_BAL_AMT
		,score
		,CATS(0,A.corptn_num,substr(A.acct_num,verify(A.acct_num,'0'))) as ACCT_NUM
	
	from work.dly_coll_&pme_my. a
	order by RISK_ACCT_ID;
quit;

data work.dldata_merged_&pme_my.;
	merge  work.dly_coll_&pme_my._v2      (in=a)   
	       work.sRV_MONTHLY_&pme_my._2  (in=b)  
	       work.PROBE_&pme_my._new     (in=c)   
	       work.sDELI_&pme_my._FINAL    (iN=d) 
	;
	by RISK_ACCT_ID;
	if a;
	array x _numeric_;
	do over x; if x=. then x=-999999; end;
	drop ROWNUM1;  
	drop ACCT_NUM;
run;

/* write the input for DL model */
proc export data=work.dldata_merged_&pme_my.
	outfile="&dldropf.dl_input_&pme_my..csv" dbms=csv replace;
run;

/* send the file name in the trigger file */
data _null_;
	file "&dldropf.startdl.txt";
	put "dl_input_&pme_my..csv";
run;

/* end of RISKDL_PRT1_MTHLY.sas */
