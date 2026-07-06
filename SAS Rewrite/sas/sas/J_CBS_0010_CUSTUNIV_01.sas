proc sql;
	connect using NZWRK as nzcon;
	execute(drop table &RRAP_WRK..CIS_DATA_POP_01 if exists) by nzcon;
	execute(create table &RRAP_WRK..CIS_DATA_POP_01 as (
		select
			a.*,
			a1.tm_id as mth_tm_id,
			a1.TM_LVL_END_DT as process_date,
			b.basel_acct_id,
			c.PIT_STAT_VER_2_CD as PIT_STAT_REV,
			c.CONSM_PRD_TREATMNT_CD as PRD_TREATMNT_CD_REV,
			c.CONSM_SCORECRD_EXCLSN_F as SCORECRD_EXCLSN_F_REV,
			d.PIT_STATUS_V2 as PIT_STAT_SPL,
			d.prd_id as PRD_ID_SPL,
			d.MODEL_EXCL_F as SCORECRD_EXCLSN_F_SPL,
			d.COMM_F_V2 as COMM_FLG_SPL,
			d.OS_BAL_AMT_V2 as OS_BAL_AMT_SPL,
			d.TREATMNT_F as PRD_TREATMNT_CD_SPL,
			g.RECD_STAT_CD as RECD_STAT_CD_SPL,
			f.BNS_DLQNT_DAY as BNS_DLQNT_DAY_REV,
			g.DAY_ODUE as DAY_ODUE_SPL,
			e.DLQNT_DAY_CNT as DLQNT_DAY_CNT_MOR,
			k.STATUS as PIT_STAT_MOR,
			k.MODEL_EXCL as SCORECRD_EXCLSN_F_MOR,
			k.LRA_STATUS as LRA_STATUS_MOR,
			k.PAID_OFF_DATE as PAID_OFF_DATE_MOR,
			k.CURRENT_BAL as CURRENT_BAL_MOR,
			k.TOTAL_SUSPENSE as TOTAL_SUSPENSE_MOR,
			e.CONSM_PRD_TREATMNT_CD as PRD_TREATMNT_CD_MOR,
			e.COMM_TP_CD as COMM_TP_CD_MOR,
			e.OS_BAL_AMT as OS_BAL_AMT_MOR,
			h.FRCLSR_F as FRCLSR_F_MOR,
			h.PD_OFF_F as PD_OFF_F_MOR,
			h.FUND_CD as FUND_CD_MOR,
			h.MTH_IN_ARRS_CNT as MTH_IN_ARRS_CNT_MOR,
			h.LIFE_INSUR_CD as LIFE_INSUR_CD_MOR,
			f.TRNST_NUM as TRNST_NUM_REV,
			f.SRC_CD as SOURCE_CD,
			f.BLOCK_RECL_CD,
			f.ACCT_STAT_CD,
			f.CR_LMT_AMT,
			f.TOT_NEW_BAL_AMT,
			f.NON_ACCRL_DT,
			f.WRITE_OFF_DT,
			f.ACCT_CLS_RSN_CD,
			f.PRD_CD as PRD_CD_REV,
			i.LAST_NEW_DFT_DT as DFT_DT_REV,
			i.LAST_NEW_DFT_BAL_AMT DFT_BAL_REV,
			i.MODEL_DFT_F as MODEL_DFT_F_REV,
			j.LAST_NEW_DFT_DT as DFT_DT_SPL,
			j.LAST_NEW_DFT_BAL_AMT DFT_BAL_SPL,
			j.MODEL_DFT_F as MODEL_DFT_F_SPL,
			l.DEFAULT_DATE as DEFAULT_DATE_MOR,
			l.DEFAULT_BAL as DEFAULT_BAL_MOR,
			l.DEFAULT_IND AS DEFAULT_IND_MOR,
		case 
			when ( SUBSTR(f.BLOCK_RECL_CD,1,1)='V' ) then 1 
			else 0 
		end 
	as blocked,
		case 
			when ( f.BLOCK_RECL_CD='B4' ) then 1 
			else 0 
		end 
	as deceased,
		case 
			when ( SUBSTR(f.BLOCK_RECL_CD,1,1)='S' or SUBSTR(f.BLOCK_RECL_CD,1,1)=' S') then 1 
			else 0 
		end 
	as stolen,
		case 
			when a.product in ('LOC','MOR','SCL','SPL','SSL','VAX','VCL','VFA','VFF','VGD','VIC','VLR','VUS','VZX','VZZ') then 1 
			else 0 
		end 
	as lend_prods
		from 
			credit_risk.CIS_DATA_NEW2 as a
		left join &RRAP_DB..TM_DIM as a1 on a.file_date=a1.TM_LVL_ST_DT and a1.TM_LVL='Month' and a.file_yr_mth in (&dt.)
		left join &RRAP_DB..BASEL_ACCT_DIM as b on lpad(a.account,23,0)=b.acct_num
		left join &RRAP_DB..BASEL_REVLVNG_CR_BASE_DRVD_VARS as c on b.basel_acct_id=c.basel_acct_id and a1.tm_id=c.mth_tm_id and c.mth_tm_id=&tm_id.
		left join &RRAP_DB..BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 as d on b.basel_acct_id=d.basel_acct_id and a1.tm_id=d.mth_tm_id and d.mth_tm_id=&tm_id.
		left join &RRAP_DB..BASEL_MORT_ACCT_DRVD_VARS as e on b.basel_acct_id=e.basel_acct_id and a1.tm_id=e.mth_tm_id and e.mth_tm_id=&tm_id.
		left join &RRAP_DB..BASEL_REVLVNG_CR_MTH_SNAPSHOT as f on b.basel_acct_id=f.basel_acct_id and a1.tm_id=f.mth_tm_id and f.mth_tm_id=&tm_id.
		left join &RRAP_DB..BASEL_PSNL_LOAN_MTH_SNAPSHOT as g on b.basel_acct_id=g.basel_acct_id and a1.tm_id=g.mth_tm_id and g.mth_tm_id=&tm_id.
		left join &RRAP_DB..BASEL_MORT_MTH_SNAPSHOT as h on b.basel_acct_id=h.basel_acct_id and a1.tm_id=h.mth_tm_id and h.mth_tm_id=&tm_id.
		left join &RRAP_DB..REVLVNG_CR_OBSVTN_PT_DRVD_VAR as i on b.basel_acct_id=i.basel_acct_id and a1.tm_id=i.OBSVTN_MTH_TM_ID and i.OBSVTN_MTH_TM_ID=&tm_id.
		left join &RRAP_DB..PSNL_LOAN_OBSVTN_PT_DRVD_VAR as j on b.basel_acct_id=j.basel_acct_id and a1.tm_id=j.OBSVTN_MTH_TM_ID and j.OBSVTN_MTH_TM_ID=&tm_id.
		left join &FRG_DB..STATUS_FINAL as k on to_number(h.MORT_NUM,'9999999')=k.MORTGAGE_NO and a1.TM_LVL_END_DT=date_trunc('day',k.process_date) and date_trunc('day',k.process_date)=&string.
		left join &FRG_DB..TWELVE_MON_DEF_WINDOW as l on to_number(h.MORT_NUM,'9999999')=l.MORTGAGE_NO and a1.TM_LVL_END_DT=l.process_date and l.process_date=&string.
			where a.file_yr_mth in (&dt.) and a.RELATION_CODE <>'POA' and isnull(a.PRODUCT,'') not in ('SEA') and isnull(f.PRD_CD,'') not in ('VFB','BLV')
	) WITH DATA
				) by nzcon;
	disconnect from nzcon;
quit;

proc sql;
	connect using NZWRK as nzcon;
	execute(drop table &RRAP_WRK..CIS_DATA_POP_02 if exists) by nzcon;
quit;
data NZWRK.CIS_DATA_POP_02 
	(bulkload=yes BL_METHOD=CLILOAD drop=DFT_DT_REV DFT_DT_SPL DEFAULT_DATE_MOR DFT_BAL_REV DFT_BAL_SPL DEFAULT_BAL_MOR MODEL_DFT_F_REV MODEL_DFT_F_SPL 
		DEFAULT_IND_MOR SCORECRD_EXCLSN_F_: PRD_TREATMNT_CD_: PIT_STAT_: BNS_DLQNT_DAY_REV DAY_ODUE_SPL DLQNT_DAY_CNT_MOR);

	/* set NZWRK.CIS_DATA_POP_01(bulkunload=YES BL_DELIMITER='|' bl_options="escapechar '\'"); */
	set NZWRK.CIS_DATA_POP_01;

	cid_num=input(cid,15.);
	acct_numeric=input(account,13.);


	mor_ind=0;
	spl_ind=0;
	rev_ind=0;
	ssl_ind=0;

	lend_prods_CUR=0;
	lend_prods_CLO=0;
	lend_prods_BNK=0;
	lend_prods_DEF=0;
	lend_prods_CHG=0;
	lend_prods_WO=0;

	lend_prods_COMM=0;
	lend_prods_COMM_CUR=0;
	lend_prods_COMM_CLO=0;
	lend_prods_COMM_DEF=0;
	lend_prods_COMM_CHG=0;
	lend_prods_COMM_WO=0;

	*Add default definitions for commercial mortgages;
	IF product='MOR' and COMM_TP_CD_MOR='COMMERCIAL' then
		do;
			if  upcase(COMM_TP_CD_MOR) in ('RESIDENTIAL', 'COMMERCIAL')  and PD_OFF_F_MOR ^='Y'  and (( DLQNT_DAY_CNT_MOR <90) 
				and upcase(FRCLSR_F_MOR) ^= 'Y' and CURRENT_BAL_MOR ^= 0 and upcase(lra_status_mor) ^= 'Y' ) 
				or CURRENT_BAL_MOR < 0 then
				PIT_STAT_MOR='CUR';
			else if (upcase(COMM_TP_CD_MOR) in ('RESIDENTIAL', 'COMMERCIAL') and PD_OFF_F_MOR ='Y' 
				and ((DLQNT_DAY_CNT_MOR >= 90) or upcase(FRCLSR_F_MOR) = "Y" or upcase(lra_status_mor) ="Y") 
				and CURRENT_BAL_MOR > 0 ) 
				or 
				(upcase(COMM_TP_CD_MOR)in ('RESIDENTIAL', 'COMMERCIAL') 
				and upcase(FRCLSR_F_MOR) = "Y" 
				and upcase(PD_OFF_F_MOR) = "Y" 
				and (max(CURRENT_BAL_MOR, -TOTAL_SUSPENSE_MOR)>0)) then
				PIT_STAT_MOR='DEF';
		end;
/*case1 - a*/
	if product in ('LOC' 'SCL','SSL','VAX','VCL','VFA','VFF','VGD','VIC','VLR','VUS','VZX','VZZ') and PRD_CD_REV ne 'BLV' then
		do;
			if lend_prods=1 and PIT_STAT_REV='CUR' and blocked=0 and stolen=0 and CR_LMT_AMT>0 and BLOCK_RECL_CD ne 'B5' then
				lend_prods_CUR=1;
			else if lend_prods=1 and PIT_STAT_REV='CUR' and blocked=1 and CR_LMT_AMT>0 and TOT_NEW_BAL_AMT>0 and BLOCK_RECL_CD ne 'B5' and stolen=0 and deceased=0 then
				lend_prods_CUR=1;
			else if lend_prods=1 and PIT_STAT_REV='CUR' and blocked=0 and CR_LMT_AMT=0 and TOT_NEW_BAL_AMT<=0 and BLOCK_RECL_CD ne 'B5' and deceased=0  then
				lend_prods_CLO=1;
			else if lend_prods=1 and PIT_STAT_REV='CUR' and blocked=1 and TOT_NEW_BAL_AMT<=0 and deceased=0 and BLOCK_RECL_CD ne 'B5'  then
				lend_prods_CLO=1;
			else if lend_prods=1 and PIT_STAT_REV='CUR' and blocked=0 and stolen=1 and BLOCK_RECL_CD ne 'B5' and deceased=0 then
				lend_prods_CLO=1;
			else if lend_prods=1 and PIT_STAT_REV='CUR' and blocked=0 and stolen=0 and deceased=1 and BLOCK_RECL_CD ne 'B5' then
				lend_prods_CLO=1;
			else if lend_prods=1 and PIT_STAT_REV='CUR' and blocked=1 and stolen=0 and deceased=0 and BLOCK_RECL_CD eq 'B5' then
				lend_prods_BNK=1;
			else if lend_prods=1 and PIT_STAT_REV='DEF' then
				lend_prods_DEF=1;
			else if lend_prods=1 and PIT_STAT_REV='CHG' then
				lend_prods_CHG=1;
			else if lend_prods=1 and PIT_STAT_REV='' then
				lend_prods_WO=1;
		end;
/*case2 - b*/
	if product in ('LOC' 'SCL','SSL','VAX','VCL','VFA','VFF','VGD','VIC','VLR','VUS','VZX','VZZ') and PRD_CD_REV eq 'BLV' then
		do;
			if lend_prods=1 and PIT_STAT_REV='CUR' and blocked=0 and stolen=0 and CR_LMT_AMT>0 then
				lend_prods_COMM_CUR=1;
			else if lend_prods=1 and PIT_STAT_REV='CUR' and blocked=1 and CR_LMT_AMT>0 and TOT_NEW_BAL_AMT>0 and stolen=0 and deceased=0 then
				lend_prods_COMM_CUR=1;
			else if lend_prods=1 and PIT_STAT_REV='CUR' and blocked=0 and CR_LMT_AMT=0 and TOT_NEW_BAL_AMT<=0 and deceased=0  then
				lend_prods_COMM_CLO=1;
			else if lend_prods=1 and PIT_STAT_REV='CUR' and blocked=1 and TOT_NEW_BAL_AMT<=0 and deceased=0  then
				lend_prods_COMM_CLO=1;
			else if lend_prods=1 and PIT_STAT_REV='CUR' and blocked=0 and stolen=1 and deceased=0 then
				lend_prods_COMM_CLO=1;
			else if lend_prods=1 and PIT_STAT_REV='CUR' and blocked=0 and stolen=0 and deceased=1 then
				lend_prods_COMM_CLO=1;
			else if lend_prods=1 and PIT_STAT_REV='DEF' then
				lend_prods_COMM_DEF=1;
			else if lend_prods=1 and PIT_STAT_REV='CHG' then
				lend_prods_COMM_CHG=1;
			else if lend_prods=1 and PIT_STAT_REV='' then
				lend_prods_COMM_WO=1;
		end;
/*case3 - c*/
	else if product in ('SPL') and COMM_FLG_SPL ne '1' then
		do;
			if lend_prods=1 and PIT_STAT_SPL='CUR' and OS_BAL_AMT_SPL>0 then
				lend_prods_CUR=1;
			else if lend_prods=1 and PIT_STAT_SPL='CUR' and OS_BAL_AMT_SPL=0 then
				lend_prods_CLO=1;
			else if lend_prods=1 and PIT_STAT_SPL='DEF' then
				lend_prods_DEF=1;
			else if lend_prods=1 and PIT_STAT_SPL='CHG' then
				lend_prods_CHG=1;
			else if lend_prods=1 and PIT_STAT_SPL='' then
				lend_prods_WO=1;
		end;
/*case4 - d*/
	else if product in ('SPL') and COMM_FLG_SPL eq '1' then
		do;
			if lend_prods=1 and PIT_STAT_SPL='CUR' and OS_BAL_AMT_SPL>0 then
				lend_prods_COMM_CUR=1;
			else if lend_prods=1 and PIT_STAT_SPL='CUR' and OS_BAL_AMT_SPL=0 then
				lend_prods_COMM_CLO=1;
			else if lend_prods=1 and PIT_STAT_SPL='DEF' then
				lend_prods_COMM_DEF=1;
			else if lend_prods=1 and PIT_STAT_SPL='CHG' then
				lend_prods_COMM_CHG=1;
			else if lend_prods=1 and PIT_STAT_SPL='' then
				lend_prods_COMM_WO=1;
		end;
/*case5 - e*/
	else if product='MOR' and COMM_TP_CD_MOR eq 'RESIDENTIAL' then
		do;
			if lend_prods=1 and PIT_STAT_MOR='CUR' and OS_BAL_AMT_MOR>0 then
				lend_prods_CUR=1;
			else if lend_prods=1 and PIT_STAT_MOR='CUR' and OS_BAL_AMT_MOR=0 then
				lend_prods_CLO=1;
			else if lend_prods=1 and PIT_STAT_MOR='DEF' then
				lend_prods_DEF=1;
			else if lend_prods=1 and PIT_STAT_MOR='CHG' then
				lend_prods_CHG=1;
			else if lend_prods=1 and PIT_STAT_MOR='' then
				lend_prods_WO=1;
		end;
	else if product='MOR' and COMM_TP_CD_MOR ne 'RESIDENTIAL' then
		do;
			if lend_prods=1 and PIT_STAT_MOR='CUR' and OS_BAL_AMT_MOR>0 then
				lend_prods_COMM_CUR=1;
			else if lend_prods=1 and PIT_STAT_MOR='CUR' and OS_BAL_AMT_MOR=0 then
				lend_prods_COMM_CLO=1;
			else if lend_prods=1 and PIT_STAT_MOR='DEF' then
				lend_prods_COMM_DEF=1;
			else if lend_prods=1 and PIT_STAT_MOR='' then
				lend_prods_COMM_WO=1;
		end;

	check_ind=max(lend_prods_CUR,lend_prods_CLO,lend_prods_DEF,lend_prods_CHG,lend_prods_WO,lend_prods_COMM);

	if lend_prods_CUR=1 then
		do;
			mor_ind=(product='MOR');
			spl_ind=(product='SPL');
			rev_ind=(product in ('LOC' 'SCL','VAX','VCL','VFA','VFF','VGD','VIC','VLR','VUS','VZX','VZZ'));
			ssl_ind=(product='SSL');
		end;

	PRIVATE_BANK_IND=(TRNST_NUM_REV in ('7096', '8896', '21436', '30957', '32839', '38786', '38836', '41731',
		'47308', '52100', '62299', '65912', '67116', '72793', '73270', '75523', '83170', '87106'));

	*****************************************************************************************;
	*Account level exclusions;
	*****************************************************************************************;

	*corporate exclusion;
	if SOURCE_CD = '980' and rev_ind=1 and (lend_prods_CUR=1 or lend_prods_CLO=1) then
		CORP_COMM_EXCL='Y';
		else if (mor_ind=1 or spl_ind=1) and (lend_prods_COMM_CUR=1 or lend_prods_COMM_CLO=1) then CORP_COMM_EXCL='Y';
		else CORP_COMM_EXCL='N';

	if SOURCE_CD = '911' and rev_ind=1 and (lend_prods_CUR=1 or lend_prods_CLO=1) then
		STAFF_EXCL='Y';
	else STAFF_EXCL='N';



	*Definition of default consolidate fields;
	*****************************************************************************************;
	format default_date date9.;

	default_date=coalesce(DFT_DT_REV,DFT_DT_SPL,DEFAULT_DATE_MOR);
	default_bal=coalesce(DFT_BAL_REV,DFT_BAL_SPL,DEFAULT_BAL_MOR);
	default_ind=(MODEL_DFT_F_REV='Y' or MODEL_DFT_F_SPL='Y' or DEFAULT_IND_MOR=1);

	* Combine exclusion flags;
	MODEL_EXCL=coalescec(SCORECRD_EXCLSN_F_REV,SCORECRD_EXCLSN_F_SPL,SCORECRD_EXCLSN_F_MOR);

	* Adjustment for SSL and VUS without block codes;
	if product in ('SSL' 'VUS') and BLOCK_RECL_CD='' then MODEL_EXCL='N';

	PROD_TREAT=coalescec(PRD_TREATMNT_CD_REV,PRD_TREATMNT_CD_SPL,PRD_TREATMNT_CD_MOR);
	PIT_STAT=coalescec(PIT_STAT_REV,PIT_STAT_MOR,PIT_STAT_SPL);

			days_dlq=(max((BNS_DLQNT_DAY_REV-30),DAY_ODUE_SPL,DLQNT_DAY_CNT_MOR));
	*****************************************************************************************;

run;
