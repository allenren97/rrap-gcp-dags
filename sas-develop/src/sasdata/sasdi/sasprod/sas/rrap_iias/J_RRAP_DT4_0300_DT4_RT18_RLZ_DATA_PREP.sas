***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0300_DT4_RT18_RLZ_DATA_PREP.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRFRGP1D
*  Target Table:  DT4_RT18_RLZ_DATA_PREP
*  
*  Purpose: Derive DT4 specific variables to be used in downstream DT4 jobs
*
*  Frequency: Quarter End runs
*
*  Notes: This is a work table that holds the 9 months of data from multiple tables in one table, 
*  		  which is referenced in the next job.
*
*	Change Log:
*	2021-09-21: Hadi Dimashkieh - Initial Development
*
*
*
***************************************************************************************************************************;


%rrap_dt4_autoexec();


data dates;
	process_mth_tm_id = &mth_tm_id.;
	pd_obs_mth_tm_id  = &mth_tm_id. -  3*40;
	lgd_obs_mth_tm_id = &mth_tm_id. - 36*40;

	pd_window_st      = &mth_tm_id. - 2*40;
	pd_window_end     = &mth_tm_id. - 0*40;

	lgd_window_1	  = &mth_tm_id. - 27*40;
	lgd_window_st     = &mth_tm_id. - 26*40;
	lgd_window_end    = &mth_tm_id. - 24*40;
	
	call symputx('pd_obs_mth_tm_id',pd_obs_mth_tm_id);
run;

proc transpose data=dates out=dtran(rename=(_name_=month col1=mth_tm_id)); run;

proc sql;
	create table date_desc as
	select a.month, a.mth_tm_id, t.tm_lvl_end_dt as month_end_dt
	from dtran a, nzrrap.tm_dim t
	where a.mth_tm_id = t.tm_id and t.tm_lvl='Month'
	order by 2;
quit;

proc sql;
	create table rundates as
	select distinct t.tm_id as mth_tm_id, t.tm_lvl_end_dt 
	from nzrrap.tm_dim t, dates d
	where t.tm_lvl='Month' and 
	(t.tm_id = d.process_mth_tm_id or
	 t.tm_id = d.pd_obs_mth_tm_id or 
	 t.tm_id = d.lgd_obs_mth_tm_id or
	(t.tm_id between d.pd_window_st and d.pd_window_end) or 
	(t.tm_id between d.lgd_window_1 and d.lgd_window_end))
	order by 1;
quit;


/*	((DRV.MTH_TM_ID >= (&mth_tm_id.-3 * 40) AND DRV.MTH_TM_ID <= &mth_tm_id.) */
/*		or (DRV.mth_tm_id <= (&mth_tm_id.-12*2*40) AND DRV.mth_tm_id >= (&mth_tm_id.-12*2*40-2*40)) or DRV.MTH_TM_ID = (&mth_tm_id.-12*3*40))*/
data _null_;
	set date_desc;
	put month= mth_tm_id= month_end_dt =;
	put;
run;




proc sql;
connect using db2rrap as dbcon;
create table rt18_ks_temp as select * from connection to dbcon(
select basel_acct_id
	
,max(0,NVL(ADJUSTED_OS_BAL_AMT, OS_BAL_AMT)) as drawn
,MAX(0,EAD_FINAL_RPTG_RTO*MAX((af_zero_net_undrawn_amt + max(0,NVL(ADJUSTED_OS_BAL_AMT, OS_BAL_AMT))), 
                               max(0,NVL(ADJUSTED_OS_BAL_AMT, OS_BAL_AMT)))
       - max(0,NVL(ADJUSTED_OS_BAL_AMT, OS_BAL_AMT))) 
    as undrawn 

/*,max(CR_LMT_AMT,OS_BAL_AMT) AS EAD_OBS_AUTHORIZED_AMT	*/
, CASE 
 	WHEN REGEXP_LIKE(CCAR_BASEL_PRD_TP_NM, 'HELOC|OTHER_CON_CC|OTHER_CON_CL|REV_CON_CC|REV_CON_CL|REV_CON_SLR|OTHER_CON_SLR|OTHER_CON_SLT', 'i' ) = 1 THEN 1
 	ELSE NULL
 END AS EAD_INCL
from &RRAP_DB2..BASEL_ANALYTCL_BL_INSTRMNT_FACT
where mth_tm_id = (&mth_tm_id.-3*40) and src_sys_cd = 'KS'  
/*AND CONSM_PRD_TREATMNT_CD = 'A'
	AND SML_BUS_F = 'N'
	AND PIT_STAT_CD IN ('CUR')
	AND TRNST_EXCLSN_F = 'N' */
	);
quit;


proc sql;
connect using nzuser as nzcon;
execute(drop table &RRAP_WRK..rt18_ks_temp if exists;) by nzcon;
/*execute(delete from EDRTLRD1D.rt18_ks_temp;) by nzcon;*/
execute(commit;) by nzcon;
quit;
proc append base=nzuser.rt18_ks_temp(BULKLOAD=YES BL_METHOD=CLILOAD) data=rt18_ks_temp force; run;

proc sql noprint;
	select distinct mth_tm_id into :all_rundates separated by ', '
		from rundates
	order by 1;
quit;



proc sql;
connect using nzuser as nzcon;
execute(DROP TABLE &RRAP_WRK..DT4_RT18_RLZ_DATA_PREP IF EXISTS;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
connect using nzuser as nzcon;
execute(
CREATE TABLE &RRAP_WRK..DT4_RT18_RLZ_DATA_PREP (
		SRC_SYS_CD VARCHAR(10 OCTETS), 
		BASEL_ACCT_ID BIGINT, 
		MORT_NO VARCHAR(20 OCTETS), 
		MTH_TM_ID INTEGER NOT NULL, 
		OS_BAL_AMT DECIMAL(17 , 3), 
		TOT_CRNT_BAL_AMT DECIMAL(17 , 3), 
		TOT_UNPAID_FNCL_CHRG_AMT DECIMAL(17 , 3), 
		ACCRL_STAT_F CHAR(1 OCTETS), 
		DRAWN DECIMAL(17 , 3), 
		UNDRAWN DECIMAL(17 , 3), 
		EAD_INCL INTEGER, 
		EAD_OBS_AUTHORIZED_AMT DECIMAL(17 , 3), 
		PIT_STATUS_CD CHAR(10 OCTETS), 
		PD_BAND CHAR(10 OCTETS), 
		DT4_RISK_RT_KEY_VAL CHAR(4 OCTETS), 
		DT4_EXPSR_CL_KEY_VAL CHAR(4 OCTETS), 
		CONSM_PRD_TREATMNT_CD CHAR(10 OCTETS), 
		SML_BUS_F CHAR(1 OCTETS), 
		TRNST_EXCLSN_F CHAR(1 OCTETS)
	)
	ORGANIZE BY COLUMN
	DATA CAPTURE NONE 
	DISTRIBUTE BY HASH (BASEL_ACCT_ID, MORT_NO, MTH_TM_ID);
) by nzcon;
execute(commit;) by nzcon;
quit;


%macro multirun;

%let nobs=0;
data _null_;
	set rundates;
	n=compress(put(_n_,best.));
	call symputx('mth_tm_id'!!n,put(mth_tm_id,5.));
	call symputx('nobs',_n_);
run;

%do i = 1 %to &nobs.;

%let mth_tm_id = &&mth_tm_id&i.;
%put ******** Now Loading mth_tm_id = &mth_tm_id.;



proc sql;
connect using NZUSER as nzcon;
execute(
	insert into &RRAP_WRK..DT4_RT18_RLZ_DATA_PREP 
	SELECT
		 'KS' as SRC_SYS_CD
		,drv.basel_acct_id
		,null as mort_no
		,drv.mth_tm_id 
		,snp.TOT_NEW_BAL_AMT AS OS_BAL_AMT 
		,NULL as TOT_CRNT_BAL_AMT
		,snp.TOT_UNPAID_FNCL_CHRG_AMT
		,drv.ACCRL_STAT_F
		,k.drawn
		,k.undrawn
		,k.EAD_INCL
		/*,EAD_OBS_AUTHORIZED_AMT*/
		,max(snp.CR_LMT_AMT,snp.TOT_NEW_BAL_AMT) AS EAD_OBS_AUTHORIZED_AMT
		,drv.PIT_STAT_VER_2_CD AS PIT_STATUS_CD 
		,E.PD_BAND
		,E.DT4_RISK_RT_KEY_VAL 
		,E.DT4_EXPSR_CL_KEY_VAL 
		,DRV.CONSM_PRD_TREATMNT_CD
		,DRV.SML_BUS_F
		,DRV.TRNST_EXCLSN_F

	FROM
		&RRAP_DB..BASEL_REVLVNG_CR_BASE_DRVD_VARS DRV
	INNER JOIN &RRAP_DB..BASEL_REVLVNG_CR_MTH_SNAPSHOT SNP ON
		DRV.BASEL_ACCT_ID = SNP.BASEL_ACCT_ID
		AND DRV.MTH_TM_ID = SNP.MTH_TM_ID
	LEFT JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS E
		on e.basel_acct_id = drv.basel_acct_id and e.mth_tm_id = drv.mth_tm_id

	LEFT JOIN &RRAP_WRK..RT18_KS_TEMP K
		ON k.basel_acct_id = drv.basel_acct_id
	WHERE
	/*	((DRV.MTH_TM_ID >= (&mth_tm_id.-3 * 40) AND DRV.MTH_TM_ID <= &mth_tm_id.) */
	/*		or (DRV.mth_tm_id <= (&mth_tm_id.-12*2*40) AND DRV.mth_tm_id >= (&mth_tm_id.-12*2*40-2*40)) or DRV.MTH_TM_ID = (&mth_tm_id.-12*3*40))*/
		DRV.mth_tm_id = &mth_tm_id.
		/*	AND DRV.PIT_STAT_VER_2_CD IN ('CUR')*/
	/*	AND DRV.CONSM_PRD_TREATMNT_CD = 'A'
		AND DRV.SML_BUS_F = 'N'
		AND DRV.TRNST_EXCLSN_F = 'N' */
;) by nzcon;
execute(commit;) by nzcon;


execute(
	insert into &RRAP_WRK..DT4_RT18_RLZ_DATA_PREP 
	SELECT
		drv.SRC_SYS_CD
		,drv.basel_acct_id
		,drv.mort_no
		,drv.mth_tm_id 
		,case 
			when drv.src_sys_cd = 'SPL' then (spl_snp.TOT_CRNT_BAL_AMT + spl_snp.ACCR_INTR + spl_snp.ADD_ON_BAL_AMT)
			else drv.OS_BAL_AMT 
		end as OS_BAL_AMT
		,spl_snp.TOT_CRNT_BAL_AMT
		,null as TOT_UNPAID_FNCL_CHRG_AMT
		,null as ACCRL_STAT_F
		/*,cast(round(max(0,NVL(drv.ADJUSTED_OS_BAL_AMT, drv.OS_BAL_AMT)),3) as decimal(17,3)) as drawn
		,cast(round(MAX(0,drv.EAD_FINAL_RPTG_RTO*MAX((drv.af_zero_net_undrawn_amt + max(0,NVL(drv.ADJUSTED_OS_BAL_AMT, drv.OS_BAL_AMT))),  max(0,NVL(drv.ADJUSTED_OS_BAL_AMT, drv.OS_BAL_AMT)))
					- max(0,NVL(drv.ADJUSTED_OS_BAL_AMT, drv.OS_BAL_AMT))),3) as decimal(17,3))
		as undrawn */
		
		,max(0,NVL(drv.ADJUSTED_OS_BAL_AMT, drv.OS_BAL_AMT)) as drawn
		,MAX(0,drv.EAD_FINAL_RPTG_RTO*MAX((drv.af_zero_net_undrawn_amt + max(0,NVL(drv.ADJUSTED_OS_BAL_AMT, drv.OS_BAL_AMT))),  max(0,NVL(drv.ADJUSTED_OS_BAL_AMT, drv.OS_BAL_AMT)))
					- max(0,NVL(drv.ADJUSTED_OS_BAL_AMT, drv.OS_BAL_AMT)))
		as undrawn
		,NULL as EAD_INCL
		,NULL as EAD_OBS_AUTHORIZED_AMT
		,drv.PIT_STAT_CD AS PIT_STATUS_CD 
		,dt4.PD_BAND
		,dt4.DT4_RISK_RT_KEY_VAL 
		,dt4.DT4_EXPSR_CL_KEY_VAL
		
		,DRV.CONSM_PRD_TREATMNT_CD
		,DRV.SML_BUS_F
		,DRV.TRNST_EXCLSN_F
		
	FROM
	(SELECT bns.SRC_SYS_CD, bns.mth_tm_id, dt4.basel_acct_id, bns.mort_num as mort_no, CAST(bns.mort_num AS integer) AS mortgage_no, EAD_FINAL_RPTG_RTO, ADJUSTED_OS_BAL_AMT, OS_BAL_AMT, af_zero_net_undrawn_amt, bns.PIT_STAT_CD, SML_BUS_F, TRNST_EXCLSN_F, CONSM_PRD_TREATMNT_CD, PRD_ID 
		FROM &MOR_DB..BASEL_ANLYT_BL_INST_FCT_BNS_DLGD bns 
			LEFT JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS dt4 ON bns.mort_num = dt4.mort_num AND bns.MTH_TM_ID = dt4.MTH_TM_ID 
	UNION ALL
	SELECT tng.SRC_SYS_CD, tng.mth_tm_id, dt4.basel_acct_id, tng.mort_num as mort_no, NULL AS mortgage_no, EAD_FINAL_RPTG_RTO, ADJUSTED_OS_BAL_AMT, OS_BAL_AMT, af_zero_net_undrawn_amt, tng.PIT_STAT_CD, SML_BUS_F, TRNST_EXCLSN_F, CONSM_PRD_TREATMNT_CD, PRD_ID 
		FROM &MOR_DB..BASEL_ANLYT_BL_INST_FCT_TNG_DLGD tng
			LEFT JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS dt4 ON tng.mort_num = dt4.mort_num AND tng.MTH_TM_ID = dt4.MTH_TM_ID 
	UNION ALL 
	SELECT SRC_SYS_CD, mth_tm_id, basel_acct_id, mort_num as mort_no, NULL AS mortgage_no, EAD_FINAL_RPTG_RTO, ADJUSTED_OS_BAL_AMT, OS_BAL_AMT, af_zero_net_undrawn_amt, PIT_STAT_CD, SML_BUS_F, TRNST_EXCLSN_F, CONSM_PRD_TREATMNT_CD, PRD_ID 
		FROM &RRAP_DB..BASEL_PSNL_LN_ANL_BL_INST_FACT) 
		drv

	LEFT JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS dt4
		ON drv.basel_acct_id = dt4.BASEL_ACCT_ID AND drv.mth_tm_id = dt4.MTH_TM_ID 
		
	LEFT JOIN &RRAP_DB..BASEL_PSNL_LOAN_MTH_SNAPSHOT spl_snp
		ON drv.basel_acct_id = spl_snp.basel_acct_id and drv.mth_tm_id = spl_snp.mth_tm_id
			
	WHERE 
	/*	((DRV.MTH_TM_ID >= (&mth_tm_id.-3 * 40) AND DRV.MTH_TM_ID <= &mth_tm_id.) */
	/*		or (DRV.mth_tm_id <= (&mth_tm_id.-12*2*40) AND DRV.mth_tm_id >= (&mth_tm_id.-12*2*40-2*40)) or DRV.MTH_TM_ID = (&mth_tm_id.-12*3*40))*/
		DRV.mth_tm_id = &mth_tm_id.
			/*	--AND DRV.PIT_STAT_CD IN ('CUR') */
	/*	AND DRV.CONSM_PRD_TREATMNT_CD = 'A'
		AND DRV.SML_BUS_F = 'N'   
		AND DRV.TRNST_EXCLSN_F = 'N' */

/* WITH DATA DISTRIBUTE BY HASH (BASEL_ACCT_ID,MORT_NO)*/
;) by nzcon;
execute(commit;) by nzcon;
execute(CALL SYSPROC.ADMIN_CMD (%nrbquote('RUNSTATS ON TABLE &RRAP_WRK..DT4_RT18_RLZ_DATA_PREP on KEY COLUMNS and INDEXES ALL'));) by nzcon;
execute(COMMIT;) by nzcon;
quit;

%end;

%mend multirun;
%multirun;





