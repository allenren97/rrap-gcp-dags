***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0210_DT4_RLZ_DATA_PREP.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRFRGP1D
*  Target Table:  DT4_RLZ_DATA_PREP
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
*	2023-03-14: Hadi Dimashkieh - Add BASEL_PRD_CD and HELOC_F to KS. For use in new Basel III defaulter model
*	2023-03-27: Hadi Dimashkieh - Add DT4_PD_MODEL_INCL_F from DT4_SEGMENT_XREF and filter BASEL_ACCT_ID = -1
*	2024-08-16: Kalind Patel - RRMSS-2993 -  STEP CROSS_DFLT: DT4 PATCH - EAD_FINAL_RPTG_RTO
*	2025-04-25: Kalind Patel - RRMSS-3630 -  DT4_RLZ_DATA_PREP - Modify rundates to pull 48 months of history for lgd
***************************************************************************************************************************;


%rrap_dt4_autoexec();


data RT18_dates;
	process_mth_tm_id = &mth_tm_id.;
	pd_obs_mth_tm_id  = &mth_tm_id. -  3*40;
	lgd_obs_mth_tm_id = &mth_tm_id. - 36*40;
	/* 2025-04-25: Kalind Patel - RRMSS-3630 -  DT4_RLZ_DATA_PREP - Modify rundates to pull 48 months of history for lgd */
	lgd_obs_mth_tm_id2 = &mth_tm_id. -(36+12)*40  ;
	
	pd_window_st      = &mth_tm_id. - 2*40;
	pd_window_end     = &mth_tm_id. - 0*40;

	lgd_window_1	  = &mth_tm_id. - 27*40;
	lgd_window_st     = &mth_tm_id. - 26*40;
	lgd_window_end    = &mth_tm_id. - 24*40;

	
	call symputx('pd_obs_mth_tm_id',pd_obs_mth_tm_id);
run;

proc transpose data=RT18_dates out=dtran(rename=(_name_=month col1=mth_tm_id)); run;

proc sql;
	create table date_desc as
	select a.month, a.mth_tm_id, t.tm_lvl_end_dt as month_end_dt
	from dtran a, nzrrap.tm_dim t
	where a.mth_tm_id = t.tm_id and t.tm_lvl='Month'
	order by 2;
quit;

proc sql;
	create table rundates_rt18 as
	select distinct t.tm_id as mth_tm_id, t.tm_lvl_end_dt 
	from nzrrap.tm_dim t, RT18_dates d
	where t.tm_lvl='Month' and 
	(t.tm_id = d.process_mth_tm_id or
	 t.tm_id = d.pd_obs_mth_tm_id or 
	 t.tm_id = d.lgd_obs_mth_tm_id or
	 /* 2025-04-25: Kalind Patel - RRMSS-3630 -  DT4_RLZ_DATA_PREP - Modify rundates to pull 48 months of history for lgd */
	 t.tm_id = d.lgd_obs_mth_tm_id2 or
	(t.tm_id between d.pd_window_st and d.pd_window_end) or 
	(t.tm_id between d.lgd_window_1 and d.lgd_window_end))
	order by 1;
quit;



proc sql;
	create table rundates_pd12 as
	select distinct t.tm_id as mth_tm_id, t.tm_lvl_end_dt 
	from nzrrap.tm_dim t
	where t.tm_lvl='Month' and t.tm_id between &mth_tm_id. and &mth_tm_id.-12*40
	order by 1;
quit;

/*proc sql;
	create table rundates_rt30 as
	select distinct t.tm_id as mth_tm_id, t.tm_lvl_end_dt 
	from nzrrap.tm_dim t
	where t.tm_lvl='Month' and t.tm_id between &mth_tm_id.-36*40 and &mth_tm_id.-24*40
	order by 1;
quit;*/

data rundates;
merge rundates_pd12 rundates_rt18 ;
by mth_tm_id;
run;
proc sort nodupkey; by mth_tm_id; run;


data _null_;
	set rundates;
	if _n_=1 then put 'The Following months will be processed:';
	put mth_tm_id= tm_lvl_end_dt=;
	put;
run;

**************************************************************************************;


proc sql;
connect using nzuser as nzcon;
execute(DROP TABLE &RRAP_WRK..DT4_RLZ_DATA_PREP IF EXISTS;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
connect using nzuser as nzcon;
execute(
CREATE TABLE &RRAP_WRK..DT4_RLZ_DATA_PREP (
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
		TRNST_EXCLSN_F CHAR(1 OCTETS),
		BASEL_PRD_CD VARCHAR(10 OCTETS),
		HELOC_F CHAR(1 OCTETS),
		PD_BASEL_SEG_NUM INTEGER,
		DT4_PD_MODEL_INCL_F INTEGER
	)
	ORGANIZE BY COLUMN
	DATA CAPTURE NONE 
	DISTRIBUTE BY HASH (BASEL_ACCT_ID, MORT_NO, MTH_TM_ID);
) by nzcon;
execute(commit;) by nzcon;
quit;

**************************************************************************************;

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

%if &mth_tm_id >= 20036 and &mth_tm_id <= 20316 %then %do;
%put ********* RUNNING STEP CODE PATCH &mth_tm_id;

proc sql;
connect using NZUSER as nzcon;
execute(
	insert into &RRAP_WRK..DT4_RLZ_DATA_PREP 
SELECT
		 'KS' as SRC_SYS_CD
		,drv.basel_acct_id
		,null as mort_no
		,drv.mth_tm_id 
		,snp.TOT_NEW_BAL_AMT AS OS_BAL_AMT 
		,NULL as TOT_CRNT_BAL_AMT
		,snp.TOT_UNPAID_FNCL_CHRG_AMT
		,drv.ACCRL_STAT_F
		,max(0,NVL(k.ADJUSTED_OS_BAL_AMT, k.OS_BAL_AMT)) as drawn
		,MAX(0,k.COL_EAD_FINAL_RPTG_RTO*MAX((k.af_zero_net_undrawn_amt + max(0,NVL(k.ADJUSTED_OS_BAL_AMT, k.OS_BAL_AMT))), 
									   max(0,NVL(k.ADJUSTED_OS_BAL_AMT, k.OS_BAL_AMT)))
			   - max(0,NVL(k.ADJUSTED_OS_BAL_AMT, k.OS_BAL_AMT))) 
			as undrawn 

		, CASE 
			WHEN REGEXP_LIKE(k.CCAR_BASEL_PRD_TP_NM, 'HELOC|OTHER_CON_CC|OTHER_CON_CL|REV_CON_CC|REV_CON_CL|REV_CON_SLR|OTHER_CON_SLR|OTHER_CON_SLT', 'i' ) = 1 THEN 1
			ELSE NULL
		 END AS EAD_INCL
		,max(snp.CR_LMT_AMT,snp.TOT_NEW_BAL_AMT) AS EAD_OBS_AUTHORIZED_AMT
		,COALESCE(pcr.STEP_PIT_STATUS,drv.PIT_STAT_VER_2_CD) AS PIT_STATUS_CD 
		,E.PD_BAND
		,E.DT4_RISK_RT_KEY_VAL 
		,E.DT4_EXPSR_CL_KEY_VAL 
		,DRV.CONSM_PRD_TREATMNT_CD
		,DRV.SML_BUS_F
		,DRV.TRNST_EXCLSN_F
		,DRV.BASEL_PRD_CD
		,DRV.HELOC_F
		,XREF.PD_BASEL_SEG_NUM
		,XREF.DT4_PD_MODEL_INCL_F

	FROM
		&RRAP_DB..BASEL_REVLVNG_CR_BASE_DRVD_VARS DRV
	INNER JOIN &RRAP_DB..BASEL_REVLVNG_CR_MTH_SNAPSHOT SNP ON
		DRV.BASEL_ACCT_ID = SNP.BASEL_ACCT_ID
		AND DRV.MTH_TM_ID = SNP.MTH_TM_ID
	LEFT JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS E
		on e.basel_acct_id = drv.basel_acct_id and e.mth_tm_id = drv.mth_tm_id

	LEFT JOIN 
    (SELECT
	ifk.ADJUSTED_OS_BAL_AMT,
	ifk.OS_BAL_AMT,
	ifk.af_zero_net_undrawn_amt,
	ifk.MTH_TM_ID,
	ifk.BASEL_ACCT_ID,
	COALESCE (sd.EAD_FINAL_RPTG_RTO, ifk.EAD_FINAL_RPTG_RTO ) AS COL_EAD_FINAL_RPTG_RTO,
	ifk.CCAR_BASEL_PRD_TP_NM
FROM
&RRAP_DB..BASEL_ANALYTCL_BL_INSTRMNT_KS ifk
	LEFT JOIN &RRAP_DB..STEP_DEF_ACCT_POST_CCAR_RMA sd ON
		sd.BASEL_ACCT_ID = ifk.BASEL_ACCT_ID
		AND sd.MTH_TM_ID = ifk.MTH_TM_ID
	WHERE
		ifk.MTH_TM_ID = &mth_tm_id) 
    AS K
		ON k.basel_acct_id = drv.basel_acct_id and k.mth_tm_id = drv.mth_tm_id
    LEFT JOIN &RRAP_DB..STEP_DEF_ACCT_POST_CCAR_RMA pcr ON
		pcr.BASEL_ACCT_ID = drv.BASEL_ACCT_ID
		AND pcr.MTH_TM_ID = drv.MTH_TM_ID		
	LEFT JOIN &RRAP_DB..DT4_SEGMENT_XREF XREF
		ON DRV.MTH_TM_ID = XREF.MTH_TM_ID AND DRV.BASEL_ACCT_ID = XREF.BASEL_ACCT_ID AND &yrmth. BETWEEN cast(XREF.EFF_FROM_YR_MTH AS integer) AND cast(XREF.EFF_TO_YR_MTH AS integer)
	WHERE
	
		DRV.mth_tm_id = &mth_tm_id. and DRV.BASEL_ACCT_ID <> -1

;) by nzcon;
execute(commit;) by nzcon;


execute(
	INSERT
	INTO
	&RRAP_WRK..DT4_RLZ_DATA_PREP
SELECT
	drv.SRC_SYS_CD ,
	drv.basel_acct_id ,
	drv.mort_no ,
	drv.mth_tm_id ,
	CASE WHEN drv.src_sys_cd = 'SPL' THEN (spl_snp.TOT_CRNT_BAL_AMT + spl_snp.ACCR_INTR + spl_snp.ADD_ON_BAL_AMT)
	ELSE drv.OS_BAL_AMT
END AS OS_BAL_AMT ,
spl_snp.TOT_CRNT_BAL_AMT ,
NULL AS TOT_UNPAID_FNCL_CHRG_AMT ,
NULL AS ACCRL_STAT_F ,
MAX(0, NVL(drv.ADJUSTED_OS_BAL_AMT, drv.OS_BAL_AMT)) AS drawn ,
MAX(0, drv.EAD_FINAL_RPTG_RTO*MAX((drv.af_zero_net_undrawn_amt + MAX(0, NVL(drv.ADJUSTED_OS_BAL_AMT, drv.OS_BAL_AMT))), MAX(0, NVL(drv.ADJUSTED_OS_BAL_AMT, drv.OS_BAL_AMT))) - MAX(0, NVL(drv.ADJUSTED_OS_BAL_AMT, drv.OS_BAL_AMT))) AS undrawn ,
NULL AS EAD_INCL ,
NULL AS EAD_OBS_AUTHORIZED_AMT ,
COALESCE(pcrfv.STEP_PIT_STATUS,
drv.PIT_STATUS_CD) AS PIT_STATUS_CD ,
dt4.PD_BAND ,
dt4.DT4_RISK_RT_KEY_VAL ,
dt4.DT4_EXPSR_CL_KEY_VAL ,
DRV.CONSM_PRD_TREATMNT_CD ,
DRV.SML_BUS_F ,
DRV.TRNST_EXCLSN_F ,
NULL AS BASEL_PRD_CD ,
NULL AS HELOC_F ,
XREF.PD_BASEL_SEG_NUM ,
XREF.DT4_PD_MODEL_INCL_F
FROM
(
SELECT
	drvon.SRC_SYS_CD ,
	drvon.basel_acct_id ,
	drvon.mort_no ,
	drvon.mth_tm_id ,
	drvon.OS_BAL_AMT ,
	drvon.ADJUSTED_OS_BAL_AMT ,
	COALESCE (sdapc.EAD_FINAL_RPTG_RTO,
	drvon.EAD_FINAL_RPTG_RTO) AS EAD_FINAL_RPTG_RTO ,
	drvon.af_zero_net_undrawn_amt ,
	drvon.PIT_STAT_CD AS PIT_STATUS_CD ,
	drvon.CONSM_PRD_TREATMNT_CD ,
	drvon.SML_BUS_F ,
	drvon.TRNST_EXCLSN_F
FROM
	(
	SELECT
		bns.SRC_SYS_CD,
		bns.mth_tm_id,
		dt4.basel_acct_id,
		bns.mort_num AS mort_no,
		CAST(bns.mort_num AS INTEGER) AS mortgage_no,
		EAD_FINAL_RPTG_RTO,
		ADJUSTED_OS_BAL_AMT,
		OS_BAL_AMT,
		af_zero_net_undrawn_amt,
		bns.PIT_STAT_CD,
		SML_BUS_F,
		TRNST_EXCLSN_F,
		CONSM_PRD_TREATMNT_CD,
		PRD_ID
	FROM
		&MOR_DB..BASEL_ANLYT_BL_INST_FCT_BNS_DLGD bns
	LEFT JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS dt4 ON
		bns.mort_num = dt4.mort_num
		AND bns.MTH_TM_ID = dt4.MTH_TM_ID
UNION ALL
	SELECT
		tng.SRC_SYS_CD,
		tng.mth_tm_id,
		dt4.basel_acct_id,
		tng.mort_num AS mort_no,
		NULL AS mortgage_no,
		EAD_FINAL_RPTG_RTO,
		ADJUSTED_OS_BAL_AMT,
		OS_BAL_AMT,
		af_zero_net_undrawn_amt,
		tng.PIT_STAT_CD,
		SML_BUS_F,
		TRNST_EXCLSN_F,
		CONSM_PRD_TREATMNT_CD,
		PRD_ID
	FROM
		&MOR_DB..BASEL_ANLYT_BL_INST_FCT_TNG_DLGD tng
	LEFT JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS dt4 ON
		tng.mort_num = dt4.mort_num
		AND tng.MTH_TM_ID = dt4.MTH_TM_ID
UNION ALL
	SELECT
		bplab.SRC_SYS_CD,
		bplab.mth_tm_id,
		bplab.basel_acct_id,
		bplab.mort_num AS mort_no,
		NULL AS mortgage_no,
		bplab.EAD_FINAL_RPTG_RTO,
		bplab.ADJUSTED_OS_BAL_AMT,
		bplab.OS_BAL_AMT,
		bplab. af_zero_net_undrawn_amt,
		bplab.PIT_STAT_CD,
		SML_BUS_F,
		TRNST_EXCLSN_F,
		CONSM_PRD_TREATMNT_CD,
		PRD_ID
	FROM
		&RRAP_DB..BASEL_PSNL_LN_ANL_BL_INST_FACT bplab) drvon
LEFT JOIN &RRAP_DB..STEP_DEF_ACCT_POST_CCAR_RMA sdapc ON sdapc.BASEL_ACCT_ID = drvon.BASEL_ACCT_ID AND sdapc.MTH_TM_ID = drvon.MTH_TM_ID ) drv
LEFT JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS dt4 ON drv.basel_acct_id = dt4.BASEL_ACCT_ID AND drv.mth_tm_id = dt4.MTH_TM_ID
LEFT JOIN &RRAP_DB..BASEL_PSNL_LOAN_MTH_SNAPSHOT spl_snp ON drv.basel_acct_id = spl_snp.basel_acct_id AND drv.mth_tm_id = spl_snp.mth_tm_id
LEFT JOIN &RRAP_DB..DT4_SEGMENT_XREF XREF ON DRV.MTH_TM_ID = XREF.MTH_TM_ID AND DRV.BASEL_ACCT_ID = XREF.BASEL_ACCT_ID AND &yrmth. BETWEEN CAST(XREF.EFF_FROM_YR_MTH AS INTEGER) AND CAST(XREF.EFF_TO_YR_MTH AS INTEGER)
LEFT JOIN &RRAP_DB..STEP_DEF_ACCT_POST_CCAR_RMA pcrfv ON pcrfv.BASEL_ACCT_ID = drv.BASEL_ACCT_ID AND pcrfv.MTH_TM_ID = drv.MTH_TM_ID
	WHERE DRV.mth_tm_id = &mth_tm_id. AND DRV.BASEL_ACCT_ID <> -1


;) by nzcon;
execute(commit;) by nzcon;

quit;
%end;
%else %do;

proc sql;
connect using NZUSER as nzcon;
execute(
	insert into &RRAP_WRK..DT4_RLZ_DATA_PREP 
	SELECT
		 'KS' as SRC_SYS_CD
		,drv.basel_acct_id
		,null as mort_no
		,drv.mth_tm_id 
		,snp.TOT_NEW_BAL_AMT AS OS_BAL_AMT 
		,NULL as TOT_CRNT_BAL_AMT
		,snp.TOT_UNPAID_FNCL_CHRG_AMT
		,drv.ACCRL_STAT_F
		,max(0,NVL(k.ADJUSTED_OS_BAL_AMT, k.OS_BAL_AMT)) as drawn
		,MAX(0,k.EAD_FINAL_RPTG_RTO*MAX((k.af_zero_net_undrawn_amt + max(0,NVL(k.ADJUSTED_OS_BAL_AMT, k.OS_BAL_AMT))), 
									   max(0,NVL(k.ADJUSTED_OS_BAL_AMT, k.OS_BAL_AMT)))
			   - max(0,NVL(k.ADJUSTED_OS_BAL_AMT, k.OS_BAL_AMT))) 
			as undrawn 

		, CASE 
			WHEN REGEXP_LIKE(k.CCAR_BASEL_PRD_TP_NM, 'HELOC|OTHER_CON_CC|OTHER_CON_CL|REV_CON_CC|REV_CON_CL|REV_CON_SLR|OTHER_CON_SLR|OTHER_CON_SLT', 'i' ) = 1 THEN 1
			ELSE NULL
		 END AS EAD_INCL
		,max(snp.CR_LMT_AMT,snp.TOT_NEW_BAL_AMT) AS EAD_OBS_AUTHORIZED_AMT
		,drv.PIT_STAT_VER_2_CD AS PIT_STATUS_CD 
		,E.PD_BAND
		,E.DT4_RISK_RT_KEY_VAL 
		,E.DT4_EXPSR_CL_KEY_VAL 
		,DRV.CONSM_PRD_TREATMNT_CD
		,DRV.SML_BUS_F
		,DRV.TRNST_EXCLSN_F
		,DRV.BASEL_PRD_CD
		,DRV.HELOC_F
		,XREF.PD_BASEL_SEG_NUM
		,XREF.DT4_PD_MODEL_INCL_F

	FROM
		&RRAP_DB..BASEL_REVLVNG_CR_BASE_DRVD_VARS DRV
	INNER JOIN &RRAP_DB..BASEL_REVLVNG_CR_MTH_SNAPSHOT SNP ON
		DRV.BASEL_ACCT_ID = SNP.BASEL_ACCT_ID
		AND DRV.MTH_TM_ID = SNP.MTH_TM_ID
	LEFT JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS E
		on e.basel_acct_id = drv.basel_acct_id and e.mth_tm_id = drv.mth_tm_id

	LEFT JOIN &RRAP_DB..BASEL_ANALYTCL_BL_INSTRMNT_KS K
		ON k.basel_acct_id = drv.basel_acct_id and k.mth_tm_id = drv.mth_tm_id
		
	LEFT JOIN &RRAP_DB..DT4_SEGMENT_XREF XREF
		ON DRV.MTH_TM_ID = XREF.MTH_TM_ID AND DRV.BASEL_ACCT_ID = XREF.BASEL_ACCT_ID AND &yrmth. BETWEEN cast(XREF.EFF_FROM_YR_MTH AS integer) AND cast(XREF.EFF_TO_YR_MTH AS integer)
	WHERE
	
		DRV.mth_tm_id = &mth_tm_id. and DRV.BASEL_ACCT_ID <> -1

;) by nzcon;
execute(commit;) by nzcon;


execute(
	insert into &RRAP_WRK..DT4_RLZ_DATA_PREP 
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
		,NULL as BASEL_PRD_CD
		,NULL as HELOC_F
		,XREF.PD_BASEL_SEG_NUM
		,XREF.DT4_PD_MODEL_INCL_F
		
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
		
	LEFT JOIN &RRAP_DB..DT4_SEGMENT_XREF XREF
		ON DRV.MTH_TM_ID = XREF.MTH_TM_ID AND DRV.BASEL_ACCT_ID = XREF.BASEL_ACCT_ID AND &yrmth. BETWEEN cast(XREF.EFF_FROM_YR_MTH AS integer) AND cast(XREF.EFF_TO_YR_MTH AS integer)
			
	WHERE 

		DRV.mth_tm_id = &mth_tm_id. and DRV.BASEL_ACCT_ID <> -1

;) by nzcon;
execute(commit;) by nzcon;
quit;
%end;


%end;

proc sql;
connect using NZUSER as nzcon;
execute(CALL SYSPROC.ADMIN_CMD (%nrbquote('RUNSTATS ON TABLE &RRAP_WRK..DT4_RLZ_DATA_PREP on KEY COLUMNS and INDEXES ALL'));) by nzcon;
execute(COMMIT;) by nzcon;
quit; 

%mend multirun;
%multirun;