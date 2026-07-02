
***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*  Job Name: J_RRII_KS10_2510_REVLVNG_CR_OBSVTN_PT_DRVD_VAR.sas
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  REVLVNG_CR_OBSVTN_PT_DRVD_VAR
*  
*  Purpose: Load realized variables. 
*
*  Frequency: Monthly
*
*  Notes: 
* 
* 
*
*	Change Log: INITIAL DEVELOPMENT - JANUARY 6, 2025
*
***************************************************************************************************************************;

options source;
options mprint;

%put WORK LOCATION: %sysfunc(getoption(work));
%include '&rrap_dir/macro/rrap_iias/rrap_pll_autoexec.sas';
%rrap_pll_autoexec(RRAPENV=REVOLVING_CREDIT);

%let dataprep_table = &net_wrk..obs_pt_dataprep;
%let defaulter_pdead_table = &net_wrk..obs_pt_pd_ead;
%let defaulter_lgd_table = &net_wrk..obs_pt_lgd;

proc sql;
connect using NZRRAP as nzcon;
execute(drop table &dataprep_table. if exists; commit;) by nzcon;
quit; 

proc sql;
connect using NZRRAP as nzcon;
execute(
CREATE TABLE &dataprep_table. AS (

	SELECT
		drv.basel_acct_id
		,'KS' as SRC_SYS_CD
		,drv.mth_tm_id 
		,snp.TOT_NEW_BAL_AMT AS OS_BAL_AMT 
		,snp.TOT_UNPAID_FNCL_CHRG_AMT
		,drv.ACCRL_STAT_F
		,drv.PIT_STAT_VER_2_CD AS PIT_STATUS_CD 
		,drv.BASEL_PRD_CD
		,NULL as TOT_CRNT_BAL_AMT
		,DRV.CONSM_PRD_TREATMNT_CD
		,DRV.SML_BUS_F
		,DRV.TRNST_EXCLSN_F
		,DRV.HELOC_F

	FROM
		&net_db..BASEL_REVLVNG_CR_BASE_DRVD_VARS DRV
	INNER JOIN &net_db..BASEL_REVLVNG_CR_MTH_SNAPSHOT SNP ON
		DRV.BASEL_ACCT_ID = SNP.BASEL_ACCT_ID
		AND DRV.MTH_TM_ID = SNP.MTH_TM_ID
	WHERE
	
		(DRV.mth_tm_id >= &mth_tm_id.-12*40 AND DRV.mth_tm_id <= &mth_tm_id.)
			or
		(DRV.mth_tm_id >= &mth_tm_id.-48*40 AND DRV.mth_tm_id <= &mth_tm_id.-24*40 )
		ORDER BY basel_acct_id, mth_tm_id 
		
) WITH DATA; COMMIT;
) by nzcon;
quit;


%rrap_defaulter_model(
		 IIAS_LIBREF = NZRRAP
		,_DEFAULTER_TABLE = &defaulter_pdead_table.
		,_DATA_PREP_TABLE = &dataprep_table.
		,WINDOW_START = (&mth_tm_id.-12*40)
		,WINDOW_END = &mth_tm_id.
);

%rrap_defaulter_model(
		 IIAS_LIBREF = NZRRAP
		,_DEFAULTER_TABLE = &defaulter_lgd_table.
		,_DATA_PREP_TABLE = &dataprep_table.
		,WINDOW_START = (&mth_tm_id.-48*40)
		,WINDOW_END = (&mth_tm_id.-24*40)
);


proc sql;
connect using NZRRAP as nzcon;
execute(delete from &net_db..REVLVNG_CR_OBSVTN_PT_DRVD_VAR where process_mth_tm_id = &mth_tm_id.; commit;) by nzcon;
quit; 
proc sql;
connect using NZRRAP as nzcon;
execute(
insert into &net_db..REVLVNG_CR_OBSVTN_PT_DRVD_VAR 

			SELECT 

				 a.MTH_TM_ID AS OBSVTN_MTH_TM_ID
				,b.basel_acct_id
				,b.LAST_NEW_DEFAULT_OS_BAL_AMT AS LAST_NEW_DFT_BAL_AMT
				,b.LAST_NEW_DEFAULT_DATE AS LAST_NEW_DFT_TM_ID
				,t.tm_lvl_end_dt AS LAST_NEW_DFT_DT
				,CASE WHEN b.MODEL_DFT_F = 1 THEN 'Y' ELSE 'N' END AS MODEL_DFT_F
/*				,CASE WHEN b.MODEL_DFT_F = 1 AND b.LAST_NEW_DEFAULT_DATE = a.MTH_TM_ID + 12*40 THEN 'Y' ELSE 'N' END AS EAD_MODEL_DFT_F*/
				,b.LAST_NEW_DEFAULT_DATE + 24*40 AS RCVRY_WINDOW_CUTOFF_TM_ID
				,ADD_MONTHS(t.tm_lvl_end_dt,24)  AS RCVRY_WINDOW_CUTOFF_DT
				,now() as INSRT_PROCESS_TMSTMP
				,now() as UPDT_PROCESS_TMSTMP
				,&mth_tm_id. AS process_mth_tm_id
				
			FROM &net_db..BASEL_REVLVNG_CR_BASE_DRVD_VARS a, &defaulter_pdead_table. b, &net_db..TM_DIM t
			WHERE a.BASEL_ACCT_ID = b.BASEL_ACCT_ID 
			AND b.LAST_NEW_DEFAULT_DATE = t.TM_ID AND t.tm_lvl = 'Month'
			AND a.PIT_STAT_VER_2_CD = 'CUR'
			AND a.SML_BUS_F = 'N'
			AND a.CONSM_PRD_TREATMNT_CD = 'A'
			AND a.MTH_TM_ID = &mth_tm_id. - 12*40
; COMMIT;
) by nzcon;
quit;

proc sql;
connect using NZRRAP as nzcon;
execute(
insert into &net_db..REVLVNG_CR_OBSVTN_PT_DRVD_VAR 

			SELECT 
				 a.MTH_TM_ID AS OBSVTN_MTH_TM_ID
				,b.basel_acct_id
				,b.LAST_NEW_DEFAULT_OS_BAL_AMT AS LAST_NEW_DFT_BAL_AMT
				,b.LAST_NEW_DEFAULT_DATE AS LAST_NEW_DFT_TM_ID
				,t.tm_lvl_end_dt AS LAST_NEW_DFT_DT
				,CASE WHEN b.MODEL_DFT_F = 1 THEN 'Y' ELSE 'N' END AS MODEL_DFT_F
				,b.LAST_NEW_DEFAULT_DATE + 24*40 AS RCVRY_WINDOW_CUTOFF_TM_ID
				,ADD_MONTHS(t.tm_lvl_end_dt,24)  AS RCVRY_WINDOW_CUTOFF_DT
				,now() as INSRT_PROCESS_TMSTMP
				,now() as UPDT_PROCESS_TMSTMP
				,&mth_tm_id. AS process_mth_tm_id
				
			FROM &net_db..BASEL_REVLVNG_CR_BASE_DRVD_VARS a, &defaulter_lgd_table. b, &net_db..TM_DIM t
			WHERE a.BASEL_ACCT_ID = b.BASEL_ACCT_ID 
			AND b.LAST_NEW_DEFAULT_DATE = t.TM_ID AND t.tm_lvl = 'Month'
			AND a.PIT_STAT_VER_2_CD <> 'CUR'
			AND a.SML_BUS_F = 'N'
			AND a.CONSM_PRD_TREATMNT_CD = 'A'
			AND a.MTH_TM_ID = &mth_tm_id. - 24*40
; COMMIT;
) by nzcon;
quit;

