
***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  &_DEFAULTER_TABLE as defined by the macro call
*  
*  Purpose: Implementation of the RRAP defaulter logic across all models
*
*  Frequency: Called from SAS program
*
*	Usage:
*	%rrap_defaulter_model(
*		 IIAS_LIBREF = NZUSER
*		,_DEFAULTER_TABLE = &RAP_WRK..DT4_PD12_RLZ_OBS_WINDOW
*		,_DATA_PREP_TABLE = &RRAP_WRK..DT4_RLZ_DATA_PREP
*		,WINDOW_START = (&mth_tm_id.-12*40)
*		,WINDOW_END = &mth_tm_id.
*                                 );
*
*  Inputs:
* 	IIAS_LIBREF : Libref where the DEFAULTER_TABLE will reside  
*	DEFAULTER_TABLE : Output table to be created on IIAS  
*	DATA_PREP_TABLE : Input table (on IIAS) 
*	WINDOW_START : mth_tm_id of the start of the performance window 
*	WINDOW_END : mth_tm_id of the end of the performance window 
*
*  Notes: This macro was developed to accept an IIAS source table and output to an IIAS target table.
*  		  The following fields need to be populated in the DATA_PREP_TABLE:
*			basel_acct_id, mth_tm_id, src_sys_cd, pit_status_cd, OS_BAL_AMT, TOT_CRNT_BAL_AMT, TOT_UNPAID_FNCL_CHRG_AMT, ACCRL_STAT_F, HELOC_F
*	Change Log:
*	2021-11-04: Hadi Dimashkieh - Initial Development
*	2022-10-17: Hadi Dimashkieh - Unify logic with RRAP and apply Basel III changes. HELOC_F added as a required variable
*   2022-11-15: Hadi Dimashkieh - Add BASEL_PRD_CD = 'CC' AND HELOC_F = 'Y' 
*
***************************************************************************************************************************;



%macro rrap_defaulter_model(
 IIAS_LIBREF = 
,_DEFAULTER_TABLE = 
,_DATA_PREP_TABLE = 
,WINDOW_START = 
,WINDOW_END = 
);
%macro cc; %mend cc;
%global DEFAULTER_TABLE DATA_PREP_TABLE; 
%let DEFAULTER_TABLE = &_DEFAULTER_TABLE.;
%let DATA_PREP_TABLE = &_DATA_PREP_TABLE.;

proc sql;
connect using &IIAS_LIBREF. as nzcon;
execute(drop table &DEFAULTER_TABLE. if exists;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
connect using &IIAS_LIBREF. as nzcon;
execute(
CREATE TABLE &DEFAULTER_TABLE. AS 
(	



	with defaults as (
		SELECT basel_acct_id, max(mth_tm_id) as  LAST_NEW_DEFAULT_DATE from (

			select basel_acct_id, mth_tm_id
			,case 

				WHEN src_sys_cd IN ('SPL') AND pit_status_cd IN ('DEF','CHG') AND OS_BAL_AMT >= 1 and TOT_CRNT_BAL_AMT > 0
					AND COALESCE(lag_pit_status_cd,'CUR') NOT IN ('DEF','CHG') THEN 1
						
				WHEN src_sys_cd IN ('TNG-MOR','MOR') AND pit_status_cd IN ('DEF') AND lag_pit_status_cd ='CUR' THEN 1
						
				WHEN src_sys_cd IN ('KS') AND BASEL_PRD_CD = 'CC' AND HELOC_F = 'N' AND pit_status_cd IN ('DEF','CHG') AND lag_pit_status_cd = 'CUR' 
					AND OS_BAL_AMT > 0 AND lag_TOT_UNPAID_FNCL_CHRG_AMT <> lag_OS_BAL_AMT THEN 1
					
				WHEN src_sys_cd IN ('KS') AND BASEL_PRD_CD = 'CC' AND HELOC_F = 'N' AND pit_status_cd IN ('DEF','CHG') AND lag_pit_status_cd = 'CUR' 
					AND OS_BAL_AMT > 0 AND lag_TOT_UNPAID_FNCL_CHRG_AMT = lag_OS_BAL_AMT
					AND lag_TOT_UNPAID_FNCL_CHRG_AMT <= 0 THEN 1
					
				WHEN src_sys_cd IN ('KS') AND BASEL_PRD_CD = 'CC' AND HELOC_F = 'N' AND pit_status_cd IN ('DEF','CHG') AND lag_pit_status_cd = 'CUR' 
					AND OS_BAL_AMT > 0 AND lag_TOT_UNPAID_FNCL_CHRG_AMT = lag_OS_BAL_AMT
					AND lag_TOT_UNPAID_FNCL_CHRG_AMT > 0 
					AND TOT_UNPAID_FNCL_CHRG_AMT <> OS_BAL_AMT
					AND PIT_STATUS_CD <> 'CHG'
					AND OS_BAL_AMT >= 5 THEN 1

				WHEN src_sys_cd IN ('KS') AND BASEL_PRD_CD = 'CC' AND HELOC_F = 'N' AND pit_status_cd IN ('DEF','CHG') AND lag_pit_status_cd = 'CUR'
					THEN 0
					
			   WHEN src_sys_cd IN ('KS') AND BASEL_PRD_CD = 'CC' AND HELOC_F = 'Y' AND pit_status_cd IN ('DEF','CHG') AND lag_pit_status_cd = 'CUR'
					  AND lag_TOT_UNPAID_FNCL_CHRG_AMT <> lag_OS_BAL_AMT AND OS_BAL_AMT > 0 THEN 1                                 


				WHEN src_sys_cd IN ('KS') AND BASEL_PRD_CD <> 'CC' AND pit_status_cd IN ('DEF','CHG') AND lag_pit_status_cd = 'CUR'
					AND lag_TOT_UNPAID_FNCL_CHRG_AMT <> lag_OS_BAL_AMT AND OS_BAL_AMT > 0 THEN 1

				/* Original KS */ 
/*				WHEN src_sys_cd IN ('KS') AND pit_status_cd IN ('DEF','CHG') AND (lag(pit_status_cd) OVER (PARTITION BY BASEL_ACCT_ID  ORDER BY BASEL_ACCT_ID, MTH_TM_ID ) ) ='CUR' */
/*					AND (lag(TOT_UNPAID_FNCL_CHRG_AMT) OVER (PARTITION BY BASEL_ACCT_ID  ORDER BY BASEL_ACCT_ID, MTH_TM_ID )) <> (lag(OS_BAL_AMT) OVER (PARTITION BY BASEL_ACCT_ID  ORDER BY BASEL_ACCT_ID, MTH_TM_ID ))*/
/*					AND OS_BAL_AMT > 0	THEN 1*/

					
			ELSE 0 
			END AS new_default_flg

			FROM (SELECT *,
		 			 lag(OS_BAL_AMT) OVER (PARTITION BY BASEL_ACCT_ID  ORDER BY BASEL_ACCT_ID, MTH_TM_ID ) AS lag_OS_BAL_AMT
					,lag(TOT_UNPAID_FNCL_CHRG_AMT) OVER (PARTITION BY BASEL_ACCT_ID  ORDER BY BASEL_ACCT_ID, MTH_TM_ID ) AS lag_TOT_UNPAID_FNCL_CHRG_AMT
					,lag(PIT_STATUS_CD) OVER (PARTITION BY BASEL_ACCT_ID  ORDER BY BASEL_ACCT_ID, MTH_TM_ID ) AS lag_PIT_STATUS_CD
				  FROM &DATA_PREP_TABLE. WHERE mth_tm_id >= &WINDOW_START. and mth_tm_id <= &WINDOW_END.
				  )	
		order by basel_acct_id, mth_tm_id
	)
	where new_default_flg = 1 GROUP BY basel_acct_id )  



			select cte1.basel_acct_id, 1 as MODEL_DFT_F, LAST_NEW_DEFAULT_DATE
				,CASE 
					WHEN snp_def.src_sys_cd IN ('KS') AND (snp_def.pit_status_cd = 'CHG' or snp_def.ACCRL_STAT_F = 'N') AND snp_def.OS_BAL_AMT = 0 THEN max(snp_def_ks_lag.OS_BAL_AMT,0)
					ELSE max(snp_def.OS_BAL_AMT,0)
				END AS LAST_NEW_DEFAULT_OS_BAL_AMT
			FROM 
			defaults cte1
				LEFT JOIN &DATA_PREP_TABLE. snp_def
					ON snp_def.BASEL_ACCT_ID = cte1.basel_acct_id AND snp_def.MTH_TM_ID = cte1.LAST_NEW_DEFAULT_DATE
				LEFT JOIN &DATA_PREP_TABLE. snp_def_ks_lag
					ON snp_def_ks_lag.BASEL_ACCT_ID = cte1.basel_acct_id AND snp_def_ks_lag.MTH_TM_ID = (cte1.LAST_NEW_DEFAULT_DATE -40)
)	
	
	WITH DATA DISTRIBUTE BY HASH (BASEL_ACCT_ID);) by nzcon;
execute(commit;) by nzcon;
quit;

%mend rrap_defaulter_model;


