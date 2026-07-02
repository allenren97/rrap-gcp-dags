options mprint errorabend;

***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0200_DT4_RPTG_DRVD_VARS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RPTG_DRVD_VARS
*  
*  Purpose: Derive DT4 specific variables to be used in downstream DT4 jobs
*
*  Frequency: Quarter End runs
*
*  Notes: Since this job is executed quarterly, 
*  		  it will run iteratively to load the past 3 months since the past quarter end.
*
*	Change Log:
*	2021-09-21: Hadi Dimashkieh - Initial Development
*   2021-11-04: Hadi Dimashkieh - Remove DB2 KS dependencies
*   2021-12-04: Hadi Dimashkieh - Remove Segment variables 
*   2023-01-25: Hadi Dimashkieh - DRV.REVISED_EXPSR_AMT > 150000
*	2023-03-16: Hadi Dimashkieh - RRMSS-2031 : Dynamic lookup of threshold amount 
*	2023-04-04: Hadi Dimashkieh - Excluded basel_acct_id = -1 
*	2023-06-22: RRMSS-2043 - Update KS lookup to new exposure limit flag 
***************************************************************************************************************************;



%rrap_dt4_autoexec(FREQ=MONTH);


/*
%macro multirun;

%do mth_tm_id = %eval(&mth_tm_id. -40*2) %to &mth_tm_id. %by 40;
*/
proc sql;
connect using db2rrap as dbcon;
create table DT4_MOR_ACCT_ID as select * from connection to dbcon(
	SELECT distinct basel_acct_id, mth_tm_id, mort_num FROM &RRAP_DB2..BASEL_ANALYTCL_BL_INSTRMNT_FACT 
	WHERE mth_tm_id = &mth_tm_id. AND src_sys_cd IN ('MOR','TNG-MOR'));
quit;

proc sql;
	connect using NZUSER as nzcon;
	execute(drop table &RRAP_WRK..DT4_MOR_ACCT_ID if exists;) by nzcon;
	execute(commit;) by nzcon;
quit;
proc append base=NZUSER.DT4_MOR_ACCT_ID(BULKLOAD=YES BL_METHOD=CLILOAD) data=DT4_MOR_ACCT_ID force; run;





proc sql;
	connect using NZRRAP as nzcon;
	execute(delete from &RRAP_DB..DT4_RPTG_DRVD_VARS where mth_tm_id = &mth_tm_id.;) by nzcon;
	execute(commit;) by nzcon;
quit;


proc sql;
connect using NZRRAP as nzcon;
execute(
insert into &RRAP_DB..DT4_RPTG_DRVD_VARS 

SELECT 
a.mth_tm_id, a.src_sys_cd, bns_acct.basel_acct_id, a.MORT_NUM ,  a.PIT_STAT_CD, a.PD_BAND
, h.DT4_RISK_RT_KEY_VAL, h.DT4_EXPSR_CL_KEY_VAL, a.BCAR_SCHED_NUM

,now() as INSRT_PROCESS_TMSTMP, now() as UPDT_PROCESS_TMSTMP


	FROM &MOR_DB..BASEL_ANLYT_BL_INST_FCT_BNS_DLGD a
		
		LEFT JOIN &RRAP_WRK..DT4_MOR_ACCT_ID bns_acct ON bns_acct.mort_num = a.mort_num and bns_acct.mth_tm_id = a.mth_tm_id
		LEFT JOIN &RRAP_DB..TM_DIM t ON a.mth_tm_id = t.tm_id AND t.tm_lvl = 'Month'
		LEFT JOIN &MOR_DB..MORTGAGE_HIST mh 
			ON CAST(a.MORT_NUM AS integer)= CAST(mh.MORTGAGE_NO AS integer) AND t.tm_lvl_end_dt = mh.time_key 

		LEFT JOIN &RRAP_DB..RPTG_PRD_LKP_MOR as h
			ON ('MOR' = upper(h.source_system_code)
			AND trim(upper(mh.insur_grp)) = TRIM(upper(h.basel_mortgage_insurer_group_des))
			AND trim(upper(mh.bulk_ind)) = TRIM(upper(h.bulk_indicator)))
			AND &YRMTH. between cast(h.EFF_FROM_YR_MTH as integer) and cast(h.EFF_TO_YR_MTH as integer)
		
WHERE a.mth_tm_id = &mth_tm_id. and bns_acct.basel_acct_id <> -1

/* TNG */
union all 
SELECT 
a.mth_tm_id, a.src_sys_cd, bns_acct.basel_acct_id, a.MORT_NUM ,  a.PIT_STAT_CD, a.PD_BAND
, h.DT4_RISK_RT_KEY_VAL, h.DT4_EXPSR_CL_KEY_VAL, a.BCAR_SCHED_NUM

,now() as INSRT_PROCESS_TMSTMP, now() as UPDT_PROCESS_TMSTMP

	FROM &MOR_DB..BASEL_ANLYT_BL_INST_FCT_TNG_DLGD a
		
		LEFT JOIN &RRAP_WRK..DT4_MOR_ACCT_ID bns_acct ON bns_acct.mort_num = a.mort_num and bns_acct.mth_tm_id = a.mth_tm_id
		LEFT JOIN &RRAP_DB..TM_DIM t ON a.mth_tm_id = t.tm_id AND t.tm_lvl = 'Month'
		LEFT JOIN &TNG_DB..tng_acct_mo tng ON tng.ACCOUNT_ID = a.mort_num AND t.tm_lvl_end_dt = tng.MONTH_END_DT and a.src_sys_cd = 'TNG-MOR'
		

		LEFT JOIN &RRAP_DB..RPTG_PRD_LKP_MOR as h
			ON ('TNG-MOR' = upper(h.source_system_code)
			AND trim(upper(tng.insurer_desc)) = TRIM(upper(h.basel_mortgage_insurer_group_des))
			AND trim(upper(case when upper(tng.bulk_nsurer_desc)='BULKINSURED' then 'Y' else 'N' end)) = TRIM(upper(h.bulk_indicator)))
			AND &YRMTH. between cast(h.EFF_FROM_YR_MTH as integer) and cast(h.EFF_TO_YR_MTH as integer)
		
WHERE a.mth_tm_id = &mth_tm_id. and bns_acct.basel_acct_id <> -1


;) by nzcon;

execute(commit;) by nzcon;
quit;


proc sql;
connect using NZRRAP as nzcon;
execute(
insert into &RRAP_DB..DT4_RPTG_DRVD_VARS 

/* SPL */
SELECT 
a.mth_tm_id, a.src_sys_cd, a.basel_acct_id, a.MORT_NUM ,  a.PIT_STAT_CD, a.PD_BAND
, h.DT4_RISK_RT_KEY_VAL, h.DT4_EXPSR_CL_KEY_VAL, a.BCAR_SCHED_NUM

,now() as INSRT_PROCESS_TMSTMP, now() as UPDT_PROCESS_TMSTMP

	FROM &RRAP_DB..BASEL_PSNL_LN_ANL_BL_INST_FACT a
		
		LEFT JOIN &RRAP_DB..RPTG_PRD_LKP_SPL h on trim(a.PRD_ID) = trim(h.PRD_ID) and h.src_sys_cd = 'SPL'
		and &YRMTH. between cast(h.EFF_FROM_YR_MTH as integer) and cast(h.EFF_TO_YR_MTH as integer)
		
WHERE a.mth_tm_id = &mth_tm_id. and a.basel_acct_id <> -1


;) by nzcon;

execute(commit;) by nzcon;
quit;


proc sql;
connect using NZRRAP as nzcon;
execute(
insert into &RRAP_DB..DT4_RPTG_DRVD_VARS 

SELECT 
a.mth_tm_id, a.src_sys_cd,  a.basel_acct_id, a.MORT_NUM , a.PIT_STAT_CD, a.PD_BAND
, h.DT4_RISK_RT_KEY_VAL, h.DT4_EXPSR_CL_KEY_VAL, a.BCAR_SCHED_NUM

,now() as INSRT_PROCESS_TMSTMP, now() as UPDT_PROCESS_TMSTMP

	FROM &RRAP_DB..BASEL_ANALYTCL_BL_INSTRMNT_KS a
		
		left join &RRAP_DB..BASEL_REVLVNG_CR_BASE_DRVD_VARS DRV
			ON A.BASEL_ACCT_ID = DRV.BASEL_ACCT_ID AND A.MTH_TM_ID = DRV.MTH_TM_ID
		LEFT  JOIN &RRAP_DB..BASEL_REVLVNG_CR_MTH_SNAPSHOT SNP 
			ON DRV.BASEL_ACCT_ID = SNP.BASEL_ACCT_ID
			AND DRV.MTH_TM_ID = SNP.MTH_TM_ID
			
		LEFT JOIN &RRAP_DB..TM_DIM t 
			ON a.mth_tm_id = t.tm_id AND t.tm_lvl = 'Month'
			
		LEFT JOIN &RRAP_DB..RPTG_PRD_LKP_KS h ON
			trim(SNP.PRD_CD) = trim(h.PRD_CD)
			AND trim(snp.SUB_PRD_CD) = trim(h.SUB_PRD_CD)
			AND trim(DRV.TOTAL_EXPSR_ABOVE_LMT_F) = trim(h.REVISED_EXPSR_OV_125K_F)
			AND trim(drv.HELOC_F) = trim(h.HELOC_F)
			AND trim(drv.BASEL_PRD_CD) = trim(h.BASEL_PRD_CD)
			and &YRMTH. between cast(h.EFF_FROM_YR_MTH as integer) and cast(h.EFF_TO_YR_MTH as integer)
				
WHERE a.mth_tm_id = &mth_tm_id. and a.basel_acct_id <> -1

;) by nzcon;

execute(commit;) by nzcon;
quit;

/*%end;*/

proc sql;
connect using NZRRAP as nzcon;
execute(CALL SYSPROC.ADMIN_CMD (%nrbquote('RUNSTATS ON TABLE &RRAP_DB..DT4_RPTG_DRVD_VARS on KEY COLUMNS and INDEXES ALL'));) by nzcon;
execute(COMMIT;) by nzcon;
quit;
/*
%mend multirun;

%multirun;

