options errorabend;

***************************************************************************************************************************;
***************************************************************************************************************************;
*  Job Name : J_PLL_BASEL_ANALYTCL_BL_INSTRMNT_FACT.sas
*  Target Database: IIAS EDRTLRPLL
*  Target Table:  BASEL_ANALYTCL_BL_INSTRMNT_FACT  
*  
*  Purpose: Load the final IIAS Instrument Fact table to EDRTLRPLL
*
*  Frequency: Monthly
*
*  Notes:  Consolidated logic across portfolios for PD/LGD/EAD flooring
*  		   Assigns PD/LGD segment ID's for mortgages
*
*	Change Log:
*
*  Generated on: Wednesday, February 19, 2024    EDT 
 * Changes		: RRMSS-3540 - Kalind Patel - Job for parallel Run - Rework on Instrument fact - KS, MOR *
* 				: RRMSS-3881 - Kalind Patel - Revised Combined instrument fact table for SPL PLL
***************************************************************************************************************************;


%rrap_pll_ksmor_autoexec(RRAPENV=REVOLVING_CREDIT);


data _null_;
	infile "&rrap_dir/params/rrap_iias/UNDRAWN_EXPSR_PCT.txt";
	input;
	if _n_ =3 then do;
		call symput("UNDRAWN_EXPSR_PCT",_infile_);
	end;
run;

%put UNDRAWN_EXPSR_PCT= &UNDRAWN_EXPSR_PCT.;
/*%let UNDRAWN_EXPSR_PCT = 0.5;*/

data _null_;
	set nzrrap.tm_dim;
	where tm_id = &mth_tm_id.;
	call symputx('yyyymm',put(tm_lvl_end_dt,yymmn6.));
	call symputx('mth_end_dt_nz',put(tm_lvl_end_dt,yymmdd10.));
run;


*** Identify and Delete any duplicate basel_acct_id's. ;
proc sql;
connect using nzrrap as nzcon;
execute(drop table &net_db..TMP_INSTRMNT_FACT_DUP if exists; commit;)by nzcon;
execute(CREATE TABLE &net_db..TMP_INSTRMNT_FACT_DUP AS (SELECT * FROM &net_db..TMP_INSTRMNT_FACT WHERE basel_acct_id IN 
		(SELECT basel_acct_id FROM (SELECT basel_acct_id, ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID) AS rowid FROM &net_db..TMP_INSTRMNT_FACT) a  WHERE a.rowid  <> 1)) WITH DATA; COMMIT;) by nzcon;
quit;

proc sql;
connect using nzrrap as nzcon;
execute(DELETE FROM (SELECT ROW_NUMBER() OVER (PARTITION BY BASEL_ACCT_ID) AS rowid FROM &net_db..TMP_INSTRMNT_FACT) AS a WHERE a.rowid <> 1; COMMIT;) by nzcon;
execute(CALL SYSPROC.ADMIN_CMD (%nrbquote('RUNSTATS ON TABLE &net_db..TMP_INSTRMNT_FACT on KEY COLUMNS and INDEXES ALL')); commit;) by nzcon;
quit;


proc sql;
connect using NZRRAP as nzcon;
CREATE TABLE FLOOR_IF_DATAPREP AS select * from connection to nzcon(
				WITH mor_seg_id AS (
					select 
					CASE WHEN a.src_sys_cd = 'MOR' THEN 'BNS ' ||a.model_nm
						 WHEN a.src_sys_cd = 'TNG-MOR' AND a.model_nm = 'TNG-MOR LGD-D' then 'TNG MOR LGD-D'
						 WHEN a.src_sys_cd = 'TNG-MOR' AND a.model_nm = 'TNG-MOR LGD-ND' then 'TNG MOR LGD-ND'
						 WHEN a.src_sys_cd = 'TNG-MOR' AND a.model_nm = 'TNG-MOR PD' then 'TNG MOR PD'
						 ELSE a.model_nm
					END AS model_name

					,c.seg_num as SEGMENT_NO, b.basel_seg_id, b.PRE_INSURANCE_LGD, b.UNADJUSTED_RPTG_RTO 
					
					from &net_db..basel_model a, &net_db..BASEL_SEG_RPTG_PARM b, &net_db..basel_seg c
					where a.basel_model_id=b.basel_model_id 
					and b.EFF_TO_DT >= %nrbquote('&mth_end_dt_nz.') and b.EFF_FROM_DT <= %nrbquote('&mth_end_dt_nz.') and a.src_sys_cd IN ('MOR','TNG-MOR') 
					and b.basel_seg_id=c.basel_seg_id 
					ORDER BY 1,2)

	SELECT a.BASEL_ACCT_ID, a.SRC_SYS_CD, a.MORT_NUM 
		,a.EAD_FINAL_RPTG_RTO
		,a.AF_ZERO_NET_UNDRAWN_AMT AS UNDRAWN 
		,case when a.ADJUSTED_OS_BAL_AMT <=0 then 0 else a.ADJUSTED_OS_BAL_AMT end as DRAWN 
		,a.BASEL_PRD_TP_CD
		,b.CCF
		,a.ASST_CL_NUM
		,a.PD_FINAL_RPTG_RTO
		,a.TRANSACTOR_FLAG_QRR
		,case when a.SRC_SYS_CD in ('MOR','TNG-MOR') and a.BASEL_PRD_TP_CD LIKE '%CMHC%' then 'Y' 
			  else null
		 end as CMHC_F
		 ,a.LGD_FINAL_RPTG_RTO
		 ,a.DLGD_RPTG_RTO as DLGD_RPTG_RTO_V1
		 ,coalesce(a.pd_basel_seg_id,c.basel_seg_id) AS pd_basel_seg_id
		 ,coalesce(a.lgd_basel_seg_id,d.basel_seg_id) AS lgd_basel_seg_id
		 ,d.PRE_INSURANCE_LGD
		 ,a.SCRTY_TP_DESC
		 ,a.LNG_RUN_LGD_ADD_ON_RTO
		 ,a.lgd_unadjusted_rptg_rto
		 ,a.UNINSURED_LGD_RTO
		 ,a.DLGD_F
		 ,case when a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N' and a.TRNST_EXCLSN_F='N' and a.PIT_STAT_CD in ('CUR','DEF') and (a.PD_BASEL_SEG_NUM IS NOT NULL AND a.LGD_BASEL_SEG_NUM IS NOT NULL) then 1 else 0 end as CCAR_F
		 ,COALESCE(l1.PD_BAND_EXPSR_CL_KEY_VAL,l2.PD_BAND_EXPSR_CL_KEY_VAL,l3.PD_BAND_EXPOSURE_CLASS_KEY_VALUE) AS PD_BAND_EXPSR_CL_KEY_VAL
		 ,e.UNADJUSTED_RPTG_RTO as pmi_lgd_unadjusted_rptg_rto
		 ,e.PRE_INSURANCE_LGD as pmi_lgd_insured_rptg_rto
		 ,a.UNINSURED_LGD_SEG_NUM
		 ,f.HELOC_F
		 ,g.COLLATERAL_TYPE ,g.H_C ,g.LGD_S ,g.LGD_U ,g.H_E
		 ,coalesce(h.SCRTY_VAL_AMT ,i.CRNT_PRPTY_VAL_AMT) as COLLATERAL_VALUE
		 ,a.PIT_STAT_CD
		 	
		
	FROM &net_db..TMP_INSTRMNT_FACT a
		LEFT JOIN &net_db_P1D..RPTG_CCF_LKP b 
			ON upper(a.BASEL_PRD_TP_CD) = upper(b.BASEL_PRD_TP_CD) AND (&yyyymm. BETWEEN cast(b.EFF_FROM_YR_MTH as integer) AND cast(b.EFF_TO_YR_MTH as integer))
		LEFT JOIN mor_seg_id c
			ON upper(a.pd_model_nm) = upper(c.model_name) AND a.pd_basel_seg_num = c.SEGMENT_NO
		LEFT JOIN mor_seg_id d
			ON upper(a.lgd_model_nm) = upper(d.model_name) AND a.lgd_basel_seg_num = d.SEGMENT_NO
		LEFT JOIN mor_seg_id e
			ON upper(a.lgd_model_nm) = upper(e.model_name) AND a.UNINSURED_LGD_SEG_NUM = e.SEGMENT_NO
		LEFT JOIN &net_db_P1D..RPTG_PRD_LKP_KS l1
			ON upper(l1.prd_id) = upper(a.prd_id) AND (&yyyymm. between CAST(l1.EFF_FROM_YR_MTH AS integer) AND CAST(l1.EFF_TO_YR_MTH AS integer))
		LEFT JOIN &net_db_P1D..RPTG_PRD_LKP_SPL l2
			ON upper(l2.prd_id) = upper(a.prd_id) AND (&yyyymm. between CAST(l2.EFF_FROM_YR_MTH AS integer) AND CAST(l2.EFF_TO_YR_MTH AS integer))
		LEFT JOIN &net_db_P1D..RPTG_PRD_LKP_MOR l3
			ON upper(l3.product_id) = upper(a.prd_id) AND (&yyyymm. between CAST(l3.EFF_FROM_YR_MTH AS integer) AND CAST(l3.EFF_TO_YR_MTH AS integer))
		LEFT JOIN &net_db_P1D..BASEL_REVLVNG_CR_BASE_DRVD_VARS f
			ON a.BASEL_ACCT_ID = f.BASEL_ACCT_ID AND a.MTH_TM_ID = f.MTH_TM_ID and a.SRC_SYS_CD = 'KS'
		LEFT JOIN &net_db_P1D..RPTG_LGD_HC_LKP g
			ON case 
				when a.SRC_SYS_CD = 'KS' and a.PRD_CD = 'SCL' and a.SUB_PRD_CD = 'CS' and f.HELOC_F = 'N' and a.ASST_CL_NUM = 2 then 'Financial'
				when a.SRC_SYS_CD = 'SPL' and a.ASST_CL_NUM = 2 and  a.BASEL_PRD_TP_CD LIKE 'ITL_AUTO%' then 'Other Physical'
				else NULL
		 	   end = g.COLLATERAL_TYPE
			AND (&yyyymm. between CAST(g.EFF_FROM_YR_MTH AS integer) AND CAST(g.EFF_TO_YR_MTH AS integer))
		LEFT JOIN &net_db..BASEL_REVLVNG_CR_MTH_SNAPSHOT h
			ON a.BASEL_ACCT_ID = h.BASEL_ACCT_ID AND a.MTH_TM_ID = h.MTH_TM_ID and a.SRC_SYS_CD = 'KS' 
		LEFT JOIN &net_db_P1D..BASEL_ACCT_PRFM_FACT i
			ON a.BASEL_ACCT_ID = i.BASEL_ACCT_ID AND a.MTH_TM_ID = i.MTH_TM_ID and a.SRC_SYS_CD = 'SPL' 
			
	WHERE a.mth_tm_id = &mth_tm_id. 
/*			and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N' and a.TRNST_EXCLSN_F='N' and a.PIT_STAT_CD in ('CUR','DEF') */
	order by a.SRC_SYS_CD, a.BASEL_ACCT_ID);
quit;




data FLRD_INSTR_FACT_01;
set FLOOR_IF_DATAPREP;

	***** PD_FLRD_RPTG_RTO ;
	if missing(PD_FINAL_RPTG_RTO) then do;
		PD_FLR = .;
		PD_FLRD_RPTG_RTO = .;
	end;
	else do;
			 if CMHC_F = 'Y' 									then PD_FLR = 0.0005;
		else if ASST_CL_NUM = 3 AND TRANSACTOR_FLAG_QRR = 'N' 	then PD_FLR = 0.001; 
		else 												 		 PD_FLR = 0.0005;

		PD_FLRD_RPTG_RTO = MAX(PD_FINAL_RPTG_RTO, PD_FLR);
	end;
run;


proc sql;
create table FLRD_INSTR_FACT_02 as
	select a.*, b.pd_band, b.NCR_PD_BAND_KEY_VAL
	from FLRD_INSTR_FACT_01 a 
		left join 
	TLRP1D.PD_BAND_DIM b
		on a.PD_BAND_EXPSR_CL_KEY_VAL = b.NCR_EXPSR_CL_KEY_VAL and a.PD_FLRD_RPTG_RTO between b.PD_MIN_VAL and b.PD_MAX_VAL
		and coalesce(upper(a.CMHC_F),'z') = coalesce(upper(b.CMHC_F),'z') and coalesce(upper(a.TRANSACTOR_FLAG_QRR),'z') = coalesce(upper(b.TRANSACTOR_F),'z')
		and  &yyyymm. between input(b.eff_from_yr_mth,6.) and input(b.eff_to_yr_mth,6.);
quit;

data FLRD_INSTR_FACT;
	retain basel_acct_id mth_tm_id;
	set FLRD_INSTR_FACT_02;
	mth_tm_id = &mth_tm_id.;
	
	if PD_BAND = '26' then UNDRAWN = 0;

	if BASEL_PRD_TP_CD in ('CARD','CL','HELOC','SLR') then UNDRAWN_EXPSR_PCT = &UNDRAWN_EXPSR_PCT.;


	
	***** EAD_FLRD_RPTG_RTO ;
	if missing(EAD_FINAL_RPTG_RTO) then do;
		EAD_FLR = .;
		EAD_FLRD_RPTG_RTO = .;
	end;
	else do;
		if SRC_SYS_CD = 'KS' and PD_FINAL_RPTG_RTO LT 1 and BASEL_PRD_TP_CD = 'SLT' then do; 
			EAD_FLR = 0;
			EAD_FLRD_RPTG_RTO = MAX(EAD_FINAL_RPTG_RTO, EAD_FLR);
		end;
		else if SRC_SYS_CD = 'KS' and PD_FINAL_RPTG_RTO LT 1 and BASEL_PRD_TP_CD NE 'SLT' then do; 
			EAD_FLR = (DRAWN + CCF * UNDRAWN_EXPSR_PCT * UNDRAWN )/(DRAWN + UNDRAWN);
			EAD_FLRD_RPTG_RTO = MAX(EAD_FINAL_RPTG_RTO, EAD_FLR);
		end;
		else if SRC_SYS_CD = 'KS' and PD_FINAL_RPTG_RTO GE 1 then do;
			EAD_FLR = 0;
			EAD_FLRD_RPTG_RTO = 1;
		end;
		else do;
			EAD_FLR = 0;
			EAD_FLRD_RPTG_RTO = 1;
		end;
	end;



	if not find(BASEL_PRD_TP_CD,'GENW','it') and not find(BASEL_PRD_TP_CD,'GUAR','it') then do;
		UNINSURED_LGD_SEG_NUM = .;
		UNINSURED_LGD_RTO = .;
		UNINSURED_FLRD_LGD_RTO = .;
		UNINSURED_DLGD_RTO = .;
		PMI_LGD_INSURED_RPTG_RTO = .;
		PMI_LGD_UNADJUSTED_RPTG_RTO = .;
	end;

	
	* EXPOSURES;

		 if PIT_STAT_CD EQ 'CUR' then EXPOSURE = sum(DRAWN,UNDRAWN);
	else if PIT_STAT_CD NE 'CUR' then EXPOSURE = DRAWN;

	EXPOSURE_SECURED_MAXIMUM = EXPOSURE * (1 + H_E);

	EXPOSURE_SECURED = MIN(EXPOSURE_SECURED_MAXIMUM, coalesce(COLLATERAL_VALUE,0) * (1 - H_C)); 
	EXPOSURE_UNSECURED = EXPOSURE - EXPOSURE_SECURED;

	* WEIGHTS ;

	WEIGHT_SECURED = EXPOSURE_SECURED / EXPOSURE_SECURED_MAXIMUM;
    WEIGHT_UNSECURED = EXPOSURE_UNSECURED / EXPOSURE_SECURED_MAXIMUM;
	
	* FULLY SECURED FLAG;

	if not missing(COLLATERAL_TYPE) and not missing(COLLATERAL_VALUE) and EXPOSURE_SECURED GE EXPOSURE then FULLY_SECURED_F = 'Y';
		else FULLY_SECURED_F = '';

	***** LGD Variables ;

	* LGD Floor Values;
	%let LGD_FLR_CMHC = 0.0;
	%let LGD_FLR_NON_CMHC_RESL = 0.1;
	%let LGD_FLR_QRR = 0.5;
	%let LGD_FLR_OTHR_RTL_NON_FRMULAIC = 0.3;

		 if ASST_CL_NUM = 1 and CMHC_F = 'Y' 	and missing(FULLY_SECURED_F) then LGD_FLR = &LGD_FLR_CMHC. ; *0.0;
	else if ASST_CL_NUM = 1 and missing(CMHC_F) and missing(FULLY_SECURED_F) then LGD_FLR = &LGD_FLR_NON_CMHC_RESL. ;*0.1;
	else if ASST_CL_NUM = 3 and missing(CMHC_F) and missing(FULLY_SECURED_F) then LGD_FLR = &LGD_FLR_QRR. ;*0.5;

	else if ASST_CL_NUM = 2 and missing(CMHC_F) and     missing(collateral_type) then LGD_FLR = &LGD_FLR_OTHR_RTL_NON_FRMULAIC. ;*0.3;
	else if ASST_CL_NUM = 2 and missing(CMHC_F) and not missing(collateral_type) then do;
		if EXPOSURE_SECURED_MAXIMUM = 0 then LGD_FLR = &LGD_FLR_OTHR_RTL_NON_FRMULAIC. ;*0.3;
			else LGD_FLR = WEIGHT_UNSECURED * LGD_U + WEIGHT_SECURED  * LGD_S;
	end;


	LGD_FLRD_RPTG_RTO = MAX(LGD_FLR,LGD_FINAL_RPTG_RTO);
	UNINSURED_FLRD_LGD_RTO = MAX(LGD_FLR, UNINSURED_LGD_RTO);


	* DLGD Floor Calculation;
	if DLGD_F = 'Y' and SCRTY_TP_DESC='Insured' and not missing(LNG_RUN_LGD_ADD_ON_RTO) then do;

		UNINSURED_DLGD_RTO = MAX(LGD_FLR, UNINSURED_LGD_RTO, MIN(1, (pmi_lgd_insured_rptg_rto + LNG_RUN_LGD_ADD_ON_RTO)));

		DLGD_FLR = MIN(1, (PRE_INSURANCE_LGD + LNG_RUN_LGD_ADD_ON_RTO));
		DLGD_RPTG_RTO = MAX(LGD_FINAL_RPTG_RTO, DLGD_FLR, LGD_FLR);
	end;

	else if DLGD_F='Y' AND SCRTY_TP_DESC NE 'Insured' and not missing(LNG_RUN_LGD_ADD_ON_RTO) then do;
 
		UNINSURED_DLGD_RTO = MAX(LGD_FLR, UNINSURED_LGD_RTO, MIN(1, (pmi_lgd_unadjusted_rptg_rto + LNG_RUN_LGD_ADD_ON_RTO)));

		DLGD_FLR = MIN(1, (LGD_UNADJUSTED_RPTG_RTO + LNG_RUN_LGD_ADD_ON_RTO));
		DLGD_RPTG_RTO = MAX(LGD_FINAL_RPTG_RTO, DLGD_FLR, LGD_FLR);
	end;

	else do;
		UNINSURED_DLGD_RTO = MAX(UNINSURED_LGD_RTO, LGD_FLR);
		DLGD_RPTG_RTO = MAX(LGD_FINAL_RPTG_RTO, LGD_FLR);
	end;


	if missing(LGD_FINAL_RPTG_RTO) then do;
		LGD_FLR = .;
		LGD_FLRD_RPTG_RTO = .;
		DLGD_FLR = .;
		DLGD_RPTG_RTO = .;
	end;
	if missing(UNINSURED_LGD_RTO) then do;
		UNINSURED_DLGD_RTO = .;
		UNINSURED_FLRD_LGD_RTO = .;
	end;

***************************************************************************************************************************************;

		 if DLGD_F='Y' AND SCRTY_TP_DESC='Insured' and not missing(LNG_RUN_LGD_ADD_ON_RTO) then 
			DLGD_RPTG_RTO_V1_DRVD =	MAX(0.1, LGD_FINAL_RPTG_RTO, MIN(1, (PRE_INSURANCE_LGD + LNG_RUN_LGD_ADD_ON_RTO)));

	else if DLGD_F='Y' and SCRTY_TP_DESC NE 'Insured' and not missing(LNG_RUN_LGD_ADD_ON_RTO) then    
			DLGD_RPTG_RTO_V1_DRVD =	MAX(0.1, LGD_FINAL_RPTG_RTO, MIN(1, (lgd_unadjusted_rptg_rto + LNG_RUN_LGD_ADD_ON_RTO))); 

	else DLGD_RPTG_RTO_V1_DRVD = LGD_FINAL_RPTG_RTO ;
	if missing(LGD_FINAL_RPTG_RTO) then DLGD_RPTG_RTO_V1_DRVD = .;
***************************************************************************************************************************************;		

run;






proc sql;
connect using NZUSER as nzcon;
execute(drop table &net_db..FLRD_INSTR_FACT if exists; commit;) by nzcon;
quit;
proc append base=NZUSER.FLRD_INSTR_FACT(bulkload=yes BL_METHOD=CLILOAD) data=FLRD_INSTR_FACT force nowarn; run;


proc sql;
connect using NZRRAP as nzcon;
execute(delete from &net_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT where mth_tm_id = &mth_tm_id.; commit;) by nzcon;
quit;

proc sql;
connect using NZRRAP as nzcon;
execute( insert into &net_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT
select 
 a.MTH_TM_ID
,a.SRC_SYS_CD
,a.BASEL_ACCT_ID
,a.UNQ_ACCT_ID
,a.NCR_RT_SYS_KEY_VAL
	,b.NCR_PD_BAND_KEY_VAL
,a.NCR_EXPSR_SIZE_KEY_VAL
,a.NCR_GEO_KEY_VAL
,a.NCR_LTV_KEY_VAL
,a.NCR_EXPSR_CL_KEY_VAL
,a.NCR_RT_KEY_VAL
,a.NCR_DLQNT_BCKT_KEY_VAL
,a.PIT_STAT_CD
,a.UTLTN_RTO
,a.ACCT_ODT
,a.TRNST_NUM
,a.CR_LMT_AMT
,a.ADVNC_AMT
,a.AUTH_AMT
,a.OS_BAL_AMT
,a.GENL_LEDGER_BALCNG_ADJ_AMT
,a.ADJUSTED_OS_BAL_AMT
,a.UNADJUSTED_ADD_ON_BAL_AMT
,a.AF_SECRTZTN_BAL_AMT
,a.DLQNT_DAY_CNT
,a.BEFORE_ZERO_NET_UNDRAWN_AMT
,a.BEFORE_ZERO_NET_DRAWN_AMT
,a.ORIG_PRPTY_VAL_AMT
,a.LOAN_TO_VAL_RTO
,a.INDEXED_PRPTY_VAL_AMT
,a.INDEXED_LOAN_TO_VAL_RTO
,a.PD_MODEL_NM
,a.PD_MODEL_VER
,a.PD_BASEL_SCORECRD_NM
,a.PD_SCORECRD_VER
,a.PD_BASEL_SEG_NUM
	,coalesce(a.PD_BASEL_SEG_ID,b.PD_BASEL_SEG_ID) as PD_BASEL_SEG_ID
,a.PD_SEG_VER
,a.PD_BASEL_MODEL_REL_ID
,a.PD_MODEL_RTO
,a.PD_LR_RPTG_RTO
,a.PD_LR_PV_RPTG_RTO
,a.PD_LR_PV_AD_RPTG_RTO
,a.PD_LD_PV_AD_SV_RPTG_RTO
,a.PD_ACCT_SCORE
,a.PD_FINAL_RPTG_RTO
,a.LGD_MODEL_NM
,a.LGD_MODEL_VER
,a.LGD_BASEL_SCORECRD_NM
,a.LGD_SCORECRD_VER
,a.LGD_BASEL_SEG_NUM
	,coalesce(a.LGD_BASEL_SEG_ID,b.LGD_BASEL_SEG_ID) as LGD_BASEL_SEG_ID
,a.LGD_SEG_VER
,a.LGD_BASEL_MODEL_REL_ID
,a.LGD_MODEL_RTO
,a.LGD_LR_RPTG_RTO
,a.LGD_LR_PV_RPTG_RTO
,a.LGD_LR_PV_AD_RPTG_RTO
,a.LGD_LD_PV_AD_SV_RPTG_RTO
,a.LGD_ACCT_SCORE
,a.LGD_LD_PV_AD_SV_DT_RPTG_RTO
,a.EAD_MODEL_NM
,a.EAD_MODEL_VER
,a.LGD_FINAL_RPTG_RTO
,a.EAD_BASEL_SCORECRD_NM
,a.EAD_SCORECRD_VER
,a.EAD_BASEL_SEG_NUM
,a.EAD_BASEL_SEG_ID
,a.EAD_SEG_VER
,a.EAD_BASEL_MODEL_REL_ID
,a.EAD_MODEL_RTO
,a.EAD_LR_RPTG_RTO
,a.EAD_LR_PV_RPTG_RTO
,a.EAD_ACCT_SCORE
,a.EAD_LR_PV_AD_RPTG_RTO
,a.ASST_CL_DESC
,a.CONS_DFT_MTH_CNT
,a.EAD_LD_PV_AD_SV_RPTG_RTO
,a.EAD_LD_PV_AD_SV_DT_RPTG_RTO
,a.BCAR_SCHED_NUM
,a.EAD_FINAL_RPTG_RTO
	,b.PD_BAND
,a.BASEL_PRD_ABR
,a.SCRTY_TP_DESC
,a.CCAR_BASEL_PRD_TP_NM
,a.CCAR_EXPSR_CL_NM
,a.BASEL_PRD_TP_CD
,a.CONSM_PRD_TREATMNT_CD
,a.PRD_ID
,a.PRIM_CUST_CID
,a.ACCT_NUM
,a.RGNL_OFFC_CD
,a.DLQNT_STG
,a.MORT_NUM
,a.CAB_TRNST_NUM
,a.EGL_DEPRTMNT
,a.BASEL_CIF_KEY
,a.LOAN_NUM
,a.INTR_ACCR_AMT
,a.CUST_BEHV_SCORE
,a.PRD_CD
,a.SUB_PRD_CD
,a.SRC_PRD_DESC
,a.LEGAL_ENTITY
,a.MAT_DT
,a.LAST_PYMT_DT
,a.SCRTY_TP_CD
,a.STEP_F
,a.LAST_ACTY_DT
,a.HOUSE_TP_NM
,a.LAST_RGL_PAY_DT
,a.PD_OFF_F
,a.PD_OFF_DT
,a.RESIDUAL_MAT
,a.ASST_CL_NUM
,a.E_MAT_DT
,a.AMORT
,a.TRNST_EXCLSN_F
,a.SML_BUS_F
,a.NOTE_DT
,a.INSRT_PROCESS_TMSTMP
,a.UPDT_PROCESS_TMSTMP
,a.AF_ZERO_NET_DRAWN_AMT
,a.AF_ZERO_NET_UNDRAWN_AMT
,a.PD_UNADJUSTED_RPTG_RTO
,a.LGD_UNADJUSTED_RPTG_RTO
,a.EAD_UNADJUSTED_RPTG_RTO
,a.PD_LR_PV_AD_SV_DT_AA_RTO
,a.LGD_LR_PV_AD_SV_DT_AA_RTO
,a.EAD_LR_PV_AD_SV_DT_AA_RTO
,a.INSUR_F
,a.OS_PRNCPL_BAL_AMT
,a.PREV_12_QTR_PRPTY_VAL_AMT
,a.DLGD_F
,a.CRNT_LTV_RTO
,a.METRPL_AREA_NM
,a.PRPTY_VAL_CORR_PCTG
,a.LNG_RUN_LGD_ADD_ON_RTO
	,b.DLGD_RPTG_RTO
,a.LTV_PERCENTAGE
,a.RNTL_PRPTY_F
,a.CURRENCY_MISMATCH_F
,a.TOT_EXPSR_ABOVE_1500K_LMT_F
,a.ORIG_AMT_LOAN
,a.LTV_BUCKET
,a.TRANSACTOR_F
,a.PD_90_DAY_F
	,b.UNINSURED_LGD_SEG_NUM
	,b.UNINSURED_LGD_RTO
	,b.UNINSURED_DLGD_RTO
,a.TRANSACTOR_FLAG_QRR
	,b.CCF
	,b.DRAWN
	,b.EAD_FLR
	,b.EAD_FLRD_RPTG_RTO
	,b.UNDRAWN
	,b.UNDRAWN_EXPSR_PCT
	,b.CMHC_F
	,b.DLGD_FLR
	,b.FULLY_SECURED_F
	,b.LGD_FLR
	,b.PRE_INSURANCE_LGD
	,b.PD_FLR
	,b.PD_FLRD_RPTG_RTO
	,b.LGD_FLRD_RPTG_RTO
	,b.PMI_LGD_INSURED_RPTG_RTO
	,b.PMI_LGD_UNADJUSTED_RPTG_RTO
	,b.UNINSURED_FLRD_LGD_RTO
	,b.PD_BAND_EXPSR_CL_KEY_VAL
,a.DLGD_RPTG_RTO_OLD
	,b.CCAR_F
,a.CLP_FLAG /*RRMSS-1907*/
		,COALESCE(c.BCAR_SCHED_NM, d.BCAR_SCHED_NM) AS BCAR_SCHED_NM /*RRMSS-1874*/
		,COALESCE(c.BCAR_SCHED_NUM_50, d.BCAR_SCHED_NUM_50) AS BCAR_SCHED_NUM_50 /*RRMSS-1874*/
	,b.COLLATERAL_TYPE
	,b.H_C
	,b.LGD_S
	,b.LGD_U
	,b.H_E
	,b.COLLATERAL_VALUE
	,b.EXPOSURE
	,b.EXPOSURE_SECURED_MAXIMUM
	,b.EXPOSURE_SECURED
	,b.EXPOSURE_UNSECURED
	,b.WEIGHT_SECURED
	,b.WEIGHT_UNSECURED

FROM &net_db..TMP_INSTRMNT_FACT a
LEFT JOIN &net_db..FLRD_INSTR_FACT b 
ON a.MTH_TM_ID = b.MTH_TM_ID AND a.BASEL_ACCT_ID = b.BASEL_ACCT_ID 

LEFT JOIN &net_db_P1D..RMA_BASEL3_PRDCT_SCHD_BCAR50_LKP c /* 1500K override. RRMSS-1874*/
ON UPPER(a.SRC_SYS_CD) = UPPER(c.SRC_SYS_CD) 
	AND a.ASST_CL_NUM = c.ASST_CL_NUM 
	AND UPPER(a.TOT_EXPSR_ABOVE_1500K_LMT_F) = UPPER(c.TOT_EXPSR_ABOVE_LMT_F)
	AND UPPER(c.TOT_EXPSR_ABOVE_LMT_F) = 'Y'
	AND (&yyyymm. BETWEEN CAST(c.EFF_FROM_YR_MTH AS INTEGER) AND CAST(c.EFF_TO_YR_MTH AS INTEGER))
LEFT JOIN &net_db_P1D..RMA_BASEL3_PRDCT_SCHD_BCAR50_LKP d /*RRMSS-1874*/
ON UPPER(a.SRC_SYS_CD) = UPPER(d.SRC_SYS_CD) 
/*	AND a.ASST_CL_NUM = d.ASST_CL_NUM */
	AND UPPER(a.BASEL_PRD_TP_CD) = UPPER(d.BASEL_PRD_TP_CD) 
	AND COALESCE(UPPER(a.PRD_CD), '_') = COALESCE(UPPER(d.PRD_CD), '_')
	AND COALESCE(UPPER(a.SUB_PRD_CD), '_') = COALESCE(UPPER(d.SUB_PRD_CD), '_')
	AND COALESCE(UPPER(a.RNTL_PRPTY_F), '_') = COALESCE(UPPER(d.RNTL_PRPTY_F), '_')
	AND COALESCE(UPPER(a.CLP_FLAG), '_') = COALESCE(UPPER(d.CLP_FLAG), '_')
	AND COALESCE(UPPER(a.TRANSACTOR_FLAG_QRR), '_') = COALESCE(UPPER(d.TRANSACTOR_FLAG_QRR), '_')
	AND d.TOT_EXPSR_ABOVE_LMT_F IS NULL
	AND (&yyyymm. BETWEEN CAST(d.EFF_FROM_YR_MTH AS INTEGER) AND CAST(d.EFF_TO_YR_MTH AS INTEGER))



WHERE a.MTH_TM_ID = &mth_tm_id.; commit;) by nzcon;
quit;

/*Truncate table BASEL_ANALYTCL_BL_INSTRMNT_KS, BASEL_PSNL_LN_ANL_BL_INST_FACT & TMP_INSTRMNT_FACT*/
proc sql;
connect using NZRRAP as nzcon;
execute(delete from &net_db..BASEL_ANALYTCL_BL_INSTRMNT_KS where mth_tm_id = &mth_tm_id.; commit;) by nzcon;
execute(delete from &net_db..TMP_INSTRMNT_FACT where mth_tm_id = &mth_tm_id.; commit;) by nzcon;
execute(delete from &pll_db_FRG..BASEL_ANLYT_BL_INST_FCT_BNS_DLGD where mth_tm_id = &mth_tm_id.; commit;) by nzcon;
execute(delete from &pll_db_FRG..BASEL_ANALYTCL_BL_INST_FCT_BNS where mth_tm_id = &mth_tm_id.; commit;) by nzcon;
execute(delete from &net_db..BASEL_PSNL_LN_ANL_BL_INST_FACT where mth_tm_id = &mth_tm_id.; commit;) by nzcon;
quit;


proc sql;
connect using NZRRAP as nzcon;
execute(INSERT INTO &net_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT
SELECT 
MTH_TM_ID, 
SRC_SYS_CD, 
BASEL_ACCT_ID, 
UNQ_ACCT_ID, 
NCR_RT_SYS_KEY_VAL, 
NCR_PD_BAND_KEY_VAL, 
NCR_EXPSR_SIZE_KEY_VAL, 
NCR_GEO_KEY_VAL, 
NCR_LTV_KEY_VAL, 
NCR_EXPSR_CL_KEY_VAL, 
NCR_RT_KEY_VAL, 
NCR_DLQNT_BCKT_KEY_VAL, 
PIT_STAT_CD, 
UTLTN_RTO, 
ACCT_ODT, 
TRNST_NUM, 
CR_LMT_AMT, 
ADVNC_AMT, 
AUTH_AMT, 
OS_BAL_AMT, 
GENL_LEDGER_BALCNG_ADJ_AMT, 
ADJUSTED_OS_BAL_AMT, 
UNADJUSTED_ADD_ON_BAL_AMT, 
AF_SECRTZTN_BAL_AMT, 
DLQNT_DAY_CNT, 
BEFORE_ZERO_NET_UNDRAWN_AMT, 
BEFORE_ZERO_NET_DRAWN_AMT, 
ORIG_PRPTY_VAL_AMT, 
LOAN_TO_VAL_RTO, 
INDEXED_PRPTY_VAL_AMT, 
INDEXED_LOAN_TO_VAL_RTO, 
PD_MODEL_NM, 
PD_MODEL_VER, 
PD_BASEL_SCORECRD_NM, 
PD_SCORECRD_VER, 
PD_BASEL_SEG_NUM, 
PD_BASEL_SEG_ID, 
PD_SEG_VER, 
PD_BASEL_MODEL_REL_ID, 
PD_MODEL_RTO, 
PD_LR_RPTG_RTO, 
PD_LR_PV_RPTG_RTO, 
PD_LR_PV_AD_RPTG_RTO, 
PD_LD_PV_AD_SV_RPTG_RTO, 
PD_ACCT_SCORE, 
PD_FINAL_RPTG_RTO, 
LGD_MODEL_NM, 
LGD_MODEL_VER, 
LGD_BASEL_SCORECRD_NM, 
LGD_SCORECRD_VER, 
LGD_BASEL_SEG_NUM, 
LGD_BASEL_SEG_ID, 
LGD_SEG_VER, 
LGD_BASEL_MODEL_REL_ID, 
LGD_MODEL_RTO, 
LGD_LR_RPTG_RTO, 
LGD_LR_PV_RPTG_RTO, 
LGD_LR_PV_AD_RPTG_RTO, 
LGD_LD_PV_AD_SV_RPTG_RTO, 
LGD_ACCT_SCORE, 
LGD_LD_PV_AD_SV_DT_RPTG_RTO, 
EAD_MODEL_NM, 
EAD_MODEL_VER, 
LGD_FINAL_RPTG_RTO, 
EAD_BASEL_SCORECRD_NM, 
EAD_SCORECRD_VER, 
EAD_BASEL_SEG_NUM, 
EAD_BASEL_SEG_ID, 
EAD_SEG_VER, 
EAD_BASEL_MODEL_REL_ID, 
EAD_MODEL_RTO, 
EAD_LR_RPTG_RTO, 
EAD_LR_PV_RPTG_RTO, 
EAD_ACCT_SCORE, 
EAD_LR_PV_AD_RPTG_RTO, 
ASST_CL_DESC, 
CONS_DFT_MTH_CNT, 
EAD_LD_PV_AD_SV_RPTG_RTO, 
EAD_LD_PV_AD_SV_DT_RPTG_RTO, 
BCAR_SCHED_NUM, 
EAD_FINAL_RPTG_RTO, 
PD_BAND, 
BASEL_PRD_ABR, 
SCRTY_TP_DESC, 
CCAR_BASEL_PRD_TP_NM, 
CCAR_EXPSR_CL_NM, 
BASEL_PRD_TP_CD, 
CONSM_PRD_TREATMNT_CD, 
PRD_ID, 
PRIM_CUST_CID, 
ACCT_NUM, 
RGNL_OFFC_CD, 
DLQNT_STG, 
MORT_NUM, 
CAB_TRNST_NUM, 
EGL_DEPRTMNT, 
BASEL_CIF_KEY, 
LOAN_NUM, 
INTR_ACCR_AMT, 
CUST_BEHV_SCORE, 
PRD_CD, 
SUB_PRD_CD, 
SRC_PRD_DESC, 
LEGAL_ENTITY, 
MAT_DT, 
LAST_PYMT_DT, 
SCRTY_TP_CD, 
STEP_F, 
LAST_ACTY_DT, 
HOUSE_TP_NM, 
LAST_RGL_PAY_DT, 
PD_OFF_F, 
PD_OFF_DT, 
RESIDUAL_MAT, 
ASST_CL_NUM, 
E_MAT_DT, 
AMORT, 
TRNST_EXCLSN_F, 
SML_BUS_F, 
NOTE_DT, 
INSRT_PROCESS_TMSTMP, 
UPDT_PROCESS_TMSTMP, 
AF_ZERO_NET_DRAWN_AMT, 
AF_ZERO_NET_UNDRAWN_AMT, 
PD_UNADJUSTED_RPTG_RTO, 
LGD_UNADJUSTED_RPTG_RTO, 
EAD_UNADJUSTED_RPTG_RTO, 
PD_LR_PV_AD_SV_DT_AA_RTO, 
LGD_LR_PV_AD_SV_DT_AA_RTO, 
EAD_LR_PV_AD_SV_DT_AA_RTO, 
INSUR_F, 
OS_PRNCPL_BAL_AMT, 
PREV_12_QTR_PRPTY_VAL_AMT, 
DLGD_F, 
CRNT_LTV_RTO, 
METRPL_AREA_NM, 
PRPTY_VAL_CORR_PCTG, 
LNG_RUN_LGD_ADD_ON_RTO, 
DLGD_RPTG_RTO, 
LTV_PERCENTAGE, 
RNTL_PRPTY_F, 
CURRENCY_MISMATCH_F, 
TOT_EXPSR_ABOVE_1500K_LMT_F, 
ORIG_AMT_LOAN, 
LTV_BUCKET, 
TRANSACTOR_F, 
PD_90_DAY_F, 
UNINSURED_LGD_SEG_NUM, 
UNINSURED_LGD_RTO, 
UNINSURED_DLGD_RTO, 
TRANSACTOR_FLAG_QRR, 
CCF, 
DRAWN, 
EAD_FLR, 
EAD_FLRD_RPTG_RTO, 
UNDRAWN, 
UNDRAWN_EXPSR_PCT, 
CMHC_F, 
DLGD_FLR, 
FULLY_SECURED_F, 
LGD_FLR, 
PRE_INSURANCE_LGD, 
PD_FLR, 
PD_FLRD_RPTG_RTO, 
LGD_FLRD_RPTG_RTO, 
PMI_LGD_INSURED_RPTG_RTO, 
PMI_LGD_UNADJUSTED_RPTG_RTO, 
UNINSURED_FLRD_LGD_RTO, 
PD_BAND_EXPSR_CL_KEY_VAL, 
DLGD_RPTG_RTO_OLD, 
CCAR_F, 
CLP_FLAG, 
BCAR_SCHED_NM, 
BCAR_SCHED_NUM_50, 
COLLATERAL_TYPE, 
H_C, 
LGD_S, 
LGD_U, 
H_E, 
COLLATERAL_VALUE, 
EXPOSURE, 
EXPOSURE_SECURED_MAXIMUM, 
EXPOSURE_SECURED, 
EXPOSURE_UNSECURED, 
WEIGHT_SECURED, 
WEIGHT_UNSECURED
FROM &net_db_P1D..BASEL_ANALYTCL_BL_INSTRMNT_FACT babif WHERE MTH_TM_ID = &mth_tm_id. AND SRC_SYS_CD IN ('TNG-MOR')) by nzcon;
quit;