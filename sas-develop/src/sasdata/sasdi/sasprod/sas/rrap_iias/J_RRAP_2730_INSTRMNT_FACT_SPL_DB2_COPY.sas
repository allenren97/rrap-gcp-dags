 options errorabend;



%put WORK LOCATION: %sysfunc(getoption(work));
%rrap_spl_autoexec;


%get_model_period_dates(product=spl);
%put Start and End Dates for SPL Models:;
%put start_period_dt: &start_period_dt ;
%put end_period_dt: &end_period_dt;

proc sql noprint;
	select TM_ID into :mth_tm_id from nzrrap.TM_DIM
	where tm_lvl='Month' and tm_lvl_end_dt = "&end_period_dt"d
	order by 1;
quit;
%put &mth_tm_id.;




******************** SPL DB2 ;

        %MACRO DELETE_DATA;
         
         %let partitions=10;
         %DO i=0 %TO %eval(&partitions -1);
         	PROC SQL NOPRINT;
         	connect using DB2RRAP as dbcon;
         	EXECUTE( DELETE FROM EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT WHERE mod(BASEL_ACCT_ID,&partitions.) = &i. AND 
         	SRC_SYS_CD = 'SPL' AND MTH_TM_ID = &MTH_TM_ID) by dbcon;
         	QUIT;
         	%PUT i=&i;
         %END;
         	PROC SQL NOPRINT;
         	connect using DB2RRAP as dbcon;
         	EXECUTE( DELETE FROM EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT WHERE 
         	SRC_SYS_CD = 'SPL' AND MTH_TM_ID = &MTH_TM_ID) by dbcon;
         	QUIT;
         
         %MEND;
         %DELETE_DATA;
         
         %let varkeep=MTH_TM_ID SRC_SYS_CD BASEL_ACCT_ID UNQ_ACCT_ID NCR_RT_SYS_KEY_VAL NCR_PD_BAND_KEY_VAL NCR_EXPSR_SIZE_KEY_VAL NCR_GEO_KEY_VAL 
         NCR_LTV_KEY_VAL NCR_EXPSR_CL_KEY_VAL NCR_RT_KEY_VAL NCR_DLQNT_BCKT_KEY_VAL PIT_STAT_CD UTLTN_RTO ACCT_ODT TRNST_NUM CR_LMT_AMT ADVNC_AMT 
         AUTH_AMT OS_BAL_AMT GENL_LEDGER_BALCNG_ADJ_AMT ADJUSTED_OS_BAL_AMT UNADJUSTED_ADD_ON_BAL_AMT AF_SECRTZTN_BAL_AMT DLQNT_DAY_CNT BEFORE_ZERO_NET_DRAWN_AMT 
         BEFORE_ZERO_NET_UNDRAWN_AMT ORIG_PRPTY_VAL_AMT LOAN_TO_VAL_RTO INDEXED_PRPTY_VAL_AMT INDEXED_LOAN_TO_VAL_RTO PD_MODEL_NM PD_MODEL_VER PD_BASEL_SCORECRD_NM 
         PD_SCORECRD_VER PD_BASEL_SEG_NUM PD_BASEL_SEG_ID PD_SEG_VER PD_BASEL_MODEL_REL_ID PD_MODEL_RTO PD_LR_RPTG_RTO PD_LR_PV_RPTG_RTO PD_LR_PV_AD_RPTG_RTO 
         PD_LD_PV_AD_SV_RPTG_RTO PD_ACCT_SCORE PD_FINAL_RPTG_RTO LGD_MODEL_NM LGD_MODEL_VER LGD_BASEL_SCORECRD_NM LGD_SCORECRD_VER LGD_BASEL_SEG_NUM 
         LGD_BASEL_SEG_ID LGD_SEG_VER LGD_BASEL_MODEL_REL_ID LGD_MODEL_RTO LGD_LR_RPTG_RTO LGD_LR_PV_RPTG_RTO LGD_LR_PV_AD_RPTG_RTO LGD_LD_PV_AD_SV_RPTG_RTO 
         LGD_ACCT_SCORE LGD_LD_PV_AD_SV_DT_RPTG_RTO EAD_MODEL_NM EAD_MODEL_VER LGD_FINAL_RPTG_RTO EAD_BASEL_SCORECRD_NM EAD_SCORECRD_VER EAD_BASEL_SEG_NUM 
         EAD_BASEL_SEG_ID EAD_SEG_VER EAD_BASEL_MODEL_REL_ID EAD_MODEL_RTO EAD_LR_RPTG_RTO EAD_LR_PV_RPTG_RTO EAD_ACCT_SCORE EAD_LR_PV_AD_RPTG_RTO ASST_CL_DESC 
         CONS_DFT_MTH_CNT EAD_LD_PV_AD_SV_RPTG_RTO EAD_LD_PV_AD_SV_DT_RPTG_RTO BCAR_SCHED_NUM EAD_FINAL_RPTG_RTO PD_BAND BASEL_PRD_ABR SCRTY_TP_DESC 
         CCAR_BASEL_PRD_TP_NM CCAR_EXPSR_CL_NM BASEL_PRD_TP_CD CONSM_PRD_TREATMNT_CD PRD_ID PRIM_CUST_CID ACCT_NUM RGNL_OFFC_CD DLQNT_STG MORT_NUM CAB_TRNST_NUM 
         EGL_DEPRTMNT BASEL_CIF_KEY LOAN_NUM INTR_ACCR_AMT CUST_BEHV_SCORE PRD_CD SUB_PRD_CD SRC_PRD_DESC LEGAL_ENTITY MAT_DT LAST_PYMT_DT SCRTY_TP_CD 
         STEP_F LAST_ACTY_DT HOUSE_TP_NM LAST_RGL_PAY_DT PD_OFF_F PD_OFF_DT RESIDUAL_MAT ASST_CL_NUM E_MAT_DT AMORT TRNST_EXCLSN_F SML_BUS_F NOTE_DT 
         INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP AF_ZERO_NET_DRAWN_AMT AF_ZERO_NET_UNDRAWN_AMT PD_UNADJUSTED_RPTG_RTO LGD_UNADJUSTED_RPTG_RTO EAD_UNADJUSTED_RPTG_RTO 
         PD_LR_PV_AD_SV_DT_AA_RTO LGD_LR_PV_AD_SV_DT_AA_RTO EAD_LR_PV_AD_SV_DT_AA_RTO
         DLGD_F METRPL_AREA_NM PRPTY_VAL_CORR_PCTG CRNT_LTV_RTO LNG_RUN_LGD_ADD_ON_RTO DLGD_RPTG_RTO PREV_12_QTR_PRPTY_VAL_AMT
		CURRENCY_MISMATCH_F  TOT_EXPSR_ABOVE_1500K_LMT_F LTV_BUCKET RNTL_PRPTY_F Transactor_F ORIG_AMT_LOAN 
		PD_90_day_F UNINSURED_LGD_SEG_NUM UNINSURED_DLGD_RTO UNINSURED_LGD_RTO;
         
         
         data s_numbers(keep=prd_id);
         	do i = 1 to 15;
         		if i lt 10 then z='0';else z='';
         		prd_id="S"||strip(z)||trim(left(put(i,2.)));
         		output;
         	end;
         run;
         data _null_;
         	set s_numbers;
         	call symputx("prd_id_"||trim(left(_n_)),prd_id);
         	call symputx('nobs',_n_);
         run;
         
         
         %macro load_db2();
         %do i = 1 %to &nobs;
         
         	proc append base=DB2RRAP.BASEL_ANALYTCL_BL_INSTRMNT_FACT( BULKLOAD=YES BL_METHOD=CLILOAD)
         		data=NZRRAP.BASEL_PSNL_LN_ANL_BL_INST_FACT
         		( WHERE=(PRD_ID = "&&prd_id_&i" AND MTH_TM_ID=&MTH_TM_ID) /*keep=&varkeep*/) 
         		force nowarn;
         	run;
         
         %end;
         
         proc append base=DB2RRAP.BASEL_ANALYTCL_BL_INSTRMNT_FACT( BULKLOAD=YES BL_METHOD=CLILOAD)
         	data=NZRRAP.BASEL_PSNL_LN_ANL_BL_INST_FACT
         	( WHERE=((missing(PRD_ID) OR PRD_ID NOT IN ('S01','S02','S03','S04','S05','S06','S07','S08','S09','S10','S11','S12','S13','S14','S15'))  
         			AND MTH_TM_ID=&MTH_TM_ID) /*keep=&varkeep*/) 
         	force nowarn;
         run;
         %mend load_db2;
         %load_db2;
