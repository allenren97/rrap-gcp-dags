***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0410_DT4_RT18_EST_ER_VARS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RT18_EST_ER_VARS
*  
*  Purpose: Derive Expected Results variables at the account level 
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  		  
*
*	Change Log:
*	2021-09-21: Hadi Dimashkieh - Initial Development
*   2022-01-13: Hadi Dimashkieh - Add NETEAD_BEFORECRM_DRAWN
*	2023-02-14: Hadi Dimashkieh - RRMSS-1632 - Basel III additions 
*	2023-04-11: Hadi Dimashkieh - Update CORR3 to be dependent on CLP_FLAG and RNTL_PRPTY_F
*	2023-10-12: Hadi Dimashkieh - Convert job to monthly
*	2023-12-15: Kalind Patel - RRMSS-2164 - DT4 EAD $ for PMI accounts
***************************************************************************************************************************;


%rrap_dt4_autoexec(FREQ=MONTH);



proc sql;
connect using nzrrap as nzcon;
create table DT4_RT18_EST_CCAR_VARS as select * from connection to nzcon(
select a.basel_acct_id, a.src_sys_cd, a.mth_tm_id, a.DT4_EXPSR_CL_KEY_VAL, a.CCAR_BASEL_PRD_TP_NM, a.EAD_INCL
		,a.UNDRAWN, a.EXPSR_DRAWN_PRIOR_SECUR, a.ECL_POST_ADJ_DRAWN_POSTSEC_3, a.EXPOSURE_DRAWN, a.DLGD_RPTG_RTO 
		,trim(a.BASEL_GRP) as BASEL_GRP ,a.pd_band ,a.pd_value ,a.RW_INSURER
		,trim(i.RNTL_PRPTY_F) as RNTL_PRPTY_F, trim(i.CLP_FLAG) as CLP_FLAG, i.PD_FLRD_RPTG_RTO, i.LGD_FLRD_RPTG_RTO, i.EAD_FLRD_RPTG_RTO
		,i.CMHC_F, i.TRANSACTOR_FLAG_QRR, i.ORIG_AMT_LOAN, i.TOT_EXPSR_ABOVE_1500K_LMT_F
		,d.CORRELATION_PMI, d.MATURITY_ADJUSTMENT_PMI, d.PD as PD_INSURER, d.LGD as LGD_INSURER
		,i.BCAR_SCHED_NUM, i.BCAR_SCHED_NM, i.BCAR_SCHED_NUM_50
from &RRAP_DB..DT4_RT18_EST_CCAR_VARS a
	left join &RRAP_DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT i on a.basel_acct_id = i.basel_acct_id and a.mth_tm_id = i.mth_tm_id
	LEFT JOIN &RRAP_DB..DT4_RW_INSURER d ON substr(a.BASEL_PRODUCT_TYPE,1,4) = d.name and &yrmth. between cast(d.EFF_FROM_YR_MTH as integer) and cast(d.EFF_TO_YR_MTH as integer)
where a.mth_tm_id = &mth_tm_id.
);
quit;


%let output_variables=
	BASEL_ACCT_ID SRC_SYS_CD MTH_TM_ID 
	BCAR_SCHED_NUM BCAR_SCHED_NM BCAR_SCHED_NUM_50
	DT4_EXPSR_CL_KEY_VAL CCAR_BASEL_PRD_TP_NM EAD_INCL
	UNDRAWN	EXPSR_DRAWN_PRIOR_SECUR ECL_POST_ADJ_DRAWN_POSTSEC_3 EXPOSURE_DRAWN ORIG_AMT_LOAN EL_Drawnpct_for_defaulted
	Guarantee_Participation adj_for_CRM CMHC_F TRANSACTOR_FLAG_QRR TOT_EXPSR_ABOVE_1500K_LMT_F
	
	BASEL_GRP INSURER_GRP PD_BAND PD_VALUE RNTL_PRPTY_F CLP_FLAG 
	PD_FLRD_RPTG_RTO LGD_FLRD_RPTG_RTO EAD_FLRD_RPTG_RTO DLGD_RPTG_RTO 
	RW_INSURER CORRELATION_PMI MATURITY_ADJUSTMENT_PMI PD_INSURER LGD_INSURER
	M 
	PD1 LGD1 CORR1 b1 K1 
	PD2 LGD2 CORR2 b2 K2 
	PD3 LGD3 CORR3 b3 K3 
	PD4 LGD4 CORR4 b4 K4 
	PD5 LGD5 CORR5 b5 K5 
	PD6 LGD6 CORR6 b6 K6 
	PD7 LGD7 CORR7 b7 K7 
	PD8 LGD8 CORR8 b8 K8 K Retail_RW
	NETEAD_UNDRAWN NETEAD_DRAWN OLD_NETEAD_DRAWN NETEAD_BEFORECRM_DRAWN 
	EAD_DRAWN1
    EAD_DRAWN2
    EAD_DRAWN3
    EAD_DRAWN4
    EAD_DRAWN5
    EAD_DRAWN6
    EAD_DRAWN7
	EAD_DRAWN8
	RWA_UNDRAWN1 RWA_UNDRAWN2 RWA_UNDRAWN3 RWA_UNDRAWN4 RWA_UNDRAWN5 RWA_UNDRAWN6 RWA_UNDRAWN7 RWA_UNDRAWN8 RWA_UNDRAWN
	RWA_DRAWN1 RWA_DRAWN2 RWA_DRAWN3 RWA_DRAWN4 RWA_DRAWN5 RWA_DRAWN6 RWA_DRAWN7 RWA_DRAWN8 RWA_DRAWN SCENARIO
	INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP
;

data DT4_RT18_EST_ER_VARS;
	set DT4_RT18_EST_CCAR_VARS;
	where mth_tm_id = &mth_tm_id.;

*EL_Drawnpct_for_defaulted;
		 if ECL_POST_ADJ_DRAWN_POSTSEC_3 GT 0 and EXPOSURE_DRAWN GT 0 THEN 
			EL_Drawnpct_for_defaulted = ROUND(ECL_POST_ADJ_DRAWN_POSTSEC_3/EXPOSURE_DRAWN, 0.00000001);
	else EL_Drawnpct_for_defaulted = 0;

*INSURER_GRP;
		Length INSURER_GRP $ 4;
		if Basel_GRP in ('GE','GC') then INSURER_GRP = 'PMI';
			else if Basel_GRP = 'CMHC' /*CMHC_F = 'Y'*/ then INSURER_GRP = 'CMHC';
			else INSURER_GRP = 'N/A';
			
*Guarantee_Participation;
		 if INSURER_GRP = 'CMHC'	 			then Guarantee_Participation = 100;
	else if INSURER_GRP = 'PMI'				    then Guarantee_Participation = 90;
	else if INSURER_GRP = 'N/A'					then Guarantee_Participation = 0;

*adj_for_CRM;
		 if Guarantee_Participation = 0 then adj_for_CRM = 0;
	else adj_for_CRM = (EXPOSURE_DRAWN*Guarantee_Participation)/100;

*NETEAD_DRAWN;
	NETEAD_DRAWN = coalesce(EXPOSURE_DRAWN,0) - coalesce(Adj_for_CRM,0);

	*OLD_NETEAD_DRAWN;
	OLD_NETEAD_DRAWN = coalesce(EXPOSURE_DRAWN,0) - coalesce(Adj_for_CRM,0);

*NETEAD_BEFORECRM_DRAWN;
	NETEAD_BEFORECRM_DRAWN = coalesce(EXPOSURE_DRAWN,0) ;  

*NETEAD_UNDRAWN;
		 if UNDRAWN = 0 then NETEAD_UNDRAWN = 0;
	else if (EAD_FLRD_RPTG_RTO*max(UNDRAWN+EXPSR_DRAWN_PRIOR_SECUR, EXPSR_DRAWN_PRIOR_SECUR) - EXPSR_DRAWN_PRIOR_SECUR) LT 0 THEN NETEAD_UNDRAWN = 0;
	else 
		NETEAD_UNDRAWN = (EAD_FLRD_RPTG_RTO*max(UNDRAWN+EXPSR_DRAWN_PRIOR_SECUR, EXPSR_DRAWN_PRIOR_SECUR) - EXPSR_DRAWN_PRIOR_SECUR);



*MATURITY_ADJUSTMENT_FACTOR - Part 1;
		M = 2.5;
		b = coalesce(MATURITY_ADJUSTMENT_PMI,0);

*CORRELATION;

	if INSURER_GRP = 'N/A' then do;
	
		*if TOT_EXPSR_ABOVE_1500K_LMT_F = Y and (BASEL_GRP NE 'HELOCs' or BASEL_GRP NE 'Real Estate Secured') then do;
		if TOT_EXPSR_ABOVE_1500K_LMT_F = 'Y' and (BASEL_GRP EQ 'Other Retail' or BASEL_GRP EQ 'Revolving Credit') then do;
			S=7.5;
			CORR8 = ROUND((0.24 - 0.12*((1-EXP(-50*PD_Value))/(1-EXP(-50))) - 0.04*(1-((S-7.5)/67.5))),0.000000000000001);
			b = (0.11852-0.05478*log(PD_Value))**2;
		end;

		else if BASEL_GRP = 'Revolving Credit' 			  then CORR8 = 0.04;

		else if BASEL_GRP = 'Real Estate Secured' then do;
				if RNTL_PRPTY_F = 'N' then do;
						 if CLP_FLAG = 'Y' then CORR8 = 0.15;
					else if CLP_FLAG = 'N' then CORR8 = 0.22;
					else if CLP_FLAG = ''  then CORR8 = 0.15; 
				end;
			else if RNTL_PRPTY_F = 'Y' then do;
						 if CLP_FLAG = 'Y' then CORR8 = 0.22;
					else if CLP_FLAG = 'N' then CORR8 = 0.22;
					else if CLP_FLAG = ''  then CORR8 = 0.22;	
				end;
			else if RNTL_PRPTY_F = '' then do;
						 if CLP_FLAG = 'Y' then CORR8 = 0.22;
					else if CLP_FLAG = 'N' then CORR8 = 0.22;
					else if CLP_FLAG = ''  then CORR8 = 0.22;	
				end;
		end;

		else if BASEL_GRP = 'HELOCs' then do;
				if RNTL_PRPTY_F = 'N' then do;
						 if CLP_FLAG = 'Y' then CORR8 = 0.15;
					else if CLP_FLAG = 'N' then CORR8 = 0.22;
					else if CLP_FLAG = ''  then CORR8 = 0.15; 
				end;
			else if RNTL_PRPTY_F = 'Y' then do;
						 if CLP_FLAG = 'Y' then CORR8 = 0.22;
					else if CLP_FLAG = 'N' then CORR8 = 0.22;
					else if CLP_FLAG = ''  then CORR8 = 0.22;	
				end;
			else if RNTL_PRPTY_F = '' then do;
						 if CLP_FLAG = 'Y' then CORR8 = 0.22;
					else if CLP_FLAG = 'N' then CORR8 = 0.22;
					else if CLP_FLAG = ''  then CORR8 = 0.22;	
				end;
		end;

		else	
			CORR8 =  ROUND((0.16-0.13*((1-EXP(-35*Pd_Value ))/(1-EXP(-35)))),0.000000000000001);
	end;
	
*MATURITY_ADJUSTMENT_FACTOR - Part 2;
		**MATURITY_ADJUSTMENT_FACTOR = ((1+(M-2.5)*b)/(1-1.5*b));  

*Capital Requirement - K;

		if INSURER_GRP = 'PMI' then do;
			* PMI Accounts.;
			
			if RNTL_PRPTY_F = 'N' and CLP_FLAG = 'Y' then CORR2_3 = 0.15;
	   else if RNTL_PRPTY_F = 'N' and CLP_FLAG = ''  then CORR2_3 = 0.15;
				else CORR2_3 = 0.22; 
				
			if pd_band NE '26' then do;
				* Scenario 1; PD1 = PD_INSURER; 		LGD1 = 1;				CORR1= CORRELATION_PMI;	b1=b;	EAD_DRAWN1 = min(coalesce(ORIG_AMT_LOAN,0)*((100 - Guarantee_Participation)/100),NETEAD_BEFORECRM_DRAWN);
				* Scenario 2; PD2 = PD_FLRD_RPTG_RTO; 	LGD2 = 1;				CORR2= CORR2_3;			b2=0;	EAD_DRAWN2 = min(coalesce(ORIG_AMT_LOAN,0)*((100 - Guarantee_Participation)/100),NETEAD_BEFORECRM_DRAWN);
				* Scenario 3; PD3 = PD_FLRD_RPTG_RTO; 	LGD3 = DLGD_RPTG_RTO;	CORR3= CORR2_3;			b3=0;	EAD_DRAWN3 = NETEAD_BEFORECRM_DRAWN; 
				* Scenario 4; PD4 = PD_FLRD_RPTG_RTO; 	LGD4 = 0.45;			CORR4= CORRELATION_PMI;	b4=b;	EAD_DRAWN4 = NETEAD_BEFORECRM_DRAWN;
				* Scenario 5; PD5 = PD_INSURER; 		LGD5 = DLGD_RPTG_RTO;	CORR5= CORRELATION_PMI;	b5=b;	EAD_DRAWN5 = NETEAD_BEFORECRM_DRAWN;
				* Scenario 6; PD6 = PD_INSURER;			LGD6 = LGD_INSURER;		CORR6= CORRELATION_PMI;	b6=b;	EAD_DRAWN6 = .;
			end;
	   else if pd_band EQ '26' then do;			
				* Scenario 1; PD1 = PD_INSURER; 		LGD1 = 1;				CORR1= CORRELATION_PMI;	b1=b;	EAD_DRAWN1 = min(coalesce(ORIG_AMT_LOAN,0)*((100 - Guarantee_Participation)/100),NETEAD_BEFORECRM_DRAWN);
				* Scenario 2; PD2 = .; 					LGD2 = .;				CORR2= .;				b2=.;	EAD_DRAWN2 = min(coalesce(ORIG_AMT_LOAN,0)*((100 - Guarantee_Participation)/100),NETEAD_BEFORECRM_DRAWN);
				* Scenario 3; PD3 = .; 					LGD3 = DLGD_RPTG_RTO;	CORR3= .;				b3=.;	EAD_DRAWN3 = NETEAD_BEFORECRM_DRAWN;
				* Scenario 4; PD4 = .;				 	LGD4 = 0.45;			CORR4= .;				b4=.;	EAD_DRAWN4 = NETEAD_BEFORECRM_DRAWN;
				* Scenario 5; PD5 = PD_INSURER; 		LGD5 = DLGD_RPTG_RTO;	CORR5= CORRELATION_PMI;	b5=b;	EAD_DRAWN5 = NETEAD_BEFORECRM_DRAWN;
				* Scenario 6; PD6 = PD_INSURER;			LGD6 = LGD_INSURER;		CORR6= CORRELATION_PMI;	b6=b;	EAD_DRAWN6 = .;
			end;
		end;
  else if INSURER_GRP = 'CMHC' then do;
				* CMHC Accounts.;
				* Scenario 7; PD7 = PD_INSURER; 		LGD7 = LGD_INSURER;		CORR7 = CORRELATION_PMI; b7=b;	EAD_DRAWN7 = NETEAD_DRAWN;
		end;	
  else if INSURER_GRP = 'N/A' then do;
				* Non PMI Non CMHC Accounts.;
				* Scenario 8; PD8 = PD_Value; 			LGD8 = DLGD_RPTG_RTO;	CORR8 = CORR8;			 b8=b;	EAD_DRAWN8 = NETEAD_DRAWN;
		end;


* K Calculation.;
		array b_array    {8} b1   -   b8;
		array K_array    {8} K1   -   K8; 
		array PD_array   {8} PD1  -  PD8; 
		array LGD_array  {8} LGD1 - LGD8;
		array CORR_array {8} CORR1 - CORR8;



			do i = 1 to 8;
					MATURITY_ADJUSTMENT_FACTOR = ((1+(M-2.5)*b_array{i})/(1-1.5*b_array{i}));
			 	K_array{i} = COALESCE((PROBNORM((PROBIT(PD_array{i})+PROBIT(0.999)*(CORR_array{i}**0.5))/(1- CORR_array{i})**0.5)-PD_array{i})*LGD_array{i}*MATURITY_ADJUSTMENT_FACTOR ,0);
			end;

		length DEF_IND $ 8 SCENARIO $ 40;

		if INSURER_GRP = 'PMI' then do;
			K7 = .; K8 = .; 

				if pd_band NE '26' then do;
					K4 = max(K4,K6); K5 = max(K5,K6);
				end;
		   		if pd_band EQ '26' then do;
					K3 = MAX(0,LGD3 - EL_Drawnpct_for_defaulted);  
					K4 = MAX(0,LGD4 - EL_Drawnpct_for_defaulted); 
					K5 = max(K5,K6);
					DEF_IND = 'Default';
				end;
		end;

		if INSURER_GRP = 'CMHC' then do;
			do i = 1,2,3,4,5,6,8;
				K_array{i} = .;
			end;

	   			 *if pd_band NE '26' then K = K7;
				 if pd_band EQ '26' then do;
					K7 = MAX(0,DLGD_RPTG_RTO - EL_Drawnpct_for_defaulted); 
					DEF_IND = 'Default';
				 end;
		end;

		if INSURER_GRP = 'N/A' then do;
			do i = 1 to 7;
				K_array{i} = .;
			end;

	   			 *if pd_band NE '26' then K = K8;
			     if pd_band EQ '26' then do;
					K8 = MAX(0,DLGD_RPTG_RTO - EL_Drawnpct_for_defaulted); 
					DEF_IND = 'Default';
				 end;
		end;



* RWA Calculation;

		array EAD_array  {8} EAD_DRAWN1   -   EAD_DRAWN8;
		array RWA_array  {8} RWA_DRAWN1   -   RWA_DRAWN8;
		array RWAU_array {8} RWA_UNDRAWN1 - RWA_UNDRAWN8;
		
		
		do i = 1 to 8;
			RWA_array{i}  = 12.5 * K_array{i} * EAD_array{i};
			RWAU_array{i} = 12.5 * K_array{i} * NETEAD_UNDRAWN;
		end;
		
		if INSURER_GRP = 'PMI' then do;
			do i = 6 to 8;
				RWA_array{i} = .; RWAU_array{i} = .;
			end;

			if pd_band NE '26' then do;
				RWA_DRAWN   = min(RWA_DRAWN1  , RWA_DRAWN2  , RWA_DRAWN3  , RWA_DRAWN5);
				RWA_UNDRAWN = min(RWA_UNDRAWN1, RWA_UNDRAWN2, RWA_UNDRAWN3, RWA_UNDRAWN5);
			end;
				
	   else if pd_band EQ '26' then do; 
				RWA_DRAWN   = min(RWA_DRAWN1  , RWA_DRAWN3  , RWA_DRAWN5);
				RWA_UNDRAWN = min(RWA_UNDRAWN1, RWA_UNDRAWN3, RWA_UNDRAWN5);
			end;
		end;

		if INSURER_GRP = 'CMHC' then do;
			do i = 1,2,3,4,5,6,8;
				RWA_array{i} = .; RWAU_array{i} = .;
			end;
			RWA_DRAWN = RWA_DRAWN7; RWA_UNDRAWN = RWA_UNDRAWN7;
		end;


		if INSURER_GRP = 'N/A' then do;
			do i = 1 to 7;
				RWA_array{i} = .; RWAU_array{i} = .;
			end;
			RWA_DRAWN = RWA_DRAWN8; RWA_UNDRAWN = RWA_UNDRAWN8;
		end;




		if INSURER_GRP = 'PMI' then do;
			if pd_band NE '26' then do;
				do i = 1,2,3,5;
					if RWA_DRAWN = RWA_array{i} then do;
						SCENARIO = strip(DEF_IND||'Scenario'||put(i,best1.));
						K = K_array{i};
					end;
				end;
			end;
				
	   else if pd_band EQ '26' then do; 
				do i = 1,3,5;
					if RWA_DRAWN = RWA_array{i} then do;
						SCENARIO = strip(DEF_IND||'Scenario'||put(i,best1.));
						K = K_array{i};
					end;
				end;
			end;
		end;
		
		
  else if INSURER_GRP = 'CMHC' then do;
  			K = K7;
			SCENARIO = strip(DEF_IND||'CMHC(7)');
	   end;
  else if INSURER_GRP = 'N/A'  then do;
  			K = K8;
			if TOT_EXPSR_ABOVE_1500K_LMT_F = 'Y' and (BASEL_GRP EQ 'Other Retail' or BASEL_GRP EQ 'Revolving Credit') then do;
				SCENARIO = strip(DEF_IND||'Non-Regulatory Retail(8)');
			end;
			else SCENARIO = strip(DEF_IND||'Retail(8)');
		end;

		Retail_RW = 12.5 * K;


	format RWA_DRAWN RWA_UNDRAWN dollar12.2 INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.6;
	INSRT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
	UPDT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
	
	keep &output_variables.;
run;

/*data DT4_RT18_EST_ER_VARS;
	retain &output_variables.;
	set DT4_RT18_EST_ER_VARS;
run;*/
data DT4_RT18_EST_ER_VARS;
	set DT4_RT18_EST_ER_VARS;
	*NETEAD_DRAWN;
		if INSURER_GRP = 'PMI' then do;
		* PMI Accounts.;
	* Scenario 1;	if scenario="Scenario1" then NETEAD_DRAWN=EAD_DRAWN1;
	* Scenario 2;	if scenario="Scenario2"  then NETEAD_DRAWN = EAD_DRAWN2;
	* Scenario 3;	if scenario="Scenario3"  then	NETEAD_DRAWN = EAD_DRAWN3;
	* Scenario 4;	if scenario="Scenario4" then NETEAD_DRAWN = EAD_DRAWN4;
	* Scenario 5;	if scenario="Scenario5"  then NETEAD_DRAWN = EAD_DRAWN5;
	* Scenario 6;	if scenario="Scenario6"  then NETEAD_DRAWN = EAD_DRAWN6;
	* Default Scenario 1;	if scenario="Default Scenario1" then NETEAD_DRAWN=EAD_DRAWN1;
	* Default Scenario 2;	if scenario="Default Scenario2"  then NETEAD_DRAWN = EAD_DRAWN2;
	* Default Scenario 3;	if scenario="Default Scenario3"  then	NETEAD_DRAWN = EAD_DRAWN3;
	* Default Scenario 4;	if scenario="Default Scenario4" then NETEAD_DRAWN = EAD_DRAWN4;
	* Default Scenario 5;	if scenario="Default Scenario5"  then NETEAD_DRAWN = EAD_DRAWN5;
	* Default Scenario 6;	if scenario="Default Scenario6"  then NETEAD_DRAWN = EAD_DRAWN6;
     
end;
run;


proc sql;
	connect using NZRRAP as nzcon;
	execute(truncate table &RRAP_DB..BASEL_CC_SEC_ACCT_MTH_SNAP_TMP)by nzcon;
	execute(truncate table &RRAP_DB..BASEL_SEC_ADJ_FACTR_MTH_SNAP_TMP)by nzcon;
	execute(truncate table &RRAP_DB..DT4_RT18_EST_ER_VARS_TEMP)by nzcon;
	execute(delete from &RRAP_DB..DT4_RT18_EST_ER_VARS where mth_tm_id = &mth_tm_id.;) by nzcon;
	disconnect from nzcon;
quit;

/* Loading data to IIAS for a faster join*/
proc sql noprint;
create table BASEL_CC_SEC_ACCT_MTH_SNAP_TMP as (
select *
from DB2RRAP.BASEL_CC_SEC_ACCT_MTH_SNAP where mth_tm_id = &mth_tm_id.
);
quit;

proc sql noprint;
create table BASEL_SEC_ADJ_FACTR_MTH_SNAP_TMP as (
select *
from DB2RRAP.BASEL_SEC_ADJ_FACTR_MTH_SNAP where mth_tm_id = &mth_tm_id.
);
quit;

proc append base=NZRRAP.BASEL_CC_SEC_ACCT_MTH_SNAP_TMP(BULKLOAD=YES BL_METHOD=CLILOAD) data=BASEL_CC_SEC_ACCT_MTH_SNAP_TMP force nowarn; run;
proc append base=NZRRAP.BASEL_SEC_ADJ_FACTR_MTH_SNAP_TMP(BULKLOAD=YES BL_METHOD=CLILOAD) data=BASEL_SEC_ADJ_FACTR_MTH_SNAP_TMP force nowarn; run;
proc append base=NZRRAP.DT4_RT18_EST_ER_VARS_TEMP(BULKLOAD=YES BL_METHOD=CLILOAD) data=DT4_RT18_EST_ER_VARS force nowarn; run;


proc sql;
connect using NZRRAP as nzcon;
execute(
INSERT INTO &RRAP_DB..DT4_RT18_EST_ER_VARS

select a.*,
	b.ACCT_NUM,
	d.CUST_CID as PRIM_CUST_CID,
	c.BASEL_CUST_ID as PRIM_BASEL_CUST_ID,
	f.SECRTZTN_OS_ADJ_FACTR,
	e.SECRTZTN_TP_CD,
	e.SECURITIZATION_DATE,
	g.STEP_PLN_AGRMNT_NUM,

	SUBSTR(CCAR_BASEL_PRD_TP_NM, 1, LENGTH(CCAR_BASEL_PRD_TP_NM) - 9)
		AS PRODUCT_TYPE,
	COALESCE(EXPSR_DRAWN_PRIOR_SECUR ,0)- COALESCE(EXPOSURE_DRAWN,0)
		AS SECURITIZED_EXPOSURE,
	SUBSTR(CCAR_BASEL_PRD_TP_NM, LENGTH(CCAR_BASEL_PRD_TP_NM) - 7, 2)
		AS PD_SEG_RWA,
	SUBSTR(CCAR_BASEL_PRD_TP_NM, LENGTH(CCAR_BASEL_PRD_TP_NM) - 4, 2)
		AS EAD_SEG_RWA,
	SUBSTR(CCAR_BASEL_PRD_TP_NM, LENGTH(CCAR_BASEL_PRD_TP_NM), 2)
		AS LGD_SEG_RWA,
	COALESCE(NETEAD_BEFORECRM_DRAWN,0) + COALESCE(NETEAD_UNDRAWN,0)
		AS EAD_BEFORE_CRM,
	COALESCE(NETEAD_DRAWN,0) + COALESCE(NETEAD_UNDRAWN,0)
		AS EAD_AFTER_CRM,
	COALESCE(RWA_UNDRAWN,0) + COALESCE(RWA_DRAWN,0)
		AS RWA,
	REGEXP_REPLACE(SCENARIO, '[^0-9]', '')
		AS SCENARIO_NO,
	CASE WHEN TRIM(SCENARIO) LIKE '%Default%' THEN 1 ELSE 0 END
		AS DEFAULT_SCENARIO,
	CASE
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='1' THEN PD1
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='2' THEN PD2
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='3' THEN PD3
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='4' THEN PD4
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='5' THEN PD5
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='6' THEN PD6
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='7' THEN PD7
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='8' THEN PD8
	END
		AS SCENARIO_BASED_PD,
	CASE
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='1' THEN LGD1
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='2' THEN LGD2
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='3' THEN LGD3
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='4' THEN MAX(LGD4,LGD6)
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='5' THEN MAX(LGD5,LGD6)
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='6' THEN LGD6
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='7' THEN LGD7
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='8' THEN LGD8
	END
		AS SCENARIO_BASED_LGD,
	CASE
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='1' THEN (PD1 * LGD1)
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='2' THEN (PD2 * LGD2)
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='3' THEN (PD3 * LGD3)
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='4' THEN max((PD4* LGD4),(PD4 * LGD6))
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='5' THEN max((PD5 *LGD5),(PD5 * LGD6))
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='6' THEN (PD6 * LGD6)
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='7' THEN (PD7 * LGD7)
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='8' THEN (PD8 * LGD8)
	END
		AS SCENARIO_BASED_EL_PCT,
	CASE
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='1' THEN (PD1 * LGD1) * (COALESCE(NETEAD_DRAWN,0) + COALESCE(NETEAD_UNDRAWN,0))
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='2' THEN (PD2 * LGD2) * (COALESCE(NETEAD_DRAWN,0) + COALESCE(NETEAD_UNDRAWN,0))
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='3' THEN (PD3 * LGD3) * (COALESCE(NETEAD_DRAWN,0) + COALESCE(NETEAD_UNDRAWN,0))
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='4' THEN max((PD4 * LGD4),(PD4 * LGD6)) * (COALESCE(NETEAD_DRAWN,0) + COALESCE(NETEAD_UNDRAWN,0))
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='5' THEN max((PD5 * LGD5),(PD5 * LGD6)) * (COALESCE(NETEAD_DRAWN,0) + COALESCE(NETEAD_UNDRAWN,0))
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='6' THEN (PD6 * LGD6) * (COALESCE(NETEAD_DRAWN,0) + COALESCE(NETEAD_UNDRAWN,0))
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='7' THEN (PD7 * LGD7) * (COALESCE(NETEAD_DRAWN,0) + COALESCE(NETEAD_UNDRAWN,0))
		WHEN REGEXP_REPLACE(SCENARIO, '[^0-9]', '')='8' THEN (PD8 * LGD8) * (COALESCE(NETEAD_DRAWN,0) + COALESCE(NETEAD_UNDRAWN,0))
	END
		AS EL



from &RRAP_DB..DT4_RT18_EST_ER_VARS_TEMP a
	left join &RRAP_DB..BASEL_ACCT_DIM b
		on a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
	LEFT JOIN (select * from &RRAP_DB..BASEL_CUST_ACCT_RLTNP_SNAPSHOT WHERE PRIM_CUST_F = 'Y') c
		ON a.BASEL_ACCT_ID = c.BASEL_ACCT_ID AND a.MTH_TM_ID = c.MTH_TM_ID
	LEFT JOIN &RRAP_DB..BASEL_CUST_DIM d
		ON c.BASEL_CUST_ID = d.BASEL_CUST_ID
	left join &RRAP_DB..BASEL_CC_SEC_ACCT_MTH_SNAP_TMP e
		ON a.mth_tm_id = e.mth_tm_id and a.BASEL_ACCT_ID = e.BASEL_ACCT_ID_CCAR_MATCHED
	left join &RRAP_DB..BASEL_SEC_ADJ_FACTR_MTH_SNAP_TMP f
		on a.mth_tm_id = f.mth_tm_id and e.SECRTZTN_TP_CD = f.SECRTZTN_TP_CD
	left join &RRAP_DB..PIT_STATUS_PRE_STEP g
		on a.mth_tm_id = g.mth_tm_id and a.BASEL_ACCT_ID = g.BASEL_ACCT_ID
where a.mth_tm_id = &mth_tm_id.
)by nzcon;
quit;


proc sql noprint;
create table START_SECURITIZATION as (
select ACCT_NUM,
	PRIM_CUST_CID,
	PRIM_BASEL_CUST_ID,
	SECRTZTN_OS_ADJ_FACTR,
	SECRTZTN_TP_CD,
	SECURITIZATION_DATE,
	PRODUCT_TYPE,
	SECURITIZED_EXPOSURE,
	PD_SEG_RWA,
	EAD_SEG_RWA,
	LGD_SEG_RWA,
	EAD_BEFORE_CRM,
	EAD_AFTER_CRM,
	RWA,
	Retail_RW AS RISK_WEIGHT,
	SCENARIO_NO,
	SCENARIO,
	INSURER_GRP,
	BASEL_GRP,
	DEFAULT_SCENARIO,
	SCENARIO_BASED_PD,
	SCENARIO_BASED_LGD,
	SCENARIO_BASED_EL_PCT,
	EL
from NZRRAP.DT4_RT18_EST_ER_VARS where
	TRIM(UPPER(SECRTZTN_TP_CD)) = 'AUTO')
	order by SECURITIZATION_DATE, PRODUCT_TYPE, PRIM_BASEL_CUST_ID;
quit;

data _null_;
	set START_SECURITIZATION;
	call symput('nobs',_n_);
run;

proc sql threads noprint;
	select 
	sum('SECURITIZED_EXPOSURE'n) format 25.2 as hash_total into :hashtot
	from START_SECURITIZATION
	;
quit;

DATA _null_;
	length 
		YYYYMMDD 
		rundt $10
		nobs $6
		hashtot $15;
	file "&OUTPATH./cmf/outgoing/CCAR_ACAP/DR-START-SECURITIZATION-&YRMTH..ctl";

	dt = "&mth_end_dt."d;
	format dt yymmdd10.;
	YYYYMMDD = compress(put(dt,yymmdd10.),'-');
	rundt = compress(put(today(),yymmdd10.),'-');
	nobs = left(put(&nobs. ,6.));
	n2= &hashtot.;
	hashtot = left(put(n2 , 25.2));

	DLM=",";
	mtxtx=compress((YYYYMMDD||DLM||rundt||DLM||nobs||DLM||hashtot),' ');
	put mtxtx;
	output;
run;

PROC EXPORT DATA= START_SECURITIZATION
	OUTFILE="&OUTPATH./cmf/outgoing/CCAR_ACAP/DR-START-SECURITIZATION-&YRMTH..csv"
	DBMS=CSV
	REPLACE;
	PUTNAMES= YES;
RUN;

DATA _NULL_;
	CALL SYSTEM("openssl dgst -sha256 &OUTPATH./cmf/outgoing/CCAR_ACAP/DR-START-SECURITIZATION-&YRMTH..csv | sed -e 's/^.*= //g' >> &OUTPATH./cmf/outgoing/CCAR_ACAP/DR-START-SECURITIZATION-&YRMTH._chk.ctl");
run;