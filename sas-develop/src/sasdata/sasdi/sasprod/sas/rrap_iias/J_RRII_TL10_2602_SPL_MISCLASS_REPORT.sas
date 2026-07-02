OPTIONS VALIDVARNAME=ANY;
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

PROC SQL;
	CREATE TABLE GL_RECON_SPL AS
		SELECT DISTINCT 
			d.tm_lvl_end_dt AS month_end_date format=yymmdd10.,
			SUBSTR(a.ACCT_NUM,12,12) as acct_num,
			a.BASEL_ACCT_ID,
			f.CCAR_BASEL_PRD_TP_NM,
			f.BASEL_PRD_TP_CD,
			b.TOT_CRNT_BAL_AMT  AS Os_Bal_Coa_Final,
			b.GL_ACCT_NUM AS GL_Account_Number LABEL="GL_Account_Number",
			b.GL_TRNST_NUM AS Transit_Number LABEL="Transit_Number",
		CASE 
			WHEN e.pit_status_v2 = 'DEF' THEN 'Y'
			ELSE 'N'
		END 
	AS Defaulted_Exposure_Flag,
		CASE 
			WHEN b.day_odue >=90 THEN 'Y'
			ELSE 'N' 
		END 
	AS Day91_Past_Due_Flag,
		CASE 
			WHEN f.BASEL_PRD_TP_CD IN ('ITL AUTO RS', 'ITL AUTO REG') THEN 'Indirect Auto Loans'
			ELSE  'Other Personal Loan'
		END 
	AS Sub_Product_Types,
		b.CRNCY_CD AS Currency_of_Account LABEL="Currency_of_Account",
		f.INTR_ACCR_AMT AS Accrued_Interest_Balance, 
		f.os_bal_amt AS Os_Bal_Coa_Final,
		f.TOT_EXPSR_ABOVE_1500K_LMT_F AS Total_Exposure_Above_Limit_Flag LABEL="Total_Exposure_Above_Limit_Flag",
		'Other Retail' AS Basel_Product_Name,
		'RECON-Retail-AIRB' AS Source_System_Code,
		'Y' as Unconditionally_Cancelable_Flag,
	CASE 
		WHEN e.pit_status_v2 = 'DEF' THEN 'Y'
		ELSE 'N'
	END 
AS Defaulted_Exposure_Flag_01,
	CASE 
		WHEN e.PIT_STATUS_V2 = 'DEF' then 'STANDARD LOANS'
		ELSE ''
	END 
as Past_Due_Loan_Type,
	'' AS Country,
	'' AS Adjustment_Flag, 
	'' AS LTV_Bucket, 
	'' AS Credit_Conversion_Factor_Code, 
	'' AS EMDRIF LABEL="Exposure_Materially_Dependent_Rental_Income_Flag", 
	'' AS Currency_Mismatch_Flag, 
	'' AS Insured_Flag,
	'' AS Transactor_Non_Transactor_Flag, 
	'' AS OALID LABEL="Original_Amount_Loan_at_Insurance_Date", 
	'' AS Undrawn_Amount, 
	'' AS Drawn_Amount, 
	'' AS Sum_Amt, 
	'' AS Os_Bal_Coefficient, 
	'' AS Adj_Bal, 
	'' AS Adj_Os_Bal_Coa_Amt, 
	'' AS RNENC LABEL="Redistribution_Net_Exposure_NonCash_Collateral",
	'' AS RNACC LABEL="Redistribution_Net_Exposure_Cash_Collateral",
	'' AS ECL_Stage_1, 
	'' AS ECL_Stage_2, 
	'' AS Specific_Provision, 
	'' AS Clp_Flag
FROM NETCON.BASEL_PSNL_LOAN_MTH_SNAPSHOT b 
	inner join NETCON.BASEL_PSNL_LOAN_ACCT_DRVD_VARS c
		on b.basel_acct_id = c.basel_acct_id
		AND b.mth_tm_id = c.mth_tm_id
	inner join NETCON.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 e
		on c.basel_acct_id = e.basel_acct_id
		AND c.mth_tm_id = e.mth_tm_id
	inner join NETCON.TM_DIM d
		ON b.mth_tm_id = d.tm_id
	inner join NETCON.BASEL_ACCT_DIM a
		ON b.basel_acct_id = a.basel_acct_id
	inner join NETCON.BASEL_PSNL_LN_ANL_BL_INST_FACT f
		on b.basel_acct_id = f.basel_acct_id
		AND b.mth_tm_id = f.mth_tm_id
	WHERE d.tm_lvl = 'Month'
		AND c.TRNST_EXCLSN_F = 'N'
		AND e.PIT_STATUS_V2 IN ('CUR','DEF')
		AND b.COMM_LOAN_CD = '1'
		AND b.scrty_cd <> '99'
		AND d.TM_ID = &MTH_TM_ID.
		AND a.SRC_SYS_DEL_F = 'N'
	;
quit;

proc sql noprint;
	select tm_id as mth_tm_id, tm_lvl_st_dt, tm_lvl_end_dt , datetime() as dtime 
		format datetime25.
	into :mth_tm_id, :tm_lvl_st_dt, :tm_lvl_end_dt, :dtime
		from  NETCON.TM_DIM
	where tm_id=&mth_tm_id.;
quit;

%PUT &MTH_TM_ID;
%PUT &tm_lvl_st_dt;
%PUT &tm_lvl_end_dt;
%PUT &dtime;

data _NULL_;
	call symput('EXT_DATE_FILE_NAME', "&owftp/cmf/outgoing/DR-STD-EXPOSURES-SPL-" || put("&tm_lvl_end_dt"d, yymmddn8.));
run;

/*       CREATING THE "DATA FILE"    FILE IN csv Format      */
PROC EXPORT DATA= GL_RECON_SPL
	OUTFILE="&EXT_DATE_FILE_NAME..csv"
	DBMS=CSV
	LABEL 
	REPLACE;
	PUTNAMES= YES;

	/*   NO LABELS AT 1ST ROW  */
RUN;

/* RRMSS-3507 - Generating Files for JO delivery with CCAR_ACAP files */

data _NULL_;
	call symput('EXT_DATE_FILE_NAME', "&owftp/cmf/outgoing/CCAR_ACAP/DR-STD-EXPOSURES-SPL-" || put("&tm_lvl_end_dt"d, yymmddn8.));
run;

PROC EXPORT DATA= GL_RECON_SPL
	OUTFILE="&EXT_DATE_FILE_NAME..csv"
	DBMS=CSV
	LABEL 
	REPLACE;
	PUTNAMES= YES;

	/*   NO LABELS AT 1ST ROW  */
RUN;

/* RRMSS-3507 - Generating Files for JO delivery with CCAR_ACAP files - END */


proc sql;
	create table summary_01 as 
		select 
			input("&tm_lvl_end_dt", DATE9.) as Month_End_Date format=yymmdd10.,
			"" as Adjustment_Flag,
			Basel_Product_Name,
			Sub_Product_Types,
			"" as Country,
			Source_System_Code,
			Transit_Number,
			GL_Account_Number,
			Currency_of_Account,
			"" as LTV_Bucket,
			"" as Credit_Conversion_Factor_Code,
			"" as EMDRIFE LABEL="Exposure_Materially_Dependent_rental_Income_Flag", 
			"" as Currency_Mismatch_Flag,
			Defaulted_Exposure_Flag,
			Total_Exposure_Above_Limit_Flag,
			Unconditionally_Cancelable_Flag,
			Day91_Past_Due_Flag,
			Insured_Flag,
			Past_Due_Loan_Type,
			Transactor_Non_Transactor_Flag, 
			sum(Accrued_Interest_Balance) as Accrued_Interest_Balance,
			OALID,
			Undrawn_Amount,
			'' as Drawn_Amount, 
			Sum_Amt, 
			Os_Bal_Coefficient, 
			Adj_Bal, 
			Adj_Os_Bal_Coa_Amt,
			sum(Os_Bal_Coa_Final) as Os_Bal_Coa_Final,
			'' AS RNENC LABEL="Redistribution_Net_Exposure_NonCash_Collateral",
			'' AS RNACC LABEL="Redistribution_Net_Exposure_Cash_Collateral",
			"" as ECL_Stage_1,
			"" as ECL_Stage_2,
			"" as Specific_Provision,
			0 as Os_Bal_Coa_Adj_S1,
			0 as Os_Bal_Coa_Adj_S2,
			0 as Os_Bal_Coa_Adj_S3
		from GL_RECON_SPL
			group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,22,23,25,26,27,28,30,31,32,33,34,35,36,37
				order by 8,7
	;
quit;

proc sql noprint;
	select clndr_yr, clndr_qtr_cd
		into :yr_tm_id, :qtr_tm_id
			from  NETCON.TM_DIM
				where tm_id=&mth_tm_id.;
quit;

data _NULL_;
	call symput('SPL_ECL', "&owftp/Indirect_SPL_ECL_" || "&yr_tm_id" || "_Q" || "&qtr_tm_id");
run;

proc import datafile="&SPL_ECL..csv"
	out=Indirect
	REPLACE;
run;

proc sql;
	create table totals_02 as 
		select 
			a.Defaulted_Exposure_Flag,
			sum(a.Os_Bal_Coa_Final) as Os_Bal_Coa_Final,
			b.Defaulted_Exposure_Flag,
			b.STG_01,
			b.STG_02,
			b.STG_03,
			b.stg_01/sum(a.Os_Bal_Coa_Final) as prov_stg_01,
			b.stg_02/sum(a.Os_Bal_Coa_Final) as prov_stg_02,
			b.stg_03/sum(a.Os_Bal_Coa_Final) as prov_stg_03
		from summary_01 a left join (
			select 
				"N" as Defaulted_Exposure_Flag,
				'STG 1'n as STG_01,
				'STG 2'n as STG_02,
				'STG 3'n as STG_03
			from indirect union
				select
					"Y" as Defaulted_Exposure_Flag,
					'STG 1'n as STG_01,
					'STG 2'n as STG_02,
					'STG 3'n as STG_03
				from indirect) as b
					on a.Defaulted_Exposure_Flag = b.Defaulted_Exposure_Flag
				group by 1,3,4,5,6;
run;

proc sql;
	create table Summary_02 as
		select summary_01.*, totals_02.*  from summary_01 
			inner join totals_02 
				on summary_01.Defaulted_Exposure_Flag = 
				totals_02.Defaulted_Exposure_Flag
	;
run;

data summary_03 (drop=prov_stg_01 prov_stg_02 prov_stg_03
	STG_01 STG_02 STG_03);
	length Unique_Identifier $10.;
	set summary_02;

	if _n_ < 10 then
		do;
			Unique_Identifier = 'DR_SP0000'||left(trim(_n_));
		end;

	if _n_ >= 10 then
		do;
			Unique_Identifier = 'DR_SP000'||left(trim(_n_));
		end;

	if Defaulted_Exposure_Flag="N" then
		do;
			Os_Bal_Coa_Adj_S1 = Os_Bal_Coa_Final * prov_stg_01*-1;
			Os_Bal_Coa_Adj_S2 = Os_Bal_Coa_Final * prov_stg_02*-1;
			Os_Bal_Coa_Adj_S3=0;
		end;

	if Defaulted_Exposure_Flag="Y" then
		do;
			Os_Bal_Coa_Adj_S3 = Os_Bal_Coa_Final * prov_stg_03*-1;
		end;
run;

data _NULL_;
	call symput('EXT_DATE_FILE_NAME', "&owftp/cmf/outgoing/DR-STD-EXPOSURES-SPL-SUMMARY-" || put("&tm_lvl_end_dt"d, yymmddn8.));
run;

/*       CREATING THE "DATA FILE"    FILE IN csv Format      */
PROC EXPORT DATA= summary_03
	OUTFILE="&EXT_DATE_FILE_NAME..csv"
	DBMS=CSV
	LABEL 
	REPLACE;
	PUTNAMES= YES;

	/*   NO LABELS AT 1ST ROW  */
RUN;

/* RRMSS-3507 - Generating Files for JO delivery with CCAR_ACAP files */

data _NULL_;
	call symput('EXT_DATE_FILE_NAME', "&owftp/cmf/outgoing/CCAR_ACAP/DR-STD-EXPOSURES-SPL-SUMMARY-" || put("&tm_lvl_end_dt"d, yymmddn8.));
run;

/*       CREATING THE "DATA FILE"    FILE IN csv Format      */
PROC EXPORT DATA= summary_03
	OUTFILE="&EXT_DATE_FILE_NAME..csv"
	DBMS=CSV
	LABEL 
	REPLACE;
	PUTNAMES= YES;

	/*   NO LABELS AT 1ST ROW  */
RUN;

/* RRMSS-3507 - Generating Files for JO delivery with CCAR_ACAP files  - END */

data totals_work_01 (drop= Defaulted_Exposure_Flag	Os_Bal_Coa_Final
	);
	set totals_02;

	if Defaulted_Exposure_Flag ='N';
run;

proc transpose data = totals_02 out= totals_work_02;
run;

data totals_work_03 (drop= COL2);
	rename COL1=Os_Bal_Coa_Final _name_=Defaulted_Exposure_Flag;
	set totals_work_02;

	if _n_ in (2,3,4);

	if _n_ = 2 then
		do;
			_name_ = 'ECL Stg 1';
		end;

	if _n_ = 3 then
		do;
			_name_ = 'ECL Stg 2';
		end;

	if _n_ = 4 then
		do;
			_name_ = 'ECL Stg 3';
		end;
run;

data totals_work_04(drop= COL1);
	rename COL2=provisioning_level _name_=Defaulted_Exposure_Flag;
	set totals_work_02;

	if _n_ in (5,6,7);

	if _n_ = 5 then
		do;
			_name_ = 'ECL Stg 1';
			COL2=COL1;
		end;

	if _n_ = 6 then
		do;
			_name_ = 'ECL Stg 2';
			COL2=COL1;
		end;

	if _n_ = 7 then
		do;
			_name_ = 'ECL Stg 3';

			/*COL2=COL1*/;
		end;
run;

proc sql;
	create table totals_work_05 as 
		select 
			totals_work_03.Defaulted_Exposure_Flag,
			Os_Bal_Coa_Final,
			provisioning_level
		from totals_work_03 inner join totals_work_04
			on totals_work_03.Defaulted_Exposure_Flag = totals_work_04.Defaulted_Exposure_Flag
	;
quit;

data totals_work_06 (keep=Defaulted_Exposure_Flag Os_Bal_Coa_Final);
	rename Defaulted_Exposure=Defaulted_Exposure_Flag;
	length Defaulted_Exposure $10.;
	set totals_02;
	Defaulted_Exposure=Defaulted_Exposure_Flag;
run;

data totals_work;
	set totals_work_06
		totals_work_05;
run;

data _NULL_;
	call symput('EXT_DATE_FILE_NAME', "&owftp/cmf/outgoing/DR-STD-EXPOSURES-SPL-TOTALS-" || put("&tm_lvl_end_dt"d, yymmddn8.));
run;

/*       CREATING THE "DATA FILE"    FILE IN csv Format      */
PROC EXPORT DATA= totals_work
	OUTFILE="&EXT_DATE_FILE_NAME..csv"
	DBMS=CSV
	LABEL 
	REPLACE;
	PUTNAMES= YES;
RUN;

/* RRMSS-3507 - Generating Files for JO delivery with CCAR_ACAP files  - END */

data _NULL_;
	call symput('EXT_DATE_FILE_NAME', "&owftp/cmf/outgoing/CCAR_ACAP/DR-STD-EXPOSURES-SPL-TOTALS-" || put("&tm_lvl_end_dt"d, yymmddn8.));
run;

/*       CREATING THE "DATA FILE"    FILE IN csv Format      */
PROC EXPORT DATA= totals_work
	OUTFILE="&EXT_DATE_FILE_NAME..csv"
	DBMS=CSV
	LABEL 
	REPLACE;
	PUTNAMES= YES;
RUN;

/* RRMSS-3507 - Generating Files for JO delivery with CCAR_ACAP files  - END */