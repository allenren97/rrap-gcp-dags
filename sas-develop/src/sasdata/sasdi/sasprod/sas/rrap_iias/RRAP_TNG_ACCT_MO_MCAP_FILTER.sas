/**************************************************************************** 
 * Job:             RRAP_TNG_ACCT_MO_MCAP_FILTER                            * 
 * Description:     THIS JOB DELETES THE NEW MCAP RECORDS                   *
FLOWING INTO  TNGSTP1D.TNG_ACCT_MO TABLE                * 
*                                                                          * 
 * Metadata Server: 10.56.70.48                                             * 
 * Port:            8561                                                    * 
 * Location:        /sasdata/sasdi/sasprod/sas/rrap_iias                    * 
 *                                                                          * 
 * Server:          SASApp                                                  * 
 *                                                                          * 
 * TARGET TABLE: TNGSTP1D.TNG_ACCT_MO                                       *
 * BACKUP TABLE:  TNGSTP1D.MCAP_ACCOUNTS_BACKUP                             * 
 * REVISION HISTORY:  
 * 1) RRMSS-994, 15-Mar-2022, Added code for preparing report of deleted MCAP acounts and send to RMA* 
 ****************************************************************************/
%rrap_mor_tng_autoexec;
%get_model_period_dates(product=mor);
%put &start_period_dt;

data A;
	birthd="&start_period_dt"d;
	DAY=PUT(birthd,DOWNAME.);
	NZDATEFORMAT="'"||PUT(birthd,yymmdd10.)||"'";
run;

DATA _NULL_;
	SET A;
	CALL SYMPUTX("CURR_MTH",NZDATEFORMAT);
RUN;

DATA _null_;
	CALL SYMPUT('MonthYear', CATX(' ',PUT(INTNX('month',"&start_period_dt"d,0,'E'),monname10.),PUT(YEAR(INTNX('month',"&start_period_dt"d,0,'E')),4.)));
RUN;

%PUT &=CURR_MTH;
%LET TARGET_TABLE=TNG_ACCT_MO;
%LET BKUP_TARGET=MCAP_ACCOUNTS_BACKUP;

PROC SQL;
	CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD AUTHDOMAIN="IIAS_Auth");
	CREATE TABLE WORK.TNG_MCAP_EXCLUDE AS
		SELECT * FROM CONNECTION TO IIASCON
		(SELECT * FROM &TNG_DB..&TARGET_TABLE tam 
			WHERE MONTH_END_DT = &CURR_MTH AND MTG_PROVIDER_DESC ='MCAP'
				AND  NOT exists (SELECT 1 FROM &TNG_DB..&TARGET_TABLE tam2 
			WHERE tam2.MONTH_END_DT='2021-05-31' 
				AND tam2.MTG_PROVIDER_DESC='MCAP' 
				AND tam2.ACCOUNT_NUM=tam.ACCOUNT_NUM));
	DISCONNECT FROM IIASCON;
QUIT;

%MACRO BACKUP_DEL_RECS;
	%IF %SYSFUNC(EXIST(TNGDATA.MCAP_ACCOUNTS_BACKUP)) %THEN
		%DO;
			%PUT "BACKUP TABLE ALREADY EXISTS - APPENDING DATA TO THE TABLE";

			PROC APPEND BASE=TNGDATA.MCAP_ACCOUNTS_BACKUP(BULKLOAD=YES BL_METHOD=CLILOAD)
				DATA=WORK.TNG_MCAP_EXCLUDE;
			RUN;

		%END;
	%ELSE
		%DO;
			%PUT "BACKUP TABLE DOES NOT EXISTS - CREATING BACKUP TABLE";

			PROC SQL;
				CREATE TABLE TNGDATA.MCAP_ACCOUNTS_BACKUP AS
					SELECT * FROM WORK.TNG_MCAP_EXCLUDE;
			QUIT;

		%END;
%MEND BACKUP_DEL_RECS;

%BACKUP_DEL_RECS

PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD AUTHDOMAIN="IIAS_Auth");
EXECUTE
	(DELETE FROM &TNG_DB..&TARGET_TABLE tam 
	WHERE MONTH_END_DT = &CURR_MTH AND MTG_PROVIDER_DESC ='MCAP'
	AND  NOT exists (SELECT 1 FROM &TNG_DB..&TARGET_TABLE tam2 
	WHERE tam2.MONTH_END_DT='2021-05-31' 
	AND tam2.MTG_PROVIDER_DESC='MCAP' 
	AND tam2.ACCOUNT_NUM=tam.ACCOUNT_NUM))BY IIASCON;
DISCONNECT FROM IIASCON;
QUIT;

/*================== code for preparing Deleted MCAP Accounts Report by Nikhil ===============*/
%put $$$$$$$$$$$$$$$$$$$$;
%put &CURR_MTH;
/*%let CURR_MTH=2022-01-31;
%put &CURR_MTH;*/  /*for Dev Test*/
%put $$$$$$$$$$$$$$$$$$$$;

DATA _NULL_;
INFILE "&rrap_dir/params/rrap/mcap_report_email_list.txt";
/*INFILE "/sasdata/sasdi/sasdev/Common_Tasks/mcap_report_email_list.txt";*/ /*for Dev Test*/
input;
if _n_=1 then do;
call symput("email_list",_infile_);
end;
run;
%put &=email_list;

PROC SQL;
	CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD AUTHDOMAIN="IIAS_Auth");
	CREATE TABLE WORK.TNG_MCAP_EXCLUDE_REP1 AS
		SELECT * FROM CONNECTION TO IIASCON
		(SELECT 
			LOAN_TO_VALUE_RATIO,DAYS_ARREARS_CNT,END_PRINCIPAL_BALANCE,INSURER_DESC,OCCUPANCY_TYPE_DESC,LST_PROP_APPRAISAL_VAL,PROP_USAGE_TYPE,DEFAULT_IND
		FROM &TNG_DB..MCAP_ACCOUNTS_BACKUP WHERE MONTH_END_DT =&CURR_MTH.);
	DISCONNECT FROM IIASCON;
QUIT;

proc export data=TNG_MCAP_EXCLUDE_REP1 REPLACE
	outfile="&rrap_dir/flat_files/rrap_iias/deleted_mcap_report.csv"
	/*outfile="&rrap_dir/Common_Tasks/deleted_mcap_report.csv"*/  /*for Dev Test*/
	dbms=csv;
run;

%macro csv_file_check;
%if %sysfunc(fileexist(&rrap_dir./flat_files/rrap_iias/deleted_mcap_report.csv)) %then %do;
%put "File Exists";
%end;
%else %do;
%put "File Does not Exist";
%abort abend 255;
%end;
%mend;

%csv_file_check

filename mymail1 email
to=(&email_list.) /*Include all the recipients*/
subject="[RRAP] Tangerine MCAP Mortgages Report - &MonthYear."
attach="&rrap_dir/flat_files/rrap_iias/deleted_mcap_report.csv";
/*attach="&rrap_dir/Common_Tasks/deleted_mcap_report.csv";*/  /*for Dev Test*/

data _null_;
	file mymail1;
	put 'Hi,';
	msgline = "Please find attached the Tangerine MCAP Mortgages report (excluded from the AIRB portfolio, to be added to the DR Standardized portfolio) as at &CURR_MTH..";
	put msgline;
	put ' ';
	put 'This is an automated mail. Please inform RRAP team if any issues.';
run;

/* report 2 - full report*/
PROC SQL;
	CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD AUTHDOMAIN="IIAS_Auth");
	CREATE TABLE WORK.TNG_MCAP_EXCLUDE_REP2 AS
		SELECT * FROM CONNECTION TO IIASCON
		(SELECT
			'' as ACCOUNT_ID,
			MONTH_START_DT,
			MONTH_END_DT,
			MATURITY_DT,
			OPEN_DT,
			COMMITTED_AMT,
			INTEREST_ARREARS_AMT,
			PRINCIPAL_ARREARS_AMT,
			ESCROW_ARREARS_AMT,
			TOT_ARREARS_AMT,
			NOMINAL_INTEREST_RATE,
			LOAN_TO_VALUE_RATIO,
			REMAINING_TERM,
			NON_PERFORMING_IND,
			NON_ACCRUAL_IND,
			DAYS_ARREARS_CNT,
			PYMTS_ARREARS_CNT,
			NSF_CNT,
			NSF_YTD_CNT,
			NSF_LIFE_CNT,
			START_PRINCIPAL_BALANCE,
			END_PRINCIPAL_BALANCE,
			SUNDRY_BALANCE,
			ESCROW_BALANCE,
			TOT_ADVANCED_AMT,
			REMAIN_AMORT,
			'' as ACCOUNT_KEY,
			MTG_APPLICATION_KEY,
			POOL_KEY,
			'' as CUSTOMER_KEY,
			AMORT_MATURITY_DT,
			LST_NSF_PYMT_RTN_DT,
			LST_PAYMENT_DT,
			NON_PERFM_DT,
			LATEST_90_DT,
			TOT_SCH_PYMT,
			LST_KNWN_COVER_PCT,
			EVER_ARREARS_CNT,
			EVER_30_CNT,
			EVER_60_CNT,
			EVER_90_CNT,
			DEFAULT_TYPE_CODE,
			MTG_ORIGINATION_KEY,
			TERM_DESC,
			RATE_TYPE_DESC,
			ACCELERATED_PMNT_IND,
			ANNUAL_FACTOR,
			MTG_PROVIDER_DESC,
			PROPERTY_TYPE_DESC,
			TENURE_DESC,
			STATUS_DESC,
			INSURER_DESC,
			POOL_DESC,
			PURPOSE_DESC,
			OCCUPANCY_TYPE_DESC,
			DWELLING_TYPE,
			BIRTH_DT,
			OCCUPATION_INDSTRY_CODE,
			GENDER,
			EARLY_RNWL_IND,
			ORG_ADJ_BUREAU_SCR,
			SUBMIT_DT,
			ORG_GDSR,
			ORG_TDSR,
			FRST_PYMT_RTN_DT,
			FIRST_90_DT,
			GURNTR_IND,
			ORG_LTV_RATIO,
			AMORT_PERIOD,
			ORG_ADVANCED_AMT,
			DIRECT_IND,
			FIRST_DEFAULT_DT,
			FRST_ADVANCE_DT,
			LOAN_TO_VALUE_AT_FIRST_DEFAULT,
			LATEST_INTEREST_ADJUST_DT,
			ADVANCE_EFF_DT,
			PROP_CITY,
			PROP_COUNTRY_CODE,
			'' as PROP_POSTAL_CODE,
			PROP_PROVINCE_CODE,
			LST_PROP_APPRAISAL_VAL,
			FSA,
			LST_PROP_APPRAISAL_DT,
			LIEN_PRIORITY_NUM,
			PROP_BUILDING_TYPE,
			ORG_COVER_EXEC_VAL,
			PROP_USAGE_TYPE,
			SECURITY_OWNER_TYPE,
			SECURITY_PROVIDER,
			'' as SECURITY_REG_BEGIN_DT,
			'' as SECURITY_REG_END_DT,
			'' as SECURITY_REG_NUM,
			ASSET_TYPE,
			ORIG_PROP_APPRAISAL_VAL,
			ORIG_PROP_APPRAISAL_DT,
			PROP_PURCHASE_AMT,
			PROP_PURCHASE_DT,
			ORG_TOTAL_INCOME,
			PI_PYMT_AMT,
			NEXT_INTEREST_RESET_DT,
			RATE_MODIFIER,
			DOWN_PYMT_SOURCE_DESC,
			CLIENT_DT,
			CIF_CREATED_ON_DT,
			DEFAULT_IND,
			STATED_INCOME_INDICATOR,
			ACCRUED_INTEREST_AMT,
			CLOSE_DT,
			BULK_NSURER_DESC,
			SECURITIZATION_INDICATOR,
			CIF_TYPE_DESC,
			CUSTOMER_TYPE_2,
			INTEREST_PAYMENTS,
			INTEREST_COMPOUNDING_FREQ,
			OPEN_CLOSED_TERM,
			PAYOUT_REASON,
			LAST_RENEWAL_DT,
			'' as ACCOUNT_NUM
		FROM &TNG_DB..MCAP_ACCOUNTS_BACKUP WHERE MONTH_END_DT =&CURR_MTH.);
	DISCONNECT FROM IIASCON;
QUIT;

proc export data=TNG_MCAP_EXCLUDE_REP2 REPLACE
	outfile="&rrap_dir/flat_files/rrap_iias/deleted_mcap_report_detail.csv"
	/*outfile="&rrap_dir/Common_Tasks/deleted_mcap_report_detail.csv"*/ /*for Dev Test*/
	dbms=csv;
run;

%macro csv_file_check;
%if %sysfunc(fileexist(&rrap_dir./flat_files/rrap_iias/deleted_mcap_report_detail.csv)) %then %do;
%put "File Exists";
%end;
%else %do;
%put "File Does not Exist";
%abort abend 255;
%end;
%mend;

%csv_file_check

filename mymail2 email
to=(&email_list.) /*Include all the recipients*/
subject="[RRAP] Tangerine MCAP Mortgages Detail Report - &MonthYear."
attach="&rrap_dir/flat_files/rrap_iias/deleted_mcap_report_detail.csv";
/*attach="&rrap_dir/Common_Tasks/deleted_mcap_report_detail.csv";*/ /*for Dev Test*/

data _null_;
	file mymail2;
	put 'Hi,';
	msgline = "Please find attached the Tangerine MCAP Mortgages full report (excluded from the AIRB portfolio, to be added to the DR Standardized portfolio) as at &CURR_MTH..";
	put msgline;
	put ' ';
	put 'This is an automated mail. Please inform RRAP team if any issues.';
run;


