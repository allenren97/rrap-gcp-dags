/* ************************************************************************ 
	Job name : j_rrap_dq_report_all_portfolios.sas

	Data Source: 
		Data: EDRTLRP1D..BASEL_MODEL_SNAPSHOT_POST_VAR_RPT_DTL
		Time variables: EDRTLRP1D...tm_dim

	Job dependency: Informatica job:

	Description: Generates DQ report for all portfolios and 
			sends output to excel.

************************************************************************ */

/* ----------------------------
	Set Job options
   ---------------------------- */

/* Start timer */
%let _timer_start = %sysfunc(datetime());

/* Set options */
/* Create metadata macro variables */
%let metaPort       = %nrquote(8561);
%let metaServer     = %nrquote(iw503);

/* Set metadata options */
options metaport       = &metaPort
        metaserver     = "&metaServer";

/*%let rrap_dir=/sasdata/sasdi/sasprod;*/
/*options mautosource sasautos=("&rrap_dir./macro/rrap", sasautos);*/

/* global SAS session options */
OPTIONS STIMER THREADS DBSLICEPARM=(ALL,10);
options nosymbolgen nomlogic mprint compress=yes;

/* ----------------------------
	Set Variables and Libname statements
   ---------------------------- */

/* Set libname statements

%let nzuser=owprdsas;
%let nzpassword={SAS002}0F833D4941CA4F644D8724214AE838AC3F916580;
%let dataBase=EDRTLRP1D_ic; /* Change this when pointing to different environments 
libname NZRRAP netezza server=cs2iwntzp01 database=&dataBase user="&nzuser" pwd="&nzpassword";  

*/
/* Setup to capture return codes  */ 
%global job_rc trans_rc sqlrc syscc;
%let sysrc = 0;
%let job_rc = 0;
%let trans_rc = 0;
%let sqlrc = 0;
%let syscc = 0;
%global etls_stepStartTime; 
/* initialize syserr to 0 */
data _null_; run;


%macro rcSet(error); 
   %if (&error gt &trans_rc) %then 
      %let trans_rc = &error;
   %if (&error gt &job_rc) %then 
      %let job_rc = &error;
%mend rcSet;

%macro rcSetDS(error); 
   if &error gt input(symget('trans_rc'),12.) then 
      call symput('trans_rc',trim(left(put(&error,12.))));
   if &error gt input(symget('job_rc'),12.) then 
      call symput('job_rc',trim(left(put(&error,12.))));
%mend rcSetDS; 


%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);
%rcSet(&syserr); 
%rcSet(&sysrc); 
%rcSet(&sqlrc); 
%rcSet(&syscc); 

%let dataBase=EDRTLRP1D; /* Change this when pointing to different environments */


/* --- Set Variables --- */
/* --- Date Variables */
/* Select previous month end date to execute the report */
/*proc sql noprint;*/
/*	select tm_id*/
/*		into :mth_tm_id*/
/*	from nzrrap.tm_dim*/
/*	where tm_lvl_end_dt = intnx('month',date(), -1, 'E')  and tm_lvl='Month';*/
/*quit;*/
%put mth_tm_id= &mth_tm_id;


/*Variables with dates to be added to Labels in Report*/
data _null_;
	format current_mth base_mth qty_mth half_mth nine_mth yearly_mth date9.;
	current_mth=intnx('month',"&MTH_END_DT."d, 0, 'E');
	base_mth=intnx('month',current_mth, -1, 'E');
	qty_mth=intnx('month',current_mth, -3, 'E');
	half_mth=intnx('month',current_mth, -6, 'E');
	nine_mth=intnx('month',current_mth, -9, 'E');
	yearly_mth=intnx('month',current_mth, -12, 'E');

	call symput ('current_mth', cat(put(current_mth, monname3.), " ", put(year(current_mth), best4.)));
	call symput ('base_mth', cat(put(base_mth, monname3.), " ", put(year(base_mth), best4.)));
	call symput ('qty_mth', cat(put(qty_mth, monname3.), " ", put(year(qty_mth), best4.)));
	call symput ('half_mth', cat(put(half_mth, monname3.), " ", put(year(half_mth), best4.)));
	call symput ('nine_mth', cat(put(nine_mth, monname3.), " ", put(year(nine_mth), best4.)));
	call symput ('yearly_mth' ,cat(put(yearly_mth, monname3.), " ", put(year(yearly_mth), best4.)));
	call symput ('excelNameDt', cat(put(current_mth, monname3.), "_", put(year(current_mth), best4.)));
	CALL SYMPUT ('MonthYear', CATX(' ',PUT(current_mth,monname10.),PUT(YEAR(current_mth),4.)));
run;
%put tmp_current_mth = &current_mth;
%put tmp_base_mth = &base_mth;
%put tmp_qty_mth = &qty_mth;
%put tmp_half_mth = &half_mth;
%put nine_mth = &nine_mth;
%put yearly_mth = &yearly_mth;
%put excelNameDt = &excelNameDt;


/* Report(file) Variables */
/* List of Portfolios for the report */
/* Modify this section when a new portfolio is added 
	1. Add SRC_SYS_CD
	2. Add sheet name variable
	3. Add title variable

this information can be stored in a control table in the future. */
%let list=MOR TNG KS_CC KS_LOC KS_HELOC SPL_ITL SPL_DTL;

/* Excel sheets names variables*/
%let MORSheetName=BNS Mortgage;
%let TNGSheetName=Tangerine;
%let KS_CCSheetName=KS CC;
%let KS_LOCSheetName=KS LOC;
%let KS_HELOCSheetName=KS HELOC;
%let SPL_ITLSheetName=SPL ITL;
%let SPL_DTLSheetName=SPL DTL;

/* Report titles variables by portfolio */
%let MORTitle=BNS Mortgage source data quality check report;
%let TNGTitle=Tangerine source data quality check report;
%let KS_CCTitle=KS Credit Card source data quality check report;
%let KS_LOCTitle=KS Line of Credit source data quality check report;
%let KS_HELOCTitle=KS House Equity Line of Credit source data quality check report;
%let SPL_ITLTitle=SPL Indirect Term Loan source data quality check report;
%let SPL_DTLTitle=SPL Direct Term Loan source data quality check report;

/* File location*/
%let file_path=&rrap_dir./flat_files/rrap/;
%let file_name=Data_quality_report_&excelNameDt..xlsx;
%put &file_path;
%put &file_name;

/* --- Email variables */
Data _null_;
	infile "&rrap_dir./params/rrap/rrap_exception_email_list.txt";
	input;
	if _N_ =2 then do;
		CALL SYMPUT("email_list",_infile_);
	end;
	if _N_ =3 then do;
		CALL SYMPUT("si_email_list",_infile_);
	end;
run;
%put &email_list;
%put &si_email_list;

/* --- Set Variables End --- */


/* ----------------------------
	Extract data and load
   ---------------------------- */

/* Extract data for Month end month from Netezza */
proc sql;											
	connect using NZRRAP as nzcon;										
	create table tmp_BASEL_SNAPSHOT_POST_VAR_DTL as
	select PROCESSING_MTH_TM_ID,
			MTH_TM_ID,
			BASEL_MODEL_SNAPSHOT_DTL_ID,
			GOVERNANCE_DOMAIN label='Governance Domain',
			PRINCIPLE label='Principle',
			SEVERITY label='Severity',
			ACTION_REQUIRED label='Action Required',
			SRC_SYS_CD,
			VAR_NM label='Field ',
			VAR_DESC label='Selection Criteria|Bins',
			CURRENT_MTH label="Current Month (&current_mth)",
			BASE_MTH label="1 Month Period (&base_mth)",
			QUARTER_MTH label="3 Month Period (&qty_mth)",
			HALFYEAR_MTH label="6 Month Period (&half_mth)",
			NINE_MTH label="9 Month Period (&nine_mth)",
			YEARLY_MTH label="12 Month Period (&yearly_mth)",
			input(THRESHOLD,best23.10) as THRESHOLD label='Threshold',
			VARIANCE_MONTHLY label='Variance % 1 Month',
			VARIANCE_QUARTER label='Variance % 3 Month',
			VARIANCE_HALFYEAR label='Variance % 6 Month',
			VARIANCE_NINE_MTH label='Variance % 9 Month',
			VARIANCE_YEARLY label='Variance % 12 Month',
			MODEL_NAME label='Model Name'
	from connection to nzcon (
		select PROCESSING_MTH_TM_ID,
			MTH_TM_ID,
			BASEL_MODEL_SNAPSHOT_DTL_ID,
			GOVERNANCE_DOMAIN,
			PRINCIPLE, SEVERITY,
			ACTION_REQUIRED,
			SRC_SYS_CD, 
			VAR_NM,
			VAR_DESC,
			CURRENT_MTH,
			BASE_MTH,
			QUARTER_MTH,
			HALFYEAR_MTH,
			NINE_MTH,
			YEARLY_MTH,
			THRESHOLD,
			VARIANCE_MONTHLY,
			VARIANCE_QUARTER,
			VARIANCE_HALFYEAR,
			VARIANCE_NINE_MTH,
			VARIANCE_YEARLY,
			MODEL_NAME
		from &dataBase..BASEL_MODEL_SNAPSHOT_POST_VAR_RPT_DTL
		where MTH_TM_ID = &mth_tm_id
		order by BASEL_MODEL_SNAPSHOT_DTL_ID
	);
	disconnect from nzcon;											
quit;

/* Verify that Input table has information
 If there is no data then report will be sent without attachment */
proc sql noprint;
	select count(*)
	into :row_cnt
	from tmp_BASEL_SNAPSHOT_POST_VAR_DTL;
quit;
%put &row_cnt;

%if %eval(&row_cnt > 0) %then %do;
	/* ----------------------------
		Modify data and generate report structure
	   ---------------------------- */
	%macro createRptSections(src_sys); 
		/*  Input parameters are MOR, TNG, SPL and KS
			Macro divided in 2 sections: 1. Create tables to set the structure of each section of the report 
										2. create proc reports before building the report in excel */

		/* 1. Start --- Create tables to set the structure of each section of the report */
		data tmp_&src_sys._audit (drop=VARIANCE_MONTHLY VARIANCE_QUARTER VARIANCE_HALFYEAR
														VARIANCE_NINE_MTH VARIANCE_YEARLY
														CURRENT_MTH BASE_MTH QUARTER_MTH 
														HALFYEAR_MTH NINE_MTH YEARLY_MTH
												rename=(VARIANCE_MONTHLY_num=VARIANCE_MONTHLY
														VARIANCE_QUARTER_num=VARIANCE_QUARTER
														VARIANCE_HALFYEAR_num=VARIANCE_HALFYEAR
														VARIANCE_NINE_MTH_num=VARIANCE_NINE_MTH
														VARIANCE_YEARLY_num=VARIANCE_YEARLY
														CURRENT_MTH_num=CURRENT_MTH
														BASE_MTH_num=BASE_MTH
														QUARTER_MTH_num=QUARTER_MTH
														HALFYEAR_MTH_num=HALFYEAR_MTH
														NINE_MTH_num=NINE_MTH
														YEARLY_MTH_num=YEARLY_MTH))		/* Audit table */
			/* Data Validation table */
			tmp_&src_sys._data_val (drop= VARIANCE_MONTHLY_num VARIANCE_QUARTER_num 
											VARIANCE_HALFYEAR_num VARIANCE_NINE_MTH_num VARIANCE_YEARLY_num)		
			/* Data Integrity table */
			tmp_&src_sys._data_ingty (drop=VARIANCE_MONTHLY VARIANCE_QUARTER VARIANCE_HALFYEAR
														VARIANCE_NINE_MTH VARIANCE_YEARLY
														CURRENT_MTH BASE_MTH QUARTER_MTH 
														HALFYEAR_MTH NINE_MTH YEARLY_MTH
												rename=(VARIANCE_MONTHLY_num=VARIANCE_MONTHLY
														VARIANCE_QUARTER_num=VARIANCE_QUARTER
														VARIANCE_HALFYEAR_num=VARIANCE_HALFYEAR
														VARIANCE_NINE_MTH_num=VARIANCE_NINE_MTH
														VARIANCE_YEARLY_num=VARIANCE_YEARLY
														CURRENT_MTH_num=CURRENT_MTH
														BASE_MTH_num=BASE_MTH
														QUARTER_MTH_num=QUARTER_MTH
														HALFYEAR_MTH_num=HALFYEAR_MTH
														NINE_MTH_num=NINE_MTH
														YEARLY_MTH_num=YEARLY_MTH))	
			/* Data Quality table */
			tmp_&src_sys._data_qlty (drop=VARIANCE_MONTHLY VARIANCE_QUARTER VARIANCE_HALFYEAR
														VARIANCE_NINE_MTH VARIANCE_YEARLY
														CURRENT_MTH BASE_MTH QUARTER_MTH 
														HALFYEAR_MTH NINE_MTH YEARLY_MTH
												rename=(VARIANCE_MONTHLY_num=VARIANCE_MONTHLY
														VARIANCE_QUARTER_num=VARIANCE_QUARTER
														VARIANCE_HALFYEAR_num=VARIANCE_HALFYEAR
														VARIANCE_NINE_MTH_num=VARIANCE_NINE_MTH
														VARIANCE_YEARLY_num=VARIANCE_YEARLY
														CURRENT_MTH_num=CURRENT_MTH
														BASE_MTH_num=BASE_MTH
														QUARTER_MTH_num=QUARTER_MTH
														HALFYEAR_MTH_num=HALFYEAR_MTH
														NINE_MTH_num=NINE_MTH
														YEARLY_MTH_num=YEARLY_MTH));
			set tmp_BASEL_SNAPSHOT_POST_VAR_DTL;
			where src_sys_cd="&src_sys";
			label 
					CURRENT_MTH_num="Current Month (&current_mth)"
					BASE_MTH_num="1 Month Period (&base_mth)"
					QUARTER_MTH_num="3 Month Period (&qty_mth)"
					HALFYEAR_MTH_num="6 Month Period (&half_mth)"
					NINE_MTH_num="9 Month Period (&nine_mth)"
					YEARLY_MTH_num="12 Month Period (&yearly_mth)"
					VARIANCE_MONTHLY_num='Variance % 1 Month'
					VARIANCE_QUARTER_num='Variance % 3 Month'
					VARIANCE_HALFYEAR_num='Variance % 6 Month'
					VARIANCE_NINE_MTH_num='Variance % 9 Month'
					VARIANCE_YEARLY_num='Variance % 12 Month';
			if GOVERNANCE_DOMAIN='Data Validation' then output tmp_&src_sys._data_val;
			else do;
				VARIANCE_MONTHLY_num= input(VARIANCE_MONTHLY,best12.6);
				VARIANCE_QUARTER_num= input(VARIANCE_QUARTER,best12.6);
				VARIANCE_HALFYEAR_num= input(VARIANCE_HALFYEAR,best12.6);
				VARIANCE_NINE_MTH_num= input(VARIANCE_NINE_MTH,best12.6);
				VARIANCE_YEARLY_num= input(VARIANCE_YEARLY,best12.6);
				CURRENT_MTH_num=input(CURRENT_MTH, best32.);
				BASE_MTH_num=input(BASE_MTH, best32.);
				QUARTER_MTH_num=input(QUARTER_MTH, best32.);
				HALFYEAR_MTH_num=input(HALFYEAR_MTH, best32.);
				NINE_MTH_num=input(NINE_MTH, best32.);
				YEARLY_MTH_num=input(YEARLY_MTH, best32.);
				if GOVERNANCE_DOMAIN='Audit' then output tmp_&src_sys._audit;
				if GOVERNANCE_DOMAIN='Data Validation' then output tmp_&src_sys._data_val;
				if GOVERNANCE_DOMAIN='Data Integrity' then output tmp_&src_sys._data_ingty;
				if GOVERNANCE_DOMAIN='Data Quality' then output tmp_&src_sys._data_qlty;
			end;

			drop PROCESSING_MTH_TM_ID MTH_TM_ID SRC_SYS_CD; /* Drop variables for all tables -- Specific table variables are set in DS */
		run;
	/* 1. End --- Create tables to set the structure of each section of the report */

	/* 2. Start --- create proc reports before building the report in excel */
		title height=10 "&&&src_sys.Title";

		/* Audit section of the report ---*/
		proc report data=tmp_&src_sys._audit spanrows /*nowd*/
					style(report)= [bordercolor=black borderwidth=1pt]
					style(summary)=[FONT_WEIGHT = BOLD]
					style(lines)=[fontweight=bold fontsize=5 color=black];

			column governance_domain
					principle
					Severity
					action_required
					model_name
					var_nm
					BASEL_MODEL_SNAPSHOT_DTL_ID
					var_desc
					current_mth
					BASE_MTH
					QUARTER_MTH
					HALFYEAR_MTH
					NINE_MTH
					YEARLY_MTH
					threshold
					VARIANCE_MONTHLY
					VARIANCE_QUARTER
					VARIANCE_HALFYEAR
					VARIANCE_NINE_MTH
					VARIANCE_YEARLY;
		/*#7c95bc - blue */

			define governance_domain /order center style(column)=[vjust=m just=c background=#bfbfbf fontweight=bold] ;
			define principle /order style(column)=[vjust=m just=c background=#bfbfbf];
			define severity /order style(column)=[vjust=m just=c background=#bfbfbf];
			define action_required/order style(column)=[vjust=m just=c background=#bfbfbf];
			define model_name/style(column)=[vjust=m just=c background=#bfbfbf];
			define var_nm/ right style(column)=[background=beige textalign=l fontweight=bold];
			define BASEL_MODEL_SNAPSHOT_DTL_ID/ order noprint;
			define CURRENT_MTH    /right;
			define BASE_MTH       /right;
			define QUARTER_MTH    /right;
			define HALFYEAR_MTH   /right;
			define NINE_MTH       /right;
			define YEARLY_MTH     /right;
			define threshold/ center f=percent9.2 style(column)=[background=#d9d9d9];
			define VARIANCE_MONTHLY/ center style(column)={tagattr='format:####0.0000%'};
			define VARIANCE_QUARTER/ center style(column)={tagattr='format:####0.0000%'};
			define VARIANCE_HALFYEAR/ center style(column)={tagattr='format:####0.0000%'};
			define VARIANCE_NINE_MTH/ center style(column)={tagattr='format:####0.0000%'};
			define VARIANCE_YEARLY/ center style(column)={tagattr='format:####0.0000%'};

			/* IDs listed below are rows that have an amount. These compute statements format amounts in the report */
			compute CURRENT_MTH;
				if BASEL_MODEL_SNAPSHOT_DTL_ID in (10004, 20004, 30004, 40004, 50004, 70004, 60004) then 
					call define(_col_,'style', 'style=[tagattr="format:$#,##0.00;$-#,##0.00"]');
			endcomp;

			compute BASE_MTH;
				if BASEL_MODEL_SNAPSHOT_DTL_ID in (10004, 20004, 30004, 40004, 50004, 70004, 60004) then 
					call define(_col_,'style', 'style=[tagattr="format:$#,##0.00;$-#,##0.00"]');
			endcomp;

			compute QUARTER_MTH;
				if BASEL_MODEL_SNAPSHOT_DTL_ID in (10004, 20004, 30004, 40004, 50004, 70004, 60004) then 
					call define(_col_,'style', 'style=[tagattr="format:$#,##0.00;$-#,##0.00"]');
			endcomp;

			compute HALFYEAR_MTH;
				if BASEL_MODEL_SNAPSHOT_DTL_ID in (10004, 20004, 30004, 40004, 50004, 70004, 60004) then 
					call define(_col_,'style', 'style=[tagattr="format:$#,##0.00;$-#,##0.00"]');
			endcomp;

			compute NINE_MTH;
				if BASEL_MODEL_SNAPSHOT_DTL_ID in (10004, 20004, 30004, 40004, 50004, 70004, 60004) then 
					call define(_col_,'style', 'style=[tagattr="format:$#,##0.00;$-#,##0.00"]');
			endcomp;

			compute YEARLY_MTH;
				if BASEL_MODEL_SNAPSHOT_DTL_ID in (10004, 20004, 30004, 40004, 50004, 70004, 60004) then 
					call define(_col_,'style', 'style=[tagattr="format:$#,##0.00;$-#,##0.00"]');
			endcomp;

			compute VARIANCE_MONTHLY;
				if abs(VARIANCE_MONTHLY.sum) < threshold.sum then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if abs(VARIANCE_MONTHLY.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;

			compute VARIANCE_QUARTER;
				if abs(VARIANCE_QUARTER.sum) < threshold.sum then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if abs(VARIANCE_QUARTER.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;

			compute VARIANCE_HALFYEAR;
				if abs(VARIANCE_HALFYEAR.sum) < threshold.sum then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if abs(VARIANCE_HALFYEAR.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;

			compute VARIANCE_NINE_MTH;
				if abs(VARIANCE_NINE_MTH.sum) < threshold.sum then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if abs(VARIANCE_NINE_MTH.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;

			compute VARIANCE_YEARLY;
				if abs(VARIANCE_YEARLY.sum) < threshold.sum then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if abs(VARIANCE_YEARLY.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;
		run;


		/* Data Validation section of the report --- */
		title;
		options missing=' ';
		proc report data=tmp_&src_sys._data_val spanrows noheader
					style(report)= [bordercolor=black borderwidth=1pt]
					style(summary)=[FONT_WEIGHT = BOLD]
					style(lines)=[fontweight=bold fontsize=5 color=black];

			column governance_domain
					principle
					Severity
					action_required
					model_name
					var_nm
					var_desc
					current_mth
					BASE_MTH
					QUARTER_MTH
					HALFYEAR_MTH
					NINE_MTH
					YEARLY_MTH
					threshold
					VARIANCE_MONTHLY
					VARIANCE_QUARTER
					VARIANCE_HALFYEAR
					VARIANCE_NINE_MTH
					VARIANCE_YEARLY;

			define governance_domain /order center style(column)=[vjust=m just=c background=#bfbfbf fontweight=bold] ;
			define principle /order style(column)=[vjust=m just=c background=#bfbfbf];
			define severity /order style(column)=[vjust=m just=c background=#bfbfbf];
			define action_required/order style(column)=[vjust=m just=c background=#bfbfbf];
	/* If condition to merge columns for model_name on porfolios that have values. Otherwise it will not generate the report properly */
			%if &src_sys=%quote(KS_CC) 
				or &src_sys=%quote(KS_LOC) 
				or &src_sys=%quote(KS_HELOC) %then define model_name/order style(column)=[vjust=m just=c background=#bfbfbf];
			%else define model_name/style(column)=[vjust=m just=c background=#bfbfbf];;
			define var_nm/ right style=[background=beige];
			define var_desc/left style(column)=[flyover='Missing or not Null'];
			define CURRENT_MTH    /right;
			define BASE_MTH       /right;
			define QUARTER_MTH    /right;
			define HALFYEAR_MTH   /right;
			define NINE_MTH       /right;
			define YEARLY_MTH     /right;
			define threshold/ center style=[background=#d9d9d9];
			define VARIANCE_MONTHLY/ center;
			define VARIANCE_QUARTER/ center;
			define VARIANCE_HALFYEAR/ center;
			define VARIANCE_NINE_MTH/ center;
			define VARIANCE_YEARLY/ center;

			compute var_nm;
				if var_nm = 'Missing or not null' or
					var_nm = 'Time stamp' or
					var_nm = 'Duplicate records'
				then call define(_col_,'style', 'style=[textalign=l fontweight=bold]');
			endcomp;

			compute VARIANCE_MONTHLY;
				if VARIANCE_MONTHLY = 'PASS' then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if VARIANCE_MONTHLY = 'FAIL' then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;

			compute VARIANCE_QUARTER;
				if VARIANCE_QUARTER = 'PASS' then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if VARIANCE_QUARTER = 'FAIL' then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;

			compute VARIANCE_HALFYEAR;
				if VARIANCE_HALFYEAR = 'PASS' then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if VARIANCE_HALFYEAR = 'FAIL' then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;

			compute VARIANCE_NINE_MTH;
				if VARIANCE_NINE_MTH = 'PASS' then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if VARIANCE_NINE_MTH = 'FAIL' then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;

			compute VARIANCE_YEARLY;
				if VARIANCE_YEARLY = 'PASS' then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if VARIANCE_YEARLY = 'FAIL' then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;
		run;


		/* Data Integrity section of the report --- */
		title;
		options missing=' ';

		proc report data=tmp_&src_sys._data_ingty spanrows noheader
					style(report)= [bordercolor=black borderwidth=1pt]
					style(summary)=[FONT_WEIGHT = BOLD]
					style(lines)=[fontweight=bold fontsize=5 color=black];

			column governance_domain
					principle
					Severity
					action_required
					model_name
					var_nm
					BASEL_MODEL_SNAPSHOT_DTL_ID
					var_desc
					current_mth
					BASE_MTH
					QUARTER_MTH
					HALFYEAR_MTH
					NINE_MTH
					YEARLY_MTH
					threshold
					VARIANCE_MONTHLY
					VARIANCE_QUARTER
					VARIANCE_HALFYEAR
					VARIANCE_NINE_MTH
					VARIANCE_YEARLY;

			define governance_domain /order center style(column)=[vjust=m just=c background=#bfbfbf fontweight=bold] ;
			define principle /order style(column)=[vjust=m just=c background=#bfbfbf];
			define severity /order style(column)=[vjust=m just=c background=#bfbfbf];
			define action_required/order style(column)=[vjust=m just=c background=#bfbfbf];
			define model_name/order style(column)=[vjust=m just=c background=#bfbfbf];
			define var_nm/ order right style(column)=[vjust=m just=r background=beige];
			define BASEL_MODEL_SNAPSHOT_DTL_ID/order noprint;
			define CURRENT_MTH    /right;
			define BASE_MTH       /right;
			define QUARTER_MTH    /right;
			define HALFYEAR_MTH   /right;
			define NINE_MTH       /right;
			define YEARLY_MTH     /right;
			define threshold/ center f=percent9.2 style=[background=#d9d9d9];
			define VARIANCE_MONTHLY/ center style(column)={tagattr='format:####0.0000%'};
			define VARIANCE_QUARTER/ center style(column)={tagattr='format:####0.0000%'};
			define VARIANCE_HALFYEAR/ center style(column)={tagattr='format:####0.0000%'};
			define VARIANCE_NINE_MTH/ center style(column)={tagattr='format:####0.0000%'};
			define VARIANCE_YEARLY/ center style(column)={tagattr='format:####0.0000%'};



			compute var_nm;
				if strip(var_desc)='X > = 270' then call define(_row_,'style', 'style=[borderbottomstyle=double borderbottomwidth=1pt borderbottomcolor=black]');;
			endcomp;

			/* IDs listed below are rows that have an amount. These compute statements format amounts in the report */
			compute CURRENT_MTH;
				if BASEL_MODEL_SNAPSHOT_DTL_ID in (13103, 32201, 32202, 32203, 42301, 42302, 42303, 52201, 52202, 52203, 62501, 62502) then 
					call define(_col_,'style', 'style=[tagattr="format:$#,##0.00;$-#,##0.00"]');
			endcomp;

			compute BASE_MTH;
				if BASEL_MODEL_SNAPSHOT_DTL_ID in (13103, 32201, 32202, 32203, 42301, 42302, 42303, 52201, 52202, 52203, 62501, 62502) then 
					call define(_col_,'style', 'style=[tagattr="format:$#,##0.00;$-#,##0.00"]');
			endcomp;

			compute QUARTER_MTH;
				if BASEL_MODEL_SNAPSHOT_DTL_ID in (13103, 32201, 32202, 32203, 42301, 42302, 42303, 52201, 52202, 52203, 62501, 62502) then 
					call define(_col_,'style', 'style=[tagattr="format:$#,##0.00;$-#,##0.00"]');
			endcomp;

			compute HALFYEAR_MTH;
				if BASEL_MODEL_SNAPSHOT_DTL_ID in (13103, 32201, 32202, 32203, 42301, 42302, 42303, 52201, 52202, 52203, 62501, 62502) then 
					call define(_col_,'style', 'style=[tagattr="format:$#,##0.00;$-#,##0.00"]');
			endcomp;

			compute NINE_MTH;
				if BASEL_MODEL_SNAPSHOT_DTL_ID in (13103, 32201, 32202, 32203, 42301, 42302, 42303, 52201, 52202, 52203, 62501, 62502) then 
					call define(_col_,'style', 'style=[tagattr="format:$#,##0.00;$-#,##0.00"]');
			endcomp;

			compute YEARLY_MTH;
				if BASEL_MODEL_SNAPSHOT_DTL_ID in (13103, 32201, 32202, 32203, 42301, 42302, 42303, 52201, 52202, 52203, 62501, 62502) then 
					call define(_col_,'style', 'style=[tagattr="format:$#,##0.00;$-#,##0.00"]');
			endcomp;

			compute VARIANCE_MONTHLY;
				if abs(VARIANCE_MONTHLY.sum) < threshold.sum then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if abs(VARIANCE_MONTHLY.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;

			compute VARIANCE_QUARTER;
				if abs(VARIANCE_QUARTER.sum) < threshold.sum then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if abs(VARIANCE_QUARTER.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;

			compute VARIANCE_HALFYEAR;
				if abs(VARIANCE_HALFYEAR.sum) < threshold.sum then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if abs(VARIANCE_HALFYEAR.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;

			compute VARIANCE_NINE_MTH;
				if abs(VARIANCE_NINE_MTH.sum) < threshold.sum then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if abs(VARIANCE_NINE_MTH.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;

			compute VARIANCE_YEARLY;
				if abs(VARIANCE_YEARLY.sum) < threshold.sum then call define(_col_,'style', 'style=[background=#c6e0b4]');
				else if abs(VARIANCE_YEARLY.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
			endcomp;
		run; 

		/* Data Quality Section of the report --- */
		title;
		options missing=' ';
		proc report data=tmp_&src_sys._data_qlty spanrows noheader
					style(report)= [bordercolor=black borderwidth=1pt]
					style(summary)=[FONT_WEIGHT = BOLD]
					style(lines)=[fontweight=bold fontsize=5 color=black];

			column governance_domain
					principle
					Severity
					action_required
					model_name
					var_nm
					var_desc
					current_mth
					BASE_MTH
					QUARTER_MTH
					HALFYEAR_MTH
					NINE_MTH
					YEARLY_MTH
					threshold
					VARIANCE_MONTHLY
					VARIANCE_QUARTER
					VARIANCE_HALFYEAR
					VARIANCE_NINE_MTH
					VARIANCE_YEARLY;

			define governance_domain /order center style(column)=[vjust=m just=c background=#bfbfbf fontweight=bold] ;
			define principle /order style(column)=[vjust=m just=c background=#bfbfbf];
			define severity /order style(column)=[vjust=m just=c background=#bfbfbf];
			define action_required/order style(column)=[vjust=m just=c background=#bfbfbf];
			define model_name/order style(column)=[vjust=m just=c background=#bfbfbf];
			define var_nm/order right style(column)=[vjust=m just=r background=beige];
			define CURRENT_MTH    /right;
			define BASE_MTH       /right;
			define QUARTER_MTH    /right;
			define HALFYEAR_MTH   /right;
			define NINE_MTH       /right;
			define YEARLY_MTH     /right;
			define threshold/ center style=[background=#d9d9d9];
			define VARIANCE_MONTHLY/ center style(column)=[flyover='PSI'] f=9.4;
			define VARIANCE_QUARTER/ center style(column)=[flyover='PSI'] f=9.4;
			define VARIANCE_HALFYEAR/ center style(column)=[flyover='PSI'] f=9.4;
			define VARIANCE_NINE_MTH/ center style(column)=[flyover='PSI'] f=9.4;
			define VARIANCE_YEARLY/ center style(column)=[flyover='PSI'] f=9.4;

			compute var_desc;
				if strip(lowcase(var_desc))='subtotal' then call define(_row_,'style', 'style=[background=beige/*#bfbfbf bordertopstyle=solid bordertopwidth=1pt bordertopcolor=black borderbottomstyle=solid borderbottomwidth=1pt borderbottomcolor=black */]');
			endcomp;

			compute VARIANCE_MONTHLY;
				if var_desc='Subtotal' then do;
					if abs(VARIANCE_MONTHLY.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
				end;
				else call define(_col_,'style', 'style=[background=#c6e0b4]');
			endcomp;

			compute VARIANCE_QUARTER;
				if var_desc='Subtotal' then do;
					if abs(VARIANCE_QUARTER.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
				end;
				else call define(_col_,'style', 'style=[background=#c6e0b4]');
			endcomp;

			compute VARIANCE_HALFYEAR;
				if var_desc='Subtotal' then do;
					if abs(VARIANCE_HALFYEAR.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
				end;
				else call define(_col_,'style', 'style=[background=#c6e0b4]');
			endcomp;

			compute VARIANCE_NINE_MTH;
				if var_desc='Subtotal' then do;
					if abs(VARIANCE_NINE_MTH.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
				end;
				else call define(_col_,'style', 'style=[background=#c6e0b4]');
			endcomp;

			compute VARIANCE_YEARLY;
				if var_desc='Subtotal' then do;
					if abs(VARIANCE_YEARLY.sum) > threshold.sum then call define(_col_,'style', 'style=[background=#fc4141]');
				end;
				else call define(_col_,'style', 'style=[background=#c6e0b4]');
			endcomp;
		run;

	/* 2. End --- create proc reports before building the report in excel */
	%mend createRptSections;

	/* ----------------------------
		Build Report
	   ---------------------------- */
	/* 3. Start --- Build Report */
	ODS LISTING CLOSE;
	ods excel file="&file_path.&file_name" options(embedded_titles='yes'
															sheet_interval="none"
															flow='header,data'
															absolute_column_width='13.5, 11, 7.5, 8.5, 10,
																					26, 28.5,
																					16.22, 16.22, 16.22, 16.22, 16.22, 16.22,
																					9,
																					10.67, 10.67, 10.67, 10.67, 10.67'
															zoom='80'
															); 
		%macro buildRptContent(vlist);
			%let nwords=%sysfunc(countw(&vlist));

			%do i=1 %to &nwords;
			%let src_sys=%scan(&vlist, &i);
				ods excel options(sheet_interval='none' sheet_name="&&&src_sys.SheetName");
				%createRptSections(&src_sys);

				/* Code used to make ods work and send data to other sheet */
				ods excel options(sheet_interval="table");
				ods exclude all;
				data _null_;
				file print;
				put _all_;
				run;
				ods select all;
				/**/
			%end;
		%mend buildRptContent;
		%buildRptContent(&list)
	ods excel close;
	/* 3. End --- Build Report */


	/* ----------------------------
		Send email
	   ---------------------------- */
	/* 4. Start --- Send email to users */
	FILENAME OUTMAIL EMAIL
		SUBJECT= "[RRAP] DQ Report - &MonthYear."
                attach=("&file_path.&file_name" content_type="application/xlsx" lrecl=32000);

	DATA _NULL_;
		FILE OUTMAIL
			TO= (&email_list)
			CC= (&si_email_list);

		put;
		put "RMSS Data Quality report for &current_mth";
	RUN;

	/* 4. End --- Send email to users */
%end;
%else %do;
	/* Send email with notification that no data was extracted */
	FILENAME OUTMAIL EMAIL
		SUBJECT= "[RRAP] DQ Report - &MonthYear. : No data in source table"; 

	DATA _NULL_;
		FILE OUTMAIL
			TO= (&email_list)
			CC= (&si_email_list);

		put;
		put "RMSS Data Quality report for &current_mth was not generated properly because of there is no data in source table ";
		put "Please contact production support";
	RUN;
%end;
/* Stop timer */
data _null_;
  dur = datetime() - &_timer_start;
  put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-';
run;
