
***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0900_DT4_XML_OUTPUT.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: 
*  Target Table:  
*  
*  Purpose: Generate XML file for OSFI Submission
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  		  
*
*	Change Log:
*	2022-01-07: Hadi Dimashkieh - Initial Development
*	2022-04-07: Hadi Dimashkieh - Remove FirmID from xml output
*
*
***************************************************************************************************************************;



%rrap_dt4_autoexec();


******************************************************************************************************************;
******************************************************************************************************************;
******************************************************************************************************************;


data RT05;
	set NZRRAP.DT4_RT05_DECLARATION;
	where mth_tm_id = &mth_tm_id.;
	keep RRS RRS_NAME;
run;
proc sort data=RT05; by RRS; run;


proc contents noprint data=rt05 out=rt05vars(keep=name varnum); run;
data rt05vars; 
	set rt05vars;
	displayname=name;
run;
proc sort data=rt05vars; by varnum; run;

******************************************************************************************************************;
******************************************************************************************************************;
******************************************************************************************************************;

data RT10;
	set NZRRAP.DT4_RT10_DECLARATION;
	where mth_tm_id = &mth_tm_id.;
	keep RRS PD_SEG PD_Segment_Name;
run;
proc sort data=RT10; by RRS PD_SEG; run;


proc contents noprint data=RT10 out=rt10vars(keep=name varnum); run;
data rt10vars; 
	set rt10vars;
	displayname=name;
	if name = 'PD_SEGMENT_NAME' then displayname = 'PD_Segment_Name';
run;
proc sort data=rt10vars; by varnum; run;

******************************************************************************************************************;
******************************************************************************************************************;
******************************************************************************************************************;

data RT11;
	set NZRRAP.DT4_RT11_DECLARATION;
	where mth_tm_id = &mth_tm_id.;
	keep RRS LGD_SEG LGD_NAME;
run;
proc sort data=RT11; by RRS LGD_SEG; run;


proc contents noprint data=RT11 out=rt11vars(keep=name varnum); run;
data rt11vars; 
	set rt11vars;
	displayname=name;
run;
proc sort data=rt11vars; by varnum; run;

******************************************************************************************************************;
******************************************************************************************************************;
******************************************************************************************************************;

******************************************************************************************************************;
******************************************************************************************************************;
******************************************************************************************************************;

data RT12;
	set NZRRAP.DT4_RT12_DECLARATION;
	where mth_tm_id = &mth_tm_id.;
	keep RRS EADF_SEG EADF_Name;
run;
proc sort data=RT12; by RRS EADF_SEG; run;


proc contents noprint data=RT12 out=rt12vars(keep=name varnum); run;
data rt12vars; 
	set rt12vars;
	displayname=name;
run;
proc sort data=rt12vars; by varnum; run;

******************************************************************************************************************;
******************************************************************************************************************;
******************************************************************************************************************;

data RT18;
set NZRRAP.DT4_RT18_FINAL_RPTG_VARS;
where mth_tm_id = &mth_tm_id.;

	Exposure_Classes = DT4_EXPSR_CL_KEY_VAL;
	Estimated_PD_Pct = compress(put(round(PRODUCTION_PD * 100,0.01),6.2));
	Realized_PD_Pct = compress(put(round(OBSERVED_PD * 100,0.01),6.2));
	Estimated_LGD_Pct = compress(put(round(PRODUCTION_LGD * 100,0.01),6.2));
	Realized_LGD_Pct = compress(put(round(OBSERVED_LGD * 100,0.01),6.2));
	Estimated_EAD_Pct = compress(put(round(PRODUCTION_EAD * 100,0.01),6.2));
	Realized_EAD_Pct = compress(put(round(OBSERVED_EAD * 100,0.01),6.2));
	EAD_MM = compress(put(round(EAD_MM_DEFAULTED_ACCOUNTS, 1),20.));
	EAD_Non_defaulted_Accounts = compress(put(round(EAD_MM_NON_DEFAULTED_ACCOUNTS, 1),20.));
	RWA_MM1 = compress(put(round(RWA_MM, 1),20.));

rename RWA_MM1=RWA_MM;
keep Exposure_Classes 
	Estimated_PD_Pct Realized_PD_Pct Estimated_LGD_Pct Realized_LGD_Pct Estimated_EAD_Pct 
	Realized_EAD_Pct EAD_Non_defaulted_Accounts EAD_MM RWA_MM1;

	array vars _character_;
	do _n_=1 to dim(vars);
		if vars(_n_) = '.' then vars(_n_)= '';
	end;
run;

proc sort data=RT18; by Exposure_Classes; run;


proc contents noprint data=rt18 out=rt18vars(keep=name varnum); run;
data rt18vars; 
	set rt18vars;
	displayname=name;
run;
proc sort data=rt18vars; by varnum; run;


******************************************************************************************************************;
******************************************************************************************************************;
******************************************************************************************************************;


data RT20;
set NZRRAP.DT4_RT20_FINAL_RPTG_VARS;
where process_mth_tm_id = &mth_tm_id.;

	RRS = DT4_RISK_RT_KEY_VAL;
/*	PD_Segments = compress(put(PD_BASEL_SEG_NUM,4.));*/
	PD_Segments = DT4_PD_SEG_KEY_VAL;
	Estimated_IRB_PD_Pct = compress(put(round(PREDICTED_PD*100,0.01),6.2));
	Realized_IRB_PD_Pct = compress(put(round(REALIZED_PD*100,0.01),6.2));
	Breach_Recognized = compress(put(BREACH,1.));
	EAD_of_Defaulted_Accounts = compress(put(round(EAD_DEF,1),20.));
	EAD_of_Non_Defaulted_Accounts = compress(put(round(EAD_NONDEF,1),20.));

keep RRS PD_Segments Estimated_IRB_PD_Pct Realized_IRB_PD_Pct Breach_Recognized 
	EAD_of_Non_Defaulted_Accounts EAD_of_Defaulted_Accounts ;

	array vars _character_;
	do _n_=1 to dim(vars);
		if vars(_n_) = '.' then vars(_n_)= '';
	end;
run;

proc sort data=RT20; by RRS PD_Segments; run;


proc contents noprint data=rt20 out=rt20vars(keep=name varnum); run;
data rt20vars; 
	set rt20vars;
	displayname=name;
	if name = 'EAD_of_Non_Defaulted_Accounts' then displayname = 'EAD_of_Non-Defaulted_Accounts';
run;
proc sort data=rt20vars; by varnum; run;



******************************************************************************************************************;
******************************************************************************************************************;
******************************************************************************************************************;

data RT30;
set NZRRAP.DT4_RT30_FINAL_RPTG_VARS;
where process_mth_tm_id = &mth_tm_id.;

	RRS = DT4_RISK_RT_KEY_VAL;
/*	LGD_Segments = compress(put(LGD_BASEL_SEG_NUM,4.));*/
	LGD_Segments = DT4_LGD_SEG_KEY_VAL;
	Estimated_IRB_LGD_Pct = compress(put(round(PREDICTED_LGD*100,0.01),6.2));
	Realized_IRB_LGD_Pct = compress(put(round(REALIZED_LGD*100,0.01),6.2));
	Breach_Recognized = compress(put(BREACH,1.));
	EAD_of_Defaulted_Accounts = compress(put(round(EAD_DEF,1),20.));
	EAD_of_Non_Defaulted_Accounts = compress(put(round(EAD_NONDEF,1),20.));

keep RRS LGD_Segments Estimated_IRB_LGD_Pct Realized_IRB_LGD_Pct Breach_Recognized 
	EAD_of_Non_Defaulted_Accounts EAD_of_Defaulted_Accounts ;

	array vars _character_;
	do _n_=1 to dim(vars);
		if vars(_n_) = '.' then vars(_n_)= '';
	end;
run;

proc sort data=RT30; by RRS LGD_Segments; run;


proc contents noprint data=rt30 out=rt30vars(keep=name varnum); run;
data rt30vars; 
	set rt30vars;
	displayname=name;
	if name = 'EAD_of_Non_Defaulted_Accounts' then displayname = 'EAD_of_Non-Defaulted_Accounts';
run;
proc sort data=rt30vars; by varnum; run;



******************************************************************************************************************;
******************************************************************************************************************;
******************************************************************************************************************;

data RT40;
set NZRRAP.DT4_RT40_FINAL_RPTG_VARS;
where process_mth_tm_id = &mth_tm_id.;

	RRS = DT4_RISK_RT_KEY_VAL;
/*	EADF_Segments = compress(put(EAD_BASEL_SEG_NUM,4.));*/
	EADF_Segments = DT4_EAD_SEG_KEY_VAL;
	Estimated_IRB_EADF_Pct = compress(put(round(PREDICTED_EAD*100,0.01),6.2));
	Realized_IRB_EADF_Pct = compress(put(round(REALIZED_EAD*100,0.01),6.2));
	Breach_Recognized = compress(put(BREACH,1.));
	EAD_of_Defaulted_Accounts = compress(put(round(EAD_DEF,1),20.));
	EAD_of_Non_Defaulted_Accounts = compress(put(round(EAD_NONDEF,1),20.));

keep RRS EADF_Segments Estimated_IRB_EADF_Pct Realized_IRB_EADF_Pct Breach_Recognized 
	EAD_of_Non_Defaulted_Accounts EAD_of_Defaulted_Accounts ;

	array vars _character_;
	do _n_=1 to dim(vars);
		if vars(_n_) = '.' then vars(_n_)= '';
	end;
run;

proc sort data=RT40; by RRS EADF_Segments; run;


proc contents noprint data=rt40 out=rt40vars(keep=name varnum); run;
data rt40vars; 
	set rt40vars;
	displayname=name;
	if name = 'EAD_of_Non_Defaulted_Accounts' then displayname = 'EAD_of_Non-Defaulted_Accounts';
run;
proc sort data=rt40vars; by varnum; run;



******************************************************************************************************************;
******************************************************************************************************************;
******************************************************************************************************************;

%macro create_xml;

%let xml_output_file = &owftp./rk/outgoing/DT4_&yrmth..xml;
/*%let xml_output_file = /u/s2809211/projects/rrap/DT4/DT4_&yrmth..xml;*/

data _null_;
	file "&xml_output_file." ;
	put @1 '<?xml version="1.0" encoding="UTF-8"?>';
	put @1 "<!--Scotiabank DT4 XML file for &YRMTH. generated on &sysdate9. at &systime. -->";
	put @1 '<DT4 type="schema" guid="f66f590c-fbca-485d-afd3-31c6d716e48f" xsi:noNamespaceSchemaLocation="https://www.osfi-bsif.gc.ca/Eng/Docs/DT4_xml.xml" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">';
	put @5 '<DT4>';
*	put @9 '<FirmID>';
*	put @13 '<value>AE</value>';
*	put @9 '</FirmID>';
run;


	%let RTs=%str(05,10,11,12,18,20,30,40);
	%let count=%sysfunc(countw(&RTs));

	%do j = 1 %to &count.;
		%let RT = %scan(&RTs.,&j,%str(,)); 
		data _null_;
			set RT&RT.VARS;
			n=compress(put(_n_,best.));
			call symputx('var'!!n,compress(name));
			call symputx('dvar'!!n,compress(displayname));
			call symputx('nvars',_n_);
		run;

		data _null_;
			file "&xml_output_file." mod;
			set rt&RT. end=last;
			if _n_ = 1 then do;
				put @9 "<_x0030_&RT.>";
			end;
			put @13 "<_x0030_&RT._x0020_Repeat_x0020_Group>";

			%do i = 1 %to &nvars.;
				if missing(&&var&i.) then do; end;
				else do;
					put @17 "<&&dvar&i.>";
					put @21 '<value>' @28 &&var&i. @(28+length(&&var&i.)) '</value>'; 
					put @17 "</&&dvar&i.>";
				end;
			%end;

			put @13 "</_x0030_&RT._x0020_Repeat_x0020_Group>";
			*put ;

			if last then do;
				put @9 "</_x0030_&RT.>";
				put; put; put;
			end;
		run;

	%end;


data _null_;
	file "&xml_output_file." mod;
	put @5 '</DT4>';
	put @1 '</DT4>';
run;

%mend create_xml;

%create_xml;


*line 167 enter null values for missing variables;
*if missing(&&var&i.) then var_length=0; *else var_length = length(&&var&i.);
