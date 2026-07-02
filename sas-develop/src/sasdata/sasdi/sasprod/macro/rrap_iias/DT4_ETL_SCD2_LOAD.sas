
***************************************************************************************************************************;
*
*  DT4_ETL_SCD2_LOAD macro
*  
*  Purpose: Macro called from ETL SCD2 job to populate target dimension or lookup table
*
*  Frequency: On demand
*
*  Notes: Code below developed for DT4 processes.  
*  		  
*
*	Change Log:
*	2021-09-24: Hadi Dimashkieh - Initial Development
*   2023-01-10: Hadi Dimashkieh - Move duplicate check and abort to this macro.
*   2023-01-25: Hadi Dimashkieh - Create back up of dimension table.
*
*
***************************************************************************************************************************;

%macro DT4_ETL_SCD2_LOAD();

** Create a backup of the dimension table.;
data _null_;
	dt=datepart(datetime());
	hr=hour(datetime());
	min=minute(datetime());
	tmstmp = cats(put(dt,yymmddn.),'_',put(hr,z2.),put(min,z2.));
	call symputx('tmstmp',tmstmp);
run;

proc sql;
connect using nzrrap as nzcon;
execute(create table &EDRTLRFRGP1D..&target._&tmstmp. as (select * from &RRAP_DB..&TARGET) with data; commit;) by nzcon;
quit;


proc sql noprint;
	select count(1) into :count_target from NZRRAP.&TARGET.;
quit;

%if &count_target eq 0 %then %do;
	%let YRMTH=200010;
	%let YRMTH_PREV=200010;
%end;

/********************************************************************************************************************/
proc sort data=&target. dupout=duplicates nodupkey; by &key_fields.; run;

proc sql noprint;
	select count(1) into :count_dups from duplicates;
quit;

%macro abort_process;
	%if &count_dups. NE 0 %then %do;
		%PUT;
		%PUT DUPLICATES IN SOURCE FILE. PLEASE FIX BEFORE PROCEEDING.;
		%PUT;
		%abort abend;
	%end;
%mend abort_process;
%abort_process;
/********************************************************************************************************************/

data &TARGET.;
set &TARGET.;
	EFF_FROM_YR_MTH = "&yrmth.";
	EFF_TO_YR_MTH = '999912';
	CRNT_F = 'Y';
	format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.6;
	INSRT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
	UPDT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
run;


/********************************************************************************************************************/
/********************************************************************************************************************/
/********************************************************************************************************************/
/********************************************************************************************************************/


proc sql noprint;
	select vars into :var_names separated by ' ' from varnames;

	select catx('=',vars,catx('','EX_',substr(vars,1,28))) into :ex_names separated by ' ' from varnames ;
	select catx('=',vars,catx('','NEW_',substr(vars,1,28))) into :NEW_names separated by ' ' from varnames ;

	select catx('=',vars,catx('','EX_',substr(vars,1,28))) into :z_ex_names separated by '; ' from varnames ;
	select catx('=',vars,catx('','NEW_',substr(vars,1,28))) into :z_NEW_names separated by '; ' from varnames ;
quit;

%put &var_names.;
%put &ex_names.;
%put &new_names.;

%put %nrquote(&z_ex_names.);
%put %nrquote(&z_new_names.);


data stagnant;
	set NZRRAP.&TARGET.;
	where CRNT_F='N';
run;

data existing;
	set NZRRAP.&TARGET.;
	where "&yrmth." between EFF_FROM_YR_MTH and EFF_TO_YR_MTH;
	DIGEST_EX = md5(catx('|',&digest_fields.));
	rename &ex_names.;
run;
proc sort data=existing; by  &key_fields.; run;


data newfile;
	set &TARGET.;
	DIGEST_NEW = md5(catx('|',&digest_fields.));
	rename &new_names.;
run;
proc sort data=newfile; by  &key_fields.; run;


data new(keep=&key_fields. &var_names.  CRNT_F INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP) 
	close(keep=&key_fields. &var_names. &surrogate_key. CRNT_F INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP) 
	unaffected(keep=&key_fields. &var_names. &surrogate_key. CRNT_F INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP) 
	changed(keep=&key_fields. &var_names. CRNT_F INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP);
merge newfile(in=new) existing(in=exist);
by &key_fields.;
if new and not exist then do;
	&z_NEW_names.;
	EFF_FROM_YR_MTH = "&yrmth.";
	EFF_TO_YR_MTH = '999912';
	INSRT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
	UPDT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
	CRNT_F='Y';
	output new ;
end;

if exist and not new then do;
/*	&z_ex_names.;*/
/*	EFF_FROM_YR_MTH = ex_EFF_FROM_YR_MTH;*/
/*	EFF_TO_YR_MTH = ex_EFF_TO_YR_MTH;*/
/*	CRNT_F='Y';*/
/*	output unaffected;*/
		&z_ex_names.;
		EFF_FROM_YR_MTH = ex_EFF_FROM_YR_MTH;
		EFF_TO_YR_MTH = "&yrmth_prev.";
			UPDT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;

		CRNT_F='N';
	output close;
end;

if exist and new then do;

	IF DIGEST_EX = DIGEST_NEW then do;
		&z_NEW_names.;
		EFF_FROM_YR_MTH = ex_EFF_FROM_YR_MTH;
		EFF_TO_YR_MTH = new_EFF_TO_YR_MTH; 
		output unaffected;
	end;
	IF DIGEST_EX NE DIGEST_NEW then do;
		&z_ex_names.;
		EFF_FROM_YR_MTH = ex_EFF_FROM_YR_MTH;
		EFF_TO_YR_MTH = "&yrmth_prev.";
			UPDT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;

		CRNT_F='N';
	output close;

	&z_NEW_names.;
	EFF_FROM_YR_MTH = "&yrmth.";
	EFF_TO_YR_MTH = '999912';
	CRNT_F='Y';
		INSRT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
		UPDT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;

	output changed;
	end;

end;
run;

%if &surrogate_key_flag. eq Y %then %do;

	%if &count_target eq 0 %then %do;
		%let new_starting_sk = &initial_surrogate_key_value.;
	%end;
	%else %do;
		proc sql noprint;
			select max(&surrogate_key.) into :new_starting_sk
			from nzrrap.&TARGET.;
		quit;
	%end;
%PUT HADI &new_starting_sk.;
	data changed_new;
		set changed new;
		retain &surrogate_key. &new_starting_sk.;
/*		if _n_=1 then &surrogate_key. = &new_starting_sk. +1;*/
/*			else &surrogate_key. = &surrogate_key. +1;*/
		&surrogate_key. = &surrogate_key. +1;
	run;
%end;
%else %do;
	data changed_new;
		set changed new;
	run;
%end;
	



data REBUILD;
set unaffected close changed_new stagnant;
run;
proc sort data=REBUILD; by &key_fields. ; run;

proc sql;
connect using nzrrap as nzcon;
execute(truncate table &RRAP_DB..&TARGET. IMMEDIATE;) by nzcon;
execute(commit;) by nzcon;
quit;

proc append base=NZRRAP.&TARGET. data=REBUILD force; run;

proc sql noprint;
	select count(1) into :count_new from new;
	select count(1) into :count_changed from changed;
	select count(1) into :count_close from close;
quit;

%if &count_new. gt 0 or &count_changed. gt 0 or &count_close. gt 0 %then %do;
	%PUT New data has been introduced into the table &TARGET..;
	FILENAME OUTMAIL EMAIL 
		ATTACH=("&sourcefile.")
		SUBJECT= "New data introduced into &RRAP_DB..&TARGET..";

		DATA _NULL_;
			FILE OUTMAIL
			TO=("hadi.dimashkieh@scotiabank.com" "hadi.dimashkieh@scotiabank.com")
			/*CC=(&email_list)*/
			;
			PUT "Hi,";
			PUT " ";
			PUT "Please find attached the new data loaded into &RRAP_DB..&TARGET. for &YRMTH..";
			PUT " ";
		run;
%end;
%else %do;
	%PUT "No new data introduced into &RRAP_DB..&TARGET. for &YRMTH..";
%end;
%mend DT4_ETL_SCD2_LOAD;
