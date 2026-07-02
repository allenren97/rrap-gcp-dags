
***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*  J_rrap_exception_report_4_RWA_by_Schedule.sas
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  N/A
*  
*  Purpose: RWA by Schedule Report
*
*  Frequency: Monthly
*
*  Notes: 
*  		  
*
*	Change Log:
*	2023-10-21: Hadi Dimashkieh - Initial Development 

***************************************************************************************************************************;

%rrap_dlgd_autoexec;



%let file_path=&rrap_dir./flat_files/rrap/;
%let file_name=RWA_by_Schedule_&mth_end_dt..xls;


data _null_;
	infile "&rrap_dir/params/rrap/rrap_exception_email_list.txt";
	input;
	if _N_ =2 then do;
	CALL SYMPUT("email_list",_infile_);
	end;
	if _N_ =3 then do;
	CALL SYMPUT("si_email_list",_infile_);
	end;
	CALL SYMPUT('MonthYear', CATX(' ',PUT(INTNX('month',"&start_period_dt"d,0,'E'),monname10.),PUT(YEAR(INTNX('month',"&start_period_dt"d,0,'E')),4.)));
run;
%put &email_list;
%put &si_email_list;



%macro rwa_sched();
	%macro cc; %mend cc;

%let loopno=0;

%do mth_tm_id = %eval(&mth_tm_id.-40) %to &mth_tm_id. %by 40;

%let loopno = %eval(&loopno. +1);

%PUT Loop Number: &loopno.;
proc sql noprint;
	select put(tm_lvl_end_dt,monyy7.) into :mth_&loopno. 
		from nzrrap.tm_dim
		where tm_id = &mth_tm_id. and tm_lvl = 'Month';
quit;


proc sql;
connect using nzrrap as nzcon;
create table temp_&mth_tm_id. as select * from connection to nzcon(
	SELECT a.basel_acct_id

	,case when a.BCAR_SCHED_NUM_50 is null then 'Missing'
				else a.BCAR_SCHED_NUM_50
	end as BCAR_SCHED_NUM_50

	,case when a.BCAR_SCHED_NM is null then 'Missing'
		  else a.BCAR_SCHED_NM
	end as BCAR_SCHED_NM 

	,case when b.SRC_SYS_CD = 'MOR' then trim(b.BASEL_PRD_TP_CD) || ' BNS'
		 when b.SRC_SYS_CD = 'TNG-MOR' then trim(b.BASEL_PRD_TP_CD) || ' TNG'
		 else b.BASEL_PRD_TP_CD

	end as BASEL_PRD_TP_CD
		,a.PD_FLRD_RPTG_RTO, a.DLGD_RPTG_RTO
		,(a.NETEAD_BEFORECRM_DRAWN + a.NETEAD_UNDRAWN)/1000 AS EAD_BEFORE_CRM
		,(a.NETEAD_DRAWN + a.NETEAD_UNDRAWN)/1000 AS EAD_AFTER_CRM
		,a.RWA_DRAWN/1000 as RWA_DRAWN 
		,a.RWA_UNDRAWN/1000 as RWA_UNDRAWN
		,(a.PD_FLRD_RPTG_RTO * a.DLGD_RPTG_RTO * (a.NETEAD_DRAWN + a.NETEAD_UNDRAWN)/1000) AS EL
	FROM &RRAP_DB..DT4_RT18_EST_ER_VARS a left JOIN  &RRAP_DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT b
		ON a.MTH_TM_ID =b.MTH_TM_ID  AND a.BASEL_ACCT_ID = b.BASEL_ACCT_ID 
	WHERE a.MTH_TM_ID = &mth_tm_id. 
/*		and a.BCAR_SCHED_NUM_50 is not null*/
);
quit;





proc sql;
	create table a AS
	SELECT 
		'Total:' as BCAR_SCHED_NUM_50, '' as BCAR_SCHED_NM, '' as BASEL_PRD_TP_CD
		,count(1) as ACCOUNT_COUNT
		,round(sum(EAD_BEFORE_CRM),0.01) format=comma20.2 as EAD_BEFORE_CRM
		,round(sum(EAD_AFTER_CRM),0.01) format=comma20.2 as EAD_AFTER_CRM
		,round(sum(RWA_DRAWN),0.01) format=comma20.2 as RWA_DRAWN
		,round(sum(RWA_UNDRAWN),0.01) format=comma20.2 as RWA_UNDRAWN
		,round(sum(EL),0.01) format=comma20.2 as EL
	FROM temp_&mth_tm_id. 
	GROUP BY 1,2,3
	ORDER BY 1,2,3;
quit;

proc sql;
	create table b AS
	SELECT 
		BCAR_SCHED_NUM_50, BCAR_SCHED_NM, '' as BASEL_PRD_TP_CD
		,count(1) as ACCOUNT_COUNT
		,round(sum(EAD_BEFORE_CRM),0.01) format=comma20.2 as EAD_BEFORE_CRM
		,round(sum(EAD_AFTER_CRM),0.01) format=comma20.2 as EAD_AFTER_CRM
		,round(sum(RWA_DRAWN),0.01) format=comma20.2 as RWA_DRAWN
		,round(sum(RWA_UNDRAWN),0.01) format=comma20.2 as RWA_UNDRAWN
		,round(sum(EL),0.01) format=comma20.2 as EL
	FROM temp_&mth_tm_id. 
	GROUP BY 1,2,3
	ORDER BY 1,2,3;
quit;

proc sql;
	create table c AS
	SELECT 
		BCAR_SCHED_NUM_50, BCAR_SCHED_NM, BASEL_PRD_TP_CD
		,count(1) as ACCOUNT_COUNT
		,round(sum(EAD_BEFORE_CRM),0.01) format=comma20.2 as EAD_BEFORE_CRM
		,round(sum(EAD_AFTER_CRM),0.01) format=comma20.2 as EAD_AFTER_CRM
		,round(sum(RWA_DRAWN),0.01) format=comma20.2 as RWA_DRAWN
		,round(sum(RWA_UNDRAWN),0.01) format=comma20.2 as RWA_UNDRAWN
		,round(sum(EL),0.01) format=comma20.2 as EL
	FROM temp_&mth_tm_id. 
	GROUP BY 1,2,3
	ORDER BY 1,2,3;
quit;

data m;
attrib BCAR_SCHED_NUM_50 length=$10 BCAR_SCHED_NM Length=$150 BASEL_PRD_TP_CD length=$25;
merge  a b c;
by BCAR_SCHED_NUM_50 BCAR_SCHED_NM BASEL_PRD_TP_CD;
mth = "mth_&loopno.";
run;


proc transpose data=m out=tran;
	by BCAR_SCHED_NUM_50 bcar_sched_nm basel_prd_tp_cd;
	var account_count ead_before_crm ead_after_crm rwa_drawn rwa_undrawn el;
	id mth;
run;

data tran;
set tran;
if missing(BASEL_PRD_TP_CD) then srt=1;
	else srt=0;
run;


proc sort data=tran out=m_&loopno.; by BCAR_SCHED_NUM_50 bcar_sched_nm basel_prd_tp_cd _name_; run;

%end;



data d;
	merge m_1 m_2;
	by BCAR_SCHED_NUM_50 bcar_sched_nm basel_prd_tp_cd _name_;
	if missing(basel_prd_tp_cd) then basel_prd_tp_cd = 'Subtotal:';
	if bcar_sched_nm = 'Total:' and basel_prd_tp_cd = 'Subtotal:' then basel_prd_tp_cd = '';
	diff = mth_2 - mth_1;
	diff_pct = diff/mth_1;
	if BCAR_SCHED_NUM_50 = 'Total:' then basel_prd_tp_cd ='';
	rename mth_1 = &mth_1. mth_2 = &mth_2. ;
run;
proc sort data=d out=d(drop=srt); by BCAR_SCHED_NUM_50 bcar_sched_nm srt; run;













*******************************************************************************************************************************;
************ Plug into existing code to generate the Excel Report *************************************************************;
*******************************************************************************************************************************;
*******************************************************************************************************************************;




%let current_qname = &mth_2;
%let prev_qname = &mth_1;

proc format;
picture dollarc
low - high = '000,000,000,009' ;
run;

data final_result;
set d; 
BCAR_SCHED_NUM_50 = "'"||BCAR_SCHED_NUM_50;
rename BCAR_SCHED_NM = BCAR_SCHED_NUM _name_ = name BASEL_PRD_TP_CD = subgroup;
format &prev_qname &current_qname diff comma20. diff_pct percent12.2;
run;


Ods tagsets.ExcelXP path="&file_path" file="&file_name" options(sheet_interval='none'); 
Title "";
%let color1=gray;
%let color2=lightgrey;
%let color3=lightblue;

proc report data=work.final_result split='~' missing spanrows
style(summary)=[FONT_WEIGHT = BOLD]
style(lines)=[fontweight=bold fontsize=5]
;
*column seq1 BCAR_SCHED_NUM seq2 subgroup seq3 direct name &prev_qname &current_qname diff diff_pct seq2 seq4;
column  BCAR_SCHED_NUM_50 BCAR_SCHED_NUM  subgroup  name &prev_qname &current_qname diff diff_pct  ;

*define seq1 /order noprint;
define BCAR_SCHED_NUM_50 /"Schedule Number" order width=5;  
define BCAR_SCHED_NUM /"Schedule" order width=30;
*define seq2 /order noprint;
define subgroup /"Product" order width=15;
*define seq3 /order noprint ;
*define direct /"Insured/Uninsured~(Direct/Indirect)" group width=10;
*define seq4 /order noprint;
define name  / order order=data "Name" width=20;
define &prev_qname  / "&prev_qname" across analysis width=5 ;* format=dollarc.;
define &current_qname /"&current_qname" analysis width=5 ;* format=dollarc.;
define diff /"$ Difference" analysis ;* format=dollarc.;
define diff_pct /"% Difference" analysis;*  format=percent8.2;


compute BCAR_SCHED_NUM_50;
CALL DEFINE('_C1_', "style", "STYLE=[FONT_WEIGHT = BOLD VERTICALALIGN=m fontsize=3]"); 
endcomp;
compute BCAR_SCHED_NUM;
CALL DEFINE('_C1_', "style", "STYLE=[FONT_WEIGHT = BOLD VERTICALALIGN=m fontsize=3]"); 
endcomp;
compute subgroup;
CALL DEFINE('_C2_', "style", "STYLE=[FONT_WEIGHT = BOLD VERTICALALIGN=m fontsize=3]"); 
endcomp;
/*compute direct;*/
/*CALL DEFINE('_C6_', "style", "STYLE=[FONT_WEIGHT = BOLD VERTICALALIGN=m fontsize=3]"); */
/*endcomp;*/
compute name;
if name = 'ead_before_crm' then CALL DEFINE('_c3_', "style", "STYLE={background=&color1}"); 
if name = 'ead_after_crm'  then CALL DEFINE('_c3_', "style", "STYLE={background=&color2}"); 
if name = 'RWA_DRAWN'      then CALL DEFINE('_c3_', "style", "STYLE={background=&color3}"); 
if name = 'RWA_UNDRAWN'    then CALL DEFINE('_c3_', "style", "STYLE={background=&color3}");
endcomp;

compute &prev_qname;
if name = 'ead_before_crm' then CALL DEFINE('_c4_', "style", "STYLE={background=&color1}"); 
if name = 'ead_after_crm'  then CALL DEFINE('_c4_', "style", "STYLE={background=&color2}"); 
if name = 'RWA_DRAWN'      then CALL DEFINE('_c4_', "style", "STYLE={background=&color3}"); 
if name = 'RWA_UNDRAWN'    then CALL DEFINE('_c4_', "style", "STYLE={background=&color3}"); 

endcomp;

compute &current_qname;
if name = 'ead_before_crm' then CALL DEFINE('_c5_', "style", "STYLE={background=&color1}"); 
if name = 'ead_after_crm'  then CALL DEFINE('_c5_', "style", "STYLE={background=&color2}"); 
if name = 'RWA_DRAWN'      then CALL DEFINE('_c5_', "style", "STYLE={background=&color3}"); 
if name = 'RWA_UNDRAWN'    then CALL DEFINE('_c5_', "style", "STYLE={background=&color3}"); 
endcomp;

compute diff;
if name = 'ead_before_crm' then CALL DEFINE('_c6_', "style", "STYLE={background=&color1}"); 
if name = 'ead_after_crm'  then CALL DEFINE('_c6_', "style", "STYLE={background=&color2}"); 
if name = 'RWA_DRAWN'      then CALL DEFINE('_c6_', "style", "STYLE={background=&color3}"); 
if name = 'RWA_UNDRAWN'    then CALL DEFINE('_c6_', "style", "STYLE={background=&color3}"); 
endcomp;

compute diff_pct;
if name = 'ead_before_crm' then CALL DEFINE('_c7_', "style", "STYLE={background=&color1}"); 
if name = 'ead_after_crm'  then CALL DEFINE('_c7_', "style", "STYLE={background=&color2}"); 
if name = 'RWA_DRAWN'      then CALL DEFINE('_c7_', "style", "STYLE={background=&color3}"); 
if name = 'RWA_UNDRAWN'    then CALL DEFINE('_c7_', "style", "STYLE={background=&color3}"); 
endcomp;
compute before _PAGE_;
line "RWA by Schedule &mth_end_dt.";
endcomp;
run;

ods tagsets.ExcelXP close; run;



***********************************************************************************************************;

FILENAME OUTMAIL EMAIL ATTACH=("&file_path.&file_name")
SUBJECT= "[RRAP] RWA by Schedule Report - &MonthYear.";
DATA _NULL_;
     FILE OUTMAIL
     TO= (&si_email_list);
     put;

RUN;


%mend rwa_sched;

%rwa_sched;
