
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

%global YYYYMMDD;
%LET OUTLANDING=&ACAPRPT;
%LET LIB=ACAPTEMP;

%let datetime_start = %sysfunc(TIME());
%put >>> START TIME: %sysfunc(datetime(),datetime14.);

options validvarname = any source source2;

										   
%PUT YEARMONTH IS &YEARMONTH;
%PUT MTH_TM_ID IS &MTH_TM_ID;

/*Hadi Dimashkieh - 19JUL2016 - Used for replacement of CRNT_F with EFF_FROM and EFF_TO dates*/
PROC SQL NOPRINT;
	*select TM_LVL_ST_DT format=yymmn6. into :mth_tm_id_yrmth from netcon.tm_dim where tm_id = &mth_tm_id and tm_lvl='Month';
     select TM_LVL_ST_DT format=yymmn6. into :mth_tm_id_yrmth from EDRTLRT.tm_dim where tm_id = &mth_tm_id and tm_lvl='Month';
QUIT;

/*CREATE A DATASET WITH YYYYMMDD FORMAT FOR THE PROCESSING DATE*/
DATA _CHARDATE_;
	TODAYDATE=today();
	FORMAT TODAYDATE YYMMDD10.;
	TODAYYEAR=PUT(YEAR(TODAYDATE),4.);
	FORMAT TODAYYEAR $4.;

	IF MONTH(TODAYDATE)<10 THEN
		DO;
			TODAYMONTH='0'||PUT(MONTH(TODAYDATE),1.);
			FORMAT TODAYMONTH $2.;
		END;
	ELSE
		DO;
			TODAYMONTH=PUT(MONTH(TODAYDATE),2.);
			FORMAT TODAYMONTH $2.;
		END;

	IF DAY(TODAYDATE)<10 THEN
		DO;
			TODAYDAY='0'||PUT(DAY(TODAYDATE),1.);
			FORMAT TODAYDAY $2.;
		END;
	ELSE
		DO;
			TODAYDAY=PUT(DAY(TODAYDATE),2.);
			FORMAT TODAYDAY $2.;
		END;

	CHAR_PROCESSING_DATE=TODAYYEAR||TODAYMONTH||TODAYDAY;
RUN;

/*STORE THE ABOVE CREATED PROCESSING DATE INTO A MACRO VARIABLE*/
PROC SQL NOPRINT;
	SELECT CHAR_PROCESSING_DATE INTO :CHAR_PROCESSING_DATE FROM _CHARDATE_;
QUIT;

/*CREATE A DATASET WITH YYYYMMDD FORMAT FOR CURRENT DATE OR SYSTEM DATE*/
DATA _CURRENTDATE_;
	TODAYDATE=DATE();
	FORMAT TODAYDATE YYMMDD10.;
	TODAYYEAR=PUT(YEAR(TODAYDATE),4.);
	FORMAT TODAYYEAR $4.;

	IF MONTH(TODAYDATE)<10 THEN
		DO;
			TODAYMONTH='0'||PUT(MONTH(TODAYDATE),1.);
			FORMAT TODAYMONTH $2.;
		END;
	ELSE
		DO;
			TODAYMONTH=PUT(MONTH(TODAYDATE),2.);
			FORMAT TODAYMONTH $2.;
		END;

	IF DAY(TODAYDATE)<10 THEN
		DO;
			TODAYDAY='0'||PUT(DAY(TODAYDATE),1.);
			FORMAT TODAYDAY $2.;
		END;
	ELSE
		DO;
			TODAYDAY=PUT(DAY(TODAYDATE),2.);
			FORMAT TODAYDAY $2.;
		END;

	CURRENTDATE=TODAYYEAR||TODAYMONTH||TODAYDAY;
RUN;

/*STORE THE ABOVE CREATED SYSTEM DATE INTO A MACRO VARIABLE*/
PROC SQL NOPRINT;
	SELECT CURRENTDATE INTO :CURRENTDATE FROM _CURRENTDATE_;
QUIT;

/*CREATE THE FINAL DATASET BEFORE EXPORTING*/


%PUT >> Processing_Month_Time_ID = &Processing_Month_Time_ID.;
%global mth_tm_id;
%global tm_lvl_st_dt;
%global tm_lvl_end_dt;
%global dtime;

proc sql noprint;
	select tm_id as mth_tm_id, tm_lvl_st_dt, tm_lvl_end_dt , datetime() as dtime format datetime25.
	into :mth_tm_id, :tm_lvl_st_dt, :tm_lvl_end_dt, :dtime from  EDRTLRT.TM_DIM
	where tm_id= &Processing_Month_Time_ID.;
quit;

%put &mth_tm_id, &tm_lvl_st_dt, &tm_lvl_end_dt, &dtime;

%GLOBAL OW_FTP;
%LET OW_FTP = %SYSGET(OW_FTP); /* SAS gets environment variable*/

/*** prod landing source path ***/
%let nobsaf=0;
%let nobsbf=0;
%let WITHZERONET=; /**BEF_NEG_NET_;**/

/** DB2 I/O Peformance Issues needed to copy DB2 files to local servers first  **/
data &lib..BASEL_ACAP_EXPSR_EXTR ;
	set EDRRAPT.BASEL_ACAP_EXPSR_EXTR (BULKLOAD=YES BL_METHOD=CLILOAD rename=(PD_BAND=PDx));
	PDn=round(input(PDx,8.));
	PD_BAND = put(PDn,$2.);
	drop pdx pdn;
	where mth_end_dt="&tm_lvl_end_dt"d;
run;

data &lib..Basel_ccar_pd_curve_extr ;
	set &DB..Basel_ccar_pd_curve_extr (BULKLOAD=YES BL_METHOD=CLILOAD rename=(PD_BAND=PDx));
	PDn=round(input(PDx,8.));
	PD_BAND = put(PDn,$2.);
	drop pdx pdn;
	where mth_end_dt="&tm_lvl_end_dt"d;
run;

DATA &LIB..BASEL_AIRB_GUARNT_LKP ;
	SET &DB..BASEL_AIRB_GUARNT_LKP (BULKLOAD=YES BL_METHOD=CLILOAD);
/*	WHERE CRNT_F='Y';*/
	WHERE "&mth_tm_id_yrmth." BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH;
run;

data &lib..BASEL_AIRB_PRD_LKP ;
	set &DB..BASEL_AIRB_PRD_LKP (BULKLOAD=YES BL_METHOD=CLILOAD);
/*	WHERE CRNT_F='Y';*/
	WHERE "&mth_tm_id_yrmth." BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH;
run;

options validvarname=any;

%MACRO PURGING;

%PUT MTH_TM_ID IS &MTH_TM_ID;
PROC SQL NOPRINT;
SELECT TRIM(MTH_CLNDR_CD) INTO :CAL_MONTH FROM EDRTLRT.TM_DIM WHERE TM_ID=&MTH_TM_ID;
QUIT;

%PUT CALENDAR MONTH IS &CAL_MONTH;

%IF (&CAL_MONTH=January OR &CAL_MONTH=April OR &CAL_MONTH=July OR &CAL_MONTH=October) %THEN %DO;
%PUT PURGING ALL REPORTS;
X rm &ACAPRPT/DR-AIRB*.*;
%END;
%MEND;

%PURGING;

%macro ACAP_FILES_GENERATE(WITHZERONET=);
options validvarname=any;

%if &WITHZERONET ne %then %do;
/** BEFORE ZERO NETTING **/
%let reqvars=
UNQ_RECD_ID
CCAR_BASEL_PRD_TP_NM
PD_BAND
TRNST_NUM
PRD_ID
LEGAL_ENTITY
PD_90_DAY_F
UNCONDTNLY_CNCLBL
BEFR_ZERO_NET_ADJ_OS_BAL_AMT
EAD_PC
CRNCY_CD
EXPCTD_LOSS_RTO_TEXT
BEFORE_ZERO_NET_UNDRAWN_AMT
ACCR_INTR_AMT
BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
PRTL_WRITE_OFF_AMT
LGD_FINAL_RPTG_RTO_TEXT
EAD_FINAL_RPTG_RTO_TEXT
INSUR_F
WGHTD_DLGD_RTO;

data BASEL_ACAP_EXPSR_EXTR_TMP0;
retain  &reqvars.;
set &LIB..BASEL_ACAP_EXPSR_EXTR
(keep= mth_end_dt  &reqvars. ) nobs = lastobs;
where (BEFR_ZERO_NET_ADJ_OS_BAL_AMT ne 0 or BEFORE_ZERO_NET_UNDRAWN_AMT ne 0);
dt = compress(put(mth_end_dt,yymmdd10.),'-');
rundt = compress(put(today(),yymmdd10.),'-');
tm = put(time(),tod8.);
call symput('YYYYMMDD',compress(dt));
call symput('rundt',compress(rundt));
format WGHTD_DLGD_RTO percent14.6;
/*call symput('nobsbf',_n_);*/
/*drop mth_end_dt;*/
run;

options validvarname = any;

data
&lib..EXPSR_&WITHZERONET.
&lib..SUB_DUNDEE_EXPSR_&WITHZERONET.
&lib..SUB_TNG_EXPSR_&WITHZERONET.
&lib..SUB_MTCC_EXPSR_&WITHZERONET.
&lib..SUB_NT_EXPSR_&WITHZERONET.
&lib..SUB_SMC_MAPLE_EXPSR_&WITHZERONET.
;
set BASEL_ACAP_EXPSR_EXTR_TMP0;
array cvar 
$ EXPCTD_LOSS_RTO_TEXT  LGD_FINAL_RPTG_RTO_TEXT EAD_FINAL_RPTG_RTO_TEXT EAD_PC;
/*do over cvar;*/
/*if indexc(cvar,'123456789') = 0 then cvar = ''; */
/*else cvar=trim(compress(cvar,'%'))||'0000%';*/
/*end;*/
if Legal_Entity IN ('DOM-SUB-SMC', 'DOM-SUB-MAPLE', 'DOM-SUB-NT','DOM-SUB-MTCC','DOM-SUB-DUNDEE') then EAD_FINAL_RPTG_RTO_TEXT = '100.00000%';
keep &reqvars.;
if Legal_Entity = 'DOM-BANK-ALONE' then output &lib..EXPSR_&WITHZERONET.;
else if Legal_Entity = 'DOM-SUB-DUNDEE' then output &lib..SUB_DUNDEE_EXPSR_&WITHZERONET.;
else if Legal_Entity = 'DOM-SUB-TNG' then output &lib..SUB_TNG_EXPSR_&WITHZERONET. ;
else if Legal_Entity = 'DOM-SUB-MTCC' then output &lib..SUB_MTCC_EXPSR_&WITHZERONET. ;
else if Legal_Entity = 'DOM-SUB-NT' then output &lib..SUB_NT_EXPSR_&WITHZERONET. ;
else if Legal_Entity IN ('DOM-SUB-SMC', 'DOM-SUB-MAPLE') then output &lib..SUB_SMC_MAPLE_EXPSR_&WITHZERONET. ;

rename
 UNQ_RECD_ID = 'Unique Identifier'n
CCAR_BASEL_PRD_TP_NM = 'Basel Product Type'n
PD_BAND = 'PD BAND'n
TRNST_NUM='TRNST NUM'n
PRD_ID='PRD ID'n
LEGAL_ENTITY = 'Legal Entity'n
PD_90_DAY_F = '90-Day-Past-Due-Flag'n
UNCONDTNLY_CNCLBL = 'Uncond Canc'n
BEFR_ZERO_NET_ADJ_OS_BAL_AMT = 'Exposures (Drawn)'n
EAD_PC = 'EAD %'n
CRNCY_CD = Currency
EXPCTD_LOSS_RTO_TEXT = EL
BEFORE_ZERO_NET_UNDRAWN_AMT = Undrawn
ACCR_INTR_AMT = 'Accrued Interest'n
BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT = ACL
PRTL_WRITE_OFF_AMT = 'Partial WO'n
LGD_FINAL_RPTG_RTO_TEXT = LGD
EAD_FINAL_RPTG_RTO_TEXT = EADF
INSUR_F= INSURER_F
WGHTD_DLGD_RTO=DLGD;
RUN;

data &lib..EXPSR_&WITHZERONET.;
SET &lib..EXPSR_&WITHZERONET.;;
call symput('nobsbf',_n_);
if ACL = . then ACL = 0;
run;

/** producing BEFORE ZERO control CTL table **/
/*** calculating HAS_TOTAL BEFORE ZERO for control file **/
proc sql threads noprint;
select
/*mth_end_dt*/
/*,legal_entity*/
sum('Exposures (Drawn)'n) format=25.2 as hash_total_bf
into :hashtotbf
from &lib..EXPSR_&WITHZERONET.
/*group by*/
/*mth_end_dt*/
/*,legal_entity*/
;
quit;

%put >>> BEFR_ZERO hash_total = &hashtotbf.;
DATA &lib..EXPSR_&WITHZERONET.&YYYYMMDD._ctl;
length 
YYYYMMDD 
rundt $10
nobs $6
hashtot $15;
file "&ACAPRPT./DR-AIRB-EXPOSURES-&WITHZERONET.&YYYYMMDD..ctl" ;**lrecl=203 
recfm=V ;
YYYYMMDD = "&YYYYMMDD.,";
rundt = "&rundt.,";
nobs = left(put(&nobsbf. ,6.));
n2= &hashtotbf.;
hashtot = left(put(n2 , 25.2));
/*hashtot = "&hashtotbf.";*/
DLM=",";
mtxtx=compress((rundt||nobs||DLM||hashtot),' ');
put
@1 YYYYMMDD
@10 mtxtx
;
/*@20 nobs*/
/*@29 DLM*/
/*@30 hashtot*/
/*;*/
output;
run;

%macro exportcsv_befznet(dsn=DR-AIRB-EXPOSURES-&WITHZERONET.);
data tmp_name;
dsname1 = trim(substr(translate("&dsn.",'_','-'),9));
dsname2 = trim(tranwrd(dsname1,'EXPOSURES','EXPSR'));
call symput ('dsname',trim(dsname2));
run;
%put >>> dsname = &dsname.;


data &dsname;
set &lib..&dsname;
pd_bandn = input('PD BAND'n,8.);
proc sort out= &lib..&dsname (drop= PD_BANDn )force;
by 'Basel Product Type'n
PD_BANDn ;
run;


proc export data = &lib..&dsname  outfile="&ACAPRPT./&dsn.&YYYYMMDD..csv"
dbms=csv replace ;
run;
/*x 'sed s/\"//g &ACAPRPT./&dsn.&YYYYMMDD..csv';*/

%mend exportcsv_befznet;
%exportcsv_befznet(dsn=DR-AIRB-EXPOSURES-&WITHZERONET.);
%exportcsv_befznet(dsn=DR-AIRB-SUB-DUNDEE-EXPOSURES-&WITHZERONET.);
%exportcsv_befznet(dsn=DR-AIRB-SUB-TNG-EXPOSURES-&WITHZERONET.);
%exportcsv_befznet(dsn=DR-AIRB-SUB-MTCC-EXPOSURES-&WITHZERONET.);
%exportcsv_befznet(dsn=DR-AIRB-SUB-NT-EXPOSURES-&WITHZERONET.);
%exportcsv_befznet(dsn=DR-AIRB-SUB-SMC-MAPLE-EXPOSURES-&WITHZERONET.);
%end;

%else %if &WITHZERONET = %then %do;
/** AFTER ZERO NETTING *************************************/
%let reqvars=
UNQ_RECD_ID
CCAR_BASEL_PRD_TP_NM
PD_BAND
TRNST_NUM
PRD_ID
LEGAL_ENTITY
PD_90_DAY_F
UNCONDTNLY_CNCLBL
AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
EAD_PC
CRNCY_CD
EXPCTD_LOSS_RTO_TEXT
AF_ZERO_NET_UNDRAWN_AMT
ACCR_INTR_AMT
AF_ZERO_NET_ALWBL_CR_LOSS_AMT
PRTL_WRITE_OFF_AMT
LGD_FINAL_RPTG_RTO_TEXT
EAD_FINAL_RPTG_RTO_TEXT
INSUR_F
WGHTD_DLGD_RTO;

data BASEL_ACAP_EXPSR_EXTR_TMP0;
retain  &reqvars.;
set &LIB..BASEL_ACAP_EXPSR_EXTR
(keep= mth_end_dt  &reqvars. ) nobs = lastobs;
where
AF_ZERO_NET_ADJUSTED_OS_BAL_AMT ne 0  OR AF_ZERO_NET_UNDRAWN_AMT ne 0;
dt = compress(put(mth_end_dt,yymmdd10.),'-');
rundt = compress(put(today(),yymmdd10.),'-');
tm = put(time(),tod8.);
if EAD_FINAL_RPTG_RTO_TEXT = '' then EAD_FINAL_RPTG_RTO_TEXT = '0.000000%';
call symput('YYYYMMDD',compress(dt));
call symput('rundt',compress(rundt));
format WGHTD_DLGD_RTO percent14.6;
/*call symput('nobs',compress(lastobs));*/
/*call symput('nobsaf',_n_);*/
/*drop mth_end_dt;*/
run;

data
&lib..EXPSR_&WITHZERONET.&YYYYMMDD.
&lib..SUB_DUNDEE_EXPSR_&WITHZERONET.&YYYYMMDD.
&lib..SUB_TNG_EXPSR_&WITHZERONET.&YYYYMMDD.
&lib..SUB_MTCC_EXPSR_&WITHZERONET.&YYYYMMDD.
&lib..SUB_NT_EXPSR_&WITHZERONET.&YYYYMMDD.
&lib..SUB_SMC_MAPLE_EXPSR_&WITHZERONET.&YYYYMMDD.
;
set BASEL_ACAP_EXPSR_EXTR_TMP0;
array cvar 
$ EXPCTD_LOSS_RTO_TEXT  LGD_FINAL_RPTG_RTO_TEXT EAD_FINAL_RPTG_RTO_TEXT EAD_PC;
/*do over cvar;*/
/*if indexc(cvar,'123456789') = 0 then cvar = ''; */
/*else cvar=trim(compress(cvar,'%'))||'0000%';*/
/*end;*/

if Legal_Entity IN ('DOM-SUB-SMC', 'DOM-SUB-MAPLE', 'DOM-SUB-NT','DOM-SUB-MTCC','DOM-SUB-DUNDEE') then EAD_FINAL_RPTG_RTO_TEXT = '100.00000%';
keep &reqvars.;
if Legal_Entity = 'DOM-BANK-ALONE' then output &lib..EXPSR_&WITHZERONET.&YYYYMMDD.;
else if Legal_Entity = 'DOM-SUB-DUNDEE' then output &lib..SUB_DUNDEE_EXPSR_&WITHZERONET.&YYYYMMDD.;
else if Legal_Entity = 'DOM-SUB-TNG' then output &lib..SUB_TNG_EXPSR_&WITHZERONET.&YYYYMMDD. ;
else if Legal_Entity = 'DOM-SUB-MTCC' then output &lib..SUB_MTCC_EXPSR_&WITHZERONET.&YYYYMMDD. ;
else if Legal_Entity = 'DOM-SUB-NT' then output &lib..SUB_NT_EXPSR_&WITHZERONET.&YYYYMMDD. ;
else if Legal_Entity IN ('DOM-SUB-SMC', 'DOM-SUB-MAPLE') then output &lib..SUB_SMC_MAPLE_EXPSR_&WITHZERONET.&YYYYMMDD. ;
/*call symput('nobsaf',_n_);*/

rename
UNQ_RECD_ID = 'Unique Identifier'n
CCAR_BASEL_PRD_TP_NM = 'Basel Product Type'n
PD_BAND = 'PD BAND'n
TRNST_NUM='TRNST NUM'n
PRD_ID='PRD ID'n
LEGAL_ENTITY = 'Legal Entity'n
PD_90_DAY_F = '90-Day-Past-Due-Flag'n
UNCONDTNLY_CNCLBL = 'Uncond Canc'n
AF_ZERO_NET_ADJUSTED_OS_BAL_AMT = 'Exposures (Drawn)'n
EAD_PC = 'EAD %'n
CRNCY_CD = Currency
EXPCTD_LOSS_RTO_TEXT = EL
AF_ZERO_NET_UNDRAWN_AMT = Undrawn
ACCR_INTR_AMT = 'Accrued Interest'n
AF_ZERO_NET_ALWBL_CR_LOSS_AMT = ACL
PRTL_WRITE_OFF_AMT = 'Partial WO'n
LGD_FINAL_RPTG_RTO_TEXT = LGD
EAD_FINAL_RPTG_RTO_TEXT = EADF
INSUR_F=INSURER_F
WGHTD_DLGD_RTO=DLGD;
run;

data &lib..EXPSR_&WITHZERONET.&YYYYMMDD.;
SET &lib..EXPSR_&WITHZERONET.&YYYYMMDD.;
call symput('nobsaf',_n_);
if ACL = . then ACL = 0;
run;

%put >>> AF_ZERO NOBS_AF = &nobsaf.;

/** producing control AFTER ZERO CTL table **/
/*** calculating HASh_TOTAL for AFTER ZERO control file **/
proc sql threads noprint;
select 

/*sai updated code due to data problems for aug 19th run with inputs from Min*/
/*sum(input(EADF,percent8.6)) format=25.2 as hash_total_af*/
sum('Exposures (Drawn)'n) format 25.2 as hash_total_af into :hashtotaf
from &lib..EXPSR_&WITHZERONET.&YYYYMMDD.
;
quit;

%put >>> AF_ZERO hash_total = &hashtotaf.;

DATA &lib..EXPSR_&WITHZERONET.&YYYYMMDD._ctl;
length 
YYYYMMDD 
rundt $10
nobs $6
hashtot $15 n1 8;
file "&ACAPRPT./DR-AIRB-EXPOSURES-&WITHZERONET.&YYYYMMDD..ctl" ;/*lrecl=203 recfm=V ;*/
YYYYMMDD = "&YYYYMMDD.,";
rundt = "&rundt.,";
nobs = left(put(&nobsaf. ,6.));
n1= &hashtotaf.;
hashtot = left(put(n1 , 25.2));
/*hashtot = "&hashtotaf.";*/
DLM=",";
mtxtx=compress((rundt||nobs||DLM||hashtot),' ');
put
@1 YYYYMMDD
@10 mtxtx
;
/*@20 nobs*/
/*@29 DLM*/
/*@30 hashtot*/
/*;*/
output;
run;

/************
DR-AIRB-EXPOSURES-YYYYMMDD.csv  Legal_Entity = ''DOM-BANK-ALONE'
DR-AIRB-SUB-DUNDEE-EXPOSURES-YYYYMMDD.csv       Legal_Entity = 'DOM-SUB-DUNDEE'
DR-AIRB-SUB-TNG-EXPOSURES-YYYYMMDD.csv  Legal_Entity = 'DOM-SUB-TNG'
DR-AIRB-SUB-MTCC-EXPOSURES-YYYYMMDD.csv Legal_Entity = 'DOM-SUB-MTCC'
DR-AIRB-SUB-NT-EXPOSURES-YYYYMMDD.csv   Legal_Entity = 'DOM-SUB-NT'
DR-AIRB-SUB-SMC-MAPLE-EXPOSURES-YYYYMMDD.csv Legal_Entity IN ('DOM-SUB-SMC', 
'DOM-SUB-MAPLE')
***************/
%macro exportcsv(dsn=DR-AIRB-EXPOSURES-&WITHZERONET.&YYYYMMDD.);
data tmp_name;
dsname1 = trim(substr(translate("&dsn.",'_','-'),9,50));
dsname2 = trim(tranwrd(dsname1,'EXPOSURES','EXPSR'));
call symput ('dsname',trim(dsname2));
run;
%put >>> dsname = &dsname.;

data &dsname;
set &lib..&dsname;
pd_bandn = input('PD BAND'n,8.);
proc sort out= &lib..&dsname (drop= PD_BANDn )force;
by 'Basel Product Type'n
PD_BANDn ;
run;

proc export data = &lib..&dsname  outfile="&ACAPRPT./&dsn..csv"
dbms=csv replace ;
run;

/*x 'sed s/\"//g &ACAPRPT./&dsn.&YYYYMMDD..csv';*/

%mend exportcsv;
%exportcsv(dsn=DR-AIRB-EXPOSURES-&WITHZERONET.&YYYYMMDD.);
%exportcsv(dsn=DR-AIRB-SUB-DUNDEE-EXPOSURES-&WITHZERONET.&YYYYMMDD.);
%exportcsv(dsn=DR-AIRB-SUB-TNG-EXPOSURES-&WITHZERONET.&YYYYMMDD.);
%exportcsv(dsn=DR-AIRB-SUB-MTCC-EXPOSURES-&WITHZERONET.&YYYYMMDD.);
%exportcsv(dsn=DR-AIRB-SUB-NT-EXPOSURES-&WITHZERONET.&YYYYMMDD.);
%exportcsv(dsn=DR-AIRB-SUB-SMC-MAPLE-EXPOSURES-&WITHZERONET.&YYYYMMDD.);
%end;
%mend ACAP_FILEs_GENERATE;
%ACAP_FILES_GENERATE(WITHZERONET= );
%ACAP_FILES_GENERATE(WITHZERONET=BEF_NEG_NET_);

/***** To create PD Curve Flat FIles **/

/*copy db2 data to sas data for table Basel_ccar_pd_curve_extr*/
%PUT MTH_END_DT = "&MTH_END_DT"d;
DATA &LIB..Basel_ccar_pd_curve_extr;
SET &DBNAME..BASEL_CCAR_PD_CURVE_EXTR (BULKLOAD=YES BL_METHOD=CLILOAD);
WHERE MTH_END_DT = "&MTH_END_DT"d;
RUN;

%macro ACAP_PD_CURVE_FILES_GENERATE(WITHZERONET=);
/** Before zero Netting **/
%let reqvars=
CCAR_BASEL_PRD_TP_NM
PD_BAND
PD_VAL
PD_MIN_VAL
PD_MAX_VAL
;

PROC SQL NOPRINT;
SELECT MAX(MTH_END_DT) INTO :MTH_END_DT FROM &lib..Basel_ccar_pd_curve_extr;
CREATE TABLE Basel_ccar_pd_curve_extr AS SELECT * FROM &LIB..Basel_ccar_pd_curve_extr WHERE MTH_END_DT = &MTH_END_DT.;
QUIT;

data BASEL_CCAR_PD_CURVE_EXTR_TMP0;
retain  &reqvars.;
set Basel_ccar_pd_curve_extr
(keep= mth_end_dt &reqvars. 
rename=(PD_VAL=pdval PD_MIN_VAL = PDMINVAL PD_MAX_VAL = PDMAXVAL)) nobs = 
lastobs;
dt = compress(put(mth_end_dt,yymmdd10.),'-');
rundt = compress(put(today(),yymmdd10.),'-');
tm = put(time(),tod8.);
PD_VAL =put(pdval*100,12.6)||"%";
PD_MIN_VAL =put(pdMINval*100,12.4);
PD_MAX_VAL =put(pdMAXval*100,12.4);
call symput('YYYYMMDD',compress(dt));
call symput('rundt',compress(rundt));
call symput('nobs',_n_);
/*call symput('nobs',compress(lastobs));*/
/*drop mth_end_dt;*/
/*drop pdval;*/
;
run;

%put pd_curve >> YYYYMMDD = &YYYYMMDD.;

/*** calculating HAS_TOTAL for control file **/
proc sql threads noprint;
select distinct
/*mth_end_dt,*/
/*CCAR_BASEL_PRD_TP_NM,*/
sum(
%if &WITHZERONET= %then %do;
	PDVAL
%end;
%else %do;
	PDVAL
%end;
) as hash_total_pd_Curve 
into :hashtot 
from BASEL_CCAR_PD_CURVE_EXTR_TMP0
/*group by */
/*mth_end_dt*/
/*,CCAR_BASEL_PRD_TP_NM*/
;
quit;

%put >>> pd_Curve hash_total = &hashtot.;
%put >>> pd_curve nobs = &nobs.;

DATA BASEL_CCAR_PD_CURVE_EXTR_TMP0;
SET BASEL_CCAR_PD_CURVE_EXTR_TMP0;
DROP PDVAL;
keep &reqvars.;
RUN;

/** producing PD Curve control CTL table **/
DATA &lib..DR_AIRB_PD_CURVE_&WITHZERONET.&YYYYMMDD._ctl;
length 
YYYYMMDD 
rundt $10
nobs $6
hashtot $15 n1 8;
file "&ACAPRPT./DR-AIRB-PD-CURVE-&WITHZERONET.&YYYYMMDD..ctl" ; /*lrecl=203 recfm=V ;*/
YYYYMMDD = "&YYYYMMDD.,";
rundt = "&rundt. ,";
nobs = left(put(&nobs. ,6.));
n1= 100*&hashtot.;
hashtot = left(put(n1, 15.2));
DLM=",";
mtxtx=compress((rundt||nobs||DLM||hashtot),' ');
put
@1 YYYYMMDD
@10 mtxtx
;
/*@20 nobs*/
/*@29 DLM*/
/*@30 hashtot*/
/*;*/
output;
run;

data
&lib..DR_AIRB_PD_CURVE_&YYYYMMDD.;
set BASEL_CCAR_PD_CURVE_EXTR_TMP0;
drop PDMINVAL PDMAXVAL;
rename
CCAR_BASEL_PRD_TP_NM = 'Basel Product Type'n
PD_BAND = 'PD Band'n
PD_VAL  = 'Pd Value'n
PD_MIN_VAL = 'PD Min'n
PD_MAX_VAL = 'PD Max'n
;
run;

%macro exportcsv(dsn=DR-AIRB-PD-CURVE-&WITHZERONET.&YYYYMMDD.);
data tmp_name;
dsname2 = trim(substr(translate("&dsn.",'_','-'),1,50));
call symput ('dsname',trim(dsname2));
run;

%put >>> dsname = &dsname.;

data &dsname;
set &lib..&dsname;
pd_bandn = input('PD Band'n,8.);
proc sort out= &lib..&dsname (drop= PD_BANDn )force;
by 'Basel Product Type'n
PD_BANDn ;
run;
run;

proc export data = &lib..&dsname  outfile="&ACAPRPT./&dsn..csv" dbms=csv replace ;
run;

/*x 'sed s/\"//g &ACAPRPT./&dsn.&YYYYMMDD..csv';*/

%mend exportcsv;
%exportcsv(dsn=DR-AIRB-PD-CURVE-&WITHZERONET.&YYYYMMDD.);
%mend ACAP_PD_CURVE_FILES_GENERATE;
%ACAP_PD_CURVE_FILES_GENERATE(WITHZERONET=);

/**** To create LOOKUP flat files ***/

%macro ACAP_LOOKUP_FILES_GENERATE(WITHZERONET=);
/** Before zero Netting **/
%let reqvars=
CCAR_BASEL_PRD_TP_NM
PRD_SHT_NM
EXPSR_SUB_TP_CD
BASEL_1_ASST_TP_CD
REGULATORY_PRD_TP_CD
PD_BAND_CD
;
data BASEL_CCAR_LOOKUP_EXTR_TMP0;
retain  &reqvars.;
set &DB..BASEL_AIRB_PRD_LKP
(keep= CRNT_F EFF_FROM_YR_MTH EFF_TO_YR_MTH &reqvars. ) nobs = lastobs;
/*WHERE CRNT_F = 'Y';*/
WHERE "&mth_tm_id_yrmth." BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH;

rundt = compress(put(today(),yymmdd10.),'-');
tm = put(time(),tod8.);
call symput('rundt',compress(rundt));
call symput('nobs',compress(lastobs));
/*drop mth_end_dt;*/
run;

data &lib..DRL_AIRB_Product_Lookup;
set BASEL_CCAR_LOOKUP_EXTR_TMP0;
keep &reqvars.;
run;

data &lib..DRL_AIRB_Product_Lookup;
set  &lib..DRL_AIRB_Product_Lookup;
rename
CCAR_BASEL_PRD_TP_NM = 'Basel Product Type'n
PRD_SHT_NM = 'Short Name'n
EXPSR_SUB_TP_CD = 'Exposure Sub Type'n
BASEL_1_ASST_TP_CD = 'Basel 1 Asset Type'n
REGULATORY_PRD_TP_CD = 'Regulatory Product Type'n
PD_BAND_CD = 'PD Band'n
;
run;

/***Basel Product Type,Short Name,Exposure Sub Type,Basel 1 Asset Type,Regulatory Product Type,PD Band ***/

DATA &LIB..DRL_AIRB_Guarantee_Lookup;
SET &DB..BASEL_AIRB_GUARNT_LKP ;
/*WHERE CRNT_F = 'Y';*/
WHERE "&mth_tm_id_yrmth." BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH;
GUARNT_PARTCPTN_PC = round(GUARNT_PARTCPTN_PC);
format GUARNT_PARTCPTN_PC 8.;
KEEP 
CCAR_BASEL_PRD_TP_NM
GUARNT_PARTCPTN_PC
;
rename
CCAR_BASEL_PRD_TP_NM = 'Basel Product'n
GUARNT_PARTCPTN_PC   = 'Guarantee Participation %'n
;
RUN;

%macro exportcsv(dsn=DRL-AIRB-Product-Lookup);
data tmp_name;
dsname2 = trim(substr(translate("&dsn.",'_','-'),1,25));
call symput ('dsname',trim(dsname2));
run;

%put >>> dsname = &dsname.;

proc export data = &lib..&dsname  outfile="&ACAPRPT./&dsn..csv" dbms=csv replace ;
run;

/*x 'sed s/\"//g &ACAPRPT./&dsn.&YYYYMMDD..csv';*/
%mend exportcsv;
%exportcsv(dsn=DRL-AIRB-Product-Lookup);
%exportcsv(dsn=DRL-AIRB-Guarantee-Lookup);
%mend ACAP_LOOKUP_FILES_GENERATE;
%ACAP_LOOKUP_FILES_GENERATE(WITHZERONET=);

