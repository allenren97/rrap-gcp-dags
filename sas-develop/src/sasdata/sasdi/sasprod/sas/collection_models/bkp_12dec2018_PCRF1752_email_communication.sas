%global itlc_to_list itlc_cc_list itlc_jucc_list;
%global uloc_to_list uloc_cc_list uloc_jucc_list;
%global ccc_to_list ccc_cc_list ccc_jucc_list;
%global failure_to_list failure_cc_list failure_jucc_list;
%global Days_delay DLY_DATE_FAILSAFE;

%macro email_declare;
data collection_model_email_list;
  infile "/sasdata/sasdi/sasprod/flat_files/collection_models/email_list.csv" truncover delimiter="," firstobs=2;
  length model $10 to_list $100 cc_list $100 jucc_list $100;
  input model to_list cc_list jucc_list;
run;

proc sql noprint;
select to_list, cc_list, jucc_list into :itlc_to_list,:itlc_cc_list,:itlc_jucc_list from collection_model_email_list where trim(model)='ITLC';
select to_list, cc_list, jucc_list into :uloc_to_list,:uloc_cc_list,:uloc_jucc_list from collection_model_email_list where trim(model)='ULOC';
select to_list, cc_list, jucc_list into :ccc_to_list,:ccc_cc_list,:ccc_jucc_list from collection_model_email_list where trim(model)='CCC';
select to_list, cc_list, jucc_list into :failure_to_list,:failure_cc_list,:failure_jucc_list from collection_model_email_list where trim(model)='FAIL';
quit;
%mend;

%macro itlc_failure_notification;
%let Days_Delay=0;
%PUT 
MTH_TM_ID=&MTH_TM_ID 
DLY_TM_ID=&DLY_TM_ID 
RUNSTARTDATE=&RUN_START_DATE 
RUNENDDATE=&RUN_END_DATE 
DAILYRUNSTARTDATE=&DLY_RUN_START_DATE 
DAILYRUNENDDATE=&DLY_RUN_END_DATE
YEARMONTH=&YEARMONTH;

proc sql noprint;
select max(eff_tm_id) into :DLY_TM_ID_FAILSAFE from db2itl.COLCTN_PROBE_DLY_ACCT_OUTPUT;
select count(*)-1 into :Days_delay from db2itl.tm_dim where tm_lvl='Day' and day_of_wk_desc <> 'Sunday' and tm_id between &dly_tm_Id_failsafe and &dly_tm_Id;
select tm_lvl_end_dt into :DLY_DATE_FAILSAFE from db2itl.tm_dim where tm_id=&dly_tm_id_failsafe;
quit;

%put DLY_TM_ID_FAILSAFE=&DLY_TM_ID_FAILSAFE;
%put Days delay=&Days_delay;
%put DLY_DATE_FAILSAFE=&DLY_DATE_FAILSAFE;

%let fileexist=%sysfunc(fileexist(/sasdata/sasdi/sasprod/flat_files/collection_models/itlc_success.donotdelete)); 

%if &fileexist EQ 1 %then %do;
%put "All is well with ITLC";
%end;
%else %do;

%put "JU data old. Email will be sent out to consumers";
%email_declare;
FILENAME OUTMAIL EMAIL
SUBJECT= "ITLC Collection models notification - delay";

DATA _NULL_;
FILE OUTMAIL
TO= (&itlc_to_list &failure_to_list)
CC= (&itlc_cc_list)
;
PUT "Hi,";
PUT " ";
PUT "ITLC scoring process has an issue for &DLY_RUN_END_DATE..";
PUT "Investigation in progress.";
PUT "ITLC JU extract generated using &dly_date_failsafe.";
PUT " ";
PUT "thank you";
RUN;

%end; 

X rm /sasdata/sasdi/sasprod/flat_files/collection_models/itlc_success.donotdelete;

%mend;


%macro ulocc_failure_notification;
%let Days_Delay=0;
%PUT 
MTH_TM_ID=&MTH_TM_ID 
DLY_TM_ID=&DLY_TM_ID 
RUNSTARTDATE=&RUN_START_DATE 
RUNENDDATE=&RUN_END_DATE 
DAILYRUNSTARTDATE=&DLY_RUN_START_DATE 
DAILYRUNENDDATE=&DLY_RUN_END_DATE
YEARMONTH=&YEARMONTH;

proc sql noprint;
select max(eff_tm_id) into :DLY_TM_ID_FAILSAFE from db2ulocc.ULOC_PROBE_DLY_ACCT_OUTPUT;
select count(*)-1 into :Days_delay from db2ulocc.tm_dim where tm_lvl='Day' and day_of_wk_desc not in ('Saturday','Sunday') and tm_id between &dly_tm_Id_failsafe and &dly_tm_Id;
select tm_lvl_end_dt into :DLY_DATE_FAILSAFE from db2ulocc.tm_dim where tm_id=&dly_tm_id_failsafe;
quit;

%put DLY_TM_ID_FAILSAFE=&DLY_TM_ID_FAILSAFE;
%put Days delay=&Days_delay;
%put DLY_DATE_FAILSAFE=&DLY_DATE_FAILSAFE;

%let fileexist=%sysfunc(fileexist(/sasdata/sasdi/sasprod/flat_files/collection_models/ulocc_success.donotdelete)); 

%if &fileexist EQ 1 %then %do;
%put "All is well with ULOCC";
%end;
%else %do;

%put "JU data old. Email will be sent out to consumers";
%email_declare;
FILENAME OUTMAIL EMAIL
SUBJECT= "ULOCC Collection models notification - delay";

DATA _NULL_;
FILE OUTMAIL
TO= (&uloc_to_list &failure_to_list)
CC= (&uloc_cc_list &uloc_jucc_list)
;
PUT "Hi,";
PUT " ";
PUT "ULOCC scoring process has an issue for &DLY_RUN_END_DATE..";
PUT "Investigation in progress.";
PUT "ULOCC JU extract generated using &dly_date_failsafe.";
PUT " ";
PUT "thank you";
RUN;

%end; 

X rm /sasdata/sasdi/sasprod/flat_files/collection_models/ulocc_success.donotdelete;

%mend;
