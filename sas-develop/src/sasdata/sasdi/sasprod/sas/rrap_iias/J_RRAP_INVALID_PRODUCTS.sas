
options mprint;

%rrap_dlgd_autoexec;

data _null_;
	set nzrrap.tm_dim;
	where tm_lvl='Month' and tm_id=&mth_tm_id.;
	call symputx('YYYYMMDD',put(tm_lvl_end_dt,yymmddn8.));
	CALL SYMPUT('MonthYear', CATX(' ',PUT(tm_lvl_end_dt,monname10.),PUT(YEAR(tm_lvl_end_dt),4.)));
run;

%put &yyyymmdd.;

proc sql;
connect using EDRRAPT as dbcon;
create table invalids as select * from connection to dbcon(
select MTH_TM_ID,BASEL_ACCT_ID,MORT_NUM,CAB_TRNST_NUM,TRNST_NUM,LOAN_NUM,ACCT_NUM,CCAR_BASEL_PRD_TP_NM,PIT_STAT_CD
		,PRD_ID,OS_BAL_AMT,OS_PRNCPL_BAL_AMT,CR_LMT_AMT,ADJUSTED_OS_BAL_AMT,CONS_DFT_MTH_CNT,DLQNT_DAY_CNT,PD_ACCT_SCORE,PD_BAND
		,PD_BASEL_SEG_NUM,PD_MODEL_NM,EAD_ACCT_SCORE,EAD_BASEL_SEG_NUM,EAD_MODEL_NM,LGD_ACCT_SCORE,LGD_BASEL_SEG_NUM,LGD_MODEL_NM
		,SRC_SYS_CD,LOAN_TO_VAL_RTO,CONSM_PRD_TREATMNT_CD,SML_BUS_F,TRNST_EXCLSN_F,BCAR_SCHED_NUM_50,BCAR_SCHED_NM,CCAR_F
from edrrapt.BASEL_ANALYTCL_BL_INSTRMNT_FACT where mth_tm_id=&mth_tm_id. and (pd_basel_seg_num is null or lgd_basel_seg_num is null)
	and CONSM_PRD_TREATMNT_CD='A' and SML_BUS_F='N' and PIT_STAT_CD in ('CUR','DEF') and TRNST_EXCLSN_F='N');
quit;


data _null_;
	infile "&rrap_dir./params/rrap/missing_segmentation_email_list.txt";
	input;
	if _N_ =2 then do;
	CALL SYMPUT("rma_email_list",_infile_);
	end;
	if _N_ =3 then do;
	CALL SYMPUT("cmf_email_list",_infile_);
	end;
run;

%put &rma_email_list;
%put &cmf_email_list;

%macro checks;

proc sql noprint;
	select count(1) into :NUM_INVALIDS from invalids;
/*	select sum(SUM_ADJ_OS) into :SUM_ADJ_OS from invalids;*/
quit;


%if &NUM_INVALIDS. EQ 0 %then %do;
	%put Invalid Count = &NUM_INVALIDS. for the Month ending &yyyymmdd.;
	%PUT This Process will now exit without sending any emails.;
	%PUT CCAR GENERATION WILL NOW PROCEED.;

	%goto exitprocess;
%end;
%if &NUM_INVALIDS. GT 0 %then %do;
proc sql noprint;
	select count(1) into :NUM_INVALIDS from invalids;
	select sum(ADJUSTED_OS_BAL_AMT) into :SUM_ADJ_OS from invalids;
quit;
%put Invalid Count = &NUM_INVALIDS.;
%put Invalid Sum OS = &SUM_ADJ_OS.;

	proc export data=invalids dbms=csv outfile="&rrap_dir./flat_files/rrap/missing_segmentation_report_&YYYYMMDD..csv" replace; run;

%end;

 %if &NUM_INVALIDS. GE 100 or &SUM_ADJ_OS. GE 5000000 %then %do;

	%PUT The total amount and number of accounts exceeds the threshold of 100 accounts and $5,000,000. ;
		FILENAME OUTMAIL EMAIL ATTACH=("&rrap_dir./flat_files/rrap/missing_segmentation_report_&YYYYMMDD..csv")
		SUBJECT= "[RRAP] Missing Segmentation Report - &MonthYear.";

		DATA _NULL_;
			FILE OUTMAIL
			TO=(&cmf_email_list.)
			/*CC=(&cc_email_list)*/
			;
			PUT "Hi,";
			PUT " ";
			PUT "Please find attached the Missing Segmentation Report for the month ending &YYYYMMDD..";
			PUT " ";
		run;
%end;
%else %do ;
	%PUT Missing Segmentation to be sent to RMA only.;

		FILENAME OUTMAIL EMAIL ATTACH=("&rrap_dir./flat_files/rrap/missing_segmentation_report_&YYYYMMDD..csv")
		SUBJECT= "[RRAP] Missing Segmentation Report - &MonthYear.";

		DATA _NULL_;
			FILE OUTMAIL
			TO=(&rma_email_list.)
			/*CC=(&cc_email_list)*/
			;
			PUT "Hi,";
			PUT " ";
			PUT "Please find attached the Missing Segmentation Report for the month ending &YYYYMMDD..";
			PUT " ";
		run;

%end;

%exitprocess:

%mend checks;
%checks;






