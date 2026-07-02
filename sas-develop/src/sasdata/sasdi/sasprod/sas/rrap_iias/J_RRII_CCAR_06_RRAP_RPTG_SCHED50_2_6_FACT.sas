*****************************************************************************************;
 * Job:           J_RRII_CCAR_06_RRAP_RPTG_SCHED50_2_6_FACT								*
 * Description:   Generates & e-mail out Schedule 2 6 report        				    * 
 *                                                                     				    * 
 * Frequency:	  Monthly																*
 *                                                                          			* 
 * Sources:		  RMA_BASEL3_PRDCT_SCHD_BCAR50_LKP.csv 			   						*
 *				  edrtlrp1d.RMA_BASEL3_PRDCT_SCHD_BCAR50_LKP							*
 *				  edrtlrp1d.BASEL_ANALYTCL_BL_INSTRMNT_FACT    							* 
 *				  dm1p1d.BASEL_ANALYTCL_BL_INSTRMNT_FACT    							* 
 *                                 										    			* 
 * Targets:		  ../flat_files/rrap/RRAP_SCHED_2_6_BCAR50_&file_date.xlsx				*
 *				  edrtlrp1d.BASEL_RPTG_SCHED_2_6_BCAR50_FACT      						* 
 *                             											    			* 
 * Change Log:    Feb 2023: Roger - New development of Schedule 2 6 report (RRMSS-1874)	*
 * 				  Aug 2023: Update e-mail list (RRMSS-2192)								*
 *                          											    			* 
*****************************************************************************************;

%put timer_start = %sysfunc(datetime(),datetime.);

%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);
%GLOBAL yyyymm srccount1 srccount2 tgtcount failcount file_name file_path file_date rep_date;

DATA _null_;
	mth=INTNX('month',"&MTH_END_DT"d,0,'E');
	CALL SYMPUT("yyyymm",PUT(mth,yymmn6.));
	CALL SYMPUT("file_date",PUT(mth,yymmddn8.));
	CALL SYMPUT("rep_date",LEFT(put(mth,worddate.)));
	CALL SYMPUT('MonthYear', CATX(' ',PUT(mth,monname10.),PUT(YEAR(mth),4.)));
RUN;


/* primary dataset for processing month from IIAS */
PROC SQL;
	CONNECT USING NETCON AS NZCON;
	CREATE TABLE RRAP_2_6_BCAR50_CURRMTH AS SELECT * FROM CONNECTION TO NZCON (
	SELECT ASST_CL_NUM, BCAR_SCHED_SORT, BCAR_SCHED_NUM_50, BCAR_SCHED_NM,
		SUM(CASE WHEN PIT_STAT_CD IN ('CUR','DEF') THEN 1 
				 ELSE 0 END) AS ACCT_CNT, 
		SUM(CASE WHEN SRC_SYS_CD IN ('KS','MOR','SPL','TNG-MOR') THEN (ADJUSTED_OS_BAL_AMT) 
				 ELSE 0 END) AS ADJ_BEFORE_ZERO_NET_OS_BAL_AMT,
		SUM(CASE WHEN SRC_SYS_CD IN ('KS','MOR','SPL','TNG-MOR') AND ADJUSTED_OS_BAL_AMT > 0 THEN ADJUSTED_OS_BAL_AMT
				 ELSE 0	END) AS ADJUSTED_AF_ZERO_NET_OS_BAL_AMT,
		SUM(CASE WHEN PIT_STAT_CD = 'DEF' THEN 1 
				 ELSE 0 END) AS DEFLTD_ACCT_CNT,
		SUM(CASE WHEN PIT_STAT_CD = 'DEF' AND SRC_SYS_CD IN ('KS','MOR','SPL','TNG-MOR') THEN (ADJUSTED_OS_BAL_AMT) 
				 ELSE 0 END) AS ADJ_BEFR_ZER_NET_DF_AC_OS_BL_AMT,
		SUM(CASE WHEN PIT_STAT_CD = 'DEF' AND SRC_SYS_CD IN ('KS','MOR','SPL','TNG-MOR') AND ADJUSTED_OS_BAL_AMT > 0 THEN ADJUSTED_OS_BAL_AMT 
			 	 ELSE 0 END) AS ADJ_AF_ZERO_NET_DF_AC_OS_BAL_AMT
	FROM(
		SELECT a.SRC_SYS_CD, a.ASST_CL_NUM, a.PIT_STAT_CD, a.ADJUSTED_OS_BAL_AMT, 
			COALESCE(c.BCAR_SCHED_SORT, d.BCAR_SCHED_SORT) AS BCAR_SCHED_SORT,
			COALESCE(c.BCAR_SCHED_NUM_50, d.BCAR_SCHED_NUM_50) AS BCAR_SCHED_NUM_50,
			COALESCE(c.BCAR_SCHED_NM, d.BCAR_SCHED_NM) AS BCAR_SCHED_NM
		FROM &net_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT a
			LEFT JOIN &net_db..RMA_BASEL3_PRDCT_SCHD_BCAR50_LKP c /* 1500K override */
			ON UPPER(a.SRC_SYS_CD) = UPPER(c.SRC_SYS_CD) 
				AND a.ASST_CL_NUM = c.ASST_CL_NUM 
				AND UPPER(a.TOT_EXPSR_ABOVE_1500K_LMT_F) = UPPER(c.TOT_EXPSR_ABOVE_LMT_F)
				AND UPPER(c.TOT_EXPSR_ABOVE_LMT_F) = 'Y'
				AND (&yyyymm. BETWEEN cast(c.EFF_FROM_YR_MTH as integer) AND cast(c.EFF_TO_YR_MTH as integer))
			LEFT JOIN &net_db..RMA_BASEL3_PRDCT_SCHD_BCAR50_LKP d 
			ON UPPER(a.SRC_SYS_CD) = UPPER(d.SRC_SYS_CD) 
			/*	AND a.ASST_CL_NUM = d.ASST_CL_NUM */
				AND UPPER(a.BASEL_PRD_TP_CD) = UPPER(d.BASEL_PRD_TP_CD) 
				AND COALESCE(UPPER(a.PRD_CD), '_') = COALESCE(UPPER(d.PRD_CD), '_')
				AND COALESCE(UPPER(a.SUB_PRD_CD), '_') = COALESCE(UPPER(d.SUB_PRD_CD), '_')
				AND COALESCE(UPPER(a.RNTL_PRPTY_F), '_') = COALESCE(UPPER(d.RNTL_PRPTY_F), '_')
				AND COALESCE(UPPER(a.CLP_FLAG), '_') = COALESCE(UPPER(d.CLP_FLAG), '_')
				AND COALESCE(UPPER(a.TRANSACTOR_FLAG_QRR), '_') = COALESCE(UPPER(d.TRANSACTOR_FLAG_QRR), '_')
				AND d.TOT_EXPSR_ABOVE_LMT_F IS NULL
				AND (&yyyymm. BETWEEN cast(d.EFF_FROM_YR_MTH as integer) AND cast(d.EFF_TO_YR_MTH as integer))
		WHERE MTH_TM_ID = &MTH_TM_ID 
			AND SML_BUS_F = 'N' AND CONSM_PRD_TREATMNT_CD ='A' AND TRNST_EXCLSN_F = 'N'
			AND PIT_STAT_CD IN ('CUR' ,'DEF')
			AND PD_BASEL_SEG_NUM IS NOT NULL AND LGD_BASEL_SEG_NUM IS NOT NULL)
	GROUP BY ASST_CL_NUM, BCAR_SCHED_SORT, BCAR_SCHED_NUM_50, BCAR_SCHED_NM
	ORDER BY ASST_CL_NUM, BCAR_SCHED_SORT, BCAR_SCHED_NUM_50, BCAR_SCHED_NM;  
	);
QUIT;


/* query past 12-month count of defaulted accounts from DB2 */
LIBNAME EDRRAPTG DB2 DATABASE="&DBNAME" SCHEMA="&DBSCHEMA" authdomain=DB2_AUTH connection=global;
LIBNAME TEMP DB2 DATABASE="&DBNAME" SCHEMA="SESSION" authdomain=DB2_AUTH connection=global;
PROC SQL;
	CONNECT USING EDRRAPTG AS DBCON;
	EXECUTE (declare global temporary table t_RRAP_2_6_BCAR50_LOOKUP ( 
		SRC_SYS_CD VARCHAR(10),
		ASST_CL_NUM SMALLINT,
		BASEL_PRD_TP_CD VARCHAR(15),
		PRD_CD VARCHAR(10),
		SUB_PRD_CD VARCHAR(10),
		RNTL_PRPTY_F CHAR(1),
		CLP_FLAG CHAR(1),
		TRANSACTOR_FLAG_QRR CHAR(1),
		TOT_EXPSR_ABOVE_LMT_F CHAR(1),
		BCAR_SCHED_NUM_50 VARCHAR(10),
		BCAR_SCHED_NM VARCHAR(150),
		BCAR_SCHED_SORT SMALLINT )
	on commit PRESERVE rows not logged) by DBCON;
	INSERT INTO TEMP.t_RRAP_2_6_BCAR50_LOOKUP /* copy lookup table from IIAS */
	SELECT SRC_SYS_CD, ASST_CL_NUM, BASEL_PRD_TP_CD, PRD_CD, SUB_PRD_CD, 
		   RNTL_PRPTY_F, CLP_FLAG, TRANSACTOR_FLAG_QRR, TOT_EXPSR_ABOVE_LMT_F, BCAR_SCHED_NUM_50, BCAR_SCHED_NM, BCAR_SCHED_SORT
	FROM NZRRAP.RMA_BASEL3_PRDCT_SCHD_BCAR50_LKP
	WHERE "&yyyymm." BETWEEN TRIM(EFF_FROM_YR_MTH) AND TRIM(EFF_TO_YR_MTH);

	CREATE TABLE RRAP_2_6_BCAR50_12MTH_COUNT AS SELECT * FROM CONNECTION TO DBCON (
	SELECT ASST_CL_NUM, BCAR_SCHED_SORT, BCAR_SCHED_NUM_50, BCAR_SCHED_NM, 
		COUNT(DISTINCT BASEL_ACCT_ID) AS PREV_12_MTH_TOT_DEFLTD_ACCT_CNT
	FROM(	
		SELECT a.SRC_SYS_CD, a.BASEL_ACCT_ID, a.ASST_CL_NUM,  
			COALESCE(c.BCAR_SCHED_SORT, d.BCAR_SCHED_SORT) AS BCAR_SCHED_SORT,
			COALESCE(c.BCAR_SCHED_NUM_50, d.BCAR_SCHED_NUM_50) AS BCAR_SCHED_NUM_50,
			COALESCE(c.BCAR_SCHED_NM, d.BCAR_SCHED_NM) AS BCAR_SCHED_NM
		FROM &DBSCHEMA..BASEL_ANALYTCL_BL_INSTRMNT_FACT a 
			LEFT JOIN SESSION.t_RRAP_2_6_BCAR50_LOOKUP c /* 1500K override */
			ON UPPER(a.SRC_SYS_CD) = UPPER(c.SRC_SYS_CD) 
				AND a.ASST_CL_NUM = c.ASST_CL_NUM 
				AND UPPER(a.TOT_EXPSR_ABOVE_1500K_LMT_F) = UPPER(c.TOT_EXPSR_ABOVE_LMT_F)
				AND UPPER(c.TOT_EXPSR_ABOVE_LMT_F) = 'Y'
			LEFT JOIN SESSION.t_RRAP_2_6_BCAR50_LOOKUP d 
			ON UPPER(a.SRC_SYS_CD) = UPPER(d.SRC_SYS_CD) 
			/*	AND a.ASST_CL_NUM = d.ASST_CL_NUM */
				AND UPPER(a.BASEL_PRD_TP_CD) = UPPER(d.BASEL_PRD_TP_CD) 
				AND COALESCE(UPPER(a.PRD_CD), '_') = COALESCE(UPPER(d.PRD_CD), '_')
				AND COALESCE(UPPER(a.SUB_PRD_CD), '_') = COALESCE(UPPER(d.SUB_PRD_CD), '_')
				AND COALESCE(UPPER(a.RNTL_PRPTY_F), '_') = COALESCE(UPPER(d.RNTL_PRPTY_F), '_')
				AND COALESCE(UPPER(a.CLP_FLAG), '_') = COALESCE(UPPER(d.CLP_FLAG), '_')
				AND COALESCE(UPPER(a.TRANSACTOR_FLAG_QRR), '_') = COALESCE(UPPER(d.TRANSACTOR_FLAG_QRR), '_')
				AND d.TOT_EXPSR_ABOVE_LMT_F IS NULL
		WHERE MTH_TM_ID >= 19676 /* default account logic effective from Feb2023 (19676) */
			AND MTH_TM_ID BETWEEN (&MTH_TM_ID-440) AND &MTH_TM_ID
			AND SML_BUS_F = 'N' AND CONSM_PRD_TREATMNT_CD ='A' AND TRNST_EXCLSN_F = 'N'
			AND PIT_STAT_CD = 'DEF'
			AND PD_BASEL_SEG_NUM IS NOT NULL AND LGD_BASEL_SEG_NUM IS NOT NULL)
	GROUP BY ASST_CL_NUM, BCAR_SCHED_SORT, BCAR_SCHED_NUM_50, BCAR_SCHED_NM;
	);
QUIT;


/* combine processing month result with previous 12-month count */
PROC SQL;
	CREATE TABLE RRAP_2_6_BCAR50_EXTRACT_F AS 
	SELECT a.ASST_CL_NUM, a.BCAR_SCHED_SORT, a.BCAR_SCHED_NUM_50, a.BCAR_SCHED_NM,
		a.ACCT_CNT, a.ADJ_BEFORE_ZERO_NET_OS_BAL_AMT, a.ADJUSTED_AF_ZERO_NET_OS_BAL_AMT, a.DEFLTD_ACCT_CNT, 
		a.ADJ_BEFR_ZER_NET_DF_AC_OS_BL_AMT,	a.ADJ_AF_ZERO_NET_DF_AC_OS_BAL_AMT, b.PREV_12_MTH_TOT_DEFLTD_ACCT_CNT 
	FROM RRAP_2_6_BCAR50_CURRMTH a LEFT JOIN RRAP_2_6_BCAR50_12MTH_COUNT b 
	ON a.ASST_CL_NUM = b.ASST_CL_NUM
	AND a.BCAR_SCHED_NUM_50 = b.BCAR_SCHED_NUM_50
	ORDER BY ASST_CL_NUM, BCAR_SCHED_SORT, BCAR_SCHED_NUM_50;
QUIT;


DATA RRAP_2_6_BCAR50_EXTRACT_F;
format MTH_TM_ID 11.
BCAR_SCHED_SORT 5.
ASST_CL_NUM 5.
ASST_CL $15.
BCAR_SCHED_NUM_50 $10. 
BCAR_SCHED_NM $150.
ACCT_CNT 11.
ADJ_BEFORE_ZERO_NET_OS_BAL_AMT	22.8
ADJUSTED_AF_ZERO_NET_OS_BAL_AMT 22.8
DEFLTD_ACCT_CNT 11.
ADJ_BEFR_ZER_NET_DF_AC_OS_BL_AMT 22.8
ADJ_AF_ZERO_NET_DF_AC_OS_BAL_AMT 22.8
PREV_12_MTH_TOT_DEFLTD_ACCT_CNT 5.
INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.6;
SET RRAP_2_6_BCAR50_EXTRACT_F;
MTH_TM_ID=&MTH_TM_ID;
ASST_CL = strip(put(ASST_CL_NUM,15.));
INSRT_PROCESS_TMSTMP = &SESSIONTIME;
UPDT_PROCESS_TMSTMP = &SESSIONTIME;
RUN;


PROC SQL;
CONNECT USING NETCON AS NZCON;
EXECUTE (DELETE FROM &net_db..BASEL_RPTG_SCHED_2_6_BCAR50_FACT WHERE MTH_TM_ID=&MTH_TM_ID) BY NZCON;
DISCONNECT FROM NZCON;
QUIT;
/* append full results to IIAS history table */
PROC APPEND BASE=NZRRAP.BASEL_RPTG_SCHED_2_6_BCAR50_FACT (BULKLOAD=YES BL_METHOD=CLILOAD)
DATA=RRAP_2_6_BCAR50_EXTRACT_F FORCE;
RUN;


/* log of input/output table counts*/
%MACRO datalog;
PROC SQL NOPRINT;
	SELECT COUNT(*) INTO :SRCCOUNT1 FROM NZRRAP.BASEL_ANALYTCL_BL_INSTRMNT_FACT WHERE MTH_TM_ID=&MTH_TM_ID.;
	SELECT COUNT(*) INTO :SRCCOUNT2 FROM EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT WHERE MTH_TM_ID=&MTH_TM_ID.;
	SELECT COUNT(*) INTO :TGTCOUNT FROM NZRRAP.BASEL_RPTG_SCHED_2_6_BCAR50_FACT WHERE MTH_TM_ID=&MTH_TM_ID.;
	SELECT SUM(ACCT_CNT) INTO :FAILCOUNT FROM RRAP_2_6_BCAR50_EXTRACT_F WHERE BCAR_SCHED_NUM_50 IS NULL;
QUIT;
data datalog;
	m1= "NOTE: &MTH_TM_ID. is processing month.";
	m2= "NOTE- %trim(&FAILCOUNT.) accounts were not mapped and removed from report.";
	m3= "NOTE- %trim(&SRCCOUNT1.) records in source table BASEL_ANALYTCL_BL_INSTRMNT_FACT (IIAS).";
	m4= "NOTE- %trim(&SRCCOUNT2.) records in source table BASEL_ANALYTCL_BL_INSTRMNT_FACT (DB2).";
	m5= "NOTE- %trim(&TGTCOUNT.) records written to target table BASEL_RPTG_SCHED_2_6_BCAR50_FACT.";
	PUT m1; put m2; put m3; put m4; put m5;
run;
%MEND;
%datalog;


%macro create_report;
%LET file_path=&rrap_dir./flat_files/rrap/;
%LET file_name=RRAP_SCHED_2_6_BCAR50_&file_date..xlsx;
%PUT &file_path&file_name &file_date &rep_date;
Title color=red  "RRAP Schedule 2 and 6 Report for &rep_date.";
ODS LISTING CLOSE;
ODS EXCEL FILE="&file_path&file_name" 
OPTIONS(ROW_HEIGHTS = '55'
		ABSOLUTE_COLUMN_WIDTH = "10,15,15,90,15,25,25,15,25,25,25" 
		EMBEDDED_TITLES = 'on' 
		SHEET_NAME='Schedule 2&6'
		START_AT='2,2' 
		SHEET_INTERVAL = 'none'
		ABSOLUTE_ROW_HEIGHT = '15'
		FLOW='Tables'
);

PROC REPORT DATA=work.RRAP_2_6_BCAR50_EXTRACT_F	SPLIT='\' style(summary)=[fontweight=bold];
	where BCAR_SCHED_NUM_50 is not null;
	column ASST_CL BCAR_SCHED_NUM_50 BCAR_SCHED_NM ACCT_CNT ADJ_BEFORE_ZERO_NET_OS_BAL_AMT 
		ADJUSTED_AF_ZERO_NET_OS_BAL_AMT DEFLTD_ACCT_CNT ADJ_BEFR_ZER_NET_DF_AC_OS_BL_AMT 
		ADJ_AF_ZERO_NET_DF_AC_OS_BAL_AMT PREV_12_MTH_TOT_DEFLTD_ACCT_CNT;
	define ASST_CL /"Asset \Class No." group center;
	define BCAR_SCHED_NUM_50 /"BCAR \Schedule No." display;
	define BCAR_SCHED_NM /"Schedule Name" display;
	define ACCT_CNT /"Account \Count (#)" analysis sum format=comma10.0;
	define ADJ_BEFORE_ZERO_NET_OS_BAL_AMT  /"Adjusted Before 0 \Netting O/S Balance \Amount ($)" analysis sum format=comma20.2;
	define ADJUSTED_AF_ZERO_NET_OS_BAL_AMT /"Adjusted After 0 \Netting O/S Balance \Amount ($)" analysis sum format=comma20.2;
	define DEFLTD_ACCT_CNT /"Defaulted Account \Count (#)" analysis sum format=comma10.0;
	define ADJ_BEFR_ZER_NET_DF_AC_OS_BL_AMT /"Adjusted Before 0 \Netting Defaulted A/C \O/S Balance \Amount ($)" analysis sum format=comma15.2;
	define ADJ_AF_ZERO_NET_DF_AC_OS_BAL_AMT /"Adjusted After 0 \Netting Defaulted A/C \O/S Balance \Amount ($)" analysis sum format=comma15.2;
	define PREV_12_MTH_TOT_DEFLTD_ACCT_CNT /"Previous 12 Months \Total Defaulted Account \Count (#)" analysis sum format=comma10.0; 
	break after ASST_CL / summarize style={background=lightcyan};
	rbreak after / summarize style={background=lightgray};

	compute after ASST_CL;
		ASST_CL = catt(ASST_CL, ' - Total');
	endcomp;
	compute after;
		ASST_CL = 'Overall - Total';
	endcomp;
/*	compute before _PAGE_;		
		line " ";
	endcomp;*/
	run;
ODS EXCEL CLOSE;
ODS LISTING;
%mend;
%create_report;


%macro sendemail;
FILENAME OUTMAIL EMAIL 
	CC=("GRT-Model-Support@scotiabank.com" "edwsupport@scotiabank.com") 
	SUBJECT="[RRAP] Schedule 2 and 6 Report - &MonthYear."
	ATTACH=("&file_path&file_name" content_type="excel");

	DATA _NULL_;
		FILE OUTMAIL
		TO=("carol.gardner@scotiabank.com" "vita.kornieva@scotiabank.com" "brent.mathers@scotiabank.com" "RMA-Dev-Support@scotiabank.com" "RMAModelCompliance@scotiabank.com");
		PUT;
		%if &FAILCOUNT > 0 %then %do;
			PUT "NOTE: &FAILCOUNT account(s) were not mapped in this run.  Please contact GRT-Model-Support for details.";
		%end;
		%else %do;
			PUT '.';
		%end;
	RUN;
%mend sendemail;
%sendemail;


%PUT timer_end = %sysfunc(datetime(),datetime.);
