***************************************************************************************************************************;

%let etls_jobname = J_RRII_000_RISK_DRIVER_REPORTS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Source Database: IIAS EDRTLRP1D
*  
*  
*  Purpose: Creating HELOC, LOC, ITL, CC_D, CC_R, and CC_T PD Risk Driver Reports
*
*  Frequency: Month End runs
*
*  Notes: J_RRII_000_RISK_DRIVER_REPORTS.sql file needs to be run before this job 
*  		  
*
*  Change Log:
*  2024-07-25: Eseroghene Omene - RRMSS-2696 - Initial Development
*  2024-08-23: Eseroghene Omene - RRMSS-3039 - Update to views naming convention, dropping uat views, dropping uat tables.  
*											   Views to be created via sql file.  CSV files to be created from the views
*
*
***************************************************************************************************************************;

%RRAP_AUTOEXEC

/**Create work tables from the views to be used to create the csv files*/

PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE CC_D_PD_MoM_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.CC_D_PD_MOM_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;
PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE CC_D_PD_QoQ_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.CC_D_PD_QoQ_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;


PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE CC_T_PD_MoM_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.CC_T_PD_MOM_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;
PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE CC_T_PD_QoQ_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.CC_T_PD_QoQ_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;


PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE CC_R_PD_MoM_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.CC_R_PD_MOM_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;
PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE CC_R_PD_QoQ_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.CC_R_PD_QoQ_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;

PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE CC_D_PD_MoM_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.CC_D_PD_MOM_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;
PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE CC_D_PD_QoQ_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.CC_D_PD_QoQ_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;


PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE HELOC_PD_MoM_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.HELOC_PD_MOM_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;
PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE HELOC_PD_QoQ_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.HELOC_PD_QoQ_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;


PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE LOC_PD_MoM_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.LOC_PD_MOM_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;
PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE LOC_PD_QoQ_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.LOC_PD_QoQ_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;

PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE ITL_PD_MoM_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.ITL_PD_MOM_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;
PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE ITL_PD_QoQ_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.ITL_PD_QoQ_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;


PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE MOR_PD_MoM_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.MOR_PD_MOM_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;
PROC SQL;
CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain='IIAS_Auth');
        CREATE TABLE MOR_PD_QoQ_RESULTS AS SELECT * FROM CONNECTION TO DB2 (
                SELECT * FROM EDRTLRP1D.MOR_PD_QoQ_V
        );	
	DISCONNECT FROM IIASCON;
QUIT;


/**MTH_TM_ID will come from the autoexec */
%LET PREV_MTH_TM_ID=%EVAL(&MTH_TM_ID-40);
%LET PREVq_MTH_TM_ID=%EVAL(&MTH_TM_ID-120);



PROC SQL;
    SELECT CLNDR_YR INTO:CURR_CAL_YR FROM NZRRAP.TM_DIM WHERE TM_LVL = 'Month' AND TM_ID = &MTH_TM_ID;
    SELECT CLNDR_YR INTO:PREV_CAL_YR FROM NZRRAP.TM_DIM WHERE TM_LVL = 'Month' AND TM_ID = &PREV_MTH_TM_ID;
    SELECT MTH_CLNDR_CD format=$9. LENGTH=3 INTO:CURR_MONTH FROM NZRRAP.TM_DIM WHERE TM_LVL = 'Month' AND TM_ID = &MTH_TM_ID;
    SELECT MTH_CLNDR_CD format=$9. LENGTH=3 INTO:PREV_MONTH FROM NZRRAP.TM_DIM WHERE TM_LVL = 'Month' AND TM_ID = &PREV_MTH_TM_ID;
    SELECT MTH_CLNDR_CD format=$9. LENGTH=3 INTO:PREV_QRT_MONTH FROM NZRRAP.TM_DIM WHERE TM_LVL = 'Month' AND TM_ID = &PREVq_MTH_TM_ID;
    SELECT CLNDR_YR INTO:PREV_QRT_CAL_YR FROM NZRRAP.TM_DIM WHERE TM_LVL = 'Month' AND TM_ID = &PREVq_MTH_TM_ID;
QUIT;

DATA _NULL_;
    CALL SYMPUT('CURR',CATS("&CURR_MONTH","&CURR_CAL_YR"));
    CALL SYMPUT('PREV',CATS("&PREV_MONTH","&PREV_CAL_YR"));	
    CALL SYMPUT('PREVq',CATS("&PREV_QRT_MONTH","&PREV_QRT_CAL_YR"));
RUN;

%PUT &=CURR;
%PUT &=PREV;
%PUT &=PREVq;
%PUT &=CURR_MTH_END_DT;
%PUT &=PREV_MTH_END_DT;


/**grab outpath from autoexec */
%LET outfile_path = &outpath./cmf/outgoing/Risk_Driver_Reports;

proc export data=CC_D_PD_MoM_RESULTS
	outfile="&outfile_path./CC_D_PD_&PREV._&CURR..csv"
	dbms=csv 
	replace;
run;

proc export data=CC_D_PD_QoQ_RESULTS
	outfile="&outfile_path./CC_D_PD_&PREVq._&CURR..csv"
	dbms=csv 
	replace;
run;



proc export data=CC_R_PD_QoQ_RESULTS
	outfile="&outfile_path./CC_R_PD_&PREVq._&CURR..csv"
	dbms=csv 
	replace;
run;

proc export data=CC_R_PD_MoM_RESULTS
	outfile="&outfile_path./CC_R_PD_&PREV._&CURR..csv"
	dbms=csv 
	replace;
run;


proc export data=CC_T_PD_QoQ_RESULTS
	outfile="&outfile_path./CC_T_PD_&PREVq._&CURR..csv"
	dbms=csv 
	replace;
run;

proc export data=CC_T_PD_MoM_RESULTS
	outfile="&outfile_path./CC_T_PD_&PREV._&CURR..csv"
	dbms=csv 
	replace;
run;

proc export data=ITL_PD_QoQ_RESULTS
	outfile="&outfile_path./ITL_PD_&PREVq._&CURR..csv"
	dbms=csv 
	replace
	label;
run;

proc export data=ITL_PD_MoM_RESULTS
	outfile="&outfile_path./ITL_PD_&PREV._&CURR..csv"
	dbms=csv 
	replace
	label;
run;

proc export data=LOC_PD_QoQ_RESULTS
	outfile="&outfile_path./LOC_PD_&PREVq._&CURR..csv"
	dbms=csv 
	replace;
run;

proc export data=LOC_PD_MoM_RESULTS
	outfile="&outfile_path./LOC_PD_&PREV._&CURR..csv"
	dbms=csv 
	replace;
run;

proc export data=HELOC_PD_QoQ_RESULTS
	outfile="&outfile_path./HELOC_PD_&PREVq._&CURR..csv"
	dbms=csv 
	replace;
run;


proc export data=HELOC_PD_MoM_RESULTS
	outfile="&outfile_path./HELOC_PD_&PREV._&CURR..csv"
	dbms=csv 
	replace;
run;

proc export data=MOR_PD_QoQ_RESULTS
	outfile="&outfile_path./MOR_PD_&PREVq._&CURR..csv"
	dbms=csv 
	replace;
run;

proc export data=MOR_PD_MoM_RESULTS
	outfile="&outfile_path./MOR_PD_&PREV._&CURR..csv"
	dbms=csv 
	replace;
run;	