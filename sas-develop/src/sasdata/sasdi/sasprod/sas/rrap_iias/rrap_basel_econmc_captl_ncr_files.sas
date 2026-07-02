
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

/*---- Start of User Written Code  ----*/ 

/*WORK IN PROGRESS        BASEL_Econmc_NCR_Extr_File.sas
SOURCE TABLES      
EDRTLRT.TM_DIM
EDRRAPT.BASEL_NCR_BUS_AGGRTD_FACT
EDRRAPT.BASEL_ECONMC_CAPTL_NCR_EXTR

TARGET TABLES
outfile.mv_ecap_dr_exp_ncr_20140430_BCR
outfile.mv_ecap_dr_exp_ncr_20140430_DEL

*/

%global outfile;
%LET P2EXTR=&OUTPATH;
%let outfile1=&P2EXTR;
%global mth_tm_id;
%global tm_lvl_st_dt;
%global tm_lvl_end_dt;
%global dtime;
proc sql noprint;
select tm_id as mth_tm_id, tm_lvl_st_dt, tm_lvl_end_dt , datetime() as dtime format datetime25.
into :mth_tm_id, :tm_lvl_st_dt, :tm_lvl_end_dt, :dtime
from  EDRTLRT.TM_DIM
/*where tm_id=(select max(mth_tm_id) from  edrrapt.BASEL_NCR_BUS_AGGRTD_FACT);         KEEP  FOR PRODUCTION*/
where tm_id=&MTH_TM_ID;
quit;

%PUT &MTH_TM_ID;
%PUT &tm_lvl_st_dt;
%PUT &tm_lvl_end_dt;
%PUT &dtime;

data _NULL_ ;
  call symput('EXT_DATE_FILE_NAME', "mv_edwout_ecap_dr_exp_ncr_f_qtrly_" || put("&tm_lvl_end_dt"d, yymmddn8.)) ;
  call symput('EXT_SAS_DATE_FILE_NAME', "mv_ecap_dr_exp_ncr_" || put("&tm_lvl_end_dt"d, yymmddn8.)) ;
run ;
/*%put &EXT_DATE_FILE_NAME;*/
/*%put &EXT_SAS_DATE_FILE_NAME;*/


/*                   CREATING THE TRAILER BCR 1 RECORD FILE      */
PROC SQL;
CREATE TABLE outfile.&EXT_SAS_DATE_FILE_NAME._BCR AS
/*CREATE TABLE &EXT_SAS_DATE_FILE_NAME._BCR AS*/
SELECT 
put(today(), yymmddn8.) AS Current_File_Creation_Dt,
compress(put(mth_end_dt,yymmdd10.),'-') AS MTH_END_DT,
SUM(Adjusted_OS_Bal_Amt) as Total_File_Outstanding_Balance FORMAT 20.2,
Count(*) as Total_File_Record_Count
From EDRRAPT.BASEL_ECONMC_CAPTL_NCR_EXTR (BULKLOAD=YES BL_METHOD=CLILOAD)
WHERE MTH_END_DT="&tm_lvl_end_dt."d
GROUP BY MTH_END_DT
ORDER BY MTH_END_DT
;
QUIT;


%put "&tm_lvl_end_dt."d;
/*                   CREATING THE "DEL"  EXRTRACT FILE        */
PROC SQL;
CREATE TABLE outfile.&EXT_SAS_DATE_FILE_NAME._DEL AS
SELECT 
PRD_ID AS PRODUCT_ID LABEL="PRODUCT_ID",
PT_IN_TM_STAT_CD AS ACCOUNT_STATUS LABEL="ACCOUNT_STATUS",
ASST_CL_NUM AS POOL LABEL="POOL",
NCR_EXPSR_CL_KEY_VAL AS NCR_EXPOSURE_CLASS LABEL="NCR_EXPOSURE_CLASS",
put(MTH_END_DT, mmddyy10.) AS OBS_DATE LABEL="OBS_DATE",
ADJUSTED_OS_BAL_AMT AS BALANCE LABEL="BALANCE"  FORMAT 20.2
From EDRRAPT.BASEL_ECONMC_CAPTL_NCR_EXTR (BULKLOAD=YES BL_METHOD=CLILOAD)
WHERE MTH_END_DT="&tm_lvl_end_dt."d
ORDER BY PRODUCT_ID, ACCOUNT_STATUS, POOL, NCR_EXPOSURE_CLASS;
QUIT;


/*       CREATING THE "DATA FILE"  -BCR-    FILE IN csv Format      */
PROC EXPORT DATA= outfile.&EXT_SAS_DATE_FILE_NAME._BCR
			OUTFILE="&outfile1./mv/outgoing/&EXT_DATE_FILE_NAME..bcr"
            DBMS=CSV
            REPLACE;
			PUTNAMES= NO;
/*   NO LABELS AT 1ST ROW  */
RUN;



/*       CREATING THE "TRAILER FILE"  --DEL--      FILE IN csv Format      */
PROC EXPORT DATA= outfile.&EXT_SAS_DATE_FILE_NAME._DEL
			OUTFILE="&outfile1./mv/outgoing/&EXT_DATE_FILE_NAME..del"
            DBMS=CSV
            REPLACE;
			PUTNAMES= YES;
/*   yes PUT LABELS AT 1ST ROW  */
RUN;


/**  Step end User Written **/

%let etls_endTime = %sysfunc(datetime(),datetime.);

/* Turn off performance statistics collection  */ 
data _null_;
   if "&_perfinit" eq "1" then 
      call execute('%perfend;');
      
run;
