
/**************************************************************************** 
 * Job:             J_RRII_BASEL_NCR_BD_ATTES_TACTICAL                      * 
 * Description:     Load IIAS table EDRTLRP1D.BASEL_NCR_BD_Attes_Tactical   * 
 * Server:          SASApp                                                  * 
 *                                                                          * 
 * Source Tables:  EDRTLRP1D.TM_DIM                                         * 
 *                 EDRTLRP1D.BASEL_ANALYTCL_BL_INSTRMNT_FACT                * 
 * Target Tables:  EDRTLRP1D.BASEL_NCR_BD_Attes_Tactical                    * 
 *                                                                          * 
 * Generated on:    April 20,2023                                           * 
 * Created by:      Gaurav Tewari                                           * 
 * Version:         1.0                                                     * 
 ****************************************************************************/ 



options mprint mlogic symbolgen;
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);
%let datetime_start = %sysfunc(TIME()) ;
%put >>> START TIME: %sysfunc(datetime(),datetime14.);
%put >>> mth_tm_id: &mth_tm_id.;
PROC SQL NOPRINT;
	select TM_LVL_END_DT format=yymmddn8. into :mth_tm_id_yrmth from nzrrap.tm_dim where tm_id = &mth_tm_id and tm_lvl='Month';
	select TM_LVL_END_DT into :end_dt from nzrrap.tm_dim where tm_id = &mth_tm_id and tm_lvl='Month';
QUIT;

%put >>> mth_tm_id_yrmth: &mth_tm_id_yrmth.;
%put >>> end_dt: &end_dt.;

/* part one-Get the data from Instrument fact */

Proc sql;
create table sum_one as 
SELECT '010' as NCR_RECD_TP_CD ,lkp.NCR_PRNT_KEY_VAL, BCAR_SCHED_NUM, BCAR_SCHED_NUM_50,
sum(CASE WHEN ADJUSTED_OS_BAL_AMT+UNADJUSTED_ADD_ON_BAL_AMT < 0 THEN 0 ELSE ADJUSTED_OS_BAL_AMT+UNADJUSTED_ADD_ON_BAL_AMT END) /1000 AS ADJUSTED_OS_BAL_AMT
FROM &LIB..BASEL_ANALYTCL_BL_INSTRMNT_FACT BL 
LEFT JOIN &LIB..BASEL_NCR_HIERARCHY_LKP lkp
ON lkp.NCR_KEY_VAL = BL.NCR_EXPSR_CL_KEY_VAL and NCR_PRNT_KEY_VAL IN ('0502', '0503','0505', '0506', '0507')
WHERE MTH_TM_ID = &mth_tm_id.
AND TOT_EXPSR_ABOVE_1500K_LMT_F = 'Y'
AND ASST_CL_NUM <>1
AND CCAR_F = 1
GROUP BY MTH_TM_ID, lkp.NCR_PRNT_KEY_VAL, BCAR_SCHED_NUM, BCAR_SCHED_NUM_50
ORDER BY MTH_TM_ID, lkp.NCR_PRNT_KEY_VAL, BCAR_SCHED_NUM, BCAR_SCHED_NUM_50
;
quit;
/* part two-Import the data from Capital Mgmt*/
%let sourcefile=&owftp./rrap/lookup/NCR_BD_Attes_045_047_NoRegRetail_&mth_tm_id_yrmth..csv;

         data WORK.SUM_TWO    ;
           %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
            infile "&sourcefile." delimiter =',' MISSOVER DSD lrecl=32767 firstobs=2 ;
               informat END_DT yymmdd10. ;
               informat NCR_RECD_TP_CD $3. ;
               informat NCR_PRNT_KEY_VAL $4.  ;
               informat BCAR_SCHED_NUM $3.  ;
               informat BCAR_SCHED_NUM_50 $10.  ;
               informat Estimated_EAD_Amount best32.  ;
               informat Pct_EAD_Allocation best32. ;
               informat IRB_Expected_Loss_Amount best32.  ;
               informat Pct_EL_Allocation best32. ;
               format END_DT yymmdd10. ;
               format NCR_RECD_TP_CD $3. ;
               format NCR_PRNT_KEY_VAL $4.  ;
               format BCAR_SCHED_NUM $3.  ;
               format BCAR_SCHED_NUM_50 $10. ;
               format Estimated_EAD_Amount best32.  ;
               format Pct_EAD_Allocation best32. ;
               format IRB_Expected_Loss_Amount best32.  ;
               format Pct_EL_Allocation best32. ;
            input
                         END_DT
                         NCR_RECD_TP_CD $
                         NCR_PRNT_KEY_VAL $
                         BCAR_SCHED_NUM $
                        BCAR_SCHED_NUM_50 $
                         Estimated_EAD_Amount  
                         Pct_EAD_Allocation  
                        IRB_Expected_Loss_Amount  
                         Pct_EL_Allocation  
            ;
			drop END_DT;
            if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
            run;
/* Create the final summary */
Data final;
set 
    sum_one sum_two ;
	format INSRT_PROCESS_TMSTMP  UPDT_PROCESS_TMSTMP datetime21.;
	INSRT_PROCESS_TMSTMP=datetime();
	UPDT_PROCESS_TMSTMP=datetime();
	mth_tm_id=&mth_tm_id;
	run;

PROC SQL NOPRINT;
	CONNECT USING NZRRAP AS NZCON;
	EXECUTE(DELETE FROM &net_db..BASEL_NCR_BD_Attes_Tactical WHERE MTH_TM_ID=&MTH_TM_ID. ) BY NZCON;
QUIT;
proc append base=NZRRAP.BASEL_NCR_BD_Attes_Tactical  (BULKLOAD=YES BL_METHOD=CLILOAD) data=final force;
run;

/* Email part starts here */
%report_validation;
Data email;
retain END_DT 	NCR_RECD_TP_CD	NCR_PRNT_KEY_VAL	BCAR_SCHED_NUM	BCAR_SCHED_NUM_50 ADJUSTED_OS_BAL_AMT Estimated_EAD_Amount	
Pct_EAD_Allocation	IRB_Expected_Loss_Amount	Pct_EL_Allocation;		
set final(drop=mth_tm_id INSRT_PROCESS_TMSTMP updt_process_tmstmp );
format END_DT yymmdd10.;
END_DT=input(put(&mth_tm_id_yrmth,8.),ANYDTDTE8.);run;
options missing = ' ';
   ods excel file="&REPORT_PATH/RRAP_BASEL_NCR_BD_Attestation_Report_NoRegRetail_&mth_tm_id_yrmth..xlsx";
     proc print data=email noobs;
	 format end_dt yymmdd10. ;
	 var END_DT ;
	 var NCR_RECD_TP_CD  NCR_PRNT_KEY_VAL/   style(data)={tagattr="format:0000"};
	 var 	BCAR_SCHED_NUM	BCAR_SCHED_NUM_50 ADJUSTED_OS_BAL_AMT Estimated_EAD_Amount	Pct_EAD_Allocation	IRB_Expected_Loss_Amount	Pct_EL_Allocation;
	     title "&sysver";
		 footnote "&sysdate";
		 FOOTNOTE1 "Generated by the SAS System (&_SASSERVERNAME, &SYSSCPL) on %TRIM(%QSYSFUNC(DATE(), NLDATE20.)) at %TRIM(%SYSFUNC(TIME(), TIMEAMPM12.))";
     run;
   ods excel close;

%MACRO SENDEMAIL;

FILENAME OUTMAIL EMAIL
SUBJECT= "NCR Attestation Tactical Report - &SYSDATE9.";
/*FROM= "";*/

DATA _NULL_;
FILE OUTMAIL
TO= ("RMA-Dev-Support@scotiabank.com" "RMAModelCompliance@scotiabank.com" "RMA-Quality@scotiabank.com")
CC= ("edwsupport@scotiabank.com" "GRT-Model-Support@scotiabank.com")
/*BCC= ("&BCC")*/
ATTACH= ("&REPORT_PATH./RRAP_BASEL_NCR_BD_Attestation_Report_NoRegRetail_&mth_tm_id_yrmth..xlsx" CONTENT_TYPE="application/xls")
;
PUT "Hi,";
PUT "The monthly Attestation Tactical Report is attached for your reference.";
RUN;
%MEND;
%sendemail;
