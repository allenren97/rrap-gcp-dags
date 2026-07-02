/******************************************************************************** 
* INFA Job Name			: wf_DM_RRAP_Load_BASEL_BRIDGE_NCR_AGR					* 
* INFA Job Name			: RRAP_IIAS_Load_BASEL_BRIDGE_NCR_AGR					* 
* Description			: This code is as a part of INFA to SAS Migration. Code	*
*							Extract data from New SQL Server and copy to DB2    * 
* Source Server			: wvdbsi00219.Testbns.bns\fcwvdbsi002191                * 
* Source Database/Schema: BASEL_DATAFEEDS / dbo                                 * 
* Source View Name		: NCR_AGGR_FCT											*
* Target Server			: cs2iw503.bns											*
* Target Database/Schema: DM1P1D / EDRRAPT										*
* Target Table Name 	: BASEL_NCR_BUS_AGGRTD_FACT								*
* SAS Code Location		: /sasdata/sasdi/sasprod/sas/rrap_iias					* 
* Server				: SASApp                                 				* 
* Created on			: Monday, Dec 20, 2021 					                * 
* Created by			: Vijay Kadiyala                                        * 
* Version				: SAS Enterprise Guide 7.1 		                  		* 
********************************************************************************/ 
*	Change Log:
*	2023-06-22: RRMSS-2226 - Modification of Basel Data Feed Portal
***************************************************************************************************************************;

%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

/* Create metadata macro variables */
%let IOMServer      = %nrquote(SASApp);
%let metaPort       = %nrquote(8561);
%let metaServer     = %nrquote(iw503);

/* Set metadata options */
options metaport       = &metaPort 
        metaserver     = "&metaServer"; 

/* Setup for capturing job status  */ 
%let etls_startTime = %sysfunc(datetime(),datetime.);
%let etls_recordsBefore = 0;
%let etls_recordsAfter = 0;
%let etls_lib = 0;
%let etls_table = 0;


/* User written code start */
/*create sas table from sqlserver database table */

proc sql;
	CONNECT USING sqlnew AS SQLSVER;
 	create table NCR_AGGR_FACT as
    select *              
    from connection to SQLSVER
	(
	SELECT  
	Month_Time_ID as MTH_TM_ID,
       Rating_System_Key_Value as RT_SYS_KEY_VAL,
	   Prior_NCR_EXPSR_CL_KEY_VAL as PRIOR_NCR_EXPSR_CL_KEY_VAL,
       NCR_Exposure_Class_Key_Value as NCR_EXPSR_CL_KEY_VAL,
       Unique_Record_Identifier as UNQ_RECD_ID,
       Fraud_Losses as FRAUD_LOSS_AMT,
       Estimated_AIRB_EAD as ESTD_EAD_AMT,
       Accounts_Written_Off as WRITE_OFF_ACCT_CNT ,
       Write_Offs as TOT_WRITE_OFF_AMT,
       IRB_Expected_Losses as IRB_EXPCTD_LOSS_AMT,
       Recoveries as RCVRY_AMT,
       Economic_Losses as ECONMC_LOSS_AMT,
       IRB_Capital as IRB_CAPTL_AMT,
       Gross_Impaired_Loans_Acceptances as GRS_IMPAIRED_LOAN_AND_ACPTNC_AMT,
       Add_Gross_Impaired_Loans_Acceptances as VAR14,
       Gross_Impaired_Loans_Acceptances_Rtn_Accrual as VAR15,
       Total_Allowances_Credit_Losses as CR_LOSS_TOT_ALWNC_AMT,
       Total_Provisions_Credit_Losses as CR_LOSS_TOT_PVSN_AMT ,
       General_Provisions_Credit_Losses as CR_LOSS_GENL_PVSN_AMT,
       Specific_Provisions_Credit_Losses as CR_LOSS_SPECIFIC_PVSN_AMT,
       Other_Gross_Impaired_Loans_Acceptances as VAR20,
       Other_Allowances_Credit_Losses as CR_LOSS_OTH_CHNG_ALWNC_AMT,
       Economic_Capital as ECONMC_CAPTL_AMT
FROM NCR_AGGR_FCT
	where Month_Time_ID= &MTH_TM_ID.				  
	);
	disconnect from SQLSVER; 
Quit;

/* creating data and time stamp columns */
data NCR_AGGR_FACT_1;
	set NCR_AGGR_FACT;
		format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.0;
		INSRT_PROCESS_TMSTMP="&SYSDATE9.:&SYSTIME."dt   ;
		UPDT_PROCESS_TMSTMP ="&SYSDATE9.:&SYSTIME."dt ;
run;

%macro chk_tmptbl;
  %if %sysfunc(exist(DB2RRAP.BASEL_NCR_BUS_AGGRTD_FACT_TMP)) %then %do;
    %put The table exists.;
	   PROC SQL NOPRINT;
	    CONNECT USING DB2RRAP AS NZCON;
	     EXECUTE(drop table &DBSCHEMA..BASEL_NCR_BUS_AGGRTD_FACT_TMP 
			) BY NZCON;
		QUIT;
  %end;
    PROC SQL NOPRINT;
         	CONNECT USING DB2RRAP AS NZCON;
         	EXECUTE(create table &DBSCHEMA..BASEL_NCR_BUS_AGGRTD_FACT_TMP  as 
			(select 	MTH_TM_ID,
			RT_SYS_KEY_VAL,
			PRIOR_NCR_EXPSR_CL_KEY_VAL,
			NCR_EXPSR_CL_KEY_VAL,
			UNQ_RECD_ID,
			FRAUD_LOSS_AMT,
			ESTD_EAD_AMT,
			TOT_WRITE_OFF_AMT,
			WRITE_OFF_ACCT_CNT,
			IRB_EXPCTD_LOSS_AMT,
			RCVRY_AMT,
			ECONMC_LOSS_AMT,
			IRB_CAPTL_AMT,
			GRS_IMPAIRED_LOAN_AND_ACPTNC_AMT,
			GRS_IMPAIRED_LOAN_AND_ACPTNC_ADDTN_AMT as VAR14,
			GRS_IMPAIRED_LOAN_AND_ACPTNC_RET_TO_ACCRL_STAT_AMT as VAR15,
			CR_LOSS_TOT_ALWNC_AMT,
			CR_LOSS_GENL_PVSN_AMT,
			CR_LOSS_TOT_PVSN_AMT,
			CR_LOSS_SPECIFIC_PVSN_AMT,
			GRS_IMPAIRED_LOAN_AND_ACPTNC_OTH_CHNG_AMT as VAR20,
			CR_LOSS_OTH_CHNG_ALWNC_AMT,
			ECONMC_CAPTL_AMT,
			INSRT_PROCESS_TMSTMP,
			UPDT_PROCESS_TMSTMP
			 FROM &DBSCHEMA..BASEL_NCR_BUS_AGGRTD_FACT) WITH NO DATA
			) BY NZCON;
		QUIT;
      %Put New Table Created;
 
%mend chk_tmptbl;
%chk_tmptbl;

/* copy data into DB2 temp table */
proc append base=DB2RRAP.BASEL_NCR_BUS_AGGRTD_FACT_TMP  (BULKLOAD=YES BL_METHOD=CLILOAD)
			data=NCR_AGGR_FACT_1  force ;		
run;

/* check for db table */
PROC SQL NOPRINT;
         	CONNECT USING DB2RRAP AS NZCON;
         	EXECUTE(DELETE FROM &DBSCHEMA..BASEL_NCR_BUS_AGGRTD_FACT WHERE MTH_TM_ID=&MTH_TM_ID) BY NZCON;
 QUIT;

/** copy data to DB2 target table **/
PROC SQL ;
connect using DB2RRAP as nzcon;	
         execute(
	INSERT INTO &DBSCHEMA..BASEL_NCR_BUS_AGGRTD_FACT(
				MTH_TM_ID,
			RT_SYS_KEY_VAL,
			PRIOR_NCR_EXPSR_CL_KEY_VAL,
			NCR_EXPSR_CL_KEY_VAL,
			UNQ_RECD_ID,
			FRAUD_LOSS_AMT,
			ESTD_EAD_AMT,
			TOT_WRITE_OFF_AMT,
			WRITE_OFF_ACCT_CNT,
			IRB_EXPCTD_LOSS_AMT,
			RCVRY_AMT,
			ECONMC_LOSS_AMT,
			IRB_CAPTL_AMT,
			GRS_IMPAIRED_LOAN_AND_ACPTNC_AMT,
			GRS_IMPAIRED_LOAN_AND_ACPTNC_ADDTN_AMT,
			GRS_IMPAIRED_LOAN_AND_ACPTNC_RET_TO_ACCRL_STAT_AMT,
			CR_LOSS_TOT_ALWNC_AMT,
			CR_LOSS_GENL_PVSN_AMT,
			CR_LOSS_TOT_PVSN_AMT,
			CR_LOSS_SPECIFIC_PVSN_AMT,
			GRS_IMPAIRED_LOAN_AND_ACPTNC_OTH_CHNG_AMT,
			CR_LOSS_OTH_CHNG_ALWNC_AMT,
			ECONMC_CAPTL_AMT,
			INSRT_PROCESS_TMSTMP,
			UPDT_PROCESS_TMSTMP)
	SELECT 
			MTH_TM_ID,
			RT_SYS_KEY_VAL,
			PRIOR_NCR_EXPSR_CL_KEY_VAL,
			NCR_EXPSR_CL_KEY_VAL,
			UNQ_RECD_ID,
			FRAUD_LOSS_AMT,
			ESTD_EAD_AMT,
			TOT_WRITE_OFF_AMT,
			WRITE_OFF_ACCT_CNT,
			IRB_EXPCTD_LOSS_AMT,
			RCVRY_AMT,
			ECONMC_LOSS_AMT,
			IRB_CAPTL_AMT,
			GRS_IMPAIRED_LOAN_AND_ACPTNC_AMT,
			VAR14	as GRS_IMPAIRED_LOAN_AND_ACPTNC_ADDTN_AMT,
			VAR15	as GRS_IMPAIRED_LOAN_AND_ACPTNC_RET_TO_ACCRL_STAT_AMT,
			CR_LOSS_TOT_ALWNC_AMT,
			CR_LOSS_GENL_PVSN_AMT,
			CR_LOSS_TOT_PVSN_AMT,
			CR_LOSS_SPECIFIC_PVSN_AMT,
			VAR20 as	GRS_IMPAIRED_LOAN_AND_ACPTNC_OTH_CHNG_AMT,
			CR_LOSS_OTH_CHNG_ALWNC_AMT,
			ECONMC_CAPTL_AMT,
			INSRT_PROCESS_TMSTMP,
			UPDT_PROCESS_TMSTMP
	FROM &DBSCHEMA..BASEL_NCR_BUS_AGGRTD_FACT_TMP
	) BY NZCON;
QUIT;

/*************************** CODE END ********************************/
