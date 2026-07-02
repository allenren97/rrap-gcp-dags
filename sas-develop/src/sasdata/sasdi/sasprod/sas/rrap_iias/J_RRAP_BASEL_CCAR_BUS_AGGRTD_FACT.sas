/******************************************************************************** 
* INFA Job Name			: wf_DM_RRAP_Load_BASEL_BRIDGE_CCAR_AGR					* 
* INFA Job Name			: RRAP_IIAS_Load_BASEL_BRIDGE_CCAR_AGR					* 
* Description			: This code is as a part of INFA to SAS Migration. Code	*
*							Extract data from New SQL Server and copy to DB2    * 
* Source Server			: wvdbsp00141.bns.bns\fcwvdbsp001411                    * 
* Source Database/Schema: BASEL_DATAFEEDS / dbo                                 * 
* Source View Name		: NCR_AGGR_FCT											*
* Target Server			: cs2iw501.bns											*
* Target Database/Schema: DM1P1D / EDRRAPT										*
* Target Table Name 	: BASEL_CCAR_BUS_AGGRTD_FACT							*
* SAS Code Location		: /sasdata/sasdi/sasprod/sas/rrap_iias					* 
* Server				: SASApp                                 				* 
* Created on			: Monday, Dec 20, 2021 					                * 
* Created by			: s3953531                                              * 
* Version				: SAS Enterprise Guide 7.1 		                  		* 
********************************************************************************/ 
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
	create table CCAR_AGGR_FCT_TMP as
	select 	Month_Time_ID as MTH_TM_ID,
       		NCR_Exposure_Class_Key_Value as NCR_EXPSR_CL_KEY,
       		ACL as CR_LOSS_ALWNC_AMT
	from sqlnew.CCAR_AGGR_FCT
	where Month_Time_ID= &MTH_TM_ID.;
quit;

/* create data and time stamp variables */	
data CCAR_AGGR_FCT;
	set CCAR_AGGR_FCT_TMP;
	format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.0;
INSRT_PROCESS_TMSTMP="&SYSDATE9.:&SYSTIME."dt   ;
UPDT_PROCESS_TMSTMP ="&SYSDATE9.:&SYSTIME."dt ;
run;


/* check for db table */
PROC SQL NOPRINT;
         	CONNECT USING DB2RRAP AS NZCON;
         	EXECUTE(DELETE FROM &DBSCHEMA..BASEL_CCAR_BUS_AGGRTD_FACT WHERE MTH_TM_ID=&MTH_TM_ID) BY NZCON;
 QUIT;

 /* copy data to DB2 target table */
 proc append base=DB2RRAP.BASEL_CCAR_BUS_AGGRTD_FACT  (BULKLOAD=YES BL_METHOD=CLILOAD)
				data=CCAR_AGGR_FCT force ; run;

/********************************* END *********************************/

