%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

/**********  Need service account kbp_b9xf into AUTHDOMAIN="EDL_AUTH" ***********/
options set=SAS_HADOOP_CONFIG_PATH="/sashome/hadoop/";
option set=SAS_HADOOP_JAR_PATH="/sashome/hadoop/lib";
libname covid19 hadoop server="sdpsvrwm0217.scglobal.ad.scotiacapital.com" schema=crz_airb_dom_retail DBMAX_TEXT=300 authdomain="EDL_Auth"  
uri="jdbc:hive2://sdpsvrwm0217.scglobal.ad.scotiacapital.com:8443/crz_airb_dom_retail;ssl=true;sslTrustStore=/sashome/hadoop/gateway.jks;trustStorePassword=Test123?hive.server2.transport.mode=http;hive.server2.thrift.http.path=gateway/default/hive"
login_timeout=0;


/* Get current month end date*/
proc sql noprint;
select cats("'",put(tm_lvl_end_dt,yymmdd10.),"'") into :MTH_END_DT from NZRRAP.TM_DIM WHERE TM_ID = &MTH_TM_ID. and tm_lvl='Month';
quit;

%put &MTH_TM_ID.;
%put &MTH_END_DT.;


/* Get EDL Data from TSZ */
Proc sql;
	connect using covid19 as hdpcon;
	create table Covid_list_EDL as
	select * from connection to hdpcon (
		select basel_id AS BASID,
			asst_num AS ASSETNUMBER,
			cast (acct_num as varchar(30))  AS ACCOUNTNUMBER ,
			cast (trnst_num as varchar(15))  AS TRANSITNUMBER ,
			cast (agrmnt_tp as varchar(255)) AS AGREEMENTTYPE,
			cast (pgm_tp as varchar(2)) AS PROGRAMTYPE ,
			cast (bal_owng as decimal(17,3))  BALANCEOWING,
			cast (prncpl_owng_at_asgnmnt as decimal(17,3))  AS PRINCIPALOWINGATASSIGNMENT ,
			cast (accr_intr_at_asgnmnt as decimal(17,3))  AS ACCRUEDINTERESTATASSIGNMENT ,
			cast (add_on_costs_at_asgnmnt as decimal(17,3))  AS ADDONCOSTSATASSIGNMENT ,
			cast(cast(asgnmnt_dt as varchar(30)) as timestamp) AS ASSIGNMENTDATE  ,
			cast (instrctn as varchar(255))  AS INSTRUCTION ,
			cast (stat as varchar(255))  AS STATUS ,
			cast (acct_stat as varchar(255))  AS ACCOUNTSTATUS ,
			cast(cast(mth_end_dt as  varchar(30)) as timestamp)  AS MONTHENDDATE,
			cast (jdgmnt_ind as varchar(3))  AS JUDGEMENTINDICATOR ,
			cast (legal_costs as decimal(17,3))  AS LEGALCOSTS ,
			cast (prpty_mgt_costs as decimal(17,3))  AS PROPERTYMANAGEMENTCOSTS ,
			cast (inspctn_fees as decimal(17,3))  AS INSPECTIONFEES ,
			cast (envmntl_fees as decimal(17,3))  AS ENVIRONMENTALFEES ,
			cast (gst_on_incm as decimal(17,3))  AS GSTONINCOME ,
			cast (utlts as decimal(17,3))  AS UTILITIES ,
			cast (repairs as decimal(17,3))  AS REPAIRS ,
			cast (cr_rptg_costs as decimal(17,3))  AS CREDITREPORTINGCOSTS ,
			cast (corp_risk_insur as decimal(17,3))  AS CORPORATERISKINSURANCE ,
			cast (taxes as decimal(17,3))  AS TAXES ,
			cast (apprsl_fees as decimal(17,3))  AS APPRAISALFEES ,
			cast (cndmnm_fees as decimal(17,3))  AS CONDOMINIUMFEES ,
			cast (misclns_fees as decimal(17,3))  AS MISCELLANEOUSFEES ,
			cast (load_fee as decimal(17,3))  AS LOADFEE ,
			cast (cmmsns as decimal(17,3))  AS COMMISIONS ,
			cast (tot_costs_or_expnss as decimal(17,3))  AS TOTALCOSTSOREXPENSES ,
			cast (dllrs_rcvrd_recvd as decimal(17,3))  AS DOLLARSRECOVEREDRECEIVED ,
			cast (prcd_to_pay_for_expnss as decimal(17,3))  AS PROCEEDSTOPAYFOREXP ,
			cast (tot_rcvrs as decimal(17,3))  AS TOTALRECOVERIES ,
			cast (hldbcks as decimal(17,3))  AS HOLDBACKS ,
			cast (apprsl as decimal(17,3))  AS APPRAISAL ,
			cast (mth_end_prncpl_bal_owng as decimal(17,3))  AS MONTHENDPRINCIPALBALOWING ,
			cast (mth_end_accr_intr_owng as decimal(17,3))  AS MONTHENDACCRUEDINTERESTOWING ,
			cast (mth_end_add_on_cost as decimal(17,3)) AS MONTHENDADDONCOST ,
			cast (hghwy as varchar(2))  AS HIGHWAY ,
			cast (state as varchar(15))  AS STATE ,
			cast(cast(clsd_dt as  varchar(30)) as timestamp) as CLOSEDDATE,
			cast (host_mnemonic as varchar(10))  HOST_MNEMONIC ,
			&SESSIONTIME AS INSRT_PROCESS_TMSTMP,
			&SESSIONTIME AS UPDT_PROCESS_TMSTMP
from 
		crz_airb_dom_retail.airb_asst_src
		where bus_eff_dt=&MTH_END_DT. 
		   	
	);
	DISCONNECT FROM hdpcon;
quit;

/* Clean up */
proc sql;
	connect using NZRRAP as nzcon;
	EXECUTE(truncate table &net_db..ASSET_SRC_CURR) by nzcon;
	disconnect from nzcon;
quit;

/* Append the current month to database*/
proc append base=NZRRAP.ASSET_SRC_CURR (bulkload=yes BL_METHOD=CLILOAD) 
data=Covid_list_EDL force; run;

