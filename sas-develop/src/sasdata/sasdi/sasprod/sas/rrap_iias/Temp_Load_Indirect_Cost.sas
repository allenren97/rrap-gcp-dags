%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

/**********  Need service account kbp_b9xf into AUTHDOMAIN="EDL_AUTH" ***********/
options set=SAS_HADOOP_CONFIG_PATH="/sashome/hadoop/";
option set=SAS_HADOOP_JAR_PATH="/sashome/hadoop/lib";
libname covid19 hadoop server="sdpsvrwm0217.scglobal.ad.scotiacapital.com" schema=tsz_b9xf_rrap DBMAX_TEXT=300 authdomain="EDL_Auth"  
uri="jdbc:hive2://sdpsvrwm0217.scglobal.ad.scotiacapital.com:8443/tsz_b9xf_rrap;ssl=true;sslTrustStore=/sashome/hadoop/gateway.jks;trustStorePassword=Test123?hive.server2.transport.mode=http;hive.server2.thrift.http.path=gateway/default/hive"
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
		select cast(transit as varchar(10)) transit,                   
			cast( pplan  as varchar(10)) pplan,                  
			cast( alphcurr as varchar(10)) alphcurr,
			cast( decum as decimal(17, 3)) decum,
			cast( ytd as decimal(17, 3)) ytd,
			&SESSIONTIME AS INSRT_PROCESS_TMSTMP,
			&SESSIONTIME AS UPDT_PROCESS_TMSTMP
from 
		tsz_b9xf_rrap.airb_mbr_src
		where businesseffectivedate=&MTH_END_DT. 
		   	
	);
	DISCONNECT FROM hdpcon;
quit;

/* Clean up */
proc sql;
	connect using NZRRAP as nzcon;
	EXECUTE(truncate table &net_db..MBR_SRC_CURR) by nzcon;
	disconnect from nzcon;
quit;

/* Append the current month to database*/
proc append base=NZRRAP.MBR_SRC_CURR (bulkload=yes BL_METHOD=CLILOAD) 
data=Covid_list_EDL force; run;

