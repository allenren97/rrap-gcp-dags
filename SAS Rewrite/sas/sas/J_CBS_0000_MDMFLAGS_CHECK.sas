
options set=SAS_HADOOP_CONFIG_PATH="/sashome/hadoop";
options set=SAS_HADOOP_JAR_PATH="/sashome/hadoop/lib/";

%macro hdlib(schema=,libname=);

libname &libname. hadoop server="sdpsvrwm0217.scglobal.ad.scotiacapital.com"
schema=&schema. DBMAX_TEXT=255
authdomain=hadoop_cbs_auth
uri="jdbc:hive2://sdpsvrwm0217.scglobal.ad.scotiacapital.com:8443/&schema.;ssl=true;
sslTrustStore=/sashome/hadoop/gateway.jks;trustStorePassword=Test123?
hive.server2.transport.mode=http;hive.server2.thrift.http.path=gateway/default/hive" login_timeout=0;

%mend hdlib;
%hdlib(schema=crz_cust_scorecard,libname=crz);

options mprint;

%macro mdmflagcheck;

proc sql noprint;
	select count(1) into :mdmcount from crz.cbs_mdm_flags(where=(eff_dt="&mth_end_dt."d));
quit;

%if &mdmcount EQ 0 %then %do;

	%PUT ERROR: No data is available in CBS_MDM_FLAGS for the processing month ending &mth_end_dt..;
	%PUT Please check that this table is loaded prior to proceeding with the schedule.;
	%PUT This job will now abort;

	FILENAME OUTMAIL EMAIL 
	SUBJECT= "CBS_MDM_FLAGS not loaded Prior to Monthly Run"
	FROM= "CBSProdRun";
	DATA _NULL_;
		FILE OUTMAIL
		TO= ("edwsupport@scotiabank.com")
		BCC=("cheng.liu@scotiabank.com","suhel.deshmukh@scotiabank.com","jason.hou@scotiabank.com");
		put "No data is available in CBS_MDM_FLAGS for the processing month ending &mth_end_dt..";
		put "Please check that this table is loaded prior to restarting this job and proceeding with the schedule.";
		put "The production job has now aborted.";
	RUN;

	%abort;
%end;

%else %do;
	%PUT NOTE: CBS_MDM_FLAGS is loaded with &mdmcount. records for the month ending &mth_end_dt..;
	%PUT NOTE: This job will now complete and the schedule will continue.;
%end;
%mend mdmflagcheck;

%mdmflagcheck;


proc sql;
connect using nzwrk as nzcon;
execute(delete from &RRAP_WRK..cbs_mdm_flags where eff_dt = &string.) by nzcon;
quit;

proc append base=nzwrk.cbs_mdm_flags(bulkload=yes BL_METHOD=CLILOAD) 
	data=crz.cbs_mdm_flags(drop= insrt_process_tmstmp op_field where=(eff_dt="&mth_end_dt."d)) force; 
run;

proc sql;
	connect using nzwrk as nzcon;
	/* execute (generate statistics on &RRAP_WRK..cbs_mdm_flags) by nzcon; */
	Execute (CALL SYSPROC.ADMIN_CMD (%nrbquote('RUNSTATS
								ON TABLE ) %nrbquote(&RRAP_WRK..cbs_mdm_flags)
											%nrbquote(ON KEY COLUMNS and INDEXES ALL'))) by nzcon;
quit;

