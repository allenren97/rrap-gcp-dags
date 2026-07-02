/* This is a copy of RRAP_SPL_UPDATE_DPD.sas file, but only difference is this code connects to EDL Server 163*/
/* created by Nikhil , date 16-Mar-2022, RRMSS-1376 */

%rrap_spl_autoexec;

/**********  Need service account kbp_b9xf into AUTHDOMAIN="EDL_AUTH" ***********/
options set=SAS_HADOOP_CONFIG_PATH="/sashome/hadoop/";
option set=SAS_HADOOP_JAR_PATH="/sashome/hadoop/lib";
libname covid19 hadoop server="sdpsvrwm0163.scglobal.ad.scotiacapital.com" schema=crz_covid19_ca DBMAX_TEXT=300 
authdomain="EDL_Auth" 
uri="jdbc:hive2://sdpsvrwm0163.scglobal.ad.scotiacapital.com:8443/crz_covid19_ca;ssl=true;sslTrustStore=/sashome/hadoop/gateway.jks;trustStorePassword=Test123?hive.server2.transport.mode=http;hive.server2.thrift.http.path=gateway/default/hive"
login_timeout=0;

%LET MTH_TM_ID;

PROC SQL NOPRINT;
SELECT MAX(MTH_TM_ID) INTO :MTH_TM_ID FROM NZRRAP.BASEL_PSNL_LOAN_MTH_SNAPSHOT;
QUIT;

%PUT >>> MTH_TM_ID IS &MTH_TM_ID.;
%LET MTH_ST_DT_CUR;
%LET MTH_END_DT_CUR;


/* Get current month end date*/
proc sql noprint;
select cats("'",put(tm_lvl_st_dt,yymmdd10.),"'") into :MTH_ST_DT_CUR from NZRRAP.TM_DIM WHERE TM_ID = &MTH_TM_ID. and tm_lvl='Month';
select cats("'",put(tm_lvl_end_dt,yymmdd10.),"'") into :MTH_END_DT_CUR from NZRRAP.TM_DIM WHERE TM_ID = &MTH_TM_ID. and tm_lvl='Month';
quit;

%put &MTH_TM_ID.;
%put &MTH_ST_DT_CUR.;
%put &MTH_END_DT_CUR.;


/* Get Covid data from CRZ, this deferral started in Mar 2020 */
/* This is just getting the max businesseffective date of the current month */
Proc sql;
	connect using covid19 as hdpcon;
	create table Covid_list_EDL as
	select * from connection to hdpcon (
		with max_bus_eff_dt_cur as
		(select max(businesseffectivedate) as max_bed from 
		crz_covid19_ca.cust_acct_deferral_detail 
		where businesseffectivedate<=&MTH_END_DT_CUR. )
		select * from crz_covid19_ca.cust_acct_deferral_detail, max_bus_eff_dt_cur mbed2
		where businesseffectivedate = mbed2.max_bed and acct_subsys_mnemonic_cd = 'SL'
		and deferral_start_date > '2020-02-29' and deferral_start_date < '2020-10-01'   	
	);
	DISCONNECT FROM hdpcon;
quit;

/* Clean up the Covid_list_SPL if the month already exist */
proc sql;
	connect using NZRRAP as nzcon;
	EXECUTE(delete from &RRAP_DB..COVID_LIST_SPL where businesseffectivedate between &MTH_ST_DT_CUR. and &MTH_END_DT_CUR. ) by nzcon;
	disconnect from nzcon;
quit;

/* Append the current month to database: Covid_list_SPL */
proc append base=NZRRAP.Covid_list_SPL (bulkload=yes BL_METHOD=CLILOAD) data=Covid_list_EDL force; run;


/* Dedup: get the latest businesseffectivedate record, before or equal to the running month */
proc sql ;
	connect using NZRRAP as nzcon;
	create table work.Covid_list_SL_NoDup as
		SELECT *								
			From connection to nzcon 
			(
			With Rank_List AS (
			SELECT PARTY_ID, ACCT_ID,DEFERRAL_START_DATE, DEFERRAL_END_DATE, BUSINESSEFFECTIVEDATE, rank() OVER (PARTITION BY PARTY_ID, ACCT_ID ORDER BY BUSINESSEFFECTIVEDATE desc) AS RID
			FROM &RRAP_DB..COVID_LIST_SPL where businesseffectivedate<=&MTH_END_DT_CUR.)
			SELECT * FROM Rank_List WHERE RID = 1  
			);
	Disconnect from nzcon;
quit;


/* Get the fields we need for DPD for running month */
proc sql;
	CONNECT using NZRRAP as nzcon;
	create table work.snapshot as									
		SELECT *								
			From connection to nzcon 							
				(select 										

					t1.BASEL_ACCT_ID,
					t1.TRNST_NUM || t1.LOAN_NUM as ACCT_NUM,
					t1.MTH_TM_ID,
					t1.DAY_ODUE,
					t1.TOT_CRNT_BAL_AMT,
					t1.cust_cid,
					t1.FRST_PAY_DT,
					case when t1.DAY_ODUE > 0 then 1 else 0 end as IND_ODUE
				from &RRAP_DB..BASEL_PSNL_LOAN_MTH_SNAPSHOT t1
					where 
					t1.MTH_TM_ID = &MTH_TM_ID.	
				)						
	;     
	Disconnect from nzcon;
QUIT;


/* looking at previous month system DPD and fixed DPD and get the month id for start and end deferral period from covid list */
proc sql; 
	connect using NZRRAP as dbcon;
	create table snp_history as 

			select 	a.*,
					b.deferral_start_date as deferral_start_date,
					b.deferral_end_date as deferral_end_date,
					t.tm_id as mth_id_def_req,
					t2.tm_id as mth_id_def_end,
					coalesce(d.DAY_ODUE, 0) as DAY_ODUE_PRE_MTH,
					coalesce(d.TOT_CRNT_BAL_AMT, 0) as TOT_CRNT_BAL_AMT_PRE_MTH,
					coalesce(sysDPD.DAY_ODUE, 0) as DAY_ODUE_S_PRE_MTH
			from work.snapshot as a
			left join Covid_list_SL_NoDup as b
				on input(a.ACCT_NUM, best32.) = input(b.acct_id, best32.)
				and a.cust_cid  = b.party_id
			left join NZRRAP.BASEL_PSNL_LOAN_MTH_SNAPSHOT as d
				on a.BASEL_ACCT_ID = d.BASEL_ACCT_ID 
				and d.MTH_TM_ID = &MTH_TM_ID. -40
			left join NZRRAP.TM_DIM as t
				on intnx('month', b.deferral_start_date,0,'E')=t.tm_lvl_end_dt and t.tm_lvl='Month'
			left join NZRRAP.TM_DIM as t2
				on intnx('month', b.deferral_end_date,0,'E')=t2.tm_lvl_end_dt and t2.tm_lvl='Month'
			left join NZRRAP.spl_SNAPSHOT_Prod as sysDPD
				on a.BASEL_ACCT_ID = sysDPD.BASEL_ACCT_ID 
				and sysDPD.MTH_TM_ID = &MTH_TM_ID. -40
			WHERE a.MTH_TM_ID = &MTH_TM_ID.;
	
	DISCONNECT FROM dbcon;
quit;

/* clean up */
%if %sysfunc(exist(NZRRAP.pre_update_snapshot)) %then %do;
   	%put The table exists. Now Deleting.....;

	proc sql;
 		drop table NZRRAP.pre_update_snapshot ;
	quit;
%end;
%else %do;
      %Put Table does NOT exist;
%end;

/* Main DPD logic */
data NZRRAP.pre_update_snapshot;
	set snp_history;
	/* No deferral accounts: no adjustment on dpd.; */
	if missing(deferral_start_date) then do;
		DAY_ODUE_NEW = DAY_ODUE;
	end;
	
	/* Adjust delinquency for deferrals that were requested in current month.; */
	else if mth_id_def_req <= &MTH_TM_ID. and  mth_id_def_end >= &MTH_TM_ID. then do;
		/* If dpd after deferral is lower than before and balance also decreases that means that a payment was made; */
		/* In this case, keep post deferral dpd; */
		if  DAY_ODUE < DAY_ODUE_PRE_MTH and TOT_CRNT_BAL_AMT < TOT_CRNT_BAL_AMT_PRE_MTH then do;
			DAY_ODUE_NEW = DAY_ODUE;
		end;
		/* For the other cases, freeze dpd as of prior do deferral; */
		else do;			
			DAY_ODUE_NEW = DAY_ODUE_PRE_MTH;
		end;
	end;
	/* deferral expired */
	else if mth_id_def_end < &MTH_TM_ID. then do;
		if DAY_ODUE = 0 then do;
			DAY_ODUE_NEW = 0;
		end;
		else do;
			if DAY_ODUE - DAY_ODUE_S_PRE_MTH + DAY_ODUE_PRE_MTH < 0 then do;
				DAY_ODUE_NEW = 0;
			end;
			else do;
				DAY_ODUE_NEW = DAY_ODUE - DAY_ODUE_S_PRE_MTH + DAY_ODUE_PRE_MTH;
			end;
		end;
	end;	
	/* not yet reach the deferral period */
	else do;			
		DAY_ODUE_NEW = DAY_ODUE;
	end;
run;

/* Clean up the backup snapshot if the month already exist */
proc sql;
	connect using NZRRAP as nzcon;
	EXECUTE(delete from &RRAP_DB..spl_SNAPSHOT_Prod where mth_tm_id = &mth_tm_id.) by nzcon;
	disconnect from nzcon;
quit;

/* back up the snapshot table for the running month */
proc sql;
	connect using NZRRAP as nzcon;
	EXECUTE (
		insert into &RRAP_DB..spl_SNAPSHOT_Prod 
		select * from &RRAP_DB..BASEL_PSNL_LOAN_MTH_SNAPSHOT
		WHERE mth_tm_id = &MTH_TM_ID.
	) by nzcon;
	disconnect from nzcon;
quit;

/* Update back the snapshot for DPD */
PROC SQL;
	CONNECT USING NZRRAP AS NZCON;
	EXECUTE (
		UPDATE &RRAP_DB..BASEL_PSNL_LOAN_MTH_SNAPSHOT A
		SET DAY_ODUE=B.DAY_ODUE_NEW,
		UPDT_PROCESS_TMSTMP = now()
		FROM &RRAP_DB..pre_update_snapshot B
		WHERE
		A.BASEL_ACCT_ID = B.BASEL_ACCT_ID AND
		A.MTH_TM_ID = B.MTH_TM_ID AND
		A.MTH_TM_ID = &MTH_TM_ID. 

	) by NZCON;

	DISCONNECT FROM NZCON;
QUIT;