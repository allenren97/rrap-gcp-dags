options errorabend;
%rrap_dlgd_autoexec;

%let count_query=WITH;
%let union_query=;

data count_queries(keep=c_query u_query);
	set nzrrap.replication_lkp;
	where REPLICATION_METHOD='Ingress' and REPLICATION_SEQUENCE = 2;
	length time_dim $ 12 time_s $ 10 schema $ 8;
	
	schema='NZRRAP';
	if iias_schema='FRG_USER_DATA' then schema='NZFRGUSR';
	/*if iias_schema='EDRTLRP1D' then iias_schema=&EDRTLRP1D.;*/
	/*if iias_schema='FRG_USER_DATA' then iias_schema=&FRG_USR.;*/

	/*COUNT QUERY*/
	c_query=catx(' ',cats('tbl_',_n_),'as',
		cats("(SELECT '",table_name,"' AS TABLE_NAME, COUNT(*) AS COUNT FROM"),
		cats(iias_schema,'.',table_name));
	time=symget('MTH_TM_ID');
	time_dim='MTH_TM_ID';
	if iias_schema='FRG_USER_DATA' then
		do;
			time_s=symget('MTH_END_DT_NZ');
			time_dim='TIME_KEY';
			if table_name='AIRB_MORT_MTH_SNAPSHOT_LGD' then time_dim='MTH_END_DT';
			if table_name in ('BNSMORT_ACCT_LVL_DATA_LGD', 'SCORECARD_VARS_FINAL', 'SCORED_SEGMENTED_ACCTS', 'SCORED_SEGMENTED_ACCTS_ANTQ') then 
				time_dim='PROCESS_DATE';
			c_query=catx(' ',c_query,'WHERE',time_dim,'=',cats("'",time_s,"'"));
		end;
		else if table_name notin ('TM_DIM','IND_COST_PRD_ALLOC_LKP','IND_COST_DFT_ALLOC_LKP','BASEL_DFT_LKP','BASEL_CUST_DIM','BASEL_MODEL_LGD','BASEL_MODEL_REL','BASEL_MODEL_SCORECRD_DTL'
	,'BASEL_MODEL_SCORECRD_HDR','BASEL_SEG')  then
		c_query=catx(' ',c_query,'WHERE',time_dim,'=',time);
	c_query=cats(c_query,')');

	if _n_=1 then
		call symput('count_query',catx(' ',symget('count_query'),c_query));
	else
		call symput('count_query',catx(', ',symget('count_query'),c_query));

	/*UNION QUERY*/
	u_query=catx(' ','SELECT * FROM',cats('tbl_',_n_));
	call symput('union_query',catx(' UNION ALL ',symget('union_query'),u_query));
run;

%put &count_query.;

%put &union_query.;

/* CLEAN UP*/
proc sql;
connect using NZRRAP as iiascon;
execute(
	DELETE FROM &EDRTLRP1D..INGRESS_AUDIT 
	WHERE PROCESS_MTH = %str(%'&MTH_END_DT_NZ.%') AND TABLE_NAME IN 
		(SELECT TABLE_NAME FROM &EDRTLRP1D..REPLICATION_LKP 
		WHERE REPLICATION_METHOD='Ingress' AND REPLICATION_SEQUENCE = 2)
;) by iiascon;
quit;

proc sql;
connect using NZRRAP as iiascon;
execute(
	INSERT into &EDRTLRP1D..INGRESS_AUDIT
	(PROCESS_MTH, TABLE_NAME, ORIGIN_RECORD_COUNT, INSERT_DATE)

	&count_query.

	SELECT
		%str(%'&MTH_END_DT_NZ.%') AS PROCESS_MTH,
		lkp.TABLE_NAME,
		cnt.COUNT AS ORIGIN_RECORD_COUNT,
		CURRENT_TIMESTAMP AS INSERT_DATE
	FROM (SELECT TABLE_NAME FROM &EDRTLRP1D..REPLICATION_LKP
	WHERE REPLICATION_METHOD='Ingress' AND REPLICATION_SEQUENCE = 2) lkp
	LEFT JOIN (&union_query.) cnt ON lkp.TABLE_NAME = cnt.TABLE_NAME
;) by iiascon;
quit;

Data _null_;
infile "&rrap_dir/params/rrap_iias/rrap_ingress_egress_mail.txt";
input;
if _N_ =2 then do;
CALL SYMPUT("email_list",_infile_);
end;
run;
%put &email_list;

%macro sendemail;
		FILENAME OUTMAIL EMAIL
			SUBJECT= "Trigger Ingress to GCP Part2 in Qlik Tool";
		DATA _NULL_;
			FILE OUTMAIL
				TO= (&email_list);
			PUT "Hi,";
			PUT " ";
			PUT "Trigger Ingress/Replicate to GCP Part2 in Qlik Tool";
			PUT "Following jobs can be triggered in parallel";
			PUT "1: IIAS_YEAR_MTH_ME_SEQ_2_APPENDONLY_EDRTLRP1D";
			PUT "2: IIAS_YEAR_MTH_ME_SEQ_2_APPENDONLY_FRG_USER_DATA";
			PUT "3: IIAS_YEAR_MTH_ME_SEQ_2_FULL_RELOAD_EDRTLRP1D";
			PUT "4: IIAS_YEAR_MTH_ME_SEQ_2_FULL_RELOAD_FRG_USER_DATA";
			RUN;
%mend sendemail;

%sendemail;
%PUT 'Ingress Audit Job completed, mail triggered for Ingress Part 2 replication in Qlik';

