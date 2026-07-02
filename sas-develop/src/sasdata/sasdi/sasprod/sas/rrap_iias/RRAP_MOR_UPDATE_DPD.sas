
%rrap_mor_bns_autoexec

options set=SAS_HADOOP_CONFIG_PATH="/sashome/hadoop/";
option set=SAS_HADOOP_JAR_PATH="/sashome/hadoop/lib";
libname covid19 hadoop server="sdpsvrwm0121.scglobal.ad.scotiacapital.com" schema=crz_covid19_ca DBMAX_TEXT=300 
authdomain="EDL_Auth" 
uri="jdbc:hive2://sdpsvrwm0121.scglobal.ad.scotiacapital.com:8443/crz_covid19_ca;ssl=true;sslTrustStore=/sashome/hadoop/gateway.jks;trustStorePassword=Test123?hive.server2.transport.mode=http;hive.server2.thrift.http.path=gateway/default/hive"
login_timeout=0;


%macro DPD_Update(month,time_id);
	%if %sysfunc(exist(work.Covid_list_UO)) %then %do;

        data covid_with_previous_date;
			set covid_with_previous_tm_id;
                process_month=&time_id;
                if process_month>=tm_id>=18276 then previous_mth_tm_id=(process_month-40);
                else previous_mth_tm_id=00000;
		run;

        proc sql;
			create table Covid_list_UO_final as
			select t1.*,t2.TM_LVL_END_DT as mth_end_prior_req_def_date from covid_with_previous_date t1 left join nzrrap.tm_dim t2 on t1.previous_mth_tm_id = t2.tm_id;
		quit;

	%end;
	%else %do;

		proc sql;
			create table Covid_list_UO as
			select acct_id, source_acct_num, deferral_start_date, deferral_end_date from covid19.cust_acct_deferral_detail
			WHERE acct_subsys_mnemonic_cd = 'UO';
		quit;

		proc sort data=Covid_list_UO nodupkey out=Covid_list_UO_less_dup dupout=dup_chk;
			by source_acct_num;
		run;

        proc sql;
			create table covid_with_previous_tm_id as
			select t1.*,t2.tm_id from Covid_list_UO_less_dup t1 left join nzrrap.tm_dim t2 on month(t1.deferral_start_date)=t2.tm_yr_seq_num and put(year(t1.deferral_start_date),4.)=t2.clndr_yr where t2.tm_lvl='Month';
		quit;

        data covid_with_previous_date;
			set covid_with_previous_tm_id;
                process_month=&time_id;
                if process_month>=tm_id>=18276 then previous_mth_tm_id=process_month-40;
                else previous_mth_tm_id=00000;
		run;

        proc sql;
			create table Covid_list_UO_final as
			select t1.*,t2.TM_LVL_END_DT as mth_end_prior_req_def_date from covid_with_previous_date t1 left join nzrrap.tm_dim t2 on t1.previous_mth_tm_id = t2.tm_id;
		quit;

	%end;

proc sql;
    insert into &target_lib..airb_mort_mth_snapshot_DPD_BKUP
    select * from  &target_lib..airb_mort_mth_snapshot
    where tm_id =&time_id;
quit;

proc sql;
   create table defer_dlq as 
   select distinct t1.mort_num, 
          t2.mth_end_prior_req_def_date,
          t1.mth_end_dt, 
          t1.tm_id, 
          t1.dlqnt_mth, 
          t1.crnt_bal, 
          t1.dlqnt_day,
		  t2.deferral_start_date,
		  month(deferral_end_date)-month(deferral_start_date) as deferral_mths
      from &target_lib..airb_mort_mth_snapshot t1
           inner join Covid_list_UO_final t2 on (t1.mort_num = input(t2.source_acct_num,best32.))
      where t1.mth_end_dt >= '29feb2020'd and t1.mth_end_dt >= mth_end_prior_req_def_date
      and intck('month',mth_end_prior_req_def_date,t1.mth_end_dt) <=1
      order by t1.mort_num,
               t1.mth_end_dt;
quit;

data defer_dlq2;
    set defer_dlq;
    prev_dlqnt_day = lag(dlqnt_day);
    prev_crnt_bal = lag(crnt_bal);
    prev_dlqnt_mth = lag(dlqnt_mth);
        by mort_num;
            if first.mort_num then do;
                prev_dlqnt_day = .;
                prev_crnt_bal = .;
                prev_dlqnt_mth = .;
            end;
run;

data defer_dlq3;
set defer_dlq2;
/*Delinquency after deferral is higher than prior deferral, use dpd prior to deferral*/
  if dlqnt_day >= prev_dlqnt_day then do;
      def_dlqnt_day = prev_dlqnt_day;
  end;
      else do;
/* Delinquency after deferral decreased and a payment was made, use current dpd*/
           if crnt_bal < prev_crnt_bal then do;
           def_dlqnt_day = dlqnt_day;
           end;
/* Delinquency after deferral decreased but no payment was made, use dpd prior to deferral*/
           else do;
           def_dlqnt_day = prev_dlqnt_day;
           end;
  end;
/*  apply the same logic to delq month*/
    if dlqnt_mth >= prev_dlqnt_mth then do;
      def_dlqnt_mth = prev_dlqnt_mth;
    end;
    else do;
           if crnt_bal < prev_crnt_bal then do;
           def_dlqnt_mth = dlqnt_mth;
           end;
           else do;
           def_dlqnt_mth = prev_dlqnt_mth;
           end;
  end;
run;

data defer_dlq_full;
    set defer_dlq3;
    by mort_num;
    if last.mort_num;
	where def_dlqnt_mth is not missing ;
run;

proc sql;
	create table &target_lib..covid_&month as
	select * from defer_dlq_full;
quit;

proc sql;
	connect using &target_lib as nzcon;
	execute(
	update &frg_db..airb_mort_mth_snapshot a
	set dlqnt_day = b.def_dlqnt_day,
			dlqnt_mth = b.def_dlqnt_mth
	from &frg_db..covid_&month b
	where a.mort_num = b.mort_num and a.tm_id = &time_id and b.tm_id = &time_id)by nzcon;
	disconnect from nzcon;
quit;

proc sql;
	connect using &target_lib as nzcon;
	execute(drop table &frg_db..covid_&month)by nzcon;
	disconnect from nzcon;
quit;

%mend DPD_Update;

%macro Trigger;

        PROC SQL NOPRINT;
            SELECT MAX(MTH_TM_ID) INTO :MTH_TM_ID FROM NZRRAP.BASEL_CUST_ACCT_RLTNP_SNAPSHOT;
            SELECT TM_LVL_END_DT INTO :MTH_END_DT FROM NZRRAP.TM_DIM WHERE TM_ID = &MTH_TM_ID.;
        QUIT;

        %put &mth_tm_id;

		data _null_;
            set nzrrap.tm_dim;
                call symputx('month_p',tm_yr_seq_num);
                call symputx('time_id_p',tm_id);
            where tm_lvl = 'Month' and tm_id=&mth_tm_id;
		run;

        %DPD_Update(&month_p,&time_id_p)

%mend Trigger;


%Trigger