options errorabend;
/*********************************************************************************** 
 * Job:             J_PLL_MOR_MODEL_60_BNS_INSTRUMENT_FACT              *	 
 * Description:     Rework on Instrument fact - KS, MOR                                                   	   * 
 * Generated on: 	Wednesday, February 19, 2024    EDT             	   * 
 * Changes: 		RRMSS-3540 - Kalind Patel - Job for parallel Run - Rework on Instrument fact - KS, MOR *
					Sourced from: 		
  					  RRAP_MOR_MODEL_60_BNS_REPORTING_INSTRUMENT_FACT_G 
					  J_RRII_KS10_2109_BASEL_ANALYTICL_BL_INSTRMNT_FACT_MOR_FRG
 **********************************************************************************/ 

/* Generate the process id for job  */ 
%put Process ID: &SYSJOBID;

/* General macro variables  */ 
%let jobID = %quote(A57SWEI7.BH000LHV);
%let etls_jobName = %nrquote(J_PLL_MOR_MODEL_60_BNS_INSTRUMENT_FACT);
%let etls_userID = %nrquote(owprdsas);

/* Setup to capture return codes  */ 
%global job_rc trans_rc sqlrc syscc;
%let sysrc = 0;
%let job_rc = 0;
%let trans_rc = 0;
%let sqlrc = 0;
%let syscc = 0;
%global etls_stepStartTime; 
/* initialize syserr to 0 */
data _null_; run;

%macro rcSet(error); 
   %if (&error gt &trans_rc) %then 
      %let trans_rc = &error;
   %if (&error gt &job_rc) %then 
      %let job_rc = &error;
%mend rcSet; 

%macro rcSetDS(error); 
   if &error gt input(symget('trans_rc'),12.) then 
      call symput('trans_rc',trim(left(put(&error,12.))));
   if &error gt input(symget('job_rc'),12.) then 
      call symput('job_rc',trim(left(put(&error,12.))));
%mend rcSetDS; 

/*/* Create metadata macro variables */*/
/*%let IOMServer      = %nrquote(SASApp);*/
/*%let metaPort       = %nrquote(8561);*/
/*%let metaServer     = %nrquote(iw503);*/
/**/
/*/* Set metadata options */*/
/*options metaport       = &metaPort */
/*        metaserver     = "&metaServer"; */

/* Setup for capturing job status  */ 
%let etls_startTime = %sysfunc(datetime(),datetime.);
%let etls_recordsBefore = 0;
%let etls_recordsAfter = 0;
%let etls_lib = 0;
%let etls_table = 0;

%global etls_debug; 
%macro etls_setDebug; 
   %if %str(&etls_debug) ne 0 %then 
      OPTIONS MPRINT%str(;); 
%mend; 
%etls_setDebug; 

/*==========================================================================* 
 * Step:            AM_rrap_mortgage_autoexec             A57SWEI7.BK0011VH * 
 * Transform:       AM_rrap_pll_ksmor_autoexec                                * 
 * Description:     BNS Mortgage jobs imported from IW503 leverage this     * 
 *                   transformation                                         * 
 *==========================================================================*/ 



%put WORK LOCATION: %sysfunc(getoption(work));
%rrap_pll_ksmor_autoexec;

%rcSet(&syserr); 
%rcSet(&sysrc); 
%rcSet(&sqlrc); 

%rcSet(&syscc); 



/**  Step end AM_rrap_mortgage_autoexec **/

/*==========================================================================* 
 * Step:            Set Dates                             A57SWEI7.BK0011VI * 
 * Transform:       User Written                                            * 
 * Description:                                                             * 
 *                                                                          * 
 * Target Table:    User Written - work.W1J0WMO           A57SWEI7.BN000Q5J * 
 *                                                                          * 
 * User Written:    SourceCode                                              * 
 *==========================================================================*/ 

/*---- Start of User Written Code  ----*/ 



%get_model_period_dates(product=mor);   

%put Start and End Dates for Mortgage Models:;

%put start_period_dt: &start_period_dt ;
%put end_period_dt: &end_period_dt;

/*data _output;*/
/*format _run_date $10.*/
/*       _run_date_yymth  $6.;*/
/**/
/*	run_date=intnx('month',"&end_period_dt"d,0,'e');*/
/*	_run_date_yymth = put(run_date,yymmn.);*/
/*	_run_date = put(run_date,yymmdd10.); */
/*	output; */
/**/
/*run;*/


%macro dt;   

data _output;
format _run_date $10.
       _run_date_yymth  $6.;

	run_date=intnx('month',"&end_period_dt"d,0,'e');
	_run_date_yymth = put(run_date,yymmn.);
	_run_date = put(run_date,yymmdd10.); 
	output; 

run;


/*data _output; */
/*        format _run_date $10. */
/*               _run_date_yymth  $6.; */
/**/
/*        %do i = 0 %to -3 %by -1; */
/*                run_date=intnx('month',"&end_period_dt"d,&i.,'e'); */
/*                _run_date_yymth = put(run_date,yymmn.); */
/*                _run_date = put(run_date,yymmdd10.); */
/*                output;  */
/*        %end; */
/**/
/*                run_date=intnx('month',"&end_period_dt"d,-12,'e'); */
/*                _run_date_yymth = put(run_date,yymmn.); */
/*                _run_date = put(run_date,yymmdd10.); */
/*                output; */
/**/
/*                run_date=intnx('month',"&end_period_dt"d,-36,'e'); */
/*                _run_date_yymth = put(run_date,yymmn.); */
/*                _run_date = put(run_date,yymmdd10.); */
/*                output;  */
/*        */
/*run; */

%mend dt; 


%dt;


/*proc sql;*/
/*create table &_output as*/
/*select a.*, b.tm_id as mth_tm_id*/
/*from _output a, nzrrap.tm_dim b*/
/*where b.tm_lvl='Month' and b.tm_lvl_end_dt=input(a._run_date,yymmdd10.)*/
/*order by b.tm_id;*/
/*quit;*/


proc sql;
connect using nzuser as nzcon;
execute (truncate table &pll_db_FRG..basel_analytcl_bl_inst_fct_BNS IMMEDIATE) by nzcon;
quit;

/*---- End of User Written Code  ----*/ 

%rcSet(&syserr); 
%rcSet(&sqlrc); 
%rcSet(&syscc); 



/**  Step end Set Dates **/


/* Capture summary statistics about this step for performance reporting  */ 
%put "DIS_SUMM";

             
         
 
         
         
         
         /**  Step end AM_rrap_pll_ksmor_autoexec **/
         
         /*---- Start of User Written Code  ----*/ 
         
         /*---------------------------------------------------------------------
          * NAME:       06_bns_rpting_instrmnt_fact.sas
          *
          * PURPOSE:    Pulls together the data needed to feed into the reporting
          *             table BASEL_ANALYTCL_BL_INSTRMNT_FACT
          *             
          *---------------------------------------------------------------------*/
         
          /*--------------------------------------------------------------------------------------
         
         SOURCES:
         FRG_USER_DATA:
         segment_reporting_parm
         pd_band
         base_bns_mor_rpt_tbl
         BRDM_BASELAYER_MOR
         basel_ncr_ltv_dim
         
         TARGETS:
         WORK.bns_with_all_ratios_&_run_date_yymth
         FRG_USER_DATA.BNS_WITH_ALL_RTOS_&_run_Date_yymth._&filedate.
         FRGPLL.BASEL_ANALYTCL_BL_INST_FCT_BNS
         
         
         
         /*---- End of User Written Code  ----*/ 
  data _null_;
set _output;
call symput('_run_date',_run_date);
call symput('_RUN_DATE_YYMTH',_RUN_DATE_YYMTH);
run;

%put &_run_date;
%put &_RUN_DATE_YYMTH;       

 
         /**** UNLOAD Month of Large_JOIN_DATA *******/
          proc sql noprint;
         	connect USING NZUSER as nzcon;
         	create table work.base_bns_mor_rpt_tbl (drop = lend_val2) as 
         		select * from connection to nzcon (	
         		select * from &net_db_FRG..base_bns_mor_rpt_tbl
         			where last_day(date_trunc('day',process_date)) = date(%nrbquote('&_run_date'))
         				);
         quit;
      
 
         /**** UNLOAD Month of BRDM_DATA *******/
         proc sql noprint;
         	select distinct cats(put(a.mth_tm_id, best32.))
         		into :tm_key separated by ','
         			from work.base_bns_mor_rpt_tbl as a;
         quit;
         
         %put tm_key = &tm_key;
  
 
         proc sql;
         	connect USING NZUSER as nzcon;
         	create table work.brdm_baselayer_mor as 
         		select * from connection to nzcon (	
         		select * from &net_db_FRG..BRDM_BASELAYER_MOR
         			where tm_id in (&tm_key);
         				);
         quit;
         
         proc sql;
         	connect USING NZUSER as nzcon;
         	create table work.BASEL_MORT_INSTRMNT_ADJ as 
         		select * from connection to nzcon (	
         		select * from &net_db_FRG..BASEL_MORT_INSTRMNT_ADJ
         			where mth_tm_id in (&tm_key);
         				);
         quit;


		proc datasets lib=work;
         modify BASEL_MORT_INSTRMNT_ADJ;
         rename GENL_LEDGER_BAL_ADJ_AMT=GENL_LEDGER_BALCNG_ADJ_AMT;
         run;
         
		 /* Basel III Changes - Add new elements */
         proc sql;
         	connect USING NZUSER as nzcon;
         	create table work.BASEL_ACCT_PRFM_FACT as 
         		select * from connection to nzcon (	
         		select A.* from &net_db_P1D..BASEL_ACCT_PRFM_FACT A 					
         			where A.mth_tm_id in (&tm_key)
					and A.src_sys_cd = 'GZ';
         				);
         quit;

         /********Join Large Join to BRDM  ****************/
         proc sql;
         	create table work.base_with_brdm  as
         		select a.*,
					d.*,
         			b.*, bb.genl_ledger_balcng_adj_amt,
         		case 
         			when upcase(pit_stat_cd) ='DEF'
         			and upcase(paid_off_ind) ='Y'
         			and total_suspense < 0 
         			then (-1*total_suspense)
         		else a.os_bal_amt + bb.GENL_LEDGER_BALCNG_ADJ_AMT  
         		end 
         	as adjusted_os_bal_amt format = 20.8,
         		a.os_bal_amt + bb.GENL_LEDGER_BALCNG_ADJ_AMT as af_secrtztn_bal_amt format = 28.8,
         		/* All missing LTVs should be set to 0405 per EDW */
         	coalesce(c.ncr_ltv_key_val, '0405') as ncr_ltv_key_val
         	from work.base_bns_mor_rpt_tbl as a 
         
				left join work.BASEL_ACCT_PRFM_FACT(drop=INSRT_PROCESS_TMSTMP) as d
					on (a.mortgage_no=input(d.acct_num,7.) and
         				a.mth_tm_id = d.mth_tm_id)

         		left join
         			work.brdm_baselayer_mor(drop=GENL_LEDGER_BALCNG_ADJ_AMT) as b
         		on	(a.mortgage_no=input(b.mort_num,7.) and
         			a.mth_tm_id = b.tm_id)
         
         		left join work.BASEL_MORT_INSTRMNT_ADJ as bb
         			on input(b.mort_num,7.)=input(bb.mort_num,7.) and b.tm_id=bb.mth_tm_id
         
         		left join
         				FRGUSER.basel_ncr_ltv_dim as c
         				on  (a.loan_to_val_rto >= c.ltv_rto_min_val and 
         				a.loan_to_val_rto < c.ltv_rto_max_val)
         
         	;
         quit;
       
         
         /*---------------------------------------------------
          * 4. Score accounts for PD and LGD segments
          *---------------------------------------------------*/
         data work.with_scores;
            set work.base_with_brdm;
         
             length ccar_basel_prd_tp_nm  $50;
         
         
            /*-----------------------------------------------
             * Base scorecard points
             *-----------------------------------------------*/ 
            pd_sc_points = 559;
         
            /*-----------------------------------------------
             * Variable: amort;
             *-----------------------------------------------*/ 
            if missing(amort) then
            do;
               pd_sc_points = pd_sc_points + 22;
               scr_amort= 22;
            end;
            else if not missing(amort) and amort < 180 then
            do;
               pd_sc_points = pd_sc_points + 22;
               scr_amort = 22;
            end;
            else if not missing(amort) and 180 <= amort and amort < 270 then
            do;
               pd_sc_points = pd_sc_points + 10;
               scr_amort = 10;
            end;
            else if not missing(amort) and 270 <= amort then
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_amort = 0;
            end;
         
            /*-----------------------------------------------
             * Variable: AT36;
             *-----------------------------------------------*/ 
            if missing(at36) then 
            do;
               pd_sc_points = pd_sc_points + 27;
               scr_at36= 27;
            end;
            else if not missing(at36) and at36 < 3 then
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_at36 = 0;
            end;
            else if not missing(at36) and 3 <= at36 and at36 < 4 then
            do;
               pd_sc_points = pd_sc_points + 5;
               scr_at36 = 5;
            end;
            else if not missing(at36) and 4 <= at36 and at36 < 10 then
            do;
               pd_sc_points = pd_sc_points + 14;
               scr_at36 = 14;
            end;
            else if not missing(at36) and 10 <= at36 and at36 < 20 then
            do;
               pd_sc_points = pd_sc_points + 20;
               scr_at36 = 20;
            end;
            else if not missing(at36) and 20 <= at36 then 
            do;
               pd_sc_points = pd_sc_points + 27;
               scr_at36 = 27;
            end;
         
            /*-----------------------------------------------
             * Variable: AT94;
             *-----------------------------------------------*/ 
            if missing(at94) then
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_at94 = 0;
            end;
            else if not missing(at94) and at94 < 5000 then 
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_at94 = 0;
            end;
            else if not missing(at94) and 5000 <= at94 and at94 < 14023 then
            do;
               pd_sc_points = pd_sc_points + 5;
               scr_at94 = 5;
            end;
            else if not missing(at94) and 14023 <= at94 and at94 < 30027 then
            do;
               pd_sc_points = pd_sc_points + 10;
               scr_at94 = 10;
            end;
            else if not missing(at94) and 30027 <= at94 and at94 < 51728 then
            do;
               pd_sc_points = pd_sc_points + 16;
               scr_at94 = 16;
            end;
            else if not missing(at94) and 51728 <= at94 then
            do;
               pd_sc_points = pd_sc_points + 23;
               scr_at94 = 23;
            end;
         
            /*-----------------------------------------------
             * Variable: BR147max6m;
             *-----------------------------------------------*/ 
            if missing(br147max6m) then 
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_br147max6m= 0;
            end;
            else if not missing(br147max6m) and br147max6m < 69 then
            do;
               pd_sc_points = pd_sc_points + 21;
               scr_br147max6m = 21;
            end;
            else if not missing(br147max6m) and 69 <= br147max6m and br147max6m < 100 then
            do;
               pd_sc_points = pd_sc_points + 15;
               scr_br147max6m = 15;
            end;
            else if not missing(br147max6m) and 100 <= br147max6m and br147max6m < 105 then
            do;
               pd_sc_points = pd_sc_points + 5;
               scr_br147max6m = 5;
            end;
            else if not missing(br147max6m) and 105 <= br147max6m then
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_br147max6m = 0;
            end;
         
            /*-----------------------------------------------
             * Variable: CUST_ACCT_CNT;
             *-----------------------------------------------*/ 
            if missing(cust_acct_cnt) then 
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_cust_acct_cnt = 0;
            end;
            else if not missing(cust_acct_cnt) and cust_acct_cnt < 5 then
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_cust_acct_cnt = 0;
            end;
            else if not missing(cust_acct_cnt) and 5 <= cust_acct_cnt and cust_acct_cnt < 10 then
            do;
               pd_sc_points = pd_sc_points + 7;
               scr_cust_acct_cnt = 7;
            end;
            else if not missing(cust_acct_cnt) and 10 <= cust_acct_cnt then
            do;
               pd_sc_points = pd_sc_points + 17;
               scr_cust_acct_cnt = 17;
            end;
         
            /*-----------------------------------------------
             * Variable: D2DBALPmin6m;
             *-----------------------------------------------*/ 
            if missing(d2dbalpmin6m) then
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_d2dbalpmin6m = 0;
            end;
            else if not missing(d2dbalpmin6m) and d2dbalpmin6m < 0 then
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_d2dbalpmin6m = 0;
            end;
            else if not missing(d2dbalpmin6m) and 0 <= d2dbalpmin6m and d2dbalpmin6m < 225.27 then
            do;
               pd_sc_points = pd_sc_points + 5;
               scr_d2dbalpmin6m = 5;
            end;
            else if not missing(d2dbalpmin6m) and 225.27 <= d2dbalpmin6m then
            do;
               pd_sc_points = pd_sc_points + 17;
               scr_d2dbalpmin6m = 17;
            end;
         
            /*-----------------------------------------------
             * Variable: DLQNT_DAY;
             *-----------------------------------------------*/ 
            if missing(dlqnt_day) then
            do;
               pd_sc_points = pd_sc_points + 71;
               scr_dlqnt_day= 71;
            end;
            else if not missing(dlqnt_day) and dlqnt_day < 1 then
            do;
               pd_sc_points = pd_sc_points + 71;
               scr_dlqnt_day = 71;
            end;
            else if not missing(dlqnt_day) and 1 <= dlqnt_day and dlqnt_day < 30 then
            do;
               pd_sc_points = pd_sc_points + 33;
               scr_dlqnt_day = 33;
            end;
            else if not missing(dlqnt_day) and 30 <= dlqnt_day and dlqnt_day < 60 then
            do;
               pd_sc_points = pd_sc_points + 15;
               scr_dlqnt_day = 15;
            end;
            else if not missing(dlqnt_day) and 60 <= dlqnt_day then 
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_dlqnt_day = 0;
            end;
         
            /*-----------------------------------------------
             * Variable: DLQNT_MTHmax24m;
             *-----------------------------------------------*/ 
            if missing(dlqnt_mthmax24m) then 
            do;
               pd_sc_points = pd_sc_points + 68;
               scr_dlqnt_mthmax24m= 68;
            end;
            else if not missing(dlqnt_mthmax24m) and dlqnt_mthmax24m < 1 then
            do;
               pd_sc_points = pd_sc_points + 68;
               scr_dlqnt_mthmax24m = 68;
            end;
            else if not missing(dlqnt_mthmax24m) and 1 <= dlqnt_mthmax24m and dlqnt_mthmax24m < 2 then 
            do;
               pd_sc_points = pd_sc_points + 38;
               scr_dlqnt_mthmax24m = 38;
            end;
            else if not missing(dlqnt_mthmax24m) and 2 <= dlqnt_mthmax24m and dlqnt_mthmax24m < 3 then
            do;
               pd_sc_points = pd_sc_points + 12;
               scr_dlqnt_mthmax24m = 12;
            end;
            else if not missing(dlqnt_mthmax24m) and 3 <= dlqnt_mthmax24m then
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_dlqnt_mthmax24m = 0;
            end;
         
            /*-----------------------------------------------
             * Variable: GO04max6m;
             *-----------------------------------------------*/ 
            if missing(go04max6m) then
            do;
               pd_sc_points = pd_sc_points + 6;
               scr_go04max6m= 6;
            end;
            else if not missing(go04max6m) and go04max6m < 5 then
            do;
               pd_sc_points = pd_sc_points + 17;
               scr_go04max6m = 17;
            end;
            else if not missing(go04max6m) and 5 <= go04max6m and go04max6m < 7 then
            do;
               pd_sc_points = pd_sc_points + 12;
               scr_go04max6m = 12;
            end;
            else if not missing(go04max6m) and 7 <= go04max6m and go04max6m < 11 then
            do;
               pd_sc_points = pd_sc_points + 6;
               scr_go04max6m = 6;
            end;
            else if not missing(go04max6m) and 11 <= go04max6m then
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_go04max6m = 0;
            end;
         
            /*-----------------------------------------------
             * Variable: LTV;
             *-----------------------------------------------*/ 
            if missing(ltv) then 
            do;
               pd_sc_points = pd_sc_points + 7;
               scr_ltv= 7;
            end;
            else if not missing(ltv) and ltv < 0.4 then
            do;
               pd_sc_points = pd_sc_points + 7;
               scr_ltv = 7;
            end;
            else if not missing(ltv) and 0.4 <= ltv and ltv < 0.76 then
            do;
               pd_sc_points = pd_sc_points + 4;
               scr_ltv = 4;
            end;
            else if not missing(ltv) and 0.76 <= ltv then
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_ltv = 0;
            end;
         
            /*-----------------------------------------------
             * Variable: NSF_NUMmax6m;
             *-----------------------------------------------*/ 
            if missing(nsf_nummax6m) then
            do;
               pd_sc_points = pd_sc_points + 24;
               scr_nsf_nummax6m= 24;
            end;
            else if not missing(nsf_nummax6m) and nsf_nummax6m < 1 then
            do;
               pd_sc_points = pd_sc_points + 24;
               scr_nsf_nummax6m = 24;
            end;
            else if not missing(nsf_nummax6m) and 1 <= nsf_nummax6m and nsf_nummax6m < 3 then
            do;
               pd_sc_points = pd_sc_points + 7;
               scr_nsf_nummax6m = 7;
            end;
            else if not missing(nsf_nummax6m) and 3 <= nsf_nummax6m then
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_nsf_nummax6m = 0;
            end;
         
            /*-----------------------------------------------
             * Variable: PAYROLL_AMTmin3m;
             *-----------------------------------------------*/ 
            if missing(payroll_amtmin3m) then
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_payroll_amtmin3m= 0;
            end;
            else if not missing(payroll_amtmin3m) and payroll_amtmin3m < 1747.32 then
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_payroll_amtmin3m = 0;
            end;
            else if not missing(payroll_amtmin3m) and 1747.32 <= payroll_amtmin3m and payroll_amtmin3m < 3520.08 then 
            do;
               pd_sc_points = pd_sc_points + 14;
               scr_payroll_amtmin3m = 14;
            end;
            else if not missing(payroll_amtmin3m) and 3520.08 <= payroll_amtmin3m then 
            do;
               pd_sc_points = pd_sc_points + 28;
               scr_payroll_amtmin3m = 28;
            end;
         
            /*-----------------------------------------------
             * Variable: PREPAY_YTDmax24m;
             *-----------------------------------------------*/ 
            if missing(prepay_ytdmax24m) then 
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_prepay_ytdmax24m= 0;
            end;
            else if not missing(prepay_ytdmax24m) and prepay_ytdmax24m < 282.6 then
            do;
               pd_sc_points = pd_sc_points + 0;
               scr_prepay_ytdmax24m = 0;
            end;
            else if not missing(prepay_ytdmax24m) and 282.6 <= prepay_ytdmax24m then
            do;
               pd_sc_points = pd_sc_points + 17;
               scr_prepay_ytdmax24m = 17;
            end;
         
            /*-----------------------------------------------
             * ASSIGN SEGMENT
             *-----------------------------------------------*/ 
         
            /* If pd_sc_points is not missing then assign segment 2 through 8 */
            if not missing(pd_sc_points) then
            do;
               if pd_sc_points < 646.5 then pd_segment  = 8;
               else if 646.5 <= pd_sc_points and pd_sc_points < 689.5 then pd_segment = 7;
               else if 689.5 <= pd_sc_points and pd_sc_points < 712.5 then pd_segment = 6;
               else if 712.5 <= pd_sc_points and pd_sc_points < 738.5 then pd_segment = 5;
               else if 738.5 <= pd_sc_points and pd_sc_points < 767.5 then pd_segment = 4;
               else if 767.5 <= pd_sc_points and pd_sc_points < 802.5 then pd_segment = 3;
               else if 802.5 <= pd_sc_points and pd_sc_points < 832.5 then pd_segment = 2;
               else pd_segment  = 1;
            end;
         
            if upcase(pit_stat_cd) = 'CUR' then
            do;
         
               /*-----------------------------------------------
                * Base scorecard points
                *-----------------------------------------------*/ 
               lgdnd_sc_points = 0;
         
               /*-----------------------------------------------
                * Variable: ltv
                *-----------------------------------------------*/ 
               if  not missing(ltv) and ltv < 0.45 then
               do;
                  b_ltv = 1;
               end;
               else if not missing(ltv) and 0.65 <= ltv then
               do;
                  b_ltv = 3;
               end;
               else
               do;
                  b_ltv = 2;
               end;
         
               /*-----------------------------------------------
                * Variable: index_teranetv
                *-----------------------------------------------*/ 
               if not missing(index_teranetv) and index_teranetv < 135000 then
               do;
                  b_prop_value = 1;
               end;
               else if not missing(index_teranetv) and 1067000 <= index_teranetv then
               do;
                 b_prop_value = 1;
               end;
               else 
               do;
                  b_prop_value = 2;
               end;
         
               /*-----------------------------------------------
                * Variable: bi33max3m
                *-----------------------------------------------*/ 
               if not missing(bi33max3m) and 14445 <= bi33max3m then
               do;
                  b_bi33max3m = 2;
               end;
               else 
               do;
                  b_bi33max3m = 1;
               end;
         
               /*-----------------------------------------------
                * Variable: amort
                *-----------------------------------------------*/ 
               if not missing(amort) and 220 <= amort and amort < 450 then
               do;
                 b_amort = 2;
               end;
               else if not missing(amort) and 450 <= amort then
               do;
                 b_amort = 3;
               end;
               else 
               do;
                 b_amort = 1;
               end;
         
               /*-----------------------------------------------
                * Variable: dlqnt_dayavg24m
                *-----------------------------------------------*/ 
               if not missing(dlqnt_dayavg24m) and 6.717803030303 <= dlqnt_dayavg24m and dlqnt_dayavg24m < 28.8452380952381 then
               do;
                 b_dlqnt_dayavg24m = 2;
               end;
               else if not missing(dlqnt_dayavg24m) and 28.8452380952381 <= dlqnt_dayavg24m then
               do;
                 b_dlqnt_dayavg24m = 2;
               end;
               else
               do;
                 b_dlqnt_dayavg24m = 1;
               end;
         
               /*-----------------------------------------------
                * Variable: ratio
                *-----------------------------------------------*/ 
               if upcase(Province_cd) in ('PE' 'NS' 'NF' 'NB') or missing(Province_cd) or 
                  upcase(Province_cd) in ('AB' 'BC' 'MB' 'SK' 'NW' 'YK') and ratio >=-0.1877 then
               do;
                  b_ratio = 1;
               end;
               else if upcase(Province_cd) ='QC' or
                       upcase(Province_cd) in ('AB' 'BC' 'MB' 'SK' 'NW' 'YK') and ratio <-0.1877 or missing(ratio) then
               do;
                  b_ratio = 2;
               end;
               else
               do;
                  b_ratio = 3;
               end;
         
         
               /*-----------------------------------------------
                * Apply scores
                *-----------------------------------------------*/ 
               if b_amort = 1 then sb_amort = 0;
               if b_amort = 2 then sb_amort = 45;
               if b_amort = 3 then sb_amort = 185;
         
               if b_bi33max3m = 1 then sb_bi33max3m = 0 ;
               if b_bi33max3m = 2 then sb_bi33max3m = 119;
         
               if b_dlqnt_dayavg24m = 1 then sb_dlqnt_dayavg24m = 74 ;
               if b_dlqnt_dayavg24m = 2 then sb_dlqnt_dayavg24m = 0 ;
         
               if b_ltv = 1 then sb_ltv = 0;
               if b_ltv = 2 then sb_ltv = 87;
               if b_ltv = 3 then sb_ltv = 151;
         
               if b_prop_value = 1 then sb_prop_value = 201 ;
               if b_prop_value = 2 then sb_prop_value = 0 ;
         
               if b_ratio = 1 then sb_ratio = 270;
               if b_ratio = 2 then sb_ratio = 115;
               if b_ratio = 3 then sb_ratio = 0;
         
               lgdnd_sc_points = sb_amort + sb_bi33max3m + sb_dlqnt_dayavg24m + sb_ltv + sb_prop_value + sb_ratio;
         
               /*-----------------------------------------------
                * ASSIGN SEGMENT
         		
         		 * UPDATED 07OCT2015
                *-----------------------------------------------*/ 
         
               /* Confirmed this is model exclusion criteria */
               if total_bal_for_ltv <= 5000 then bns_lgd_nd_segment = 13;
         
               /*--------------------------
                * UNINSURED
                *--------------------------*/ 
               else if upcase(insurance) = 'UNINSURED' then
               do;
                  if not missing(lgdnd_sc_points) and 166 <= lgdnd_sc_points and lgdnd_sc_points < 370 then
                  do;
                     bns_lgd_nd_segment = 2;
                  end;
                  else if not missing(lgdnd_sc_points) and 370 <= lgdnd_sc_points and lgdnd_sc_points < 640 then
                  do;
                     bns_lgd_nd_segment = 3;
                  end;
                  else if not missing(lgdnd_sc_points) and 640 <= lgdnd_sc_points then
                  do;
                     bns_lgd_nd_segment = 4;
                  end;
                  else
                  do;
                     bns_lgd_nd_segment = 1;
                  end;
               end;
         
               /*--------------------------
                * INSURED
                *--------------------------*/ 
               else if upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'N' then
               do;
                  if not missing(lgdnd_sc_points) and 166 <= lgdnd_sc_points and lgdnd_sc_points < 370 then
                  do;
                     bns_lgd_nd_segment =  6;
                  end;
                  else if not missing(lgdnd_sc_points) and 370 <= lgdnd_sc_points and lgdnd_sc_points < 640 then
                  do;
                     bns_lgd_nd_segment = 7;
                  end;
                  else if not missing(lgdnd_sc_points) and 640 <= lgdnd_sc_points then
                  do;
                     bns_lgd_nd_segment = 8;
                  end;
                  else
                  do;
                     bns_lgd_nd_segment = 5;
                  end;
               end;
         
               else if upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'Y' then
               do;
                  if not missing(lgdnd_sc_points) and 166 <= lgdnd_sc_points and lgdnd_sc_points < 370 then
                  do;
                     bns_lgd_nd_segment = 10;
                  end;
                  else if not missing(lgdnd_sc_points) and 370 <= lgdnd_sc_points and lgdnd_sc_points < 640 then
                  do;
                     bns_lgd_nd_segment = 11;
                  end;
                  else if not missing(lgdnd_sc_points) and 640 <= lgdnd_sc_points then
                  do;
                     bns_lgd_nd_segment = 12;
                  end;
                  else 
                  do;
                     bns_lgd_nd_segment = 9;
                  end;
               end;
            end;
         
            else if upcase(pit_stat_cd) = 'DEF' then
            do;
         
               /*-----------------------------------------------
                * Base scorecard points
                *-----------------------------------------------*/ 
               lgdd_sc_points = 0;
         
               /*-----------------------------------------------
                * Variable: _arbfmt_1 (i.e., foreclose_ind)
                *-----------------------------------------------*/ 
               if upcase(foreclose_ind) in ('Y') then 
               do;
                  b_foreclose_ind = 1;
               end;
               else
               do;
                  b_foreclose_ind =  2;
               end;
         
               /*-----------------------------------------------
                * Variable: ratio & urate
                *-----------------------------------------------*/ 
               if not missing(ratio) and 0.1478 <= ratio or urate>=0.1 then
               do;
                  b_unemp_rate_ratio = 2;
               end;
               else 
               do;
                  b_unemp_rate_ratio =  1;
               end;
         
               /*-----------------------------------------------
                * Variable: d2dbalmax12m
                *-----------------------------------------------*/ 
               if not missing(d2dbalmax12m) and d2dbalmax12m < 499.2 then
               do;
                  b_d2dbalmax12m = 1;
               end;
               else 
               do;
                  b_d2dbalmax12m = 2;
               end;
         
               /*-----------------------------------------------
                * Variable: month_def
                *-----------------------------------------------*/ 
         
         
               if not missing(cons_dft_mth_cnt) and 6.5 <= cons_dft_mth_cnt and cons_dft_mth_cnt < 11.5 then
               do;
                  b_month_def = 2;
               end;
               else if not missing(cons_dft_mth_cnt) and 11.5 <= cons_dft_mth_cnt and cons_dft_mth_cnt < 15.5 then
               do;
                  b_month_def = 3;
               end;
               else if not missing(cons_dft_mth_cnt) and 15.5 <= cons_dft_mth_cnt then
               do;
                  b_month_def = 4;
               end;
               else 
               do;
                  b_month_def = 1;
               end;
         
         
         
               /*-----------------------------------------------
                * Variable: index_teranetV
                *-----------------------------------------------*/ 
               if not missing(index_teranetV) and 151065.35 <= index_teranetV and index_teranetV < 1000000 then
               do;
                  b_index_teranetv = 2;
               end;
               else 
               do;
                  b_index_teranetv = 1;
               end;
         
               /*-----------------------------------------------
                * Variable: ltv
                *-----------------------------------------------*/       
               if not missing(ltv) and 0.6589 < ltv then
               do;
                  b_ltv = 2;
               end;
               else 
               do;
                  b_ltv = 1;
               end;
         
               /*-----------------------------------------------
                * Apply scores - From BRD (pg 29)
                *-----------------------------------------------*/ 
               if b_foreclose_ind = 1 then score_foreclose_ind = 166;
               if b_foreclose_ind = 2 then score_foreclose_ind = 0;
               if b_unemp_rate_ratio = 1 then score_unemp_ratio = 0;
               if b_unemp_rate_ratio = 2 then score_unemp_ratio = 64;
               if b_d2dbalmax12m = 1 then score_d2dbalmax12m = 42;
               if b_d2dbalmax12m = 2 then score_d2dbalmax12m = 0;
               if b_month_def = 1 then score_month_def = 0;
               if b_month_def = 2 then score_month_def = 216;
               if b_month_def = 3 then score_month_def = 361;
               if b_month_def = 4 then score_month_def = 540;
               if b_index_teranetv = 1 then score_index_teranetv = 137;
               if b_index_teranetv = 2 then score_index_teranetv = 0;
               if b_ltv = 1 then score_ltv = 0;
               if b_ltv = 2 then score_ltv = 50;
         
               lgdd_sc_points = score_foreclose_ind + score_unemp_ratio + score_d2dbalmax12m + score_month_def + score_index_teranetv + score_ltv;
         
               /*-----------------------------------------------
                * ASSIGN SEGMENT - From BRD (pg 30)
                *-----------------------------------------------*/ 
         
               /* Changed: 13JAN2015 - for total_bal: < --> <= */
               /*special segments 13 and 14*/
               if month_def >= 24 then bns_lgd_d_segment = 14;
               else if total_bal <= 5000 and month_def < 24 then bns_lgd_d_segment = 13;
         
               /* Uninsured accounts */
               else if (lgdd_sc_points = . or lgdd_sc_points < 247) and upcase(insurance) = 'UNINSURED' then
               do;
                  bns_lgd_d_segment = 1;
               end;
         
               else if lgdd_sc_points >= 247 and lgdd_sc_points <= 496 and upcase(insurance) = 'UNINSURED' then
               do;
                  bns_lgd_d_segment = 2;
               end;
               else if lgdd_sc_points >= 497 and lgdd_sc_points <= 647 and upcase(insurance) = 'UNINSURED' then
               do;
                  bns_lgd_d_segment = 3;
               end;
               else if lgdd_sc_points >= 648 and upcase(insurance) = 'UNINSURED' then
               do;
                  bns_lgd_d_segment = 4;
               end;
         
               /* Insured accounts, not bulk */
               else if (lgdd_sc_points = . or lgdd_sc_points < 247) and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'N' then
               do;
                  bns_lgd_d_segment = 5;
               end;
               else if lgdd_sc_points >= 247 and lgdd_sc_points <= 496 and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'N' then
               do;
                  bns_lgd_d_segment = 6;
               end;
               else if lgdd_sc_points >= 497 and lgdd_sc_points <= 647 and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'N' then
               do;
                  bns_lgd_d_segment = 7;
               end;
               else if lgdd_sc_points >= 648 and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'N' then
               do;
                  bns_lgd_d_segment = 8;
               end;
         
               /* Insured accounts, bulk */
               else if (lgdd_sc_points = . or lgdd_sc_points < 247) and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'Y' then
               do;
                  bns_lgd_d_segment = 9;
               end;
               else if lgdd_sc_points >= 247 and lgdd_sc_points <= 496 and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'Y' then
               do;
                  bns_lgd_d_segment = 10;
               end;
               else if lgdd_sc_points >= 497 and lgdd_sc_points <= 647 and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'Y'  then
               do;
                  bns_lgd_d_segment = 11;
               end;
               else if lgdd_sc_points >= 648 and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'Y' then
               do;
                  bns_lgd_d_segment = 12;
               end;
         
            end;
         
         /*---------------------------------------------------
             * For accounts without segment from history or who 
             * don't have segment from exclusion, missing status
             *  or PD default, give segment from current data
             *---------------------------------------------------*/
         /* FF 06FEB 2015 Changed ***/
           pd_basel_seg_num = pd_segment;
           if upcase(pit_stat_cd) = 'CUR' then lgd_basel_seg_num = bns_lgd_nd_segment;
           else if upcase(pit_stat_cd) = 'DEF' then lgd_basel_seg_num = bns_lgd_d_segment;
            
            /*-----------------------------------------------
             * Excluded accounts get 98
             *-----------------------------------------------*/ 
            if (current_bal <= 0 and total_suspense = 0) and upcase(paid_off_ind) = 'Y' then 
            do;
               pd_basel_seg_num = 98;
               lgd_basel_seg_num = 98;
            end;
         
            /*-----------------------------------------------
             * Accounts with missing status get 98
             *-----------------------------------------------*/ 
            if upcase(pit_stat_cd) = '' then 
            do;
               pd_basel_seg_num = 98;
               lgd_basel_seg_num = 98;
            end;
         
            /*-----------------------------------------------
             * Accounts in default get PD segment 99
             *-----------------------------------------------*/ 
            if upcase(pit_stat_cd) = 'DEF' then 
            do;
               pd_basel_seg_num = 99;
            end;
               
           
         
           pd_acct_score = pd_sc_points;
           if upcase(pit_stat_cd) = 'CUR' then lgd_acct_score = lgdnd_sc_points;
           else if upcase(pit_stat_cd) = 'DEF' then lgd_acct_score = lgdd_sc_points; 
         
         
            ccar_basel_prd_tp_nm = cats(ccar_basel_product_type_name,'_', cats(put(pd_basel_seg_num, z2.)), '_01_', cats(put(lgd_basel_seg_num, z2.)));
         
         run;
         /*---- End of User Written Code  ----*/ 
         
         /*---- Start of User Written Code  ----*/ 
         
         /*---------------------------------------------------
          * 5. Join BRDM table to MORT table and include LTV
          *    values
          *---------------------------------------------------*/

          proc sql;
          connect using nzrrap as nzcon;
         	create table BASEL_SEG_RPTG_PARM as select * from connection to nzcon(
         	select 'BNS '||a.model_nm as model_name, c.seg_num as SEGMENT_NO, b.* 
         	from &net_db..basel_model a, &net_db..BASEL_SEG_RPTG_PARM b, &net_db..basel_seg c
         	where a.basel_model_id=b.basel_model_id 
         	and b.EFF_TO_DT >= date(%nrbquote('&_run_date')) and b.EFF_FROM_DT <= date(%nrbquote('&_run_date')) and a.src_sys_cd='MOR' 
         	and b.basel_seg_id=c.basel_seg_id
         	order by 1,2 )
         ;
         quit;

/*RRMSS-1342: Adding PMI requirements : Add LGD_BASEL_SEG_NUM : Ganesh Patro*/
		proc sql noprint;
			create table work.with_scores_pmi as 
				select 
					a.*,
					case when (a.CCAR_BASEL_PRD_TP_NM like 'GENW%' OR a.CCAR_BASEL_PRD_TP_NM LIKE 'GUAR%')
								and (upcase(a.lgd_model_nm) LIKE 'BNS MOR LGD-ND%') and a.lgd_basel_seg_num IN (5,9) THEN 1
 						WHEN (a.CCAR_BASEL_PRD_TP_NM like 'GENW%' OR a.CCAR_BASEL_PRD_TP_NM LIKE 'GUAR%')
								and (upcase(a.lgd_model_nm) LIKE 'BNS MOR LGD-ND%') and a.lgd_basel_seg_num IN (6,10) THEN 2
						WHEN (a.CCAR_BASEL_PRD_TP_NM like 'GENW%' OR a.CCAR_BASEL_PRD_TP_NM LIKE 'GUAR%')
								and (upcase(a.lgd_model_nm) LIKE 'BNS MOR LGD-ND%') and a.lgd_basel_seg_num IN (7,11) THEN 3
						WHEN (a.CCAR_BASEL_PRD_TP_NM like 'GENW%' OR a.CCAR_BASEL_PRD_TP_NM LIKE 'GUAR%')
								and (upcase(a.lgd_model_nm) LIKE 'BNS MOR LGD-ND%') and a.lgd_basel_seg_num IN (8,12) THEN 4
						when (a.CCAR_BASEL_PRD_TP_NM like 'GENW%' OR a.CCAR_BASEL_PRD_TP_NM LIKE 'GUAR%')
								and (upcase(a.lgd_model_nm) LIKE 'BNS MOR LGD-D%') and a.lgd_basel_seg_num IN (5,9) THEN 1 
						when (a.CCAR_BASEL_PRD_TP_NM like 'GENW%' OR a.CCAR_BASEL_PRD_TP_NM LIKE 'GUAR%')
								and (upcase(a.lgd_model_nm) LIKE 'BNS MOR LGD-D%') and a.lgd_basel_seg_num IN (6,10) THEN 2
						when (a.CCAR_BASEL_PRD_TP_NM like 'GENW%' OR a.CCAR_BASEL_PRD_TP_NM LIKE 'GUAR%')
								and (upcase(a.lgd_model_nm) LIKE 'BNS MOR LGD-D%') and a.lgd_basel_seg_num IN (7,11) THEN 3
						when (a.CCAR_BASEL_PRD_TP_NM like 'GENW%' OR a.CCAR_BASEL_PRD_TP_NM LIKE 'GUAR%')
								and (upcase(a.lgd_model_nm) LIKE 'BNS MOR LGD-D%') and a.lgd_basel_seg_num IN (8,12) THEN 4
						else a.lgd_basel_seg_num
					end as UNINSURED_LGD_SEG_NUM
				from work.with_scores as a
				left join
                 BASEL_SEG_RPTG_PARM as c
                 on (upcase(a.lgd_model_nm) = upcase(c.model_name) and
                     a.lgd_basel_seg_num = c.segment_no);

		quit;
/*END Add UNINSURED_LGD_SEG_NUM */

 
         /*UPDATED to use the fields from the BASEL_SEG_RPTG_PARM table*/
		 /* Target table bns_with_all_ratios_&_run_date_yymth. is now WORK table */
         proc sql noprint;
              create table work.bns_with_all_ratios_&_run_date_yymth. (drop = pd_band_exposure_class_key_value) as  
        
            select a.*,
         
                   /* PD ratios */
                   b.LR_RTO as pd_lr_rptg_rto format = 28.8,
                   b.LR_PV_RTO as pd_lr_pv_rptg_rto format = 28.8,
                   b.LR_PV_AD_RTO as pd_lr_pv_ad_rptg_rto format = 28.8,
                   b.LR_PV_AD_SV_DT_RTO as pd_ld_pv_ad_sv_rptg_rto format = 28.8,
                   b.FINAL_RTO as pd_final_rptg_rto format = 28.8, 
                   b.LR_PV_AD_SV_DT_AA_RTO as pd_lr_pv_ad_sv_dt_aa_rto format = 28.8,
                   b.UNADJUSTED_RPTG_RTO as pd_unadjusted_rptg_rto format = 28.8,
         
                   /* LGD ratios */
                   c.LR_RTO as lgd_lr_rptg_rto format = 28.8,
                   c.LR_PV_RTO as lgd_lr_pv_rptg_rto format = 28.8,
                   c.LR_PV_AD_RTO as lgd_lr_pv_ad_rptg_rto format = 28.8,
                   c.LR_PV_AD_SV_RTO as lgd_ld_pv_ad_sv_rptg_rto format = 28.8,
                   c.LR_PV_AD_SV_DT_RTO as lgd_ld_pv_ad_sv_dt_rptg_rto format = 28.8,
                   c.FINAL_RTO as lgd_final_rptg_rto format = 28.8, 
                   c.LR_PV_AD_SV_DT_AA_RTO as lgd_lr_pv_ad_sv_dt_aa_rto format = 28.8,
                   c.UNADJUSTED_RPTG_RTO as lgd_unadjusted_rptg_rto format = 28.8,
         			c.PRE_INSURANCE_LGD as lgd_insured_rptg_rto format= 28.8,   /*DLGD Insured - Effective Nov2017*/
         			
                   /* PD band values -- Derived after Flooring PD Values*/
                   /*d.ncr_pd_band_key_value,
                   cats(put(d.ncr_pd_band_key_value,best10.)) as ncr_pd_band_key_val length = 4,
                   cats(put(d.pd_band, best10.)) as pd_band length = 10, */
				   '' as ncr_pd_band_key_value, '' as ncr_pd_band_key_val, '' as pd_band,


					/*PMI Adding UNINSURED_LGD_RTO  	*/
					e.FINAL_RTO as UNINSURED_LGD_RTO format = 28.8
            from work.with_scores_pmi as a
                 left join
                 BASEL_SEG_RPTG_PARM as b
                 on (upcase(a.pd_model_nm) = upcase(b.model_name) and
                     a.pd_basel_seg_num = b.segment_no)
         
                 left join
                 BASEL_SEG_RPTG_PARM as c
                 on (upcase(a.lgd_model_nm) = upcase(c.model_name) and
                     a.lgd_basel_seg_num = c.segment_no)
                  
                 left join
                 FRGUSER.pd_band as d
                 on (cats(a.pd_band_exposure_class_key_value) = cats(d.pd_band_exposure_class_key_value) and
          /*  changed 2/6 */           b.final_rto between d.pd_minimum_value and d.pd_maximum_value)

			/*RRMSS-1342: Adding PMI requirements : Add UNINSURED_LGD_RTO : Ganesh Patro */
			left join
                 BASEL_SEG_RPTG_PARM as e
                 on (upcase(a.lgd_model_nm) = upcase(e.model_name) and
                     a.UNINSURED_LGD_SEG_NUM = e.segment_no);

         quit;
         /*---- End of User Written Code  ----*/ 
         
         /*---- Start of User Written Code  ----*/ 
         
         /*Starting July2017 with the switch to EDL only include accounts where gl_trnst_num NOT IS MISSING*/
         %macro airb_mort_accts;
         proc sql;
         	create table airb_mort_accts as
         	select mort_num
         	from FRGUSER.airb_mort_mth_snapshot
         	where tm_id=&tm_key 
         	%if %eval(&tm_key) GE 16996 %then %do;
         	and gl_trnst_num NOT IS MISSING
         	%end;
         	order by mort_num;
         quit;
         %mend airb_mort_accts;
         %airb_mort_accts;
         
         
         /*(bulkload=yes bl_options="ctrlchars true nullvalue ' ' " )*/
         proc sql noprint;
            insert into FRGPLL.BASEL_ANALYTCL_BL_INST_FCT_BNS (bulkload=yes BL_METHOD=CLILOAD)
            select a.mth_tm_id,
                  a.src_sys_cd,
                  a.basel_acct_id,
                  a.unq_acct_id,
                  a.ncr_rt_sys_key_val,
                  a.ncr_pd_band_key_val,
                  a.ncr_expsr_size_key_val,
                  a.ncr_geo_key_val,
                  a.ncr_ltv_key_val,
                  a.ncr_expsr_cl_key_val,
                  a.ncr_rt_key_val,
                  a.ncr_dlqnt_bckt_key_val,
                  a.pit_stat_cd,
                  a.utltn_rto,
                  a.acct_odt,
                  a.trnst_num,
                  a.cr_lmt_amt,
                  a.advnc_amt,
                  a.auth_amt,
                  a.os_bal_amt,
                  a.genl_ledger_balcng_adj_amt,
                  a.adjusted_os_bal_amt,
                  a.unadjusted_add_on_bal_amt,
                  a.af_secrtztn_bal_amt,
                  a.dlqnt_day_cnt,
                  a.before_zero_net_undrawn_amt,
                  a.before_zero_net_drawn_amt,
                  a.orig_prpty_val_amt,
                  a.loan_to_val_rto,
                  a.indexed_prpty_val_amt,
                  a.indexed_loan_to_val_rto,
                  a.pd_model_nm,
                  a.pd_model_ver,
                  a.pd_basel_scorecrd_nm,
                  a.pd_scorecrd_ver,
                  a.pd_basel_seg_num,
                  a.pd_basel_seg_id,
                  a.pd_seg_ver,
                  a.pd_basel_model_rel_id,
                  a.pd_model_rto,
                  a.pd_lr_rptg_rto,
                  a.pd_lr_pv_rptg_rto,
                  a.pd_lr_pv_ad_rptg_rto,
                  a.pd_ld_pv_ad_sv_rptg_rto,
                  a.pd_acct_score,
                  a.pd_final_rptg_rto,
                  a.lgd_model_nm,
                  a.lgd_model_ver,
                  a.lgd_basel_scorecrd_nm,
                  a.lgd_scorecrd_ver,
                  a.lgd_basel_seg_num,
                  a.lgd_basel_seg_id,
                  a.lgd_seg_ver,
                  a.lgd_basel_model_rel_id,
                  a.lgd_model_rto,
                  a.lgd_lr_rptg_rto,
                  a.lgd_lr_pv_rptg_rto,
                  a.lgd_lr_pv_ad_rptg_rto,
                  a.lgd_ld_pv_ad_sv_rptg_rto,
                  a.lgd_acct_score,
                  a.lgd_ld_pv_ad_sv_dt_rptg_rto,
                  a.ead_model_nm,
                  a.ead_model_ver,
                  a.lgd_final_rptg_rto,
                  a.ead_basel_scorecrd_nm,
                  a.ead_scorecrd_ver,
                  a.ead_basel_seg_num,
                  a.ead_basel_seg_id,
                  a.ead_seg_ver,
                  a.ead_basel_model_rel_id,
                  a.ead_model_rto,
                  a.ead_lr_rptg_rto,
                  a.ead_lr_pv_rptg_rto,
                  a.ead_acct_score,
                  a.ead_lr_pv_ad_rptg_rto,
                  a.asst_cl_desc,
                  a.cons_dft_mth_cnt,
                  a.ead_ld_pv_ad_sv_rptg_rto,
                  a.ead_ld_pv_ad_sv_dt_rptg_rto,
                  a.bcar_sched_num,
                  a.ead_final_rptg_rto,
                  a.pd_band,
                  a.basel_prd_abr,
                  a.scrty_tp_desc,
                  a.ccar_basel_prd_tp_nm,
                  a.ccar_expsr_cl_nm,
                  a.basel_prd_tp_cd,
                  a.consm_prd_treatmnt_cd,
                  a.prd_id,
                  a.prim_cust_cid,
                  a.acct_num,
                  a.rgnl_offc_cd,
                  a.dlqnt_stg,
                  a.mort_num,
                  a.cab_trnst_num,
                  a.egl_deprtmnt,
                  a.basel_cif_key,
                  a.loan_num,
                  a.intr_accr_amt,
                  a.cust_behv_score,
                  a.prd_cd,
                  a.sub_prd_cd,
                  a.src_prd_desc,
                  a.legal_entity,
                  a.mat_dt,
                  a.last_pymt_dt,
                  a.scrty_tp_cd,
                  a.step_f,
                  a.last_acty_dt,
                  a.house_tp_nm,
                  a.last_rgl_pay_dt,
                  a.pd_off_f,
                  a.pd_off_dt,
                  a.residual_mat,
                  a.asst_cl_num,
                  a.e_mat_dt,
                  a.amort,
                  a.trnst_exclsn_f,
                  a.sml_bus_f,
                  a.note_dt,
                  datetime() as insrt_process_tmstmp format = datetime21.,
                  datetime() as updt_process_tmstmp format = datetime21.,
                  a.af_zero_net_drawn_amt,
                  a.af_zero_net_undrawn_amt,
                  a.pd_unadjusted_rptg_rto,
                  a.lgd_unadjusted_rptg_rto,
         		  a.lgd_insured_rptg_rto, /*DLGD Insured - Effective Nov2017*/
                  a.ead_unadjusted_rptg_rto,
                  a.pd_lr_pv_ad_sv_dt_aa_rto,
                  a.lgd_lr_pv_ad_sv_dt_aa_rto,
                  a.ead_lr_pv_ad_sv_dt_aa_rto
         		,case 		
         			when (a.OS_BAL_AMT/a.ORIG_PRPTY_VAL_AMT) LE 0.8 then '0.80'
         			when (a.OS_BAL_AMT/a.ORIG_PRPTY_VAL_AMT) GT 0.8 then '0.81'
         		 else ''
         		end as LTV_Percentage,
				a.rntl_incm_dpndcy_f,
				a.currency_mismatch_f,
				a.tot_expsr_above_1500k_lmt_f,
				a.loan_amt_at_insured_date,
				a.ltv_bckt_cd,
				a.UNINSURED_LGD_SEG_NUM, 
				a.UNINSURED_LGD_RTO,
				a.clp_flag
            from work.bns_with_all_ratios_&_run_date_yymth as a, airb_mort_accts as b 
         	where input(a.mort_num,7.)=b.mort_num;
         quit;
         
 
%GLOBAL CURR_QTR_MTH_TM_ID;
%GLOBAL MTH_TM_ID;

PROC SQL NOPRINT;
SELECT TM_ID INTO :MTH_TM_ID FROM NZUSER.TM_DIM WHERE TM_LVL_END_DT="&end_period_dt"D AND TM_LVL='Month';
QUIT;

%LET Processing_Month_Time_ID = &MTH_TM_ID.;

/*FETCH MORE TIME PERIOD DETAILS*/
proc sql NOPRINT;
CONNECT USING NZUSER AS NZCON;
CREATE table _temp_yearmonth as 
select * from connection to NZCON (
select clndr_yr, tm_yr_seq_num, '00' as charmonth, '000000' as yearmonth from &net_db_FRG..TM_DIM where TM_ID = &MTH_TM_ID;
);
DISCONNECT FROM NZCON;
quit;

/*CONVERT THE INTEGER VALUES TO CHARACTER AS NEEDED*/
data _temp_yearmonth;
set _temp_yearmonth;
if tm_yr_seq_num < 10 then do;
	charmonth='0'||put(tm_yr_seq_num,1.);
end;
else do;
charmonth=tm_yr_seq_num;
end;
	yearmonth=clndr_yr||charmonth;
	format yearmonth $char6.;
run;

/*Save the string YYYYMM for the latest MTH_TM_ID fetched from TM_DIM into a variable*/
proc sql noprint;
select yearmonth into :yearmonth from _temp_yearmonth;
quit;

%PUT >>> MTH_TM_ID IS &MTH_TM_ID.;
%PUT >>> YEARMONTH IS &YEARMONTH.;

PROC SQL NOPRINT;
SELECT MAX(TM_ID) INTO :CURR_QTR_MTH_TM_ID FROM NZUSER.TM_DIM WHERE TM_LVL='Month' AND FNCL_QTR_KEY=
(SELECT FNCL_QTR_KEY FROM NZUSER.TM_DIM WHERE TM_iD=&MTH_TM_ID AND TM_LVL='Month');
QUIT;

%PUT CURR_QTR_MTH_TM_ID IS &CURR_QTR_MTH_TM_ID;

PROC SQL NOPRINT;
CONNECT USING NZUSER AS NZCON;
EXECUTE(DELETE FROM &pll_db_FRG..BASEL_ANLYT_BL_INST_FCT_BNS_DLGD WHERE MTH_TM_ID=&MTH_TM_ID) BY NZCON;
DISCONNECT FROM NZCON;
QUIT;

/* STEP TO VERIFY SOURCE DATA*/
proc sql ;	
     select count(*) INTO: manul_error_check from TLRP1D.DLGD_SCRI_QTR_DRVD_VARS WHERE MTH_TM_ID=&CURR_QTR_MTH_TM_ID;
QUIT;

%PUT &=manul_error_check;

data _null_;
   if strip(&manul_error_check) = '0' then do;
      put "Source table EDRTLRP1D.DLGD_SCRI_QTR_DRVD_VARS has not been loaded properly. Kindly check the log for the job: J_RRAP_DLGD_0070_DLGD_SCRI_QTR_DRVD_VARS";
      abort abend 255;
   end;
   else do;
      put "Source Table has data as Expected";
   end;
run;

/* END OF STEP TO VERIFY SOURCE DATA*/


data _null_;
set _output;
call symput('run_date1',_run_date);
run;
%LET _run_date = %sysfunc(dequote("'&run_date1'")) ;
%put &run_date1;
%put &_run_date;


PROC SQL NOPRINT;
CONNECT USING NZUSER AS NZCON;
EXECUTE(
INSERT INTO &pll_db_FRG..BASEL_ANLYT_BL_INST_FCT_BNS_DLGD 
SELECT 
MTH_TM_ID
,SRC_SYS_CD
,BASEL_ACCT_ID
,UNQ_ACCT_ID
,NCR_RT_SYS_KEY_VAL
,NULL as NCR_PD_BAND_KEY_VAL   /* Derived after Flooring PD values */
,NCR_EXPSR_SIZE_KEY_VAL
,NCR_GEO_KEY_VAL
,NCR_LTV_KEY_VAL
,NCR_EXPSR_CL_KEY_VAL
,NCR_RT_KEY_VAL
,NCR_DLQNT_BCKT_KEY_VAL
,PIT_STAT_CD
,UTLTN_RTO
,ACCT_ODT
,TRNST_NUM
,CR_LMT_AMT
,ADVNC_AMT
,AUTH_AMT
,OS_BAL_AMT
,GENL_LEDGER_BALCNG_ADJ_AMT
,ADJUSTED_OS_BAL_AMT
,UNADJUSTED_ADD_ON_BAL_AMT
,AF_SECRTZTN_BAL_AMT
,DLQNT_DAY_CNT
,BEFORE_ZERO_NET_UNDRAWN_AMT
,BEFORE_ZERO_NET_DRAWN_AMT
,ORIG_PRPTY_VAL_AMT
,LOAN_TO_VAL_RTO
/*,INDEXED_PRPTY_VAL_AMT*/
,SUBQ4.INDEX_TERANETV as INDEXED_PRPTY_VAL_AMT /*This was done as part of 32 CMA Change wherein only the DLGD section is using new 32 CMA VALUE */
/*,INDEXED_LOAN_TO_VAL_RTO*/ 
,SUBQ4.LTV as INDEXED_LOAN_TO_VAL_RTO /*This was done as part of 32 CMA Change wherein only the DLGD section is using new 32 CMA VALUE */
,PD_MODEL_NM
,PD_MODEL_VER
,PD_BASEL_SCORECRD_NM
,PD_SCORECRD_VER
,PD_BASEL_SEG_NUM
,PD_BASEL_SEG_ID
,PD_SEG_VER
,PD_BASEL_MODEL_REL_ID
,PD_MODEL_RTO
,PD_LR_RPTG_RTO
,PD_LR_PV_RPTG_RTO
,PD_LR_PV_AD_RPTG_RTO
,PD_LD_PV_AD_SV_RPTG_RTO
,PD_ACCT_SCORE
,PD_FINAL_RPTG_RTO
,LGD_MODEL_NM
,LGD_MODEL_VER
,LGD_BASEL_SCORECRD_NM
,LGD_SCORECRD_VER
,LGD_BASEL_SEG_NUM
,LGD_BASEL_SEG_ID
,LGD_SEG_VER
,LGD_BASEL_MODEL_REL_ID
,LGD_MODEL_RTO
,LGD_LR_RPTG_RTO
,LGD_LR_PV_RPTG_RTO
,LGD_LR_PV_AD_RPTG_RTO
,LGD_LD_PV_AD_SV_RPTG_RTO
,LGD_ACCT_SCORE
,LGD_LD_PV_AD_SV_DT_RPTG_RTO
,EAD_MODEL_NM
,EAD_MODEL_VER
,LGD_FINAL_RPTG_RTO
,EAD_BASEL_SCORECRD_NM
,EAD_SCORECRD_VER
,EAD_BASEL_SEG_NUM
,EAD_BASEL_SEG_ID
,EAD_SEG_VER
,EAD_BASEL_MODEL_REL_ID
,EAD_MODEL_RTO
,EAD_LR_RPTG_RTO
,EAD_LR_PV_RPTG_RTO
,EAD_ACCT_SCORE
,EAD_LR_PV_AD_RPTG_RTO
,ASST_CL_DESC
,CONS_DFT_MTH_CNT
,EAD_LD_PV_AD_SV_RPTG_RTO
,EAD_LD_PV_AD_SV_DT_RPTG_RTO
,BCAR_SCHED_NUM
,EAD_FINAL_RPTG_RTO
,NULL as PD_BAND       /* Derived after Flooring PD values */
,BASEL_PRD_ABR
,SCRTY_TP_DESC
,CCAR_BASEL_PRD_TP_NM
,CCAR_EXPSR_CL_NM
,BASEL_PRD_TP_CD
,CONSM_PRD_TREATMNT_CD
,PRD_ID
,PRIM_CUST_CID
,ACCT_NUM
,RGNL_OFFC_CD
,DLQNT_STG
,MORT_NUM
,CAB_TRNST_NUM
,EGL_DEPRTMNT
,BASEL_CIF_KEY
,LOAN_NUM
,INTR_ACCR_AMT
,CUST_BEHV_SCORE
,PRD_CD
,SUB_PRD_CD
,SRC_PRD_DESC
,LEGAL_ENTITY
,MAT_DT
,LAST_PYMT_DT
,SCRTY_TP_CD
,STEP_F
,LAST_ACTY_DT
,HOUSE_TP_NM
,LAST_RGL_PAY_DT
,PD_OFF_F
,PD_OFF_DT
,RESIDUAL_MAT
,ASST_CL_NUM
,E_MAT_DT
,AMORT
,TRNST_EXCLSN_F
,SML_BUS_F
,NOTE_DT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,AF_ZERO_NET_DRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,PD_UNADJUSTED_RPTG_RTO
,LGD_UNADJUSTED_RPTG_RTO
,EAD_UNADJUSTED_RPTG_RTO
,PD_LR_PV_AD_SV_DT_AA_RTO
,LGD_LR_PV_AD_SV_DT_AA_RTO
,EAD_LR_PV_AD_SV_DT_AA_RTO
,NULL AS INSUR_F
,NULL AS OS_PRNCPL_BAL_AMT
,HPV_12_QTR_AGO AS PREV_12_QTR_PRPTY_VAL_AMT
,DLGD_F
,CRNT_LTV_RTO
,METROPOLITAN AS METRPL_AREA_NM
,SUBQ4.DELTA_P AS PRPTY_VAL_CORR_PCTG
,SUBQ4.ADDON AS LNG_RUN_LGD_ADD_ON_RTO
,NULL as DLGD_RPTG_RTO
,LTV_PERCENTAGE
,RNTL_PRPTY_F 
,CURRENCY_MISMATCH_F 
,TOT_EXPSR_ABOVE_1500K_LMT_F 
,ORIG_AMT_LOAN 
,LTV_BUCKET
,NULL AS TRANSACTOR_F 
,CASE 
	when CONSM_PRD_TREATMNT_CD = 'A' and pd_final_rptg_rto >= 1 then 'True'
	when CONSM_PRD_TREATMNT_CD = 'A' and pd_final_rptg_rto < 1 then 'False'
	else '' 
 END as PD_90_day_F
,SUBQ4.UNINSURED_LGD_SEG_NUM as UNINSURED_LGD_SEG_NUM
,SUBQ4.UNINSURED_LGD_RTO as UNINSURED_LGD_RTO
,NULL as UNINSURED_DLGD_RTO
,NULL AS TRANSACTOR_FLAG_QRR 
,NULL as CCF
,NULL as DRAWN
,NULL as EAD_FLR
,NULL as EAD_FLRD_RPTG_RTO
,NULL as UNDRAWN
,NULL as UNDRAWN_EXPSR_PCT
,NULL as CMHC_F
,NULL as DLGD_FLR
,NULL as FULLY_SECURED_F
,NULL as LGD_FLR
,NULL as PRE_INSURANCE_LGD
,NULL as PD_FLR
,NULL as PD_FLRD_RPTG_RTO
,NULL as LGD_FLRD_RPTG_RTO
,NULL as PMI_LGD_INSURED_RPTG_RTO
,NULL as PMI_LGD_UNADJUSTED_RPTG_RTO
,NULL as UNINSURED_FLRD_LGD_RTO
,NULL as PD_BAND_EXPSR_CL_KEY_VAL
,CASE 
	  WHEN SUBQ4.DLGD_F='Y' AND SUBQ4.SCRTY_TP_DESC='Uninsured' and SUBQ4.ADDON IS NOT NULL THEN 
						MAX(0.1, SUBQ4.LGD_FINAL_RPTG_RTO, MIN(1, (SUBQ4.lgd_unadjusted_rptg_rto + SUBQ4.ADDON))) 
				WHEN SUBQ4.DLGD_F='Y' AND SUBQ4.SCRTY_TP_DESC='Insured' and SUBQ4.ADDON IS NOT NULL THEN 
						MAX(0.1, SUBQ4.LGD_FINAL_RPTG_RTO, MIN(1, (SUBQ4.lgd_insured_rptg_rto + SUBQ4.ADDON)))
				ELSE SUBQ4.LGD_FINAL_RPTG_RTO 
			END /*DLGD Insured changes effective November 2017 -- Modified September 2018 */
			AS DLGD_RPTG_RTO_OLD
,NULL as CCAR_F
,CLP_FLAG
,NULL AS BCAR_SCHED_NM
,NULL AS BCAR_SCHED_NUM_50
,NULL as COLLATERAL_TYPE
,NULL as H_C 
,NULL as LGD_S 
,NULL as LGD_U 
,NULL as H_E 
,NULL as COLLATERAL_VALUE 
,NULL as EXPOSURE 
,NULL as EXPOSURE_SECURED_MAXIMUM 
,NULL as EXPOSURE_SECURED 
,NULL as EXPOSURE_UNSECURED 
,NULL as WEIGHT_SECURED 
,NULL as WEIGHT_UNSECURED

/*********************************************************************************************/
/********** END OF SELECT STATEMENT **********************************************************/
/*********************************************************************************************/



		FROM
			( SELECT SUBQ3.*, 
				  CASE 
						WHEN SUBQ3.DLGD_F='Y' AND SUBQ3.CRNT_LTV_RTO IS NOT NULL AND SUBQ3.DELTA_P IS NOT NULL 
							AND SUBQ3.INDEXED_CCLTV_RTO IS NOT NULL 
								THEN MAX((MIN(SUBQ3.CRNT_LTV_RTO, MAX(SUBQ3.INDEXED_CCLTV_RTO-0.8*(1-SUBQ3.DELTA_P),0))-MAX(SUBQ3.INDEXED_CCLTV_RTO-0.8,0))/NULLIF(SUBQ3.CRNT_LTV_RTO,0),0) 
						WHEN SUBQ3.DLGD_F='Y' AND SUBQ3.CRNT_LTV_RTO IS NOT NULL AND SUBQ3.DELTA_P IS NOT NULL 
							AND SUBQ3.INDEXED_CCLTV_RTO IS NULL 
								THEN (MAX(SUBQ3.CRNT_LTV_RTO-0.8*(1-SUBQ3.DELTA_P),0)-MAX(SUBQ3.CRNT_LTV_RTO-0.8,0))/NULLIF(SUBQ3.CRNT_LTV_RTO,0)
						ELSE NULL 
					END AS ADDON

			  FROM (

			SELECT SUBQ2.*, INDX.INDEX, INDX12.INDEX AS INDEX_12_QTR_AGO, (SUBQ2.INDEX_TERANETV/NULLIF(INDX.INDEX,0))*INDX12.INDEX AS HPV_12_QTR_AGO, COALESCE(SCRI.METRPL_BREACH_F, SCRI2.METRPL_BREACH_F) as METRPL_BREACH_F,
				CASE 
					WHEN SUBQ2.DLGD_F='N' THEN NULL 
					WHEN SUBQ2.DLGD_F<>'N' AND COALESCE(SCRI.METRPL_BREACH_F, SCRI2.METRPL_BREACH_F)='Y' THEN MAX(1-(((SUBQ2.INDEX_TERANETV/NULLIF(INDX.INDEX,0))*INDX12.INDEX)/NULLIF(SUBQ2.INDEX_TERANETV,0)),0.25) 
					WHEN SUBQ2.DLGD_F<>'N' AND (COALESCE(SCRI.METRPL_BREACH_F, SCRI2.METRPL_BREACH_F)<>'Y' OR COALESCE(SCRI.METRPL_BREACH_F, SCRI2.METRPL_BREACH_F) IS NULL) THEN MAX(1-(((SUBQ2.INDEX_TERANETV/NULLIF(INDX.INDEX,0))*INDX12.INDEX)/NULLIF(SUBQ2.INDEX_TERANETV,0)),0) 
				END AS DELTA_P
			

					FROM (
								SELECT SUBQ1.*, FIR.PROP_CITY, FIR.INDEX_TERANETV, LTVVAR.CRNT_LTV_RTO, LTVVAR.INDEXED_CCLTV_RTO,
								CASE 
								WHEN SUBQ1.DLGD_F='N' THEN NULL  
								WHEN SUBQ1.DLGD_F<>'N' AND (FIR.PROP_CITY IS NULL OR UPPER(FIR.PROP_CITY)='N/A' OR UPPER(FIR.PROP_CITY) LIKE '%OTHER%') THEN '11'
								ELSE FIR.PROP_CITY 
								END 
								AS METROPOLITAN,
								FIR.LTV,
								PMI.pmi_lgd_unadjusted_rptg_rto,
								PMI.pmi_lgd_insured_rptg_rto

							FROM (
									SELECT A.*, B.BASEL_MORT_MTH_SNAPSHOT_ID, B.INTR_ADJ_DT, B.LAST_RNEW_DT,
									CASE 
									WHEN A.MTH_TM_ID >= 17156 AND A.LGD_BASEL_SEG_NUM NOT IN (13,14) AND A.PIT_STAT_CD = 'CUR'
									AND A.SCRTY_TP_DESC in ('Uninsured','Insured') AND (B.INTR_ADJ_DT>='11-01-2016' OR B.LAST_RNEW_DT>='11-01-2016') THEN 'Y' 
									WHEN A.MTH_TM_ID < 17156 and A.LGD_BASEL_SEG_NUM NOT IN (13,14) AND A.PIT_STAT_CD = 'CUR'
									AND A.SCRTY_TP_DESC='Uninsured' AND (B.INTR_ADJ_DT>='11-01-2016' OR B.LAST_RNEW_DT>='11-01-2016') THEN 'Y'
									ELSE 'N' 
									END  /*DLGD Insured changes effective November 2017 */
									AS DLGD_F 

									FROM 
									&pll_db_FRG..BASEL_ANALYTCL_BL_INST_FCT_BNS A,&net_db..BASEL_MORT_MTH_SNAPSHOT B 
									WHERE 
									TRIM(A.MORT_NUM)=TRIM(B.MORT_NUM) AND A.MTH_TM_ID=&MTH_TM_ID AND B.MTH_TM_ID=&MTH_TM_ID
					) SUBQ1
						LEFT OUTER JOIN &net_db_FRG..LTV_VAR_CLUS_LTV_FINAL_CMA FIR ON TRIM(SUBQ1.MORT_NUM)=FIR.MORTGAGE_NO AND FIR.YYMTH=&YEARMONTH
						LEFT OUTER JOIN &net_db_P1D..BASEL_ACCT_LTV_DRVD_VARS_CMA LTVVAR ON TRIM(SUBQ1.MORT_NUM)=TRIM(LTVVAR.ACCT_NUM) AND LTVVAR.ACCT_SENRTY_CD=1 AND LTVVAR.MTH_TM_ID=&MTH_TM_ID
						/*	Adding section to calculate variable which will be used for UNINSURED_DLGD_RTO : Ganesh Patro*/
						LEFT OUTER JOIN	(
						         	select 'BNS '||a.model_nm as model_name, c.seg_num as SEGMENT_NO, 
											b.UNADJUSTED_RPTG_RTO as pmi_lgd_unadjusted_rptg_rto,
											b.PRE_INSURANCE_LGD as pmi_lgd_insured_rptg_rto
						         	from &net_db..basel_model a, &net_db..BASEL_SEG_RPTG_PARM b, &net_db..basel_seg c
						         	where a.basel_model_id=b.basel_model_id 
						         	and b.EFF_TO_DT >= &_run_date and b.EFF_FROM_DT <= &_run_date and a.src_sys_cd='MOR' 
						         	and b.basel_seg_id=c.basel_seg_id
						         	order by 1,2 ) PMI 
									ON PMI.model_name=SUBQ1.LGD_MODEL_NM
									AND PMI.SEGMENT_NO=SUBQ1.UNINSURED_LGD_SEG_NUM

					) SUBQ2


						LEFT OUTER JOIN (SELECT DISTINCT MTH_TM_ID, LABEL_2, INDEX FROM &net_db_P1D..TERANET_HOUSE_PRC_INDEX_CMA WHERE MTH_TM_ID=&MTH_TM_ID) INDX 
							ON UPPER(TRIM(INDX.LABEL_2)) =UPPER(TRIM(SUBQ2.METROPOLITAN))
						LEFT OUTER JOIN (SELECT DISTINCT MTH_TM_ID, LABEL_2, INDEX FROM &net_db_P1D..TERANET_HOUSE_PRC_INDEX_CMA WHERE MTH_TM_ID=&MTH_TM_ID-1440) INDX12 
							ON UPPER(TRIM(INDX12.LABEL_2)) =UPPER(TRIM(SUBQ2.METROPOLITAN)) 
						LEFT OUTER JOIN &net_db_P1D..DLGD_SCRI_QTR_DRVD_VARS SCRI ON UPPER(TRIM(SCRI.METRPL_AREA_NM))=UPPER(TRIM(SUBQ2.METROPOLITAN)) AND SCRI.MTH_TM_ID=&CURR_QTR_MTH_TM_ID
												/* RRMSS-1557 map new CMAs as '11' for breach flag  */
						LEFT OUTER JOIN &net_db_P1D..DLGD_SCRI_QTR_DRVD_VARS SCRI2 ON SCRI.METRPL_AREA_NM IS NULL AND TRIM(SCRI2.METRPL_AREA_NM)='11' AND SCRI2.MTH_TM_ID=&CURR_QTR_MTH_TM_ID 
				) SUBQ3
				) SUBQ4 	
					) BY NZCON;
	DISCONNECT FROM NZCON;
QUIT;




/************************************************************************************************ 
* Job:        J_RRII_KS10_2109_BASEL_ANALYTICL_BL_INSTRMNT_FACT_MOR_FRG for Parallel Run 		* 
************************************************************************************************/ 

 PROC SQL ;
	CONNECT USING NZRRAP AS NZCON;		
	 create table EXTR as
	    select *              
	    from connection to NZCON
		( 	SELECT LTRIM(RTRIM(TO_CHAR(MORT_NUM))) v_BASEL_ACCT_num_char_TNG_MOR,
				LPAD(LTRIM(RTRIM(TO_CHAR(MORT_NUM))) ,  23, '0') AS v_BASEL_ACCT_num_char_MOR,
				ba.* , 
					acct.BASEL_ACCT_ID as v_BASEL_ACCT_ID,
					TRIM(acct.SRC_APP_CD) as v_SRC_APP_CD, acct.CIS_PRD_CD as v_CIS_PRD_CD,
					round(COALESCE(OS_BAL_AMT,0.000),2) as OS_BAL_AMT0 ,
					round(GENL_LEDGER_BALCNG_ADJ_AMT,3) as GENL_LEDGER_BALCNG_ADJ_AMT0 ,
					round(COALESCE(UNADJUSTED_ADD_ON_BAL_AMT,0.000),3) as UNADJUSTED_ADD_ON_BAL_AMT0 ,
					round(COALESCE(BEFORE_ZERO_NET_DRAWN_AMT,0.000),3) as BEFORE_ZERO_NET_DRAWN_AMT0 ,
					round(COALESCE(BEFORE_ZERO_NET_UNDRAWN_AMT,0.000),3) as BEFORE_ZERO_NET_UNDRAWN_AMT0 ,
					round(COALESCE(ORIG_PRPTY_VAL_AMT,0.000),3) as ORIG_PRPTY_VAL_AMT0 ,
					round(COALESCE(INTR_ACCR_AMT,0.000),3) as INTR_ACCR_AMT0 ,
					round(COALESCE(AMORT,0.000),3) as AMORT0 ,
					round(COALESCE(AF_ZERO_NET_DRAWN_AMT,0.000),3) as AF_ZERO_NET_DRAWN_AMT0 ,
					round(COALESCE(AF_ZERO_NET_UNDRAWN_AMT,0.000),3) as AF_ZERO_NET_UNDRAWN_AMT0 ,
					round(PRPTY_VAL_CORR_PCTG,4) as PRPTY_VAL_CORR_PCTG0 ,
					round(COALESCE(UTLTN_RTO,0.00000000),8) as UTLTN_RTO0 ,
					round(COALESCE(CR_LMT_AMT,0.00000000),8) as CR_LMT_AMT0 ,
					round(COALESCE(ADVNC_AMT,0.00000000),8) as ADVNC_AMT0 ,
					round(COALESCE(AUTH_AMT,0.00000000),8) as AUTH_AMT0 ,
					round(ADJUSTED_OS_BAL_AMT,8) as ADJUSTED_OS_BAL_AMT0 ,
					round(AF_SECRTZTN_BAL_AMT,8) as AF_SECRTZTN_BAL_AMT0 ,
					round(PD_MODEL_RTO,8) as PD_MODEL_RTO0 ,
					round(LOAN_TO_VAL_RTO,8) as LOAN_TO_VAL_RTO0 ,
					round(COALESCE(INDEXED_PRPTY_VAL_AMT,0.00000000),8) as INDEXED_PRPTY_VAL_AMT0 ,
					round(INDEXED_LOAN_TO_VAL_RTO,8) as INDEXED_LOAN_TO_VAL_RTO0 ,
					round(PD_LR_RPTG_RTO,8) as PD_LR_RPTG_RTO0 ,
					round(PD_LR_PV_RPTG_RTO,8) as PD_LR_PV_RPTG_RTO0 ,
					round(PD_LR_PV_AD_RPTG_RTO,8) as PD_LR_PV_AD_RPTG_RTO0 ,
					round(PD_LD_PV_AD_SV_RPTG_RTO,8) as PD_LD_PV_AD_SV_RPTG_RTO0 ,
					round(PD_FINAL_RPTG_RTO,8) as PD_FINAL_RPTG_RTO0 ,
					round(LGD_MODEL_RTO,8) as LGD_MODEL_RTO0 ,
					round(LGD_LR_RPTG_RTO,8) as LGD_LR_RPTG_RTO0 ,
					round(LGD_LR_PV_RPTG_RTO,8) as LGD_LR_PV_RPTG_RTO0 ,
					round(LGD_LR_PV_AD_RPTG_RTO,8) as LGD_LR_PV_AD_RPTG_RTO0 ,
					round(LGD_LD_PV_AD_SV_RPTG_RTO,8) as LGD_LD_PV_AD_SV_RPTG_RTO0 ,
					round(LGD_FINAL_RPTG_RTO,8) as LGD_FINAL_RPTG_RTO0 ,
					round(LGD_LD_PV_AD_SV_DT_RPTG_RTO,8) as LGD_LD_PV_AD_SV_DT_RPTG_RTO0 ,
					round(COALESCE(EAD_MODEL_RTO,0.00000000),8) as EAD_MODEL_RTO0 ,
					round(COALESCE(EAD_LR_RPTG_RTO,0.00000000),8) as EAD_LR_RPTG_RTO0 ,
					round(COALESCE(EAD_LR_PV_RPTG_RTO,0.00000000),8) as EAD_LR_PV_RPTG_RTO0 ,
					round(COALESCE(EAD_LR_PV_AD_RPTG_RTO,0.00000000),8) as EAD_LR_PV_AD_RPTG_RTO0 ,
					round(COALESCE(EAD_LD_PV_AD_SV_RPTG_RTO,0.00000000),8) as EAD_LD_PV_AD_SV_RPTG_RTO0 ,
					round(COALESCE(EAD_LD_PV_AD_SV_DT_RPTG_RTO,0.00000000),8) as EAD_LD_PV_AD_SV_DT_RPTG_RTO0 ,
					round(COALESCE(EAD_FINAL_RPTG_RTO,0.00000000),8) as EAD_FINAL_RPTG_RTO0 ,
					round(PD_UNADJUSTED_RPTG_RTO,8) as PD_UNADJUSTED_RPTG_RTO0 ,
					round(PD_LR_PV_AD_SV_DT_AA_RTO,8) as PD_LR_PV_AD_SV_DT_AA_RTO0 ,
					round(LGD_LR_PV_AD_SV_DT_AA_RTO,8) as LGD_LR_PV_AD_SV_DT_AA_RTO0 ,
					round(EAD_LR_PV_AD_SV_DT_AA_RTO,8) as EAD_LR_PV_AD_SV_DT_AA_RTO0 ,
					round(OS_PRNCPL_BAL_AMT,8) as OS_PRNCPL_BAL_AMT0 ,
					round(PREV_12_QTR_PRPTY_VAL_AMT,8) as PREV_12_QTR_PRPTY_VAL_AMT0 ,
					round(LGD_UNADJUSTED_RPTG_RTO,8) as LGD_UNADJUSTED_RPTG_RTO0 ,
					round(COALESCE(EAD_UNADJUSTED_RPTG_RTO,0.00000000),8) as EAD_UNADJUSTED_RPTG_RTO0 ,
					round(LNG_RUN_LGD_ADD_ON_RTO,8) as LNG_RUN_LGD_ADD_ON_RTO0 ,
					round(DLGD_RPTG_RTO_OLD,8) as DLGD_RPTG_RTO_OLD0 ,
					round(CRNT_LTV_RTO,10) as CRNT_LTV_RTO0

				FROM &pll_db_FRG..BASEL_ANLYT_BL_INST_FCT_BNS_DLGD ba
				left join ( SELECT ba.BASEL_ACCT_ID as BASEL_ACCT_ID,
							TRIM(ba.SRC_APP_ID) as SRC_APP_ID, 
							TRIM(ba.SRC_APP_CD) as SRC_APP_CD, ba.SRC_SYS_DEL_DT, ba.SRC_SYS_DEL_F, CIS_PRD_CD
							FROM &net_db..BASEL_ACCT_DIM ba,
								(SELECT a.SRC_APP_ID,max(a.BASEL_ACCT_ID) AS last_updt 
								FROM &net_db..BASEL_ACCT_DIM a, 
								&pll_db_FRG..BASEL_ANLYT_BL_INST_FCT_BNS_DLGD b
								WHERE (CASE WHEN src_sys_cd='MOR' THEN LPAD(LTRIM(RTRIM(TO_CHAR(MORT_NUM))) ,  23, '0') ELSE trim(MORT_NUM) END)=a.SRC_APP_ID 
								AND SRC_SYS_DEL_F='N' AND (CIS_PRD_CD IN ('MOR','MO') OR cis_prd_cd IS NULL ) 
								AND trim(src_app_cd) in('TNG-MOR','MO','MOR') 
								GROUP BY a.SRC_APP_ID
								) mx
					WHERE ba.src_app_id=mx.src_app_id 
					AND ba.BASEL_ACCT_ID=mx.last_updt 
					AND ba.SRC_SYS_DEL_F='N' 
					AND SRC_SYS_DEL_F='N' 
					AND (CIS_PRD_CD IN ('MOR','MO') OR cis_prd_cd IS NULL ) 
					AND trim(src_app_cd) in ('TNG-MOR','MO','MOR') 
				) acct on 
					(CASE WHEN ba.src_sys_cd='MOR' THEN LPAD(LTRIM(RTRIM(TO_CHAR(ba.MORT_NUM))) ,  23, '0') 
						ELSE trim(ba.MORT_NUM) END)=TRIM(acct.SRC_APP_ID)
				where mth_tm_id=&MTH_TM_ID 
				and ba.src_sys_cd in ('MOR','TNG-MOR')
			);
  	disconnect from NZCON; 
 quit;

proc sql;
select max(mth_tm_id) 
into :MTH_TM_ID
from EXTR;
quit;

data LOAD 
(keep= MTH_TM_ID	SRC_SYS_CD	BASEL_ACCT_ID	UNQ_ACCT_ID	NCR_RT_SYS_KEY_VAL	NCR_PD_BAND_KEY_VAL	NCR_EXPSR_SIZE_KEY_VAL	NCR_GEO_KEY_VAL	NCR_LTV_KEY_VAL	NCR_EXPSR_CL_KEY_VAL	NCR_RT_KEY_VAL	NCR_DLQNT_BCKT_KEY_VAL	PIT_STAT_CD	UTLTN_RTO	ACCT_ODT	TRNST_NUM	CR_LMT_AMT	ADVNC_AMT	AUTH_AMT	OS_BAL_AMT	GENL_LEDGER_BALCNG_ADJ_AMT	ADJUSTED_OS_BAL_AMT	UNADJUSTED_ADD_ON_BAL_AMT	AF_SECRTZTN_BAL_AMT	DLQNT_DAY_CNT	BEFORE_ZERO_NET_UNDRAWN_AMT	BEFORE_ZERO_NET_DRAWN_AMT	ORIG_PRPTY_VAL_AMT	LOAN_TO_VAL_RTO	INDEXED_PRPTY_VAL_AMT	INDEXED_LOAN_TO_VAL_RTO	PD_MODEL_NM	PD_MODEL_VER	PD_BASEL_SCORECRD_NM	PD_SCORECRD_VER	PD_BASEL_SEG_NUM	PD_BASEL_SEG_ID	PD_SEG_VER	PD_BASEL_MODEL_REL_ID
PD_MODEL_RTO	PD_LR_RPTG_RTO	PD_LR_PV_RPTG_RTO	PD_LR_PV_AD_RPTG_RTO	PD_LD_PV_AD_SV_RPTG_RTO	PD_ACCT_SCORE	PD_FINAL_RPTG_RTO	LGD_MODEL_NM	LGD_MODEL_VER	LGD_BASEL_SCORECRD_NM	LGD_SCORECRD_VER	LGD_BASEL_SEG_NUM	LGD_BASEL_SEG_ID	LGD_SEG_VER	LGD_BASEL_MODEL_REL_ID	LGD_MODEL_RTO	LGD_LR_RPTG_RTO	LGD_LR_PV_RPTG_RTO	LGD_LR_PV_AD_RPTG_RTO	LGD_LD_PV_AD_SV_RPTG_RTO	LGD_ACCT_SCORE	LGD_LD_PV_AD_SV_DT_RPTG_RTO	EAD_MODEL_NM	EAD_MODEL_VER	LGD_FINAL_RPTG_RTO	EAD_BASEL_SCORECRD_NM	EAD_SCORECRD_VER	EAD_BASEL_SEG_NUM	EAD_BASEL_SEG_ID	EAD_SEG_VER	EAD_BASEL_MODEL_REL_ID	EAD_MODEL_RTO	EAD_LR_RPTG_RTO	EAD_LR_PV_RPTG_RTO	EAD_ACCT_SCORE	EAD_LR_PV_AD_RPTG_RTO	ASST_CL_DESC	CONS_DFT_MTH_CNT	
EAD_LD_PV_AD_SV_RPTG_RTO	EAD_LD_PV_AD_SV_DT_RPTG_RTO	BCAR_SCHED_NUM	EAD_FINAL_RPTG_RTO	PD_BAND	BASEL_PRD_ABR	SCRTY_TP_DESC	CCAR_BASEL_PRD_TP_NM	CCAR_EXPSR_CL_NM	BASEL_PRD_TP_CD	CONSM_PRD_TREATMNT_CD	PRD_ID	PRIM_CUST_CID	ACCT_NUM	RGNL_OFFC_CD	DLQNT_STG	MORT_NUM	CAB_TRNST_NUM	EGL_DEPRTMNT	BASEL_CIF_KEY	LOAN_NUM	INTR_ACCR_AMT	CUST_BEHV_SCORE	PRD_CD	SUB_PRD_CD	SRC_PRD_DESC	LEGAL_ENTITY	MAT_DT	LAST_PYMT_DT	SCRTY_TP_CD	STEP_F	LAST_ACTY_DT	HOUSE_TP_NM	LAST_RGL_PAY_DT	PD_OFF_F	PD_OFF_DT	RESIDUAL_MAT	ASST_CL_NUM	E_MAT_DT	AMORT	TRNST_EXCLSN_F	SML_BUS_F	NOTE_DT	AF_ZERO_NET_DRAWN_AMT	AF_ZERO_NET_UNDRAWN_AMT	PD_UNADJUSTED_RPTG_RTO	LGD_UNADJUSTED_RPTG_RTO	
EAD_UNADJUSTED_RPTG_RTO	PD_LR_PV_AD_SV_DT_AA_RTO 	LGD_LR_PV_AD_SV_DT_AA_RTO	EAD_LR_PV_AD_SV_DT_AA_RTO	INSUR_F	OS_PRNCPL_BAL_AMT	PREV_12_QTR_PRPTY_VAL_AMT	DLGD_F	CRNT_LTV_RTO	METRPL_AREA_NM	PRPTY_VAL_CORR_PCTG	LNG_RUN_LGD_ADD_ON_RTO	DLGD_RPTG_RTO_OLD
INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP LTV_PERCENTAGE 
RNTL_PRPTY_F CURRENCY_MISMATCH_F TOT_EXPSR_ABOVE_1500K_LMT_F ORIG_AMT_LOAN 
LTV_BUCKET TRANSACTOR_F PD_90_day_F UNINSURED_LGD_SEG_NUM UNINSURED_LGD_RTO UNINSURED_DLGD_RTO TRANSACTOR_FLAG_QRR 
CLP_FLAG BCAR_SCHED_NM CAR_SCHED_NUM_50
)
;
set EXTR (drop= basel_Acct_id OS_BAL_AMT GENL_LEDGER_BALCNG_ADJ_AMT
UNADJUSTED_ADD_ON_BAL_AMT BEFORE_ZERO_NET_DRAWN_AMT BEFORE_ZERO_NET_UNDRAWN_AMT ORIG_PRPTY_VAL_AMT
INTR_ACCR_AMT AMORT AF_ZERO_NET_DRAWN_AMT AF_ZERO_NET_UNDRAWN_AMT PRPTY_VAL_CORR_PCTG UTLTN_RTO CR_LMT_AMT ADVNC_AMT AUTH_AMT ADJUSTED_OS_BAL_AMT AF_SECRTZTN_BAL_AMT
PD_MODEL_RTO LOAN_TO_VAL_RTO INDEXED_PRPTY_VAL_AMT INDEXED_LOAN_TO_VAL_RTO PD_LR_RPTG_RTO PD_LR_PV_RPTG_RTO
PD_LR_PV_AD_RPTG_RTO PD_LD_PV_AD_SV_RPTG_RTO PD_FINAL_RPTG_RTO LGD_MODEL_RTO LGD_LR_RPTG_RTO  LGD_LR_PV_RPTG_RTO LGD_LR_PV_AD_RPTG_RTO LGD_LD_PV_AD_SV_RPTG_RTO
LGD_FINAL_RPTG_RTO LGD_LD_PV_AD_SV_DT_RPTG_RTO EAD_MODEL_RTO EAD_LR_RPTG_RTO  EAD_LR_PV_RPTG_RTO EAD_LR_PV_AD_RPTG_RTO EAD_LD_PV_AD_SV_RPTG_RTO EAD_LD_PV_AD_SV_DT_RPTG_RTO EAD_FINAL_RPTG_RTO PD_UNADJUSTED_RPTG_RTO
PD_LR_PV_AD_SV_DT_AA_RTO LGD_LR_PV_AD_SV_DT_AA_RTO EAD_LR_PV_AD_SV_DT_AA_RTO OS_PRNCPL_BAL_AMT PREV_12_QTR_PRPTY_VAL_AMT LGD_UNADJUSTED_RPTG_RTO
EAD_UNADJUSTED_RPTG_RTO LNG_RUN_LGD_ADD_ON_RTO DLGD_RPTG_RTO_OLD CRNT_LTV_RTO);

format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.6 OS_BAL_AMT 10.3 PD_90_day_F $5.
GENL_LEDGER_BALCNG_ADJ_AMT 26.3 UNADJUSTED_ADD_ON_BAL_AMT 25.3 BEFORE_ZERO_NET_DRAWN_AMT 25.3 BEFORE_ZERO_NET_UNDRAWN_AMT 27.3 ORIG_PRPTY_VAL_AMT 18.3 
INTR_ACCR_AMT 13.3 AMORT 5.3 AF_ZERO_NET_DRAWN_AMT 21.3 AF_ZERO_NET_UNDRAWN_AMT 23.3 PRPTY_VAL_CORR_PCTG 19.4 UTLTN_RTO 9.8 CR_LMT_AMT 10.8 
ADVNC_AMT 9.8 AUTH_AMT 10.8 ADJUSTED_OS_BAL_AMT 19.8 AF_SECRTZTN_BAL_AMT 19.8 PD_MODEL_RTO 12.8 LOAN_TO_VAL_RTO 15.8 INDEXED_PRPTY_VAL_AMT 21.8 
INDEXED_LOAN_TO_VAL_RTO 23.8 PD_LR_RPTG_RTO 14.8 PD_LR_PV_RPTG_RTO 17.8 PD_LR_PV_AD_RPTG_RTO 20.8 PD_LD_PV_AD_SV_RPTG_RTO 23.8 PD_FINAL_RPTG_RTO 17.8 LGD_MODEL_RTO 13.8 
LGD_LR_RPTG_RTO 15.8 LGD_LR_PV_RPTG_RTO 18.8 LGD_LR_PV_AD_RPTG_RTO 21.8 LGD_LD_PV_AD_SV_RPTG_RTO 24.8 LGD_FINAL_RPTG_RTO 18.8 LGD_LD_PV_AD_SV_DT_RPTG_RTO 27.8 EAD_MODEL_RTO 13.8 
EAD_LR_RPTG_RTO 15.8 EAD_LR_PV_RPTG_RTO 18.8 EAD_LR_PV_AD_RPTG_RTO 21.8 EAD_LD_PV_AD_SV_RPTG_RTO 24.8 EAD_LD_PV_AD_SV_DT_RPTG_RTO 27.8 EAD_FINAL_RPTG_RTO 18.8 
PD_UNADJUSTED_RPTG_RTO 22.8 PD_LR_PV_AD_SV_DT_AA_RTO 24.8 LGD_LR_PV_AD_SV_DT_AA_RTO 25.8 EAD_LR_PV_AD_SV_DT_AA_RTO 25.8 OS_PRNCPL_BAL_AMT 17.8 
PREV_12_QTR_PRPTY_VAL_AMT 25.8 LGD_UNADJUSTED_RPTG_RTO 23.8 EAD_UNADJUSTED_RPTG_RTO 23.8 LNG_RUN_LGD_ADD_ON_RTO 22.8 DLGD_RPTG_RTO_OLD 13.8 CRNT_LTV_RTO 12.10 
UNINSURED_LGD_RTO 13.8 UNINSURED_DLGD_RTO 13.8
CLP_FLAG $1. BCAR_SCHED_NM $150. BCAR_SCHED_NUM_50 $10.
 ;

INSRT_PROCESS_TMSTMP="&SYSDATE9.:&SYSTIME."dt   ;
UPDT_PROCESS_TMSTMP ="&SYSDATE9.:&SYSTIME."dt ;
/*basel_Acct_id=v_basel_Acct_id;*/
/*IF v_BASEL_ACCT_ID eq '' then do;*/
/*	put  "The BASEL_ACCT_ID is not found for the MORT_NUM: ' || MORT_NUM || ' for MTH_TM_ID: ' || MTH_TM_ID";*/
/*	end;*/
/*v_BASEL_ACCT_ID)*/

OS_BAL_AMT = OS_BAL_AMT0;
GENL_LEDGER_BALCNG_ADJ_AMT = GENL_LEDGER_BALCNG_ADJ_AMT0;
UNADJUSTED_ADD_ON_BAL_AMT = UNADJUSTED_ADD_ON_BAL_AMT0;
BEFORE_ZERO_NET_DRAWN_AMT = BEFORE_ZERO_NET_DRAWN_AMT0;
BEFORE_ZERO_NET_UNDRAWN_AMT = BEFORE_ZERO_NET_UNDRAWN_AMT0;
ORIG_PRPTY_VAL_AMT = ORIG_PRPTY_VAL_AMT0;
INTR_ACCR_AMT = INTR_ACCR_AMT0;
AMORT = AMORT0;
AF_ZERO_NET_DRAWN_AMT = AF_ZERO_NET_DRAWN_AMT0;
AF_ZERO_NET_UNDRAWN_AMT = AF_ZERO_NET_UNDRAWN_AMT0;
PRPTY_VAL_CORR_PCTG = PRPTY_VAL_CORR_PCTG0;
UTLTN_RTO = UTLTN_RTO0;
CR_LMT_AMT = CR_LMT_AMT0;
ADVNC_AMT = ADVNC_AMT0;
AUTH_AMT = AUTH_AMT0;
ADJUSTED_OS_BAL_AMT = ADJUSTED_OS_BAL_AMT0;
AF_SECRTZTN_BAL_AMT = AF_SECRTZTN_BAL_AMT0;
PD_MODEL_RTO = PD_MODEL_RTO0;
LOAN_TO_VAL_RTO = LOAN_TO_VAL_RTO0;
INDEXED_PRPTY_VAL_AMT = INDEXED_PRPTY_VAL_AMT0;
INDEXED_LOAN_TO_VAL_RTO = INDEXED_LOAN_TO_VAL_RTO0;
PD_LR_RPTG_RTO = PD_LR_RPTG_RTO0;
PD_LR_PV_RPTG_RTO = PD_LR_PV_RPTG_RTO0;
PD_LR_PV_AD_RPTG_RTO = PD_LR_PV_AD_RPTG_RTO0;
PD_LD_PV_AD_SV_RPTG_RTO = PD_LD_PV_AD_SV_RPTG_RTO0;
PD_FINAL_RPTG_RTO = PD_FINAL_RPTG_RTO0;
LGD_MODEL_RTO = LGD_MODEL_RTO0;
LGD_LR_RPTG_RTO = LGD_LR_RPTG_RTO0;
LGD_LR_PV_RPTG_RTO = LGD_LR_PV_RPTG_RTO0;
LGD_LR_PV_AD_RPTG_RTO = LGD_LR_PV_AD_RPTG_RTO0;
LGD_LD_PV_AD_SV_RPTG_RTO = LGD_LD_PV_AD_SV_RPTG_RTO0;
LGD_FINAL_RPTG_RTO = LGD_FINAL_RPTG_RTO0;
LGD_LD_PV_AD_SV_DT_RPTG_RTO = LGD_LD_PV_AD_SV_DT_RPTG_RTO0;
EAD_MODEL_RTO = EAD_MODEL_RTO0;
EAD_LR_RPTG_RTO = EAD_LR_RPTG_RTO0;
EAD_LR_PV_RPTG_RTO = EAD_LR_PV_RPTG_RTO0;
EAD_LR_PV_AD_RPTG_RTO = EAD_LR_PV_AD_RPTG_RTO0;
EAD_LD_PV_AD_SV_RPTG_RTO = EAD_LD_PV_AD_SV_RPTG_RTO0;
EAD_LD_PV_AD_SV_DT_RPTG_RTO = EAD_LD_PV_AD_SV_DT_RPTG_RTO0;
EAD_FINAL_RPTG_RTO = EAD_FINAL_RPTG_RTO0;
PD_UNADJUSTED_RPTG_RTO = PD_UNADJUSTED_RPTG_RTO0;
PD_LR_PV_AD_SV_DT_AA_RTO = PD_LR_PV_AD_SV_DT_AA_RTO0;
LGD_LR_PV_AD_SV_DT_AA_RTO = LGD_LR_PV_AD_SV_DT_AA_RTO0;
EAD_LR_PV_AD_SV_DT_AA_RTO = EAD_LR_PV_AD_SV_DT_AA_RTO0;
OS_PRNCPL_BAL_AMT = OS_PRNCPL_BAL_AMT0;
PREV_12_QTR_PRPTY_VAL_AMT = PREV_12_QTR_PRPTY_VAL_AMT0;
LGD_UNADJUSTED_RPTG_RTO = LGD_UNADJUSTED_RPTG_RTO0;
EAD_UNADJUSTED_RPTG_RTO = EAD_UNADJUSTED_RPTG_RTO0;
LNG_RUN_LGD_ADD_ON_RTO = LNG_RUN_LGD_ADD_ON_RTO0;
DLGD_RPTG_RTO_OLD = DLGD_RPTG_RTO_OLD0;
CRNT_LTV_RTO = CRNT_LTV_RTO0;


if v_BASEL_ACCT_ID eq . then do;
		Note1= 'The BASEL_ACCT_ID is not found for the MORT_NUM: ' || MORT_NUM || ' for MTH_TM_ID: ' || MTH_TM_ID;
		put Note1;
		abort cancel;
	end;
else do; 
BASEL_ACCT_ID=v_BASEL_ACCT_ID; 
end;
run;



/*Change for CCAR_ACAP EPIC=RRMSS-1026 JIRA=RRMSS-1478******START******/
/*Load the data into IIAS Instrument Fact Table*/

 PROC SQL NOPRINT;
     CONNECT USING NZRRAP AS NZCON;
     EXECUTE(DELETE FROM &net_db..TMP_INSTRMNT_FACT WHERE rtrim(ltrim(SRC_SYS_CD)) in ('MOR','TNG-MOR')) BY NZCON;
QUIT;

  proc append base=NZRRAP.TMP_INSTRMNT_FACT (BULKLOAD=YES BL_METHOD=CLILOAD)
	data=LOAD force ; run;




