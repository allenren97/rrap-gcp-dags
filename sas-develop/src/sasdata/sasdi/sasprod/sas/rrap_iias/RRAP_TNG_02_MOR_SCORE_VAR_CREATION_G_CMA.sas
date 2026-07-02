/*add insrt_process_tmstmp and updt_process_stmp*/
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS FRG_USER_DATA
*  Target Table:  tng_pd_var_creation_final_cma
*  
*  Purpose: Lookup CMA for each property address and calculate HPI index and LTV
*
*  Frequency: Monthly
*
*  Notes: Rewrite of the legacy 02_tng_mor_score_var_creation job. 
*  		  Changed to process monthly and leverage TERANET_ADDR_LKP table and dynamic lookup logic.
*
*	Change Log:
*
*    02JAN2014 - WP - Created
*    10JAN2014 - JL - Slight modifications (see 'CODE CHANGE')
*    28JAN2015 - WP - Added insert into NZ table at end of code and
*                     added new libref for input table.
*
*	2022-11-04: Hadi Dimashkieh - Rewrite - Initial Development of 32 CMA version
*
***************************************************************************************************************************;

/*
SOURCES:
TNGSTP1D.TNG_CUST_TU  
TNGSTP1D.TNG_ACCT_MO  
FRG_USER_DATA.TNG_STATUS 
EDRTLRP1D.TERANET_ADDR_LKP_CMA
*/

%RRAP_MOR_TNG_AUTOEXEC;

%get_model_period_dates(product=mor);   

%put Start and End Dates for Mortgage Models:;

%put start_period_dt: &start_period_dt ;
%put end_period_dt: &end_period_dt;

*%let end_period_dt = 31JUL2022;

/*%let FRG_DB=FRG_USER_DATA;*/
/*%LET RRAP_DB=EDRTLRP1D;*/
/*NZRRAP   EDRTLRP1D*/
/*NZUSER    FRG_USER_DATA*/



data _null_;
	set nz_frg.tm_dim;
	where tm_lvl = 'Month' and tm_lvl_end_dt = "&end_period_dt."d;
	call symputx('mth_tm_id',put(tm_id,5.));
	call symputx('mth_end_dt',put(tm_lvl_end_dt,date9.));
	call symputx('mth_end_dt_nz',put(tm_lvl_end_dt,yymmdd10.));
	call symputx('mth_end_dt_1_nz',put(intnx('Month',tm_lvl_end_dt,-12,'e'),yymmdd10.));
	call symputx('yrmth',put(tm_lvl_end_dt,yymmn6.));
	call symputx('yrmth_prev',put(intnx('Month',tm_lvl_end_dt,-1,'e'),yymmn6.));
run;

%put &mth_tm_id. &mth_end_dt. &mth_end_dt_nz. &mth_end_dt_1_nz. &yrmth.;

/* --------------------------------------------------------------------------------------
 * Creating variables TRADES_STSFCT_CNTslp6m and TRADES_ACTIVE_UTIL_PCTmax12m.  These two
 * variables are pulled from the CPD3 table, which is named differently in Netezza.
 * CPD3=netcon.tng_cust_tu
 *---------------------------------------------------------------------------------------*/


proc sql;
connect using NZUSER as nzcon;
create table temp as select * from connection to nzcon(
  select a.account_id 
          ,a.month_end_dt 
          ,b.trades_stsfct_cnt 
          ,b.trades_active_util_pct
  from &FRG_DB..TNG_STATUS as a 
        left join 
        &TNG_DB..tng_cust_tu as b
  		on a.account_id = b.account_id and a.month_end_dt = b.month_end_dt
   where a.month_end_dt <= %nrbquote('&mth_end_dt_nz.') and a.month_end_dt >= %nrbquote('&mth_end_dt_1_nz.')
  order by a.account_id, a.month_end_dt);
quit;



/* Leonid's macro for variables*/

%macro create_var_stat(table_name,id,time_key);

   options validvarname=any;

   proc sort data=&table_name.;
      by &id. &time_key.;
   run;

   proc contents data=&table_name. out=col_list(where=(type=1 and format not in ('DATE' 'DATETIME')) keep=NAME TYPE FORMAT) noprint;
   run;



   data col_list;
      set col_list(where=(lowcase(name) not in ("&time_key." "&id.")));
      name2=compress(name,'_');
      if length(name2)<=30 then 
                            do;
                               output;
                            end;
   run;


   %let dsid=%sysfunc(open(col_list));
   %let nvar=%sysfunc(attrn(&dsid,nlobs));
   %let rc=%sysfunc(close(&dsid));


   %if &nvar.=0 %then  
                 %do;

   data &table_name._stat;
      set &table_name.;
   run;

   %goto out;
   %end;

   proc sql noprint;
      select name 
         into :keep_list separated by ' ' 
         from col_list
         where lowcase(NAME) ne "&time_key.";
   quit;


   data _null_;
      set col_list;
      call execute("

         data _null_;

            length final lg_"||compress(NAME)||" ms_"||compress(NAME)||" ss_"||compress(NAME)||" mx_"||compress(NAME)||" mn_"||compress(NAME)||" chg_"||compress(NAME)||" chg1_"||compress(NAME)||" slp_"||compress(NAME)||" $32500.;
            retain final lg_"||compress(NAME)||" ms_"||compress(NAME)||" ss_"||compress(NAME)||" mx_"||compress(NAME)||" mn_"||compress(NAME)||" chg_"||compress(NAME)||" chg1_"||compress(NAME)||" slp_"||compress(NAME)||";

            do i=1 to 24;
               lg_"||compress(NAME)||"=compress(compress(lg_"||compress(NAME)||")||'lg_"||compress(NAME)||"'||i||'=lag'||i||'("||NAME||");');
               call symput('lg_"||compress(NAME)||"',lg_"||compress(NAME)||");
            end;



            do i=3,6,12,24;
   
               if i<=6 then 
                        do;
      
                           do k=1 to 24;

                              if k=1 then 
                                      do;
                                         chg_"||compress(NAME)||"=compbl(chg_"||compress(NAME)||"||'if N='||K||' then "||compress(NAME)||"chg'||compress(i)||'m=.;');
                                         slp_"||compress(NAME)||"=compbl(slp_"||compress(NAME)||"||'if N='||K||' then "||compress(NAME)||"slp'||compress(i)||'m=.;');
                                      end;

                              if k>1 and k<i then 
                                              do;
                                                 chg_"||compress(NAME)||"=compbl(chg_"||compress(NAME)||"||'else if N='||K||' then "||compress(NAME)||"chg'||compress(i)||'m=("||compress(NAME)||"-lg_"||compress(NAME)||"'||compress(k-1)||')/lg_"
                                                   ||compress(NAME)||"'||compress(k-1)||';');
                                                 slp_"||compress(NAME)||"=compbl(slp_"||compress(NAME)||"||'else if N='||K||' then "||compress(NAME)||"slp'||compress(i)||'m=("||compress(NAME)||"-lg_"||compress(NAME)||"'||compress(k-1)||')/'
                                                   ||compress(k-1)||';');
                                              end;

      
                              if k=i then 
                                      do;
                                         chg_"||compress(NAME)||"=compbl(chg_"||compress(NAME)||"||'else "||compress(NAME)||"chg'||compress(i)||'m=("||compress(NAME)||"-lg_"||compress(NAME)||"'||compress(k-1)||')/lg_"||compress(NAME)||"'
                                          ||compress(k-1)||';');
                                         slp_"||compress(NAME)||"=compbl(slp_"||compress(NAME)||"||'else "||compress(NAME)||"slp'||compress(i)||'m=("||compress(NAME)||"-lg_"||compress(NAME)||"'||compress(k-1)||')/'||compress(k-1)||';');
                                      end;

                              call symput('chg_"||compress(NAME)||"',chg_"||compress(NAME)||");
                              call symput('slp_"||compress(NAME)||"',slp_"||compress(NAME)||");
                           end;
                        end;

               do j=1 to i;

                  if j=1 then 
                          do;
                             ms_"||compress(NAME)||"=compbl(ms_"||compress(NAME)||"||'if N='||j||' then "||compress(NAME)||"avg'||compress(i)||'m="||compress(NAME)||";');
                             ss_"||compress(NAME)||"=compbl(ss_"||compress(NAME)||"||'if N='||j||' then "||compress(NAME)||"sum'||compress(i)||'m="||compress(NAME)||";');
                             mx_"||compress(NAME)||"=compbl(mx_"||compress(NAME)||"||'if N='||j||' then "||compress(NAME)||"max'||compress(i)||'m="||compress(NAME)||";');
                             mn_"||compress(NAME)||"=compbl(mn_"||compress(NAME)||"||'if N='||j||' then "||compress(NAME)||"min'||compress(i)||'m="||compress(NAME)||";');
                             chg1_"||compress(NAME)||"=compbl(chg1_"||compress(NAME)||"||'if N='||j||' then "||compress(NAME)||"chg1m=.;');
                          end;
                  if j>1 and j<i then 
                                  do; 
                                     ms_"||compress(NAME)||"=compbl(ms_"||compress(NAME)||"||'else if N='||j||' then "||compress(NAME)||"avg'||compress(i)||'m=(sum(of lg_"||compress(NAME)||"1-lg_"||compress(NAME)||"'||compress(j-1)||')+"||compress(NAME)
                                       ||")/'||j||';');
                                     ss_"||compress(NAME)||"=compbl(ss_"||compress(NAME)||"||'else if N='||j||' then "||compress(NAME)||"sum'||compress(i)||'m=(sum(of lg_"||compress(NAME)||"1-lg_"||compress(NAME)||"'||
                                       compress(j-1)||')+"||compress(NAME)||");');
                                     mx_"||compress(NAME)||"=compbl(mx_"||compress(NAME)||"||'else if N='||j||' then "||compress(NAME)||"max'||compress(i)||'m=max(max(of lg_"||compress(NAME)||"1-lg_"||compress(NAME)||"'||
                                       compress(j-1)||',.),"||compress(NAME)||");');            
                                     mn_"||compress(NAME)||"=compbl(mn_"||compress(NAME)||"||'else if N='||j||' then "||compress(NAME)||"min'||compress(i)||'m=min(min(of lg_"||compress(NAME)||"1-lg_"||compress(NAME)||"'||
                                       compress(j-1)||',.),"||compress(NAME)||");');            
                                     chg1_"||compress(NAME)||"=compbl(chg1_"||compress(NAME)||"||'else if N='||j||' then "||compress(NAME)||"chg1m=("||compress(NAME)||"-lg_"||compress(NAME)||"'||compress(j-1)||')/lg_"||compress(NAME)||"'||
                                       compress(j-1)||';');
                                  end;
                  if j=i then 
                          do;
                             ms_"||compress(NAME)||"=compbl(ms_"||compress(NAME)||"||'else "||compress(NAME)||"avg'||compress(i)||'m=(sum(of lg_"||compress(NAME)||"1-lg_"||compress(NAME)||"'||compress(j-1)||')+"||compress(NAME)||")/'||j||';');
                             ss_"||compress(NAME)||"=compbl(ss_"||compress(NAME)||"||'else "||compress(NAME)||"sum'||compress(i)||'m=(sum(of lg_"||compress(NAME)||"1-lg_"||compress(NAME)||"'||compress(j-1)||')+"||compress(NAME)||");');
                             mx_"||compress(NAME)||"=compbl(mx_"||compress(NAME)||"||'else "||compress(NAME)||"max'||compress(i)||'m=max(max(of lg_"||compress(NAME)||"1-lg_"||compress(NAME)||"'||compress(j-1)||'),"||compress(NAME)||");');
                             mn_"||compress(NAME)||"=compbl(mn_"||compress(NAME)||"||'else "||compress(NAME)||"min'||compress(i)||'m=min(min(of lg_"||compress(NAME)||"1-lg_"||compress(NAME)||"'||compress(j-1)||'),"||compress(NAME)||");');
                             chg1_"||compress(NAME)||"=compbl(chg1_"||compress(NAME)||"||'else "||compress(NAME)||"chg1m=("||compress(NAME)||"-lg_"||compress(NAME)||"'||compress(j-1)||')/lg_"||compress(NAME)||"'||compress(j-1)||';');
                          end;

                  call symput('ms_"||compress(NAME)||"',ms_"||compress(NAME)||");
                  call symput('ss_"||compress(NAME)||"',ss_"||compress(NAME)||");
                  call symput('mx_"||compress(NAME)||"',mx_"||compress(NAME)||");
                  call symput('mn_"||compress(NAME)||"',mn_"||compress(NAME)||");
                  call symput('chg1_"||compress(NAME)||"',chg1_"||compress(NAME)||");
               end;
            end;

         run;


         ");

   run;


   data _null_;
      set col_list;
      length string $32000.;
      retain string;
      string=compbl(string||'&lg_'||name||';'||' &ms_'||name||';'||' &ss_'||name||';'||' &mx_'||name||';'||' &mn_'||name||';'||' &chg_'||name||';'||'&chg1_'||name||';'||'&slp_'||name||';');
      call symput('final1',string);
   run;

   %if &nvar.>100 %then 
                   %do;
                      data _null_;
                         set col_list(firstobs=101 obs=200);
                         length string $32000.;
                         retain string;
                         string=compbl(string||'&lg_'||name2||';'||' &ms_'||name2||';'||' &ss_'||name2||';'||' &mx_'||name2||';'||' &mn_'||name2||';');
                         call symput('final2',string);
                      run;
                   %end;


   data &table_name._stat;
      set &table_name.;
      by &id.;
      N+1;
      if first.&id. then N=1;
      retain N;

      &final1.;

      %if &nvar.>100 %then 
                      %do;
                         &final2.;
                      %end;

      drop lg_: N;
   run;


   proc sql noprint;
      drop table &table_name.;
   quit;


   proc datasets lib=work;
      delete col_list;
   run;
   %out:;

%mend;

%create_var_stat(temp,account_id,month_end_dt);


data temp_var_statb;
   set temp_stat ( keep=account_id month_end_dt TRADES_ACTIVE_UTIL_PCTmax12m TRADES_STSFCT_CNTslp6m);
   where month_end_dt = "&mth_end_dt."d;  
run;


proc sql;
connect using NZUSER as nzcon;
create table tngdata_01 as select * from connection to nzcon(
SELECT a.*
	,b.DEROGATORY_PUBLIC_REC_CNT ,b.COLLECTIONS_CNT, b.BANKRUPTCIES_CNT
    ,CASE WHEN b.DEROGATORY_PUBLIC_REC_CNT > 0 OR b.COLLECTIONS_CNT > 0 OR b.BANKRUPTCIES_CNT > 0 THEN 1
       	ELSE 0 
     END AS credit_trble_bnkrptcy_coll_der
    ,c.DAYS_ARREARS_CNT, c.NSF_YTD_CNT, c.EVER_30_CNT
    ,c.OPEN_DT, c.REMAIN_AMORT, c.AMORT_PERIOD 
    ,CASE WHEN c.BULK_NSURER_DESC = 'Conventional' then 100
    	 WHEN c.BULK_NSURER_DESC = 'BulkInsured'  then 200
     	ELSE 300
	 END AS INS_ID_GROUP
FROM &FRG_DB..TNG_STATUS a 
	LEFT JOIN &TNG_DB..TNG_CUST_TU b 
		ON a.ACCOUNT_ID = b.ACCOUNT_ID AND a.MONTH_END_DT = b.MONTH_END_DT 
	LEFT JOIN &TNG_DB..TNG_ACCT_MO c 
		ON a.ACCOUNT_ID = c.ACCOUNT_ID AND a.MONTH_END_DT = c.MONTH_END_DT
WHERE a.MONTH_END_DT = %nrbquote('&mth_end_dt_nz.')
);
quit;

proc sql;
create table tng_scrcard_data as
select a.*
	 ,case when a.remain_amort=0 and INTCK('MONTH',a.open_dt,a.month_end_dt) = 0 then a.amort_period
		   else a.remain_amort
      end as remain_amort2
     ,b.TRADES_STSFCT_CNTslp6m 
     ,b.TRADES_ACTIVE_UTIL_PCTmax12m
from tngdata_01 as a 
   left join 
   temp_var_statb as b
   on a.account_id = b.account_id and a.month_end_dt = b.month_end_dt;
quit;

/*****************************************************************************************************************/
/*****************************************************************************************************************/
/*****************************************************************************************************************/
/*****************************************************************************************************************/
/*****************************************************************************************************************/

data SNAPSHOT ;
   set netcon.tng_acct_mo(keep=account_id MONTH_END_DT FSA prop_province_code
			END_PRINCIPAL_BALANCE LST_PROP_APPRAISAL_VAL LST_PROP_APPRAISAL_DT ORIG_PROP_APPRAISAL_DT ORIG_PROP_APPRAISAL_VAL PROP_PURCHASE_AMT PROP_PURCHASE_DT OPEN_DT);
   where month_end_dt = "&mth_end_dt."d;
/*   PRPTY_DESC = STRIP(COMPBL(compress((translate(lowcase(FSA),"aaceeeeiiouu  ","àâçéèêëîïôùû,-")),,'kadst')));*/
   PRPTY_DESC = STRIP(COMPBL(compress((translate(lowcase(FSA),"aaaceeeeiiouuuy    ","àâäçèéêëîïôùûüÿ,-'/")),,'kadst')));

   if prop_province_code='' or prop_province_code='?' then do;
      if substr(FSA ,1,1)='A' then PROV='NL';
         else if  substr(FSA ,1,1)='B' then PROV='NS';
         else if  substr(FSA ,1,1)='C' then PROV='PE';
         else if  substr(FSA ,1,1)='E' then PROV='NB';
         else if  substr(FSA ,1,1)='G' or  substr(FSA ,1,1)='H' or substr(FSA ,1,1)='J' then PROV='QC';
         else if  substr(FSA ,1,1)='R' then PROV='MB';
         else if  substr(FSA ,1,1)='S' then PROV='SK';
         else if  substr(FSA ,1,1)='T' then PROV='AB';
         else if  substr(FSA ,1,1)='V' then PROV='BC';
         else if  substr(FSA ,1,1)='X' then PROV='NT';
         else if  substr(FSA ,1,1)='Y' then PROV='YT';
         else if account_id='MBS~370528'  then PROV='MB';
         else if account_id='MBS~370579'  then PROV='BC';
         else PROV='ON';
      end;
      else do;
          PROV=prop_province_code;
    end;
	KEEP account_id MONTH_END_DT PRPTY_DESC PROV FSA
			END_PRINCIPAL_BALANCE LST_PROP_APPRAISAL_VAL LST_PROP_APPRAISAL_DT ORIG_PROP_APPRAISAL_DT ORIG_PROP_APPRAISAL_VAL PROP_PURCHASE_AMT PROP_PURCHASE_DT OPEN_DT;
run;
proc sort data=snapshot; by PROV account_id; run; 


/********************************************         
PRPTY_LOCTN_NM: City name or Postal Code
LOCTN_LABEL_1:  Province 
LOCTN_LABEL_2:  CMA
*********************************************/        
data TERANET_ADDR_LKP;
	set nzrrap.TERANET_ADDR_LKP_CMA(where=( eff_to_yr_mth GE put("&mth_end_dt."d,yymmn6.) and eff_from_yr_mth LE put("&mth_end_dt."d,yymmn6.)));
	keep loctn_label_1 loctn_label_2 prpty_loctn_nm;
run;
proc sort data=TERANET_ADDR_LKP nodupkey; by PRPTY_LOCTN_NM LOCTN_LABEL_1; run;
         
data substrings (keep=PRPTY_LOCTN_NM PRPTY_LOCTN_NM2 PROV CMA SORT ) ;
	SET TERANET_ADDR_LKP;
	LENGTH PRPTY_LOCTN_NM2 $112.;
	PRPTY_LOCTN_NM = input(PRPTY_LOCTN_NM,$112.); 
/*	PRPTY_LOCTN_NM2 = STRIP(COMPBL(compress((translate(lowcase(PRPTY_LOCTN_NM),"aaceeeeiiouu  ","àâçéèêëîïôùû,-")),,'kadst')));*/
	PRPTY_LOCTN_NM2 = STRIP(COMPBL(compress((translate(lowcase(PRPTY_LOCTN_NM),"aaaceeeeiiouuuy    ","àâäçèéêëîïôùûüÿ,-'/")),,'kadst'))); 
	PROV = LOCTN_LABEL_1;
	CMA = LOCTN_LABEL_2 ;
	if length(PRPTY_LOCTN_NM)=3 then SORT = 1;
		else SORT = 2;
RUN;

proc sort data=substrings nodupkey; by PROV SORT PRPTY_LOCTN_NM; run;
   
/* proc sql;
	create table provinces as
	select distinct prov, 'Snapshot' as TABLE from snapshot
	union all
	select distinct prov, 'Lookup' as TABLE from substrings
	order by 1;
 quit;*/

proc sql; drop table snapshot_cma; quit; 
%macro provinces();
	%macro cc; %mend cc;
 proc sql;
 create table provinces as
 select distinct prov from snapshot order by 1;
 quit;

 data _null_;
     set provinces;
     call symputx('prov'||trim(left(_n_)),prov);
     call symputx('nobs',_n_);
 run;

%do i = 1 %to &nobs;

data final_&&prov&i. ;
 attrib PROV length=$2 PRPTY_LOCTN_NM PRPTY_LOCTN_NM2 length=$50 CMA length=$40;
 if _n_=1 then do;
     declare hash h(dataset: "SUBSTRINGS(where=(prov='&&prov&i.'))", multidata: 'y'); 
     declare hiter iter('h');
     h.defineKey("PROV");
     h.defineData("CMA","PRPTY_LOCTN_NM","PRPTY_LOCTN_NM2");
     h.defineDone();
     call missing (PRPTY_LOCTN_NM,PRPTY_LOCTN_NM2, CMA); 
 end;  
 
 set  SNAPSHOT(where=(PROV="&&prov&i."));

     if 1 = 1 then do;
         rc3 = iter.first();
         do while (rc3 = 0);
         	F3=-1;
         		F3=FINDW(PRPTY_DESC,PRPTY_LOCTN_NM2,' ',"ir");
         		if F3 > 0 then do;
         			CMA_NEW=CMA; PROPERTY=PRPTY_LOCTN_NM;
         			leave;
         		end;
         		rc3 = iter.next();
         end;
     end;

     if F3 = 0 then do;
         rc33 = iter.first();
         do while (rc33 = 0);
         	F33=-1;		
         		F33=FIND(PRPTY_DESC,PRPTY_LOCTN_NM2,"it");
         		if F33 > 0 then do;
         			CMA_NEW=CMA; PROPERTY=PRPTY_LOCTN_NM;
         			leave;
         		end;
         	rc33 = iter.next();
         end;
     end;

     F=MAX(f3,f33);

     if F = 0 then do;
         PRPTY_LOCTN_NM=""; CMA=""; CMA_NEW=""; PRPTY_LOCTN_NM="";
     end;

 drop PRPTY_LOCTN_NM PRPTY_LOCTN_NM2 CMA rc3 rc33 f f3 f33 PRPTY_DESC PROPERTY;
 rename CMA_NEW = PROP_CITY;
run;

proc append base=snapshot_cma data=final_&&prov&i.; run;

%end;
%mend provinces;

%provinces;
         

data SNAPSHOT_CMA_01;
	set snapshot_cma;
	format prop_val_date date9.;

	if PROP_PURCHASE_DT ne . and PROP_PURCHASE_AMT ne . then do;
		prop_val=PROP_PURCHASE_AMT ;
		prop_val_date=intnx('month',PROP_PURCHASE_DT,0,'end');
	end;

	else if ORIG_PROP_APPRAISAL_DT ne . and ORIG_PROP_APPRAISAL_VAL ne . then do;
		prop_val=ORIG_PROP_APPRAISAL_VAL ;
		prop_val_date=intnx('month',ORIG_PROP_APPRAISAL_DT,0,'end');
	end;
	else if lst_PROP_APPRAISAL_DT ne . and lst_PROP_APPRAISAL_VAL ne . then do;
		prop_val=lst_PROP_APPRAISAL_VAL ;
		prop_val_date=intnx('month',lst_PROP_APPRAISAL_DT,0,'end');
	end;

	else if ORIG_PROP_APPRAISAL_VAL ne . and ORIG_PROP_APPRAISAL_DT= . then do;
		prop_val=ORIG_PROP_APPRAISAL_VAL ;
		prop_val_date=intnx('month',open_DT,0,'end');
	end;

	else if PROP_PURCHASE_AMT ne . and PROP_PURCHASE_DT= . then do;
		prop_val=PROP_PURCHASE_AMT ;
		prop_val_date=intnx('month',open_DT,0,'end');
	end;

	else if lst_PROP_APPRAISAL_VAL ne . and lst_PROP_APPRAISAL_DT= . then do;
		prop_val=lst_PROP_APPRAISAL_VAL ;
		prop_val_date=intnx('month',open_DT,0,'end');
	end;
run;


/*NOTE: housing price data must be pulled from teranet and then joined to the data. */

/*proc sql;
	create table teranet_data as
		select a.mth_tm_id, t.tm_lvl_end_dt as month_end_dt, a.label_1, a.label_2, a.index
		from NZRRAP.TERANET_HOUSE_PRC_INDEX_CMA a 
			left join NZRRAP.tm_dim t
				on a.mth_tm_id = t.tm_id and t.tm_lvl='Month'
		order by label_1, label_2, mth_tm_id desc;
quit;*/

proc sql;
	create table teranet_data as
		select a.mth_tm_id, t.tm_lvl_end_dt as month_end_dt, a.label_1, a.label_2, a.INDEX
			,case when p.HOUSE_INDEX_RTO = 0 then . else p.HOUSE_INDEX_RTO end as PROVNCL_INDEX
		from NZRRAP.TERANET_HOUSE_PRC_INDEX_CMA a 
			LEFT JOIN NZRRAP.PROVNCL_HOUSE_INDEX_SUM_CMA p 
			ON a.MTH_TM_ID = p.MTH_TM_ID AND a.LABEL_1 = (CASE WHEN p.PROV_CD='CO' THEN 'COMPOSITE' ELSE p.PROV_CD end )
			LEFT JOIN NZRRAP.tm_dim t
				on a.mth_tm_id = t.tm_id and t.tm_lvl='Month'
		order by label_1, label_2, mth_tm_id desc;
quit;

data teranet;
	set teranet_data;
	retain newindex newPROVNCL_INDEX;
	by label_1 label_2 descending mth_tm_id;
		if first.label_2 then do;
			newindex=index;
			newPROVNCL_INDEX=PROVNCL_INDEX;
		end;
		else do;
	 		if not missing(index) then newindex=index;
	 		if not missing(PROVNCL_INDEX) then newPROVNCL_INDEX=PROVNCL_INDEX;
		end;
run;
proc sort nodupkey; by mth_tm_id label_1 label_2; run;
/*proc sort; by  label_1 label_2 mth_tm_id; run;*/




proc sql;
	create table SNAPSHOT_CMA_02 as
	select a.*
		,coalesce(b.newindex,/*c.newPROVNCL_INDEX,*/d.COMP11) as monthly_HPI
		,coalesce(b2.newindex,/*c2.newPROVNCL_INDEX,*/d2.COMP11) as Appraisal_HPI

		,prop_val*(calculated monthly_HPI/calculated Appraisal_HPI) as prop_val_new
   		,end_principal_balance/(prop_val*(calculated monthly_HPI/calculated Appraisal_HPI)) as LTV_teranet

/*		,b.newindex as Rmonthly_HPI   ,c.newPROVNCL_INDEX as Pmonthly_HPI , d.newPROVNCL_INDEX as Cmonthly_HPI*/
/*		,b2.newindex as RAppraisal_HPI ,c2.newPROVNCL_INDEX as PAppraisal_HPI , d2.newPROVNCL_INDEX as CAppraisal_HPI*/

	from SNAPSHOT_CMA_01 as a 
		left join  TERANET as b
			on a.month_end_dt=b.month_end_dt and a.PROP_CITY=b.LABEL_2 and a.PROV = b.LABEL_1
		left join  TERANET as b2
			on a.prop_val_date=b2.month_end_dt and a.PROP_CITY=b2.LABEL_2 and a.PROV = b2.LABEL_1

		left join  (select distinct month_end_dt, label_1, newPROVNCL_INDEX from TERANET) as c
			on a.month_end_dt=c.month_end_dt and a.PROV=c.LABEL_1
		left join  (select distinct month_end_dt, label_1, newPROVNCL_INDEX from TERANET) as c2
			on a.prop_val_date=c2.month_end_dt and a.PROV=c2.LABEL_1

		left join  (select distinct month_end_dt, label_1, newINDEX as COMP11 from TERANET where LABEL_1='COMPOSITE' and LABEL_2='11') as d
			on a.month_end_dt=d.month_end_dt 
		left join  (select distinct month_end_dt, label_1, newINDEX as COMP11 from TERANET where LABEL_1='COMPOSITE' and LABEL_2='11') as d2
			on a.prop_val_date=d2.month_end_dt 
	;
quit;


proc sql noprint;
   create table tng_pd_var_creation_final as
      select a.*, b.prop_val_new, b.LTV_teranet, b.PROP_CITY, b.prov as PROP_PROVINCE_CODE
         from tng_scrcard_data as a 
              left join 
              SNAPSHOT_CMA_02 as b
         on a.account_id=b.account_id and a.month_end_dt=b.month_end_dt
		where a.month_end_dt = "&mth_end_dt."d;
quit;


/*--------------------------------------------------------
 * Code to truncate before inserting into NZ table.
 *--------------------------------------------------------*/


proc sql noprint;
connect using NZUSER as nzcon;
   execute (delete from &FRG_DB..tng_pd_var_creation_final_cma where month_end_dt = %nrbquote('&mth_end_dt_nz.')) by nzcon;
quit;

proc append base=NZUSER.tng_pd_var_creation_final_cma(BULKLOAD=YES BL_METHOD=CLILOAD) data=tng_pd_var_creation_final force; run;
