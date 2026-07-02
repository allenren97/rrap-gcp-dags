/*IDENTIFY THE CORRECT CMA32 FILE and its PATH!!!*/


***************************************************************************************************************************;
***************************************************************************************************************************;
*  Job Name : J_RRAP_TERA_0010_STANDARDIZED_CMA.sas
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  TERANET_HOUSE_PRC_INDEX_CMA_12M  
*  
*  Purpose: Load TERANET_HOUSE_PRC_INDEX_CMA_12M
*
*  Frequency: Monthly
*
*  Notes:  Loads last 12 months of AVAILABLE Teranet data (ie., JULY2021 to JULY2022)
*  		   This data is used in the J_RRAP_TERA_0030_TERANET_HOUSE_PRC_INDEX_CMA.sas job to load/update current/previous months.
*
*	Change Log:
*
*   2022-11-04: Hadi Dimashkieh - Initial Development
*
***************************************************************************************************************************;
%rrap_dlgd_autoexec;

data files;
    set nzrrap.tm_dim;
    where tm_lvl='Month' and tm_id = &mth_tm_id.-40;
    call symputx('tdate',(catx('_',year(tm_lvl_end_dt),put(month(tm_lvl_end_dt),z2.))));



   length fref $8 fname $200;
    did = filename(fref,"&outpath./rrm");
    did = dopen(fref);
    do i = 1 to dnum(did);
      fname = dread(did,i);
      output;
    end;
    did = dclose(did);
    did = filename(fref);
    keep fname;
run;

data cma_files;
set files;
	where fname like "CMA32_House_Price_Index_&tdate.%";
run;
proc sort data=cma_files; by descending fname; run;

data _null_;
	set cma_files;
	if _n_=1;
	call symputx('cmafile',fname);
run;



proc sql;
	create table months12 as select distinct tm_lvl_end_dt as month_end_dt
	from nzrrap.tm_dim t
	where t.tm_lvl='Month' and t.tm_id between &mth_tm_id. and &mth_tm_id. -12*40
	order by 1;
quit;

proc import datafile="&outpath./rrm/&cmafile."
	out=datafile dbms=csv replace;
	getnames=no; guessingrows=32000;
	datarow=1;
run;

data header;
	retain month_end_dt;
	set datafile;
	if _n_ <=2;* then output;
	format month_end_dt date9.;
	month_end_dt=.; 
run;
data datarows;
	set datafile;
	if _n_ > 2;
	format month_end_dt  date9.;
	month_end_dt=intnx('Month', input(cats('01',substr(upcase(VAR1),1,3),substr(VAR1,5,4)),date9.) ,0,'e');
	if missing(month_end_dt) /*or month_end_dt LT '31JAN2021'd*/ then delete;
run;
proc sort data=datarows; by month_end_dt; run;


data newdata;
*retain month_end_dt;
	merge datarows(in=a) months12(in=b);
	by month_end_dt;
	if a and b;
run;


proc sql;
connect using nzrrap as nzcon;
execute(delete from &RRAP_DB..TERANET_HOUSE_PRC_INDEX_CMA_12M where process_mth_tm_id = &mth_tm_id.; commit;) by nzcon;
quit;

%macro load_data;

data _null_;
	set newdata;
	n=compress(put(_n_,best.));
	call symputx('runmonths'!!n,compress(month_end_dt));
	call symputx('nobs',_n_);
run;

%do i = 1 %to &nobs.;

	%let month_end_dt = &&runmonths&i.;

	data loaddata(drop=month_end_dt);
		set header newdata(where=(month_end_dt=&month_end_dt.));
	run;

	proc transpose data=loaddata out=t1;
		var _all_;
	run;

	data t2;
		set t1;
		if compress(upcase(col2))='INDEX' then col2='INDEX_1';
		if compress(upcase(col2))='SALESPAIRCOUNT' then col2='SLS_PAIR_CNT_1';
		lag_col=lag(col1);
		if missing(col1) then prov_cma=lag_col;
			else prov_cma=col1;
		keep prov_cma col2 col3; 
		if missing(col2) then delete;
	run;

	proc sort data=t2 out=t3;
		by prov_cma col2;
	run;

	proc transpose data=t3 out=t4(drop=_NAME_);
		by prov_cma; var col3;	id col2;
	run;

	data t5;
	set t4;
		index = input(index_1,8.2);
		SLS_PAIR_CNT = input(SLS_PAIR_CNT_1,8.);
		if prov_cma in ('c11','c6') then do;
			LABEL_1 = 'COMPOSITE';
			LABEL_2 = substr(prov_cma,2);
		end;
		else do;
			LABEL_1 = upcase(substr(prov_cma,1,2));
			LABEL_2 = upcase(substr(prov_cma,4));
		end;
	run;

	data st_t5;
	attrib  STANDARDIZED_CMA length=$50.;
		if _n_=1 then do;
		     declare hash h(dataset: "NZRRAP.STANDARDIZED_CMA(rename=(SOURCE_CMA=LABEL_2))"); 
		     h.defineKey("LABEL_2");
		     h.defineData("STANDARDIZED_CMA");
		     h.defineDone();
		     call missing (STANDARDIZED_CMA); 
		 end;  
		set t5;

		if /*LABEL_2 not in ('11','6') and*/ h.find(key:LABEL_2) ne 0 then do;
			put 'ERROR: Missing lookup value';
			put LABEL_2= 'does not exist in the STANDARDIZED_CMA table.';
			put 'Please add this value then rerun this job.';
			abort;
		end;

	run;

	proc sql;
		create table t6 as
		select 
			&mth_tm_id. as process_mth_tm_id
			,t.tm_id as mth_tm_id
			,t5.LABEL_1
			,t5.STANDARDIZED_CMA as LABEL_2
			,t5.LABEL_2 as LABEL_2_ORIG
			,t5.INDEX
			,t5.SLS_PAIR_CNT

		,"&SYSDATE9.:&SYSTIME."dt format=datetime25. as INSRT_PROCESS_TMSTMP
		,"&SYSDATE9.:&SYSTIME."dt format=datetime25. as UPDT_PROCESS_TMSTMP

		from st_t5 t5 , nzrrap.tm_dim t 
		where t.tm_lvl='Month' and t.tm_lvl_end_dt = &month_end_dt.
		order by 2,3;
	quit;



	proc append base=nzrrap.TERANET_HOUSE_PRC_INDEX_CMA_12M(BULKLOAD=YES BL_METHOD=CLILOAD) data=t6 force; run;


%end;

%mend load_data;

%load_data;


