

***************************************************************************************************************************;
***************************************************************************************************************************;
*  Job Name : J_RRAP_TERA_0030_TERANET_HOUSE_PRC_INDEX_CMA.sas
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  TERANET_HOUSE_PRC_INDEX_CMA  
*  
*  Purpose: Load TERANET_HOUSE_PRC_INDEX_CMA
*
*  Frequency: Monthly
*
*  Notes:  Loads current/previous Teranet data based on available data from TERANET_HOUSE_PRC_INDEX_CMA_12M
*  		   
*
*	Change Log:
*
*   2022-11-04: Hadi Dimashkieh - Initial Development
*
***************************************************************************************************************************;
%rrap_dlgd_autoexec;



data twelve_months;
	do mth_tm_id = &mth_tm_id. -12*40 to &mth_tm_id. by 40;
		output;
	end;
run;

data TERANET_HOUSE_PRC_INDEX_CMA_12M;
set nzrrap.TERANET_HOUSE_PRC_INDEX_CMA_12M;
where process_mth_tm_id LE &mth_tm_id.;
run;


proc sql;
	create table twelve_months_data as
	select distinct &mth_tm_id. as process_mth_tm_id, a.mth_tm_id, b.label_1, b.label_2, b.label_2_orig, c.index as index1, c.sls_pair_cnt as sls_pair_cnt1
	from TERANET_HOUSE_PRC_INDEX_CMA_12M b inner join twelve_months a
		on b.process_mth_tm_id = &mth_tm_id 
	left join TERANET_HOUSE_PRC_INDEX_CMA_12M c
		on a.mth_tm_id = c.mth_tm_id and b.label_1 = c.label_1 and b.label_2 = c.label_2 and b.label_2_orig = c.label_2_orig
	
	order by b.label_1, b.label_2, a.mth_tm_id;
quit;

data cur_prev_mth;
	set twelve_months_data;
	by label_1 label_2 mth_tm_id;
	retain index sls_pair_cnt;
	if first.label_2 then do;
		index=index1; sls_pair_cnt=sls_pair_cnt1;
	end;
	if not first.label_2 then do;
		if not missing(index1) then index=index1;
		if not missing(sls_pair_cnt1) then sls_pair_cnt=sls_pair_cnt1;
	end;
	drop index1 sls_pair_cnt1 process_mth_tm_id;
	if mth_tm_id in (&mth_tm_id.,%eval(&mth_tm_id. -40));
	format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.;
	INSRT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME."dt;
	UPDT_PROCESS_TMSTMP  = "&SYSDATE9.:&SYSTIME."dt;
run;

proc sql;
connect using nzrrap as nzcon;
execute(delete from &RRAP_DB..TERANET_HOUSE_PRC_INDEX_CMA where mth_tm_id in (&mth_tm_id.,%eval(&mth_tm_id. -40)); commit;) by nzcon;
quit;

proc append base=nzrrap.TERANET_HOUSE_PRC_INDEX_CMA(BULKLOAD=YES BL_METHOD=CLILOAD) data=cur_prev_mth force; run;

