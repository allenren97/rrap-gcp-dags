options errorabend;

***************************************************************************************************************************;
***************************************************************************************************************************;
*  Job Name : J_RRAP_2710_INSTRMNT_FACT_WRITE_BACK.sas
*  Target Database: IIAS EDRTLRP1D FRG_USER_DATA
*  Target Table:  Portfolio Specific Insrument Fact Tables  
*  
*  Purpose: Write back the final IIAS Instrument Fact data into their portfolio specific counterparts
*			so that all tables are in sync.
*  Frequency: Monthly
*
*  Notes:  
*  		   
*
*	Change Log:
*
*   2022-12-08: Hadi Dimashkieh - Initial Development
*
***************************************************************************************************************************;

%rrap_mor_bns_autoexec;

 %get_model_period_dates(product=mor);
%put Start and End Dates for Mortgage Models:;
%put start_period_dt: &start_period_dt ;
%put end_period_dt: &end_period_dt;


proc sql noprint;
	select tm_id into :mth_tm_id
	from nzrrap.tm_dim
	where tm_lvl_end_dt = "&end_period_dt."d and tm_lvl='Month';
quit;


proc sql noprint;
	connect using nzrrap as nzcon;
	execute(delete from &rrap_db..BASEL_PSNL_LN_ANL_BL_INST_FACT where mth_tm_id = &mth_tm_id.; commit;) by nzcon;
	execute(insert into &rrap_db..BASEL_PSNL_LN_ANL_BL_INST_FACT select * from &rrap_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT where src_sys_cd = 'SPL' and mth_tm_id = &mth_tm_id.) by nzcon;
quit;

proc sql noprint;
	connect using nzrrap as nzcon;
	execute(delete from &rrap_db..BASEL_ANALYTCL_BL_INSTRMNT_KS where mth_tm_id = &mth_tm_id.; commit;) by nzcon;
	execute(insert into &rrap_db..BASEL_ANALYTCL_BL_INSTRMNT_KS select * from &rrap_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT where src_sys_cd = 'KS' and mth_tm_id = &mth_tm_id.) by nzcon;
quit;

proc sql noprint;
	connect using nzrrap as nzcon;
	execute(delete from &FRG_DB..BASEL_ANLYT_BL_INST_FCT_BNS_DLGD where mth_tm_id = &mth_tm_id.; commit;) by nzcon;
	execute(insert into &FRG_DB..BASEL_ANLYT_BL_INST_FCT_BNS_DLGD select * from &rrap_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT where src_sys_cd = 'MOR' and mth_tm_id = &mth_tm_id.) by nzcon;
quit;

proc sql noprint;
	connect using nzrrap as nzcon;
	execute(delete from &FRG_DB..BASEL_ANLYT_BL_INST_FCT_TNG_DLGD where mth_tm_id = &mth_tm_id.; commit;) by nzcon;
	execute(insert into &FRG_DB..BASEL_ANLYT_BL_INST_FCT_TNG_DLGD select * from &rrap_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT where src_sys_cd = 'TNG-MOR' and mth_tm_id = &mth_tm_id.) by nzcon;
quit;


