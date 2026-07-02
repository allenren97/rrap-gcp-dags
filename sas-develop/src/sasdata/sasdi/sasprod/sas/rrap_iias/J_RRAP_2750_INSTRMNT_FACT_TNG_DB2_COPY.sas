%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);
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


******************** MOR DB2 ;

PROC SQL NOPRINT;
     CONNECT USING DB2RRAP AS NZCON;
     EXECUTE(DELETE FROM &DBSCHEMA..BASEL_ANALYTCL_BL_INSTRMNT_FACT WHERE MTH_TM_ID=&MTH_TM_ID. and rtrim(ltrim(SRC_SYS_CD)) in ('TNG-MOR')) BY NZCON;
QUIT;


proc append base=DB2RRAP.BASEL_ANALYTCL_BL_INSTRMNT_FACT  (BULKLOAD=YES BL_METHOD=CLILOAD )
	data=NZRRAP.BASEL_ANALYTCL_BL_INSTRMNT_FACT(where=(mth_tm_id = &mth_tm_id. and SRC_SYS_CD = 'TNG-MOR')) force ; run;
