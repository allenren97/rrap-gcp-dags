%rrap_mor_bns_autoexec;
%get_model_period_dates(product=mor);
/*%put &start_period_dt; */
/*%let start_period_dt=30JUN2024;*/
%put &start_period_dt;


proc sql noprint;
	select tm_id into :tm_id
	from nzrrap.tm_dim
	where tm_lvl='Month' and tm_lvl_end_dt = "&start_period_dt."d;
quit;
%put &tm_id;
 /*GENERATE YYYY-MM-DD DATE FORMAT TO USE IT IN PASSTHROUGH*/
proc sql noprint;
select cats("'",put(tm_lvl_end_dt,yymmdd10.),"'") into :mth_end_dt
from nzrrap.tm_dim 
where tm_id=&tm_id. and tm_lvl='Month';
quit;
%put &mth_end_dt;

/*-----------------------------------------------
-----------------	KS	SEG ------------------------
--------------------------------------------------*/

proc sql;
connect using NZUSER as nzcon;
execute(
	delete from &RRAP_DB..LGD_SEG_ACCT_XREF 
	where 
		MTH_TM_ID=&TM_ID.
		and BASEL_MODEL_ID IN (select distinct(BASEL_MODEL_ID) 
								from &EDRLGD..LGD_ACCT_SCORE_SEG where MTH_TM_ID=&TM_ID.
									and SRC_SYS_CD='KS')
						)by nzcon;
quit;

proc sql;
connect using NZRRAP as nzcon;
execute(
INSERT INTO &RRAP_DB..LGD_SEG_ACCT_XREF (MTH_TM_ID,BASEL_ACCT_ID,BASEL_MODEL_ID,
         BASEL_SEG_ID,BASEL_MODEL_REL_ID,INSRT_PROCESS_TMSTMP,UPDT_PROCESS_TMSTMP) 
	   select 
				a.MTH_TM_ID,
                a.BASEL_ACCT_ID,
                a.BASEL_MODEL_ID,
                a.BASEL_SEG_ID,
                a.BASEL_MODEL_REL_ID,
                current_timestamp AS UPDT_PROCESS_TMSTMP,
				current_timestamp AS INSRT_PROCESS_TMSTMP
	       from &EDRLGD..LGD_ACCT_SCORE_SEG A
	       LEFT JOIN &RRAP_DB..TM_DIM tm
	           on a.MTH_TM_ID=tm.TM_ID
			WHERE A.MTH_TM_ID=&TM_ID.
				and a.SRC_SYS_CD='KS';
						)
by nzcon;
quit;

%put 'KS LGD_SEG_ACCT_XREF LOADING COMPLETED';

%put load end for &MTH_TM_ID;

