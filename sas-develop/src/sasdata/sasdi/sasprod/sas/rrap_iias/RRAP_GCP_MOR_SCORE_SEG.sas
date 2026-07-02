options errorabend;
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
-----------------	MOR	------------------------
--------------------------------------------------*/

proc sql noprint;
   create table scr_dtl as
   select 
	a.MTH_TM_ID, a.PROCESS_MTH, a.SRC_SYS_CD, a.BASEL_ACCT_ID, 
		b.MORTGAGE_NUM,a.BASEL_MODEL_ID,a.BASEL_MODEL_NM,A.VAR_NM,A.BIN,A.pt_cnt,
		b.CALC_SCORE,b.BASEL_SEG_NUM
from IIASLGD.LGD_ACCT_SCORE_DETAIL a
	left join IIASLGD.LGD_ACCT_SCORE_SEG b
		on a.MTH_TM_ID=b.MTH_TM_ID and a.BASEL_ACCT_ID=b.BASEL_ACCT_ID
	where A.MTH_TM_ID=&TM_ID and a.SRC_SYS_CD='MOR';
quit;

proc sort data = scr_dtl;
by MTH_TM_ID PROCESS_MTH SRC_SYS_CD BASEL_ACCT_ID MORTGAGE_NUM BASEL_MODEL_ID 
BASEL_MODEL_NM CALC_SCORE BASEL_SEG_NUM;
run;

proc transpose data = scr_dtl  out= bin  (drop = _NAME_ _LABEL_);
by MTH_TM_ID PROCESS_MTH SRC_SYS_CD BASEL_ACCT_ID MORTGAGE_NUM BASEL_MODEL_ID 
BASEL_MODEL_NM CALC_SCORE BASEL_SEG_NUM;
id VAR_NM;
var BIN;
run;

proc transpose data = scr_dtl  out= pt_cnt  (drop = _NAME_ _LABEL_);
by MTH_TM_ID PROCESS_MTH SRC_SYS_CD BASEL_ACCT_ID MORTGAGE_NUM BASEL_MODEL_ID 
BASEL_MODEL_NM CALC_SCORE BASEL_SEG_NUM;
id VAR_NM;
var pt_cnt;
run;
data bin_out;
    set bin (rename=(
			'B_Month_DEF (X4)'n = B_Month_DEF
			'B_Foreclose_Ind (X2)'n = B_Foreclose_Ind
			'B_LTV (X3)'n = B_LTV
			'B_D2DBALmax12m (X1)'n = B_D2DBALmax12m
			'B_UNEMP_RATE_RATIO (X5)'n = B_UNEMP_RATE_RATIO
			'B_index_teranetV (X6)'n = B_index_teranetV
			'CALC_SCORE'n = SCORECARD_POINTS
			'BASEL_SEG_NUM'n= BNS_LGD_D_SEGMENT
		));
run;
data pt_cnt_out;
    set pt_cnt (rename=(
		'B_Month_DEF (X4)'n=SCORE_Month_DEF 
		'B_Foreclose_Ind (X2)'n = SCORE_Foreclose_Ind
		'B_LTV (X3)'n = SCORE_LTV
		'B_D2DBALmax12m (X1)'n = SCORE_D2DBALmax12m
		'B_UNEMP_RATE_RATIO (X5)'n = SCORE_UNEMP_RATIO
		'B_index_teranetV (X6)'n = SCORE_index_teranetV
		'CALC_SCORE'n = SCORECARD_POINTS
		'BASEL_SEG_NUM'n= BNS_LGD_D_SEGMENT
		));
run;



proc sql;
   title 'SQL Table Combined';
   create table combined as
      select 
		a.MTH_TM_ID, a.PROCESS_MTH, a.SRC_SYS_CD, a.BASEL_ACCT_ID, 
		a.MORTGAGE_NUM,a.BASEL_MODEL_ID,a.BASEL_MODEL_NM,a.B_Month_DEF
		,a.B_Foreclose_Ind,a.B_LTV,a.B_D2DBALmax12m,a.B_UNEMP_RATE_RATIO,
		a.B_index_teranetV,b.SCORE_Month_DEF,b.SCORE_Foreclose_Ind,b.SCORE_LTV,
		b.SCORE_D2DBALmax12m,b.SCORE_UNEMP_RATIO,b.SCORE_index_teranetV,
		b.SCORECARD_POINTS,b.BNS_LGD_D_SEGMENT 
		from bin_out a
		inner join pt_cnt_out b
			on a.MTH_TM_ID=b.MTH_TM_ID 
			and a.BASEL_ACCT_ID=b.BASEL_ACCT_ID;
quit;

proc sql;
connect using NZUSER as nzcon;
execute(drop table &FRG_DB..combined if exists;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
CREATE TABLE NZUSER.combined AS 
SELECT MTH_TM_ID, PROCESS_MTH, SRC_SYS_CD, BASEL_ACCT_ID, MORTGAGE_NUM,
		BASEL_MODEL_ID,BASEL_MODEL_NM,B_Month_DEF,B_Foreclose_Ind,B_LTV,
		B_D2DBALmax12m,B_UNEMP_RATE_RATIO,B_index_teranetV,
		SCORE_Month_DEF,SCORE_Foreclose_Ind,SCORE_LTV,
		SCORE_D2DBALmax12m,SCORE_UNEMP_RATIO,SCORE_index_teranetV,
		SCORECARD_POINTS,BNS_LGD_D_SEGMENT
FROM combined;
quit;


proc sql;
connect using NZUSER as nzcon;
execute(
MERGE INTO &FRG_DB..BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ T
	USING (   select 
			A.MTH_TM_ID, A.PROCESS_MTH, A.SRC_SYS_CD, A.BASEL_ACCT_ID,A.MORTGAGE_NUM
			,A.BASEL_MODEL_ID,A.BASEL_MODEL_NM,A.B_Month_DEF,A.B_Foreclose_Ind,A.B_LTV,
			A.B_D2DBALmax12m,A.B_UNEMP_RATE_RATIO,A.B_index_teranetV,
			A.SCORE_Month_DEF,A.SCORE_Foreclose_Ind,A.SCORE_LTV,
			A.SCORE_D2DBALmax12m,A.SCORE_UNEMP_RATIO,A.SCORE_index_teranetV
			,A.SCORECARD_POINTS,A.BNS_LGD_D_SEGMENT,tm.TM_LVL_END_DT as TIME_KEY 	
	       from &FRG_DB..combined A
	       LEFT JOIN &RRAP_DB..TM_DIM tm
	           on a.MTH_TM_ID=tm.TM_ID
			   WHERE A.MTH_TM_ID=&TM_ID.
                   ) S
ON T.TIME_KEY = S.TIME_KEY and t.MORTGAGE_NO=s.MORTGAGE_NUM
AND T.TIME_KEY=&mth_end_dt.
WHEN MATCHED THEN 
	UPDATE SET 
	T.B_FORECLOSE_IND=S.B_FORECLOSE_IND,
	T.B_UNEMP_RATE_RATIO=S.B_UNEMP_RATE_RATIO,
	T.B_D2DBALMAX12M=S.B_D2DBALMAX12M,T.B_MONTH_DEF=S.B_MONTH_DEF,
	T.B_INDEX_TERANETV=S.B_INDEX_TERANETV,T.B_LTV=S.B_LTV,
	T.SCORE_Month_DEF=S.SCORE_Month_DEF,T.SCORE_Foreclose_Ind=S.SCORE_Foreclose_Ind,
	T.SCORE_LTV=S.SCORE_LTV,T.SCORE_D2DBALmax12m=S.SCORE_D2DBALmax12m,
	T.SCORE_UNEMP_RATIO=S.SCORE_UNEMP_RATIO,
	T.SCORE_index_teranetV=S.SCORE_index_teranetV,
	T.SCORECARD_POINTS=S.SCORECARD_POINTS,T.BNS_LGD_D_SEGMENT=S.BNS_LGD_D_SEGMENT
WHEN NOT MATCHED THEN
  INSERT(MORTGAGE_NO,TIME_KEY,B_Month_DEF,B_Foreclose_Ind,B_LTV,
			B_D2DBALmax12m,B_UNEMP_RATE_RATIO,B_index_teranetV,
			SCORE_Month_DEF,SCORE_Foreclose_Ind,SCORE_LTV,
			SCORE_D2DBALmax12m,SCORE_UNEMP_RATIO,SCORE_index_teranetV,
			SCORECARD_POINTS,BNS_LGD_D_SEGMENT)
  VALUES(S.MORTGAGE_NUM, S.TIME_KEY,S.B_Month_DEF,S.B_Foreclose_Ind,S.B_LTV,
			S.B_D2DBALmax12m,S.B_UNEMP_RATE_RATIO,S.B_index_teranetV,
			S.SCORE_Month_DEF,S.SCORE_Foreclose_Ind,S.SCORE_LTV,
			S.SCORE_D2DBALmax12m,S.SCORE_UNEMP_RATIO,S.SCORE_index_teranetV,
			S.SCORECARD_POINTS,S.BNS_LGD_D_SEGMENT	 
			);
						)
by nzcon;
quit;

%put 'MOR BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ LOADING COMPLETED';


proc sql;
connect using NZUSER as nzcon;
execute(
MERGE INTO &FRG_DB..BNS_LGD_D_SCORED_SEG_ACCTS T
	USING (   select 
			A.MTH_TM_ID, A.PROCESS_MTH, A.SRC_SYS_CD, A.BASEL_ACCT_ID,A.MORTGAGE_NUM
			,A.BASEL_MODEL_ID,A.BASEL_MODEL_NM,A.B_Month_DEF,A.B_Foreclose_Ind,A.B_LTV,
			A.B_D2DBALmax12m,A.B_UNEMP_RATE_RATIO,A.B_index_teranetV,
			A.SCORE_Month_DEF,A.SCORE_Foreclose_Ind,A.SCORE_LTV,
			A.SCORE_D2DBALmax12m,A.SCORE_UNEMP_RATIO,A.SCORE_index_teranetV
			,A.SCORECARD_POINTS,A.BNS_LGD_D_SEGMENT,tm.TM_LVL_END_DT as TIME_KEY 	
	       from &FRG_DB..combined A
	       LEFT JOIN &RRAP_DB..TM_DIM tm
	           on a.MTH_TM_ID=tm.TM_ID
			   WHERE A.MTH_TM_ID=&TM_ID.
                   ) S
ON T.TIME_KEY = S.TIME_KEY and t.MORTGAGE_NO=s.MORTGAGE_NUM
AND T.TIME_KEY=&mth_end_dt.
WHEN MATCHED THEN 
	UPDATE SET 
	T.B_FORECLOSE_IND=S.B_FORECLOSE_IND,
	T.B_UNEMP_RATE_RATIO=S.B_UNEMP_RATE_RATIO,
	T.B_D2DBALMAX12M=S.B_D2DBALMAX12M,T.B_MONTH_DEF=S.B_MONTH_DEF,
	T.B_INDEX_TERANETV=S.B_INDEX_TERANETV,T.B_LTV=S.B_LTV,
	T.SCORE_Month_DEF=S.SCORE_Month_DEF,T.SCORE_Foreclose_Ind=S.SCORE_Foreclose_Ind,
	T.SCORE_LTV=S.SCORE_LTV,T.SCORE_D2DBALmax12m=S.SCORE_D2DBALmax12m,
	T.SCORE_UNEMP_RATIO=S.SCORE_UNEMP_RATIO,
	T.SCORE_index_teranetV=S.SCORE_index_teranetV,
	T.SCORECARD_POINTS=S.SCORECARD_POINTS,T.BNS_LGD_D_SEGMENT=S.BNS_LGD_D_SEGMENT
WHEN NOT MATCHED THEN
  INSERT(MORTGAGE_NO,TIME_KEY,B_Month_DEF,B_Foreclose_Ind,B_LTV,
			B_D2DBALmax12m,B_UNEMP_RATE_RATIO,B_index_teranetV,
			SCORE_Month_DEF,SCORE_Foreclose_Ind,SCORE_LTV,
			SCORE_D2DBALmax12m,SCORE_UNEMP_RATIO,SCORE_index_teranetV,
			SCORECARD_POINTS,BNS_LGD_D_SEGMENT)
  VALUES(S.MORTGAGE_NUM, S.TIME_KEY,S.B_Month_DEF,S.B_Foreclose_Ind,S.B_LTV,
			S.B_D2DBALmax12m,S.B_UNEMP_RATE_RATIO,S.B_index_teranetV,
			S.SCORE_Month_DEF,S.SCORE_Foreclose_Ind,S.SCORE_LTV,
			S.SCORE_D2DBALmax12m,S.SCORE_UNEMP_RATIO,S.SCORE_index_teranetV,
			S.SCORECARD_POINTS,S.BNS_LGD_D_SEGMENT	 
			);
						)
by nzcon;
quit;

%put 'MOR BNS_LGD_D_SCORED_SEG_ACCTS LOADING COMPLETED';

proc sql;
connect using NZUSER as nzcon;
execute(
MERGE INTO &FRG_DB..BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ_LGD T
	USING (   select 
			A.MTH_TM_ID, A.PROCESS_MTH, A.SRC_SYS_CD, A.BASEL_ACCT_ID,A.MORTGAGE_NUM
			,A.BASEL_MODEL_ID,A.BASEL_MODEL_NM,A.B_Month_DEF,A.B_Foreclose_Ind,A.B_LTV,
			A.B_D2DBALmax12m,A.B_UNEMP_RATE_RATIO,A.B_index_teranetV,
			A.SCORE_Month_DEF,A.SCORE_Foreclose_Ind,A.SCORE_LTV,
			A.SCORE_D2DBALmax12m,A.SCORE_UNEMP_RATIO,A.SCORE_index_teranetV
			,A.SCORECARD_POINTS,A.BNS_LGD_D_SEGMENT,tm.TM_LVL_END_DT as TIME_KEY 	
	       from &FRG_DB..combined A
	       LEFT JOIN &RRAP_DB..TM_DIM tm
	           on a.MTH_TM_ID=tm.TM_ID
			   WHERE A.MTH_TM_ID=&TM_ID.
                   ) S
ON T.TIME_KEY = S.TIME_KEY and t.MORTGAGE_NO=s.MORTGAGE_NUM
AND T.TIME_KEY=&mth_end_dt.
WHEN MATCHED THEN 
	UPDATE SET 
	T.B_FORECLOSE_IND=S.B_FORECLOSE_IND,
	T.B_UNEMP_RATE_RATIO=S.B_UNEMP_RATE_RATIO,
	T.B_D2DBALMAX12M=S.B_D2DBALMAX12M,T.B_MONTH_DEF=S.B_MONTH_DEF,
	T.B_INDEX_TERANETV=S.B_INDEX_TERANETV,T.B_LTV=S.B_LTV,
	T.SCORE_Month_DEF=S.SCORE_Month_DEF,T.SCORE_Foreclose_Ind=S.SCORE_Foreclose_Ind,
	T.SCORE_LTV=S.SCORE_LTV,T.SCORE_D2DBALmax12m=S.SCORE_D2DBALmax12m,
	T.SCORE_UNEMP_RATIO=S.SCORE_UNEMP_RATIO,
	T.SCORE_index_teranetV=S.SCORE_index_teranetV,
	T.SCORECARD_POINTS=S.SCORECARD_POINTS,T.BNS_LGD_D_SEGMENT=S.BNS_LGD_D_SEGMENT,
	ACCT_LGD24_NOCOST=NULL,ACCT_LGD24_NOCOST_CAP=NULL,ACCT_LGD24_COST=NULL,ACCT_LGD24_COST_CAP15=NULL,
ACCT_LGD24_NOCOST_BKUP=NULL,
ACCT_LGD24_NOCOST_CAP_BKUP=NULL,ACCT_LGD24_COST_BKUP=NULL,ACCT_LGD24_COST_CAP15_BKUP=NULL
WHEN NOT MATCHED THEN
  INSERT(MORTGAGE_NO,TIME_KEY,BASEL_ACCT_ID,B_Month_DEF,B_Foreclose_Ind,B_LTV,
			B_D2DBALmax12m,B_UNEMP_RATE_RATIO,B_index_teranetV,
			SCORE_Month_DEF,SCORE_Foreclose_Ind,SCORE_LTV,
			SCORE_D2DBALmax12m,SCORE_UNEMP_RATIO,SCORE_index_teranetV,
			SCORECARD_POINTS,BNS_LGD_D_SEGMENT)
  VALUES(S.MORTGAGE_NUM, S.TIME_KEY,S.BASEL_ACCT_ID,S.B_Month_DEF,S.B_Foreclose_Ind,S.B_LTV,
			S.B_D2DBALmax12m,S.B_UNEMP_RATE_RATIO,S.B_index_teranetV,
			S.SCORE_Month_DEF,S.SCORE_Foreclose_Ind,S.SCORE_LTV,
			S.SCORE_D2DBALmax12m,S.SCORE_UNEMP_RATIO,S.SCORE_index_teranetV,
			S.SCORECARD_POINTS,S.BNS_LGD_D_SEGMENT	 
			);
						)
by nzcon;
quit;

%put 'MOR BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ_LGD UPDATE COMPLETED';


