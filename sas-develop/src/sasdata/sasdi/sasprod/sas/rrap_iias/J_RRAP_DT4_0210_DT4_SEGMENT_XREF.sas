options mprint errorabend;

***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0210_DT4_SEGMENT_XREF.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS &RRAP_DB.
*  Target Table:  DT4_SEGMENT_XREF
*  
*  Purpose: Put together account segmentation to be used in downstream DT4 jobs
*
*  Frequency: Quarter End runs
*
*  Notes: 
*           SEGMENT_RESTATEMENT = N will append the new quarters segmentation data. This is the default action.
*  	Setting SEGMENT_RESTATEMENT = Y will do a type 2 check for all historical months and capture deltas.
*   Only set this flag to Y when a restatement has occured to avoid unnecessary processing. 
*
*	Change Log:
*	2021-12-04: Hadi Dimashkieh - Initial Development
*   2022-06-15: Hadi Dimashkieh - SEGMENT_RESTATEMENT read in from parameter file.
*   2022-08-10: Hadi Dimashkieh - Added tm_lvl_end_dt to rundates where SEGMENT_RESTATEMENT=Y
*	2023-04-04: Hadi Dimashkieh - Removed basel_acct_id = -1
*	2025-02-18: Kalind Patel - RRMSS-3165 - SOU & CC EAD changes
* 	2025-02-18: Kalind Patel - RRMSS-3164 - DT4 - Collapsed segment Lookup table
***************************************************************************************************************************;



%rrap_dt4_autoexec();





/*
PD_SEG_ACCT_XREF LGD_SEG_ACCT_XREF EAD_SEG_ACCT_XREF
BASEL_PNL_LN_PD_SEG_ACCT_XREF BASEL_PNL_LN_LGD_SEG_ACCT_XREF
SCORED_SEGMENTED_ACCTS_ANTQ BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ BNS_LGD_ND_SCORED_SEG_ACCTS_ANTQ
TNG_PD_SEGMENTATION_FINAL TNG_LGDD_SEGMENTATION TNG_LGD_ND_SEGMENTATION 
*/

%macro delta_xrefs;

/*
data _null_;
	infile "&rrap_dir/params/rrap_iias/SEGMENT_RESTATEMENT.txt";
	input;
	if _n_ =3 then do;
		call symput("SEGMENT_RESTATEMENT",_infile_);
	end;
run;
*/

%let SEGMENT_RESTATEMENT = Y;
%put SEGMENT_RESTATEMENT= &SEGMENT_RESTATEMENT.;

%if &SEGMENT_RESTATEMENT EQ N %then %do;
	proc sql;
		connect using nzrrap as nzcon;
		execute(delete from &RRAP_DB..DT4_SEGMENT_XREF where mth_tm_id between &mth_tm_id. -40*2 and  &mth_tm_id.;) by nzcon;
		execute(commit;) by nzcon;
	quit;
	proc sql;
		connect using nzrrap as nzcon;
		create table rundates as select * from connection to nzcon(
			select a.mth_tm_id, b.tm_lvl_end_dt from 
			(SELECT DISTINCT mth_tm_id FROM &RRAP_DB..DT4_RPTG_DRVD_VARS
			EXCEPT
			SELECT DISTINCT mth_tm_id FROM &RRAP_DB..DT4_SEGMENT_XREF) a,
			&RRAP_DB..tm_dim b
			where b.tm_lvl='Month' and a.mth_tm_id = b.tm_id
			ORDER BY 1;);
	quit;
%end;
%else %if &SEGMENT_RESTATEMENT EQ Y %then %do;

	proc sql;
		connect using nzrrap as nzcon;
		execute(TRUNCATE table &RRAP_DB..DT4_SEGMENT_XREF;) by nzcon;
		execute(COMMIT;) by nzcon;
	quit;

	proc sql;
		connect using nzrrap as nzcon;
		create table rundates as select * from connection to nzcon(
			select distinct a.mth_tm_id, t.tm_lvl_end_dt from &RRAP_DB..DT4_RPTG_DRVD_VARS a,
			&rrap_db..tm_dim t where a.mth_tm_id = t.tm_id and t.tm_lvl = 'Month'
			order by 1;);
	quit;
%end;

%let qtrend_mth_tm_id = &mth_tm_id.;


%let nobs=0;

data _null_;
	set rundates;
	n=compress(put(_n_,best.));
	call symputx('runmonths'!!n,compress(mth_tm_id));
		call symputx('yrmth'!!n,put(tm_lvl_end_dt,yymmn6.));
		call symputx('yrmth_prev'!!n,put(intnx('Month',tm_lvl_end_dt,-1,'e'),yymmn6.));
	call symputx('mth_end_dt_nz'!!n,put(tm_lvl_end_dt,yymmdd10.));
	call symputx('nobs',_n_);
run;

%do i = 1 %to &nobs.;

%let mth_tm_id = &&runmonths&i.;

%if &mth_tm_id. GE %eval(&qtrend_mth_tm_id. -40*2) %then %do;
	%let yrmth = &&yrmth&i.;
	%let yrmth_prev = &&yrmth_prev&i.;
	%let mth_end_dt_nz = &&mth_end_dt_nz&i.;
%end;

%put ******** Now Loading mth_tm_id = &mth_tm_id.;
%put yrmth=&yrmth. yrmth_prev=&yrmth_prev. mth_end_dt_nz=&mth_end_dt_nz.;


%let loader_view = %str(
/**********--   KS *************************/
SELECT accts.mth_tm_id, COALESCE(pd.basel_acct_id,lgdd.basel_acct_id,lgdnd.basel_acct_id) AS basel_acct_id, NULL AS MORT_NUM
	,i1.seg_num AS PD_BASEL_SEG_NUM, pd.BASEL_SEG_ID AS PD_BASEL_SEG_ID, pd.BASEL_MODEL_ID AS PD_BASEL_MODEL_ID, i1.INCL_F as DT4_PD_MODEL_INCL_F
	,i2.seg_num AS LGD_D_BASEL_SEG_NUM, LGDD.BASEL_SEG_ID AS LGD_D_BASEL_SEG_ID, LGDD.BASEL_MODEL_ID AS LGD_D_BASEL_MODEL_ID, i2.INCL_F as DT4_LGD_D_MODEL_INCL_F
	,i3.seg_num AS LGD_ND_BASEL_SEG_NUM, LGDND.BASEL_SEG_ID AS LGD_ND_BASEL_SEG_ID, LGDND.BASEL_MODEL_ID AS LGD_ND_BASEL_MODEL_ID, i3.INCL_F as DT4_LGD_ND_MODEL_INCL_F
	,i4.seg_num AS EAD_BASEL_SEG_NUM, ead.BASEL_SEG_ID AS EAD_BASEL_SEG_ID, ead.BASEL_MODEL_ID AS EAD_BASEL_MODEL_ID, i4.INCL_F as DT4_EAD_MODEL_INCL_F
FROM 
(	  SELECT basel_acct_id, mth_tm_id FROM &RRAP_DB..PD_SEG_ACCT_XREF 
UNION SELECT basel_acct_id, mth_tm_id FROM &RRAP_DB..LGD_SEG_ACCT_XREF 
UNION SELECT basel_acct_id, mth_tm_id FROM &RRAP_DB..EAD_SEG_ACCT_XREF 
) accts 
INNER JOIN 
&RRAP_DB..DT4_RPTG_DRVD_VARS a ON a.BASEL_ACCT_ID = accts.BASEL_ACCT_ID AND a.MTH_TM_ID = accts.MTH_TM_ID 
LEFT JOIN 
&RRAP_DB..PD_SEG_ACCT_XREF pd
	ON accts.basel_acct_id = pd.basel_acct_id AND accts.mth_tm_id = pd.MTH_TM_ID 
LEFT JOIN 
(&RRAP_DB..LGD_SEG_ACCT_XREF lgdd
	INNER JOIN &RRAP_DB..BASEL_MODEL bm1 
	ON lgdd.BASEL_MODEL_ID = bm1.BASEL_MODEL_ID AND bm1.SRC_SYS_CD ='KS' AND bm1.LGD_DEFAULTER_F = 'Y' AND %nrbquote('&mth_end_dt_nz.') BETWEEN bm1.model_st_dt AND bm1.model_end_dt
)  ON accts.basel_acct_id = lgdd.basel_acct_id AND accts.mth_tm_id = lgdd.MTH_TM_ID 
	
LEFT JOIN 
(&RRAP_DB..LGD_SEG_ACCT_XREF lgdnd
	INNER JOIN &RRAP_DB..BASEL_MODEL bm2 
	ON lgdnd.BASEL_MODEL_ID = bm2.BASEL_MODEL_ID AND bm2.SRC_SYS_CD ='KS' AND bm2.LGD_NON_DEFAULTER_F = 'Y' AND %nrbquote('&mth_end_dt_nz.') BETWEEN bm2.model_st_dt AND bm2.model_end_dt
)  ON accts.basel_acct_id = lgdnd.basel_acct_id AND accts.mth_tm_id = lgdnd.MTH_TM_ID 
	
LEFT JOIN &RRAP_DB..EAD_SEG_ACCT_XREF ead
	ON accts.basel_acct_id = ead.basel_acct_id AND accts.mth_tm_id = ead.MTH_TM_ID 
	
LEFT JOIN 	&RRAP_DB..DT4_BASEL_SEG_INCL i1
	ON i1.BASEL_SEG_ID = pd.BASEL_SEG_ID AND i1.BASEL_MODEL_ID = pd.BASEL_MODEL_ID AND i1.MODEL_TYPE = 'PD' AND i1.SRC_SYS_CD = 'KS' AND &YRMTH. BETWEEN cast(i1.EFF_FROM_YR_MTH AS integer) AND cast(i1.EFF_TO_YR_MTH AS integer)
	
LEFT JOIN 	&RRAP_DB..DT4_BASEL_SEG_INCL i2
	ON i2.BASEL_SEG_ID = lgdd.BASEL_SEG_ID AND i2.BASEL_MODEL_ID = lgdd.BASEL_MODEL_ID AND i2.MODEL_TYPE = 'LGD-D' AND i2.SRC_SYS_CD = 'KS' AND &YRMTH. BETWEEN cast(i2.EFF_FROM_YR_MTH AS integer) AND cast(i2.EFF_TO_YR_MTH AS integer)
	
LEFT JOIN 	&RRAP_DB..DT4_BASEL_SEG_INCL i3
	ON i3.BASEL_SEG_ID = lgdnd.BASEL_SEG_ID AND i3.BASEL_MODEL_ID = lgdnd.BASEL_MODEL_ID AND i3.MODEL_TYPE = 'LGD-ND' AND i3.SRC_SYS_CD = 'KS' AND &YRMTH. BETWEEN cast(i3.EFF_FROM_YR_MTH AS integer) AND cast(i3.EFF_TO_YR_MTH AS integer)

LEFT JOIN 	&RRAP_DB..DT4_BASEL_SEG_INCL i4
	ON i4.BASEL_SEG_ID = ead.BASEL_SEG_ID AND i4.BASEL_MODEL_ID = ead.BASEL_MODEL_ID AND i4.MODEL_TYPE = 'EAD' AND i4.SRC_SYS_CD = 'KS' AND &YRMTH. BETWEEN cast(i4.EFF_FROM_YR_MTH AS integer) AND cast(i4.EFF_TO_YR_MTH AS integer)
where accts.mth_tm_id in (&mth_tm_id.)	and accts.basel_acct_id <> -1 
UNION ALL 
/**********--   SPL *************************/
SELECT accts.mth_tm_id, COALESCE(pd.basel_acct_id,lgdd.basel_acct_id,lgdnd.basel_acct_id) AS basel_acct_id, NULL AS MORT_NUM
	,i1.seg_num AS PD_BASEL_SEG_NUM, pd.BASEL_SEG_ID AS PD_BASEL_SEG_ID, pd.BASEL_MODEL_ID AS PD_BASEL_MODEL_ID, i1.INCL_F as DT4_PD_MODEL_INCL_F
	,i2.seg_num AS LGD_D_BASEL_SEG_NUM, LGDD.BASEL_SEG_ID AS LGD_D_BASEL_SEG_ID, LGDD.BASEL_MODEL_ID AS LGD_D_BASEL_MODEL_ID, i2.INCL_F as DT4_LGD_D_MODEL_INCL_F
	,i3.seg_num AS LGD_ND_BASEL_SEG_NUM, LGDND.BASEL_SEG_ID AS LGD_ND_BASEL_SEG_ID, LGDND.BASEL_MODEL_ID AS LGD_ND_BASEL_MODEL_ID, i3.INCL_F as DT4_LGD_ND_MODEL_INCL_F
	,NULL AS EAD_BASEL_SEG_NUM, NULL AS EAD_BASEL_SEG_ID, NULL AS EAD_BASEL_MODEL_ID, 0 as DT4_EAD_MODEL_INCL_F
FROM 
(	  SELECT basel_acct_id, mth_tm_id FROM &RRAP_DB..BASEL_PNL_LN_PD_SEG_ACCT_XREF 
UNION SELECT basel_acct_id, mth_tm_id FROM &RRAP_DB..BASEL_PNL_LN_LGD_SEG_ACCT_XREF 
) accts 
INNER JOIN 
&RRAP_DB..DT4_RPTG_DRVD_VARS a ON a.BASEL_ACCT_ID = accts.BASEL_ACCT_ID AND a.MTH_TM_ID = accts.MTH_TM_ID 
LEFT JOIN 
&RRAP_DB..BASEL_PNL_LN_PD_SEG_ACCT_XREF pd
	ON accts.basel_acct_id = pd.basel_acct_id AND accts.mth_tm_id = pd.MTH_TM_ID 
LEFT JOIN 
(&RRAP_DB..BASEL_PNL_LN_LGD_SEG_ACCT_XREF lgdd
	INNER JOIN &RRAP_DB..BASEL_MODEL bm1 
	ON lgdd.BASEL_MODEL_ID = bm1.BASEL_MODEL_ID AND bm1.SRC_SYS_CD ='SPL' AND bm1.LGD_DEFAULTER_F = 'Y' AND %nrbquote('&mth_end_dt_nz.') BETWEEN bm1.model_st_dt AND bm1.model_end_dt
)  ON accts.basel_acct_id = lgdd.basel_acct_id AND accts.mth_tm_id = lgdd.MTH_TM_ID 
	
LEFT JOIN 
(&RRAP_DB..BASEL_PNL_LN_LGD_SEG_ACCT_XREF lgdnd
	INNER JOIN &RRAP_DB..BASEL_MODEL bm2 
	ON lgdnd.BASEL_MODEL_ID = bm2.BASEL_MODEL_ID AND bm2.SRC_SYS_CD ='SPL' AND bm2.LGD_NON_DEFAULTER_F = 'Y' AND %nrbquote('&mth_end_dt_nz.') BETWEEN bm2.model_st_dt AND bm2.model_end_dt
)  ON accts.basel_acct_id = lgdnd.basel_acct_id AND accts.mth_tm_id = lgdnd.MTH_TM_ID 
	
LEFT JOIN 	&RRAP_DB..DT4_BASEL_SEG_INCL i1
	ON i1.BASEL_SEG_ID = pd.BASEL_SEG_ID AND i1.BASEL_MODEL_ID = pd.BASEL_MODEL_ID AND i1.MODEL_TYPE = 'PD' AND i1.SRC_SYS_CD = 'SPL' AND &YRMTH. BETWEEN cast(i1.EFF_FROM_YR_MTH AS integer) AND cast(i1.EFF_TO_YR_MTH AS integer)
	
LEFT JOIN 	&RRAP_DB..DT4_BASEL_SEG_INCL i2
	ON i2.BASEL_SEG_ID = lgdd.BASEL_SEG_ID AND i2.BASEL_MODEL_ID = lgdd.BASEL_MODEL_ID AND i2.MODEL_TYPE = 'LGD-D' AND i2.SRC_SYS_CD = 'SPL' AND &YRMTH. BETWEEN cast(i2.EFF_FROM_YR_MTH AS integer) AND cast(i2.EFF_TO_YR_MTH AS integer)
	
LEFT JOIN 	&RRAP_DB..DT4_BASEL_SEG_INCL i3
	ON i3.BASEL_SEG_ID = lgdnd.BASEL_SEG_ID AND i3.BASEL_MODEL_ID = lgdnd.BASEL_MODEL_ID AND i3.MODEL_TYPE = 'LGD-ND' AND i3.SRC_SYS_CD = 'SPL' AND &YRMTH. BETWEEN cast(i3.EFF_FROM_YR_MTH AS integer) AND cast(i3.EFF_TO_YR_MTH AS integer)
where accts.mth_tm_id in (&mth_tm_id.)	and accts.basel_acct_id <> -1 
/**********-- MOR *************************/
UNION ALL 
SELECT t.tm_id AS mth_tm_id, a.basel_acct_id, CAST(COALESCE(pd.MORTGAGE_NO,lgdd.MORTGAGE_NO,lgdnd.MORTGAGE_NO) AS VARCHAR) AS MORT_NUM
	,pd.NODE AS PD_BASEL_SEG_NUM, i1.BASEL_SEG_ID AS PD_BASEL_SEG_ID, i1.BASEL_MODEL_ID AS PD_BASEL_MODEL_ID, i1.INCL_F as DT4_PD_MODEL_INCL_F
	, lgdd.BNS_LGD_D_SEGMENT AS LGD_D_SEGMENT, i2.BASEL_SEG_ID AS LGD_D_BASEL_SEG_ID, i2.BASEL_MODEL_ID AS LGD_D_BASEL_MODEL_ID, i2.INCL_F as DT4_LGD_D_MODEL_INCL_F
	, LGDND.BNS_LGD_ND_SEGMENT AS LGD_ND_SEGMENT, i3.BASEL_SEG_ID AS LGD_ND_BASEL_SEG_ID, i3.BASEL_MODEL_ID AS LGD_ND_BASEL_MODEL_ID, i3.INCL_F as DT4_LGD_ND_MODEL_INCL_F
	,NULL AS EAD_BASEL_SEG_NUM, NULL AS EAD_BASEL_SEG_ID, NULL AS EAD_BASEL_MODEL_ID, 0 as DT4_EAD_MODEL_INCL_F
FROM 
(	  SELECT MORTGAGE_NO, PROCESS_DATE AS TIME_KEY FROM &MOR_DB..SCORED_SEGMENTED_ACCTS_ANTQ 
UNION SELECT MORTGAGE_NO, 				  TIME_KEY FROM &MOR_DB..BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ
UNION SELECT MORTGAGE_NO, 				  TIME_KEY FROM &MOR_DB..BNS_LGD_ND_SCORED_SEG_ACCTS_ANTQ 
) accts LEFT JOIN 
&RRAP_DB..tm_dim T
	ON accts.time_key = t.tm_lvl_end_dt AND t.tm_lvl='Month'
INNER JOIN 
&RRAP_DB..DT4_RPTG_DRVD_VARS a
ON trim(a.mort_num) = CAST(accts.MORTGAGE_NO AS varchar) AND a.mth_tm_id = t.tm_id
LEFT JOIN 
&MOR_DB..SCORED_SEGMENTED_ACCTS_ANTQ pd
	ON accts.MORTGAGE_NO = pd.MORTGAGE_NO AND accts.TIME_KEY = pd.PROCESS_DATE 
LEFT JOIN 
&MOR_DB..BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ lgdd
	ON accts.MORTGAGE_NO = lgdd.MORTGAGE_NO  AND accts.TIME_KEY = lgdd.TIME_KEY 
LEFT JOIN 
&MOR_DB..BNS_LGD_ND_SCORED_SEG_ACCTS_ANTQ lgdnd 
	ON accts.MORTGAGE_NO = lgdnd.MORTGAGE_NO AND accts.TIME_KEY = lgdnd.TIME_KEY 
	
LEFT JOIN 	&RRAP_DB..DT4_BASEL_SEG_INCL i1
	ON i1.SEG_NUM = pd.node AND i1.MODEL_TYPE = 'PD' AND i1.SRC_SYS_CD = 'MOR' AND &YRMTH. BETWEEN cast(i1.EFF_FROM_YR_MTH AS integer) AND cast(i1.EFF_TO_YR_MTH AS integer)
	
LEFT JOIN 	&RRAP_DB..DT4_BASEL_SEG_INCL i2
	ON i2.SEG_NUM = lgdd.BNS_LGD_D_SEGMENT AND i2.MODEL_TYPE = 'LGD-D' AND i2.SRC_SYS_CD = 'MOR' AND &YRMTH. BETWEEN cast(i2.EFF_FROM_YR_MTH AS integer) AND cast(i2.EFF_TO_YR_MTH AS integer)
	
LEFT JOIN 	&RRAP_DB..DT4_BASEL_SEG_INCL i3
	ON i3.SEG_NUM = lgdnd.BNS_LGD_ND_SEGMENT AND i3.MODEL_TYPE = 'LGD-ND' AND i3.SRC_SYS_CD = 'MOR' AND &YRMTH. BETWEEN cast(i3.EFF_FROM_YR_MTH AS integer) AND cast(i3.EFF_TO_YR_MTH AS integer)
	
where t.tm_id in (&mth_tm_id.)	and a.basel_acct_id <> -1 

/**********--   TNG *************************/
	
UNION ALL 
SELECT t.tm_id AS mth_tm_id, a.basel_acct_id, COALESCE(pd.account_id,lgdd.account_id,lgdnd.account_id) AS MORT_NUM
	,pd.segment AS PD_BASEL_SEG_NUM, i1.BASEL_SEG_ID AS PD_BASEL_SEG_ID, i1.BASEL_MODEL_ID AS PD_BASEL_MODEL_ID, i1.INCL_F as DT4_PD_MODEL_INCL_F
	,lgdd.LGDD_SEGMENT AS LGD_D_SEGMENT, i2.BASEL_SEG_ID AS LGD_D_BASEL_SEG_ID, i2.BASEL_MODEL_ID AS LGD_D_BASEL_MODEL_ID, i2.INCL_F as DT4_LGD_D_MODEL_INCL_F
	,LGDND.SEGMENT AS LGD_ND_SEGMENT, i3.BASEL_SEG_ID AS LGD_ND_BASEL_SEG_ID, i3.BASEL_MODEL_ID AS LGD_ND_BASEL_MODEL_ID, i3.INCL_F as DT4_LGD_ND_MODEL_INCL_F
	,NULL AS EAD_BASEL_SEG_NUM, NULL AS EAD_BASEL_SEG_ID, NULL AS EAD_BASEL_MODEL_ID, 0 as DT4_EAD_MODEL_INCL_F
FROM 
(	  SELECT account_id, MONTH_END_DT FROM &MOR_DB..TNG_PD_SEGMENTATION_FINAL 
UNION SELECT account_id, MONTH_END_DT FROM &MOR_DB..TNG_LGDD_SEGMENTATION 
UNION SELECT account_id, MONTH_END_DT FROM &MOR_DB..TNG_LGD_ND_SEGMENTATION 
) accts LEFT JOIN 
&RRAP_DB..tm_dim T
	ON accts.MONTH_END_DT = t.tm_lvl_end_dt AND t.tm_lvl='Month'
INNER  JOIN 
&RRAP_DB..DT4_RPTG_DRVD_VARS a
ON a.mort_num = accts.account_id AND a.mth_tm_id = t.tm_id
LEFT JOIN 
&MOR_DB..TNG_PD_SEGMENTATION_FINAL pd
	ON accts.ACCOUNT_ID = pd.account_id AND accts.MONTH_END_DT = pd.MONTH_END_DT 
LEFT JOIN 
&MOR_DB..TNG_LGDD_SEGMENTATION lgdd
	ON accts.ACCOUNT_ID = lgdd.ACCOUNT_ID AND accts.MONTH_END_DT = lgdd.MONTH_END_DT 
LEFT JOIN 
&MOR_DB..TNG_LGD_ND_SEGMENTATION lgdnd 
	ON accts.ACCOUNT_ID = lgdnd.ACCOUNT_ID AND accts.MONTH_END_DT = lgdnd.MONTH_END_DT 

LEFT JOIN 	&RRAP_DB..DT4_BASEL_SEG_INCL i1
	ON i1.SEG_NUM = pd.segment AND i1.MODEL_TYPE = 'PD' AND i1.SRC_SYS_CD = 'TNG-MOR' AND &YRMTH. BETWEEN cast(i1.EFF_FROM_YR_MTH AS integer) AND cast(i1.EFF_TO_YR_MTH AS integer)
	
LEFT JOIN 	&RRAP_DB..DT4_BASEL_SEG_INCL i2
	ON i2.SEG_NUM = lgdd.LGDD_SEGMENT AND i2.MODEL_TYPE = 'LGD-D' AND i2.SRC_SYS_CD = 'TNG-MOR' AND &YRMTH. BETWEEN cast(i2.EFF_FROM_YR_MTH AS integer) AND cast(i2.EFF_TO_YR_MTH AS integer)
	
LEFT JOIN 	&RRAP_DB..DT4_BASEL_SEG_INCL i3
	ON i3.SEG_NUM = lgdnd.SEGMENT AND i3.MODEL_TYPE = 'LGD-ND' AND i3.SRC_SYS_CD = 'TNG-MOR' AND &YRMTH. BETWEEN cast(i3.EFF_FROM_YR_MTH AS integer) AND cast(i3.EFF_TO_YR_MTH AS integer)
where t.tm_id in (&mth_tm_id.) and a.basel_acct_id <> -1 
);



/*proc sql noprint;*/
/*select 'strip(cast(coalesce('||(name)||',0) as INTEGER))' into :hash_vars separated by '||' */
/*from sashelp.vcolumn*/
/*where libname = 'NZRRAP' and memname = 'DT4_SEGMENT_XREF'*/
/*and (substr(name,1,2) contains ('PD') or substr(name,1,3) contains ('LGD') or substr(name,1,3) contains ('EAD') or substr(name,1,3) contains ('DT4'));*/
/*quit;*/

/*Kalind Patel - RRMSS-3165 - SOU & CC EAD changes*/
/* Use the old PD column to create hash values */

%let loader_hash_vars=;
%let target_hash_vars=;

proc sql noprint;
create table column_names as (select name
from sashelp.vcolumn
where libname = 'NZRRAP' and memname = 'DT4_SEGMENT_XREF'
and (substr(name,1,2) contains ('PD') or substr(name,1,3) contains ('LGD') or substr(name,1,3) contains ('EAD') or substr(name,1,3) contains ('DT4')));
quit;

data column_names;
set column_names;
if substr(name,1,12) = 'PD_BASEL_SEG' or substr(name,1,15) = 'LGD_D_BASEL_SEG' then
alt_name = cats('CCAR_',name);
else
alt_name = name;
call symput('loader_hash_vars',catx('||',symget('loader_hash_vars'),'strip(cast(coalesce('||(name)||',0) as INTEGER))'));
call symput('target_hash_vars',catx('||',symget('target_hash_vars'),'strip(cast(coalesce('||(alt_name)||',0) as INTEGER))'));
run;

proc sql noprint;
select name into :load_vars separated by ', ' 
from sashelp.vcolumn
where libname = 'NZRRAP' and memname = 'DT4_SEGMENT_XREF'
and (name not in ('EFF_FROM_YR_MTH','EFF_TO_YR_MTH','CRNT_F','INSRT_PROCESS_TMSTMP','UPDT_PROCESS_TMSTMP',
	'CCAR_PD_BASEL_SEG_NUM', 'CCAR_PD_BASEL_SEG_ID', 'CCAR_LGD_D_BASEL_SEG_NUM', 'CCAR_LGD_D_BASEL_SEG_ID'));
quit;


%let loader_hash_vars=&loader_hash_vars.;
%let target_hash_vars=&target_hash_vars.;
%let load_vars=&load_vars.;


proc sql;
	connect using nzrrap as nzcon;
	execute(drop table &RRAP_WRK..loader if exists;) by nzcon;
	execute(commit;) by nzcon;
quit;


/*2025-02-18: Kalind Patel - RRMSS-3165 - SOU & CC EAD changes*/
/*2025-02-18: Kalind Patel - RRMSS-3164 - DT4 - Collapsed segment Lookup table*/
proc sql;
	connect using nzrrap as nzcon;
	execute(create table &RRAP_WRK..loader as (
SELECT l.HASH_VALUE, l.MTH_TM_ID, l.BASEL_ACCT_ID, l.MORT_NUM,
COALESCE(d1.DT4_SEG_NUM,l.PD_BASEL_SEG_NUM) AS PD_BASEL_SEG_NUM, COALESCE(d1.DT4_BASEL_SEG_ID,l.PD_BASEL_SEG_ID) AS PD_BASEL_SEG_ID,
l.PD_BASEL_MODEL_ID, l.DT4_PD_MODEL_INCL_F, 
COALESCE(d2.DT4_SEG_NUM,l.LGD_D_BASEL_SEG_NUM) AS LGD_D_BASEL_SEG_NUM, COALESCE(d2.DT4_BASEL_SEG_ID,l.LGD_D_BASEL_SEG_ID) AS LGD_D_BASEL_SEG_ID, 
l.LGD_D_BASEL_MODEL_ID, l.DT4_LGD_D_MODEL_INCL_F, l.LGD_ND_BASEL_SEG_NUM, 
l.LGD_ND_BASEL_SEG_ID, l.LGD_ND_BASEL_MODEL_ID, l.DT4_LGD_ND_MODEL_INCL_F, l.EAD_BASEL_SEG_NUM, l.EAD_BASEL_SEG_ID, l.EAD_BASEL_MODEL_ID, l.DT4_EAD_MODEL_INCL_F, 
(cast (CCAR_PD_BASEL_SEG_NUM as INTEGER)) AS CCAR_PD_BASEL_SEG_NUM,(cast (CCAR_PD_BASEL_SEG_ID as INTEGER)) AS CCAR_PD_BASEL_SEG_ID,
(cast (CCAR_LGD_D_BASEL_SEG_NUM as INTEGER)) AS CCAR_LGD_D_BASEL_SEG_NUM,(cast (CCAR_LGD_D_BASEL_SEG_ID as INTEGER)) AS CCAR_LGD_D_BASEL_SEG_ID
FROM (
	SELECT hex(hash(&loader_hash_vars.,0)) AS hash_value
		,&load_vars., 
		(cast (PD_BASEL_SEG_NUM as INTEGER)) AS CCAR_PD_BASEL_SEG_NUM, (cast (PD_BASEL_SEG_ID as INTEGER)) AS CCAR_PD_BASEL_SEG_ID,
		(cast (LGD_D_BASEL_SEG_NUM as INTEGER)) AS CCAR_LGD_D_BASEL_SEG_NUM, (cast (LGD_D_BASEL_SEG_ID as INTEGER)) AS CCAR_LGD_D_BASEL_SEG_ID
	FROM (&loader_view.) ) l
	LEFT JOIN &RRAP_DB..BASEL_DT4_CCAR_SEG_DIM d1  ON l.PD_BASEL_SEG_ID=d1.CCAR_BASEL_SEG_ID  AND l.PD_BASEL_SEG_NUM = d1.CCAR_SEG_NUM AND &YRMTH. between cast(d1.EFF_FROM_YR_MTH as integer) and cast(d1.EFF_TO_YR_MTH as integer)
	LEFT JOIN &RRAP_DB..BASEL_DT4_CCAR_SEG_DIM d2  ON l.LGD_D_BASEL_SEG_ID=d2.CCAR_BASEL_SEG_ID  AND l.LGD_D_BASEL_SEG_NUM = d2.CCAR_SEG_NUM AND &YRMTH. between cast(d2.EFF_FROM_YR_MTH as integer) and cast(d2.EFF_TO_YR_MTH as integer)
) with data
	DISTRIBUTE BY HASH (BASEL_ACCT_ID, MTH_TM_ID);) by nzcon;
	execute(commit;) by nzcon;
quit;



proc sql;
connect using nzrrap as nzcon;
execute(drop table &RRAP_WRK..target if exists;) by nzcon;
execute(commit;) by nzcon;
quit;

/*Kalind Patel - RRMSS-3165 - SOU & CC EAD changes*/
/* Use the old columns to compare hash values */
proc sql;
connect using nzrrap as nzcon;
execute(create table &RRAP_WRK..target as (
SELECT hex(hash(&target_hash_vars.,0)) AS hash_value
	,&load_vars.
	,EFF_FROM_YR_MTH,EFF_TO_YR_MTH,CRNT_F,INSRT_PROCESS_TMSTMP,UPDT_PROCESS_TMSTMP,
	CCAR_PD_BASEL_SEG_NUM, CCAR_PD_BASEL_SEG_ID, CCAR_LGD_D_BASEL_SEG_NUM, CCAR_LGD_D_BASEL_SEG_ID
FROM 

%if &SEGMENT_RESTATEMENT EQ N %then %do;
	&RRAP_DB..DT4_SEGMENT_XREF 
%end;
%else %if &SEGMENT_RESTATEMENT EQ Y %then %do;
	&RRAP_WRK..DT4_SEGMENT_XREF_BKUP 
%end;
where mth_tm_id in (&mth_tm_id.) and basel_acct_id <> -1) with data 
DISTRIBUTE BY HASH (BASEL_ACCT_ID, MTH_TM_ID);) by nzcon;
execute(commit;) by nzcon;
quit;

**************************************************************************;






			
*--Step2. Changes. Expire matched Row.*;

proc sql;
connect using nzrrap as nzcon;
execute(
MERGE into &RRAP_WRK..TARGET as T
	USING &RRAP_WRK..loader as L 
	ON ( T.mth_tm_id = L.mth_tm_id AND T.basel_acct_id = L.basel_acct_id AND T.CRNT_F = 'Y' AND 
		T.HASH_VALUE <> L.HASH_VALUE
		)
	WHEN MATCHED 
		THEN UPDATE SET
				EFF_TO_YR_MTH = %bquote(&yrmth_prev.)
				,CRNT_F='N'
/*				,INSRT_PROCESS_TMSTMP=now()*/
				,UPDT_PROCESS_TMSTMP=now();) by nzcon;
execute(COMMIT;) by nzcon;
quit;


/*--Step 3. Changes. Insert new row.*/
proc sql;
connect using nzrrap as nzcon;
execute(
insert into &RRAP_WRK..TARGET 
select l.HASH_VALUE, l.MTH_TM_ID, l.BASEL_ACCT_ID, l.MORT_NUM, l.PD_BASEL_SEG_NUM, l.PD_BASEL_SEG_ID, l.PD_BASEL_MODEL_ID, l.DT4_PD_MODEL_INCL_F, l.LGD_D_BASEL_SEG_NUM, 
l.LGD_D_BASEL_SEG_ID, l.LGD_D_BASEL_MODEL_ID, l.DT4_LGD_D_MODEL_INCL_F, l.LGD_ND_BASEL_SEG_NUM, l.LGD_ND_BASEL_SEG_ID, l.LGD_ND_BASEL_MODEL_ID, l.DT4_LGD_ND_MODEL_INCL_F, 
l.EAD_BASEL_SEG_NUM, l.EAD_BASEL_SEG_ID, l.EAD_BASEL_MODEL_ID, l.DT4_EAD_MODEL_INCL_F, %bquote(&YRMTH.) AS EFF_FROM_YR_MTH, '999912' as EFF_TO_YR_MTH, 'Y' as CRNT_F,
	now() AS INSRT_PROCESS_TMSTMP, now() AS UPDT_PROCESS_TMSTMP, l.CCAR_PD_BASEL_SEG_NUM, l.CCAR_PD_BASEL_SEG_ID, l.CCAR_LGD_D_BASEL_SEG_NUM, l.CCAR_LGD_D_BASEL_SEG_ID
from &RRAP_WRK..loader as L inner join &RRAP_WRK..TARGET T
on T.mth_tm_id = L.mth_tm_id AND T.basel_acct_id = L.basel_acct_id and T.CRNT_F='Y'

where T.HASH_VALUE <> L.HASH_VALUE;) by nzcon;
execute(COMMIT;) by nzcon;
quit;

/*-- Step 4. Net new. insert row.*/
proc sql;
connect using nzrrap as nzcon;
execute(
insert into &RRAP_WRK..TARGET
select l.HASH_VALUE, l.MTH_TM_ID, l.BASEL_ACCT_ID, l.MORT_NUM, l.PD_BASEL_SEG_NUM, l.PD_BASEL_SEG_ID, l.PD_BASEL_MODEL_ID, l.DT4_PD_MODEL_INCL_F, l.LGD_D_BASEL_SEG_NUM, 
l.LGD_D_BASEL_SEG_ID, l.LGD_D_BASEL_MODEL_ID, l.DT4_LGD_D_MODEL_INCL_F, l.LGD_ND_BASEL_SEG_NUM, l.LGD_ND_BASEL_SEG_ID, l.LGD_ND_BASEL_MODEL_ID, l.DT4_LGD_ND_MODEL_INCL_F, 
l.EAD_BASEL_SEG_NUM, l.EAD_BASEL_SEG_ID, l.EAD_BASEL_MODEL_ID, l.DT4_EAD_MODEL_INCL_F,  %bquote(&YRMTH.) AS EFF_FROM_YR_MTH, '999912' as EFF_TO_YR_MTH, 'Y' as CRNT_F,
	   now() AS INSRT_PROCESS_TMSTMP, now() AS UPDT_PROCESS_TMSTMP, l.CCAR_PD_BASEL_SEG_NUM, l.CCAR_PD_BASEL_SEG_ID, l.CCAR_LGD_D_BASEL_SEG_NUM, l.CCAR_LGD_D_BASEL_SEG_ID
from &RRAP_WRK..loader as L left join &RRAP_WRK..TARGET T
on T.mth_tm_id = L.mth_tm_id AND T.basel_acct_id = L.basel_acct_id and T.CRNT_F='Y'
where (T.mth_tm_id is NULL AND T.basel_acct_id IS NULL);) by nzcon;
execute(COMMIT;) by nzcon;
quit;


/*-- Step 5. Truncate/Load target table*/



proc sql;
connect using nzrrap as nzcon;
execute(INSERT INTO &RRAP_DB..DT4_SEGMENT_XREF
	SELECT &load_vars.
	,EFF_FROM_YR_MTH,EFF_TO_YR_MTH,CRNT_F,INSRT_PROCESS_TMSTMP,UPDT_PROCESS_TMSTMP, CCAR_PD_BASEL_SEG_NUM, CCAR_PD_BASEL_SEG_ID, CCAR_LGD_D_BASEL_SEG_NUM, CCAR_LGD_D_BASEL_SEG_ID
FROM &RRAP_WRK..target;) by nzcon;
execute(COMMIT;) by nzcon;
quit;

%end;

%mend delta_xrefs;

%delta_xrefs;


proc sql;
connect using NZRRAP as nzcon;
execute(CALL SYSPROC.ADMIN_CMD (%nrbquote('RUNSTATS ON TABLE &RRAP_DB..DT4_SEGMENT_XREF on KEY COLUMNS and INDEXES ALL'));) by nzcon;
execute(COMMIT;) by nzcon;
quit;

