***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0400_DT4_RT18_EST_CCAR_VARS.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RT18_EST_CCAR_VARS
*  
*  Purpose: Derive CCAR variables at the account level to be used to recreate the Expected Results file
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  		  
*
*	Change Log:
*	2021-09-21: Hadi Dimashkieh - Initial Development
*	2023-03-14: Hadi Dimashkieh - Changed reference to pd curve table 
*	2023-04-17: Hadi Dimashkieh - Added filter to PD Curve table RRMSS-2084
*	2023-09-06: Hadi Dimashkieh - Convert job to run monthly.
***************************************************************************************************************************;

%rrap_dt4_autoexec(FREQ=MONTH);


proc sql noprint;
select tm_lvl_end_dt format=date9. , tm_lvl_end_dt format=yymmn6. into :mth_end_dt, :yrmth
from nzrrap.tm_dim where tm_lvl='Month' and tm_id = &mth_tm_id.;
quit;
%put &mth_end_dt. &yrmth.;

***********************************************************************************************************************************;
proc sql;
connect using db2rrap as dbcon;
create table INST_FACT_ACCTS as select * from connection to dbcon(
	SELECT basel_acct_id, mth_tm_id FROM &RRAP_DB2..BASEL_ANALYTCL_BL_INSTRMNT_FACT where mth_tm_id in (&mth_tm_id.,&mth_tm_id.-40));
quit;

proc sql;
connect using nzuser as nzcon;
execute(drop table &RRAP_WRK..INST_FACT_ACCTS if exists;) by nzcon;
execute(commit;) by nzcon;
quit;

proc append base=nzuser.INST_FACT_ACCTS(BULKLOAD=YES BL_METHOD=CLILOAD) data=INST_FACT_ACCTS force nowarn; run;

***********************************************************************************************************************************;
%let instr_fact_columns =
SRC_SYS_CD, CCAR_BASEL_PRD_TP_NM, OS_BAL_AMT, 
GENL_LEDGER_BALCNG_ADJ_AMT, PIT_STAT_CD, CONSM_PRD_TREATMNT_CD, TRNST_EXCLSN_F, prd_id,
ADJUSTED_OS_BAL_AMT, UNADJUSTED_ADD_ON_BAL_AMT,
PD_FINAL_RPTG_RTO ,LGD_FINAL_RPTG_RTO, EAD_FINAL_RPTG_RTO, PD_BAND, 
BEFORE_ZERO_NET_UNDRAWN_AMT, AF_ZERO_NET_UNDRAWN_AMT, DLGD_RPTG_RTO
,LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, PD_BASEL_SEG_NUM, 
PD_BASEL_SEG_ID, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID ,INSUR_F, SCRTY_TP_DESC, DLGD_F, CMHC_F, TRANSACTOR_FLAG_QRR, TOT_EXPSR_ABOVE_1500K_LMT_F;

%let instr_fact_filter =
CONSM_PRD_TREATMNT_CD='A' and SML_BUS_F='N' and PIT_STAT_CD in ('CUR','DEF')
and TRNST_EXCLSN_F='N' and (pd_basel_seg_num is not null and lgd_basel_seg_num is not NULL);
***********************************************************************************************************************************;

proc sql;
connect using db2rrap as dbcon;
create table RT18_EST_KS_INST_FACT_CURR as select * from connection to dbcon(
	SELECT basel_acct_id, mth_tm_id, &instr_fact_columns. , LTV_PERCENTAGE
	FROM &RRAP_DB2..BASEL_ANALYTCL_BL_INSTRMNT_FACT where  SRC_SYS_CD in ('KS') and mth_tm_id in (&mth_tm_id.) and BASEL_ACCT_ID <> -1 and &instr_fact_filter. ;);
quit;


proc sql;
connect using nzrrap as nzcon;
execute(drop table &RRAP_WRK..RT18_EST_KS_INST_FACT_CURR if exists;) by nzcon;
execute(commit;) by nzcon;
quit;

proc append base=nzuser.RT18_EST_KS_INST_FACT_CURR(BULKLOAD=YES BL_METHOD=CLILOAD) data=RT18_EST_KS_INST_FACT_CURR force nowarn; run;

***********************************************************************************************************************************;

proc sql;
connect using nzuser as nzcon;
execute(drop table &RRAP_WRK..RT18_EST_00 if exists;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
connect using nzuser as nzcon;
execute(
create table &RRAP_WRK..RT18_EST_00 as (
select 	b.basel_acct_id, 
				(case when b.final_ecl_stage = 1 then b.final_ecl_cad_drawn else 0 end) as ECL_Drawn_1,
				(case when b.final_ecl_stage = 2 then b.final_ecl_cad_drawn else 0 end) as ECL_Drawn_2,
				(case when b.final_ecl_stage = 3 then b.final_ecl_cad_drawn else 0 end) as ECL_Drawn_3,				
				(case when b.final_ecl_stage = 1 then b.final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_1,
				(case when b.final_ecl_stage = 2 then b.final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_2,
				(case when b.final_ecl_stage = 3 then b.final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_3,
				(case when b.final_ecl_stage = 1 then b.final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_1,
				(case when b.final_ecl_stage = 2 then b.final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_2,
				(case when b.final_ecl_stage = 3 then b.final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_3
 FROM &RRAP_DB..basel_ifrs9_ecl_profile_fact as b INNER JOIN &RRAP_WRK..INST_FACT_ACCTS as A
 ON b.basel_acct_id = a.basel_acct_id and a.mth_tm_id-40 = b.mth_tm_id and a.mth_tm_id = &mth_tm_id.
) WITH DATA DISTRIBUTE BY HASH (BASEL_ACCT_ID)) by nzcon;
execute(commit;) by nzcon;
quit;



proc sql;
connect using nzuser as nzcon;
execute(drop table &RRAP_WRK..RT18_EST_01 if exists;) by nzcon;
execute(commit;) by nzcon;
quit;


proc sql;
connect using nzuser as nzcon;
execute(
create table &RRAP_WRK..RT18_EST_01 as (
SELECT  	 
 a.basel_acct_id
,a.MTH_TM_ID
,a.SRC_SYS_CD
,a.ADJUSTED_OS_BAL_AMT
,a.CCAR_BASEL_PRD_TP_NM
,a.EAD_FINAL_RPTG_RTO
,a.LGD_FINAL_RPTG_RTO
,a.PD_BAND
,a.PD_FINAL_RPTG_RTO
,a.UNADJUSTED_ADD_ON_BAL_AMT
,a.BEFORE_ZERO_NET_UNDRAWN_AMT
,case 
	when a.pd_band = 26 then 0
	else a.AF_ZERO_NET_UNDRAWN_AMT
 end as AF_ZERO_NET_UNDRAWN_AMT
,a.DLGD_RPTG_RTO 
,a.DLGD_F
,a.CMHC_F, a.TRANSACTOR_FLAG_QRR, a.TOT_EXPSR_ABOVE_1500K_LMT_F
,'CURR' AS PERIOD_IND
,b.ECL_Drawn_3
,b.ECL_Undrawn_3
,b.ECL_Drawn_PostSec_3

,a.LGD_BASEL_SEG_NUM, a.EAD_BASEL_SEG_NUM, a.PD_BASEL_SEG_NUM, a.PD_BASEL_SEG_ID, a.LGD_BASEL_SEG_ID, a.EAD_BASEL_SEG_ID , a.LTV_PERCENTAGE
,case 
	when A.SRC_SYS_CD = 'KS' AND A.INSUR_F='YES' THEN 'YES'
	when A.SRC_SYS_CD in ('MOR','TNG-MOR') AND A.SCRTY_TP_DESC='Insured' THEN 'YES' 
	ELSE 'NO' end as INSURER_FLAG 
FROM 
(
	SELECT basel_acct_id, mth_tm_id, &instr_fact_columns. , LTV_PERCENTAGE
	FROM &RRAP_WRK..RT18_EST_KS_INST_FACT_CURR
	UNION ALL 
	SELECT basel_acct_id, mth_tm_id, &instr_fact_columns. , NULL as LTV_PERCENTAGE
	FROM &RRAP_DB..BASEL_PSNL_LN_ANL_BL_INST_FACT WHERE mth_tm_id = &mth_tm_id. and BASEL_ACCT_ID <> -1 and &instr_fact_filter.
	UNION ALL 
	SELECT y.basel_acct_id, x.mth_tm_id, &instr_fact_columns. , LTV_PERCENTAGE
	FROM &MOR_DB..BASEL_ANLYT_BL_INST_FCT_BNS_DLGD x, (select basel_acct_id, mort_num, mth_tm_id from &RRAP_DB..DT4_RPTG_DRVD_VARS) y 
	WHERE x.mort_num=y.mort_num AND x.mth_tm_id = y.mth_tm_id and x.mth_tm_id = &mth_tm_id. and y.BASEL_ACCT_ID <> -1 and &instr_fact_filter.
	UNION ALL 
	SELECT y.basel_acct_id, x.mth_tm_id, &instr_fact_columns. , LTV_PERCENTAGE
	FROM &MOR_DB..BASEL_ANLYT_BL_INST_FCT_TNG_DLGD x, (select basel_acct_id, mort_num, mth_tm_id from &RRAP_DB..DT4_RPTG_DRVD_VARS) y 
	WHERE x.mort_num=y.mort_num AND x.mth_tm_id = y.mth_tm_id and x.mth_tm_id = &mth_tm_id. and y.BASEL_ACCT_ID <> -1 and &instr_fact_filter.
) a LEFT JOIN 
&RRAP_WRK..RT18_EST_00 b
on a.basel_acct_id = b.basel_acct_id 
/*where &instr_fact_filter.*/
) WITH DATA DISTRIBUTE BY HASH (BASEL_ACCT_ID)) by nzcon;
execute(commit;) by nzcon;
quit;





***********************************************************************************************************************************;

 
proc sql;
connect using nzrrap as nzcon;
create table ECL_PREV as select * from connection to nzcon(
 SELECT
	a.acct_num, 'PREV' AS PERIOD_IND, a.BASEL_ACCT_ID
	,(case when a.final_ecl_stage = 3 then a.final_ecl_cad_drawn else 0 end) as ECL_Drawn_3				
	,(case when a.final_ecl_stage = 3 then a.final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_3
	,(case when a.final_ecl_stage = 3 then a.final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_3
FROM
	&RRAP_DB..basel_ifrs9_ecl_profile_fact a
INNER JOIN &RRAP_WRK..INST_FACT_ACCTS b ON
	a.mth_tm_id = b.mth_tm_id
	AND a.BASEL_ACCT_ID = b.BASEL_ACCT_ID
INNER JOIN &RRAP_DB..BASEL_ACCT_DIM c ON
	a.acct_num = c.acct_num
	AND b.basel_acct_id = c.basel_acct_id
LEFT JOIN 
	(SELECT bb.acct_num FROM &RRAP_WRK..INST_FACT_ACCTS aa
		INNER JOIN &RRAP_DB..BASEL_ACCT_DIM bb ON
		aa.basel_acct_id = bb.basel_acct_id
		WHERE aa.MTH_TM_ID = &mth_tm_id.
	 ) d ON a.acct_num = d.acct_num
WHERE
	a.mth_tm_id = &mth_tm_id.-40 AND d.acct_num IS NULL ;);
quit;


proc sql;
connect using db2rrap as dbcon;
create table prev_accts_01 as select * from connection to dbcon(
SELECT  	 
 a.basel_acct_id
,a.MTH_TM_ID
,a.SRC_SYS_CD
/*,a.ADJUSTED_OS_BAL_AMT*/
,a.CCAR_BASEL_PRD_TP_NM
,a.EAD_FINAL_RPTG_RTO
,a.LGD_FINAL_RPTG_RTO
,a.PD_BAND
,a.PD_FINAL_RPTG_RTO
,a.UNADJUSTED_ADD_ON_BAL_AMT
,a.BEFORE_ZERO_NET_UNDRAWN_AMT
/*,a.AF_ZERO_NET_UNDRAWN_AMT*/
,a.DLGD_RPTG_RTO 
,a.DLGD_F
,a.CMHC_F, a.TRANSACTOR_FLAG_QRR, a.TOT_EXPSR_ABOVE_1500K_LMT_F
,a.LGD_BASEL_SEG_NUM, a.EAD_BASEL_SEG_NUM, a.PD_BASEL_SEG_NUM, a.PD_BASEL_SEG_ID, a.LGD_BASEL_SEG_ID, a.EAD_BASEL_SEG_ID , a.LTV_PERCENTAGE
,case 
	when A.SRC_SYS_CD = 'KS' AND A.INSUR_F='YES' THEN 'YES'
	when A.SRC_SYS_CD in ('MOR','TNG-MOR') AND A.SCRTY_TP_DESC='Insured' THEN 'YES' 
	ELSE 'NO' end as INSURER_FLAG 
FROM &RRAP_DB2..BASEL_ANALYTCL_BL_INSTRMNT_FACT a
	WHERE mth_tm_id in (&mth_tm_id.-40)
	and CONSM_PRD_TREATMNT_CD='A' and SML_BUS_F='N' and PIT_STAT_CD in ('CUR','DEF')
and TRNST_EXCLSN_F='N' and pd_basel_seg_num is not null and lgd_basel_seg_num is not NULL
;);
quit;

proc sql;
create table prev_accts as
	SELECT  	 
	 a.basel_acct_id
	,&mth_tm_id. as MTH_TM_ID
	,a.SRC_SYS_CD
	,0 as ADJUSTED_OS_BAL_AMT
	,a.CCAR_BASEL_PRD_TP_NM
	,a.EAD_FINAL_RPTG_RTO
	,a.LGD_FINAL_RPTG_RTO
	,a.PD_BAND
	,a.PD_FINAL_RPTG_RTO
	,a.UNADJUSTED_ADD_ON_BAL_AMT
	,a.BEFORE_ZERO_NET_UNDRAWN_AMT
	,0 as AF_ZERO_NET_UNDRAWN_AMT
	,a.DLGD_RPTG_RTO
	,a.DLGD_F
	,a.CMHC_F, a.TRANSACTOR_FLAG_QRR, a.TOT_EXPSR_ABOVE_1500K_LMT_F

	,a.LGD_BASEL_SEG_NUM, a.EAD_BASEL_SEG_NUM, a.PD_BASEL_SEG_NUM, a.PD_BASEL_SEG_ID, a.LGD_BASEL_SEG_ID, a.EAD_BASEL_SEG_ID , a.LTV_PERCENTAGE, a.INSURER_FLAG

	,'PREV' AS PERIOD_IND
	,b.ECL_Drawn_3				
	,b.ECL_Undrawn_3
	,b.ECL_Drawn_PostSec_3 
	FROM prev_accts_01 a, ECL_PREV b
		WHERE a.basel_acct_id = b.basel_acct_id and a.basel_acct_id in (select basel_acct_id FROM ECL_PREV ) ;
quit;

proc append base=nzuser.RT18_EST_01(BULKLOAD=YES BL_METHOD=CLILOAD) data=prev_accts force; run;

***********************************************************************************************************************************;

***********************************************************************************************************************************;



proc sql;
connect using db2rrap as dbcon;
create table Securitization as select * from connection to dbcon(
WITH a AS (
	SELECT
		AA.basel_acct_id,
		CASE
			WHEN SRC_SYS_CD = 'KS' THEN (OS_BAL_AMT + GENL_LEDGER_BALCNG_ADJ_AMT)
			WHEN SRC_SYS_CD = 'SPL' AND PIT_STAT_CD IN ('CUR') AND CONSM_PRD_TREATMNT_CD = 'A' AND TRNST_EXCLSN_F = 'N' AND prd_id IN ('S09', 'S10') THEN (OS_BAL_AMT + GENL_LEDGER_BALCNG_ADJ_AMT)
			ELSE OS_BAL_AMT
		END AS expsr_drawn_prior_secur
	FROM
		&RRAP_DB2..BASEL_ANALYTCL_BL_INSTRMNT_FACT AA
	WHERE mth_tm_id in (&mth_tm_id.) AND BASEL_ACCT_ID <> -1 and &instr_fact_filter. AND 
	(	(SRC_SYS_CD = 'KS' 
			AND (CCAR_BASEL_PRD_TP_NM LIKE 'REV_CON_CC%'
				OR CCAR_BASEL_PRD_TP_NM LIKE 'OTHER_CON_CC%'
				OR CCAR_BASEL_PRD_TP_NM LIKE 'REV_CON_CL%'
				OR CCAR_BASEL_PRD_TP_NM LIKE 'OTHER_CON_CL%')
			AND OS_BAL_AMT > 0)
		OR (SRC_SYS_CD = 'SPL'
			AND (CCAR_BASEL_PRD_TP_NM LIKE 'ITL_AUTO_REG%' OR CCAR_BASEL_PRD_TP_NM LIKE 'ITL_AUTO_RS%')
				AND ADJUSTED_OS_BAL_AMT >= 0)   )
	)
, b AS (		
	SELECT
		cc.basel_acct_id,
		CASE
			WHEN CCAR_BASEL_PRD_TP_NM LIKE 'REV_CON_CC%' OR CCAR_BASEL_PRD_TP_NM LIKE 'OTHER_CON_CC%' THEN 'Trillium (CC)'
				WHEN CCAR_BASEL_PRD_TP_NM LIKE 'REV_CON_CL%' OR CCAR_BASEL_PRD_TP_NM LIKE 'OTHER_CON_CL%' THEN 'Halifax (CL)'
				WHEN CCAR_BASEL_PRD_TP_NM LIKE 'ITL_AUTO_REG%' OR CCAR_BASEL_PRD_TP_NM LIKE 'ITL_AUTO_RS%' THEN 'START'
				ELSE NULL 
			END AS SECRTZTN_FLAG
		FROM
			&RRAP_DB2..BASEL_ANALYTCL_BL_INSTRMNT_FACT cc
		WHERE mth_tm_id IN (&mth_tm_id.) AND BASEL_ACCT_ID <> -1 AND &instr_fact_filter. AND 
			ADJUSTED_OS_BAL_AMT <> BEFORE_ZERO_NET_DRAWN_AMT
			AND  (
				((SRC_SYS_CD = 'KS')
				AND (CCAR_BASEL_PRD_TP_NM LIKE 'REV_CON_CC%'
					OR CCAR_BASEL_PRD_TP_NM LIKE 'OTHER_CON_CC%'
					OR CCAR_BASEL_PRD_TP_NM LIKE 'REV_CON_CL%'
					OR CCAR_BASEL_PRD_TP_NM LIKE 'OTHER_CON_CL%'))
			OR ((SRC_SYS_CD = 'SPL')
				AND (CCAR_BASEL_PRD_TP_NM LIKE 'ITL_AUTO_REG%' OR CCAR_BASEL_PRD_TP_NM LIKE 'ITL_AUTO_RS%')) )
			)
SELECT COALESCE(a.basel_acct_id,b.basel_acct_id) AS BASEL_ACCT_ID, a.expsr_drawn_prior_secur, b.SECRTZTN_FLAG
FROM a FULL OUTER JOIN B 
ON a.basel_acct_id = b.basel_acct_id;);
quit;

***********************************************************************************************************************************;

proc sql;
connect using nzuser as nzcon;
execute(drop table &RRAP_WRK..RT18_EST_SECURITIZATION if exists;) by nzcon;
execute(commit;) by nzcon;
quit;

proc append base=nzuser.RT18_EST_SECURITIZATION(BULKLOAD=YES BL_METHOD=CLILOAD) data=Securitization force nowarn; run;

***********************************************************************************************************************************;

proc sql;
connect using nzuser as nzcon;
execute(drop table &RRAP_WRK..RT18_EST_02 if exists;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
connect using nzuser as nzcon;
execute(create table &RRAP_WRK..RT18_EST_02 as 
(SELECT
	 A.*
	,REGEXP_REPLACE(TRIM(A.CCAR_BASEL_PRD_TP_NM), '\_\d+','',1,0,'i') as BASEL_PRODUCT_TYPE

	,CASE WHEN A.ADJUSTED_OS_BAL_AMT >0 THEN A.ADJUSTED_OS_BAL_AMT ELSE 0 END AS EXPOSURE_DRAWN
	,CASE WHEN A.PD_BAND <>'26' AND A.PERIOD_IND='CURR' THEN AF_ZERO_NET_UNDRAWN_AMT ELSE 0 END AS UNDRAWN
	,CASE
		WHEN B.SECRTZTN_FLAG IS NOT NULL THEN B.EXPSR_DRAWN_PRIOR_SECUR
		WHEN B.SECRTZTN_FLAG IS NULL AND A.ADJUSTED_OS_BAL_AMT >0 THEN A.ADJUSTED_OS_BAL_AMT
		ELSE NULL
	 END AS EXPSR_DRAWN_PRIOR_SECUR
FROM
	&RRAP_WRK..RT18_EST_01 A
LEFT JOIN &RRAP_WRK..RT18_EST_SECURITIZATION B ON
	A.BASEL_ACCT_ID = B.BASEL_ACCT_ID
) WITH DATA DISTRIBUTE BY HASH (BASEL_ACCT_ID);) by nzcon;
execute(commit;) by nzcon;
quit;

***********************************************************************************************************************************;
***********************************************************************************************************************************;
***********************************************************************************************************************************;

*** Convert job to monthly. Derive last quarter end mth_tm_id and use it to query POST_ECL_PROVISIONS.;
data last_qtr_end;
	set nzrrap.tm_dim;
	where tm_id = &mth_tm_id.;
	format mth_end_dt_qtr mth_end_dt_curqtr tm_lvl_end_dt date9.;
	mth_end_dt_qtr= intnx('YEAR',intnx('QTR.2',tm_lvl_end_dt,-1,'e'),0,'s');
	mth_end_dt_curqtr= intnx('YEAR',intnx('QTR.2',tm_lvl_end_dt,0,'e'),0,'s');

	if tm_lvl_end_dt = mth_end_dt_curqtr then mth_end_dt_qtr = mth_end_dt_curqtr;
	keep mth_end_dt_qtr; * mth_end_dt_curqtr tm_lvl_end_dt;
run;

proc sql noprint;
	select tm_id into :qtrend_mth_tm_id
	from nzrrap.tm_dim t, last_qtr_end a
	where a.mth_end_dt_qtr = t.tm_lvl_end_dt and t.tm_lvl = 'Month';
quit;

%put &qtrend_mth_tm_id.;

proc sql;
connect using nzuser as nzcon;
execute(drop table &RRAP_WRK..RT18_EST_03 if exists;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
connect using nzuser as nzcon;
execute(create table &RRAP_WRK..RT18_EST_03 as /*exposer_02*/
(
WITH post_adj_values AS (
SELECT 	 
	 a.provisions_product
	,a.basel_product_type
	,a.security_type_desc
	,b.ecl_drawn_3_adj
	,b.ecl_undrawn_3_adj
FROM &RRAP_DB..CCAR_PROVISIONS_MAPPING a 
	LEFT JOIN &RRAP_DB..POST_ECL_PROVISIONS b
ON upper(a.SECURITY_TYPE_DESC) = upper(b.SECURITY_TYPE_DESC) AND upper(a.PROVISIONS_PRODUCT) = upper(b.PRODUCTS) 
WHERE &yrmth. between CAST(a.EFF_FROM_YR_MTH AS integer) AND cast(a.EFF_TO_YR_MTH AS integer)
	AND b.mth_tm_id = &qtrend_mth_tm_id.
)	
	
SELECT
	t1.*
	,CASE WHEN t3.sum_of_ecl_drawn_3 = 0 THEN NULL 
	 	ELSE ((t2.ecl_drawn_3_adj / t3.sum_of_ecl_drawn_3) * t1.ecl_drawn_3) END  AS ecl_post_adj_drawn_3  
		/*'ecl post adj drawn 3'n,*/

	,CASE WHEN t3.sum_of_ecl_undrawn_3 = 0 THEN NULL 
     	ELSE ((t2.ecl_undrawn_3_adj / t3.sum_of_ecl_undrawn_3)* t1.ecl_undrawn_3) END AS ecl_post_adj_undrawn_3 
/*--'ecl post adj undrawn 3'n, */

    ,CASE WHEN t3.sum_of_ecl_drawn_3 = 0 OR t1.ecl_drawn_3 = 0 THEN NULL 
     	ELSE ((((t2.ecl_drawn_3_adj / t3.sum_of_ecl_drawn_3) * t1.ecl_drawn_3)/ t1.ecl_drawn_3)* t1.ecl_drawn_postsec_3) END AS ecl_post_adj_drawn_postsec_3  
/*--'ecl post adj drawn postsec 3'n*/

FROM &RRAP_WRK..rt18_est_02 t1
LEFT JOIN (post_adj_values t2 
	
			LEFT JOIN ( /*Pre_Adj_Sum as t3*/
				SELECT
					t22.provisions_product,
					sum(t11.exposure_drawn) exposure_drawn,
					sum(t11.undrawn) undrawn,
					sum(t11.ecl_drawn_3) sum_of_ecl_drawn_3,
					sum(t11.ecl_undrawn_3) sum_of_ecl_undrawn_3,
					sum(t11.ecl_drawn_postsec_3) sum_of_ecl_drawn_postsec_3
				FROM
					&RRAP_WRK..rt18_est_02 t11
				LEFT JOIN post_adj_values t22 ON
					t11.basel_product_type = t22.basel_product_type
				GROUP BY
					provisions_product) t3 

			ON (t2.provisions_product = t3.provisions_product)

		) ON t1.basel_product_type = t2.basel_product_type

) WITH DATA DISTRIBUTE BY HASH (BASEL_ACCT_ID);) by nzcon;
execute(commit;) by nzcon;
quit;

***********************************************************************************************************************************;
***********************************************************************************************************************************;




proc sql;
	create table student_lines as
	select 
		
		a.basel_acct_id, a.pd_band
		,'Student Lines' as Product_Type
		,b.ECL_Drawn_1_Adj,b.ECL_Drawn_2_Adj,b.ECL_Drawn_3_Adj

		,case 
			when a.pd_band eq '26' then sum(c.exposure_drawn)  
			when a.pd_band ne '26' then sum(d.exposure_drawn)
			else .
		 end format=22.2 AS SUM_EXPSR_DRAWN_SPSP 

		,case 
			when a.pd_band eq '26' then sum(c.Undrawn)  
			when a.pd_band ne '26' then sum(d.Undrawn)
			else .
		 end format=22.2 AS SUM_EXPSR_UNDRAWN_SPSP 

		,case 
			when a.pd_band eq '26' then sum(c.exposure_drawn) + sum(c.Undrawn) 
			when a.pd_band ne '26' then sum(d.exposure_drawn) + sum(d.Undrawn) 
			else .
		 end format=22.2 AS TOTAL_SPSP_PRE 
		 
		,case 
			when a.pd_band ne '26' then ((d.exposure_drawn + d.Undrawn) / (sum(d.exposure_drawn) + sum(d.Undrawn))) * b.ECL_Drawn_1_Adj
			else .
		 end format=22.2 AS ECL_POST_ADJ_DRAWN_POSTSEC_1 	

		,case 
			when a.pd_band ne '26' then ((d.exposure_drawn + d.Undrawn) / (sum(d.exposure_drawn) + sum(d.Undrawn))) * b.ECL_Drawn_2_Adj
			else .
		 end format=22.2 AS ECL_POST_ADJ_DRAWN_POSTSEC_2

		,case 
			when a.pd_band eq '26' then ((c.exposure_drawn + c.Undrawn) / (sum(c.exposure_drawn) + sum(c.Undrawn))) * b.ECL_Drawn_3_Adj
			else .
		 end format=22.2 AS ECL_POST_ADJ_DRAWN_POSTSEC_3 


	from nzuser.RT18_EST_02 a 
		left join nzrrap.POST_ECL_PROVISIONS b
			on b.products = 'Student Lines' and b.mth_tm_id = &qtrend_mth_tm_id.
		left join nzuser.RT18_EST_02 c
			on a.basel_acct_id = c.basel_acct_id and c.pd_band eq '26'
		left join nzuser.RT18_EST_02 d
			on a.basel_acct_id = d.basel_acct_id and d.pd_band ne '26'
	where a.basel_product_type in ('OTHER_CON_SLR','OTHER_CON_SLT','REV_CON_SLR');
quit;

proc sql;
connect using nzuser as nzcon;
execute(drop table &RRAP_WRK..RT18_EST_04 if exists;) by nzcon;
execute(commit;) by nzcon;
quit;

data nzuser.RT18_EST_04(BULKLOAD=YES BL_METHOD=CLILOAD);
	set student_lines;
	keep basel_acct_id ECL_POST_ADJ_DRAWN_POSTSEC_3;
run;


/******************************************************************************************************/
proc sql;
connect using nzuser as nzcon;
execute(drop table &RRAP_WRK..RT18_EST_PRD_LKP_PD_CURVE if exists;) by nzcon;
execute(commit;) by nzcon;
quit;
proc sql;
create table /*prodinfo*/ NZUSER.RT18_EST_PRD_LKP_PD_CURVE as 
select a.CCAR_BASEL_PRD_TP_NM
		,b.BASEL_1_ASST_TP_CD as basel_grp
		,a.pd_band
		,a.pd_val as pd_value
		,a.cmhc_f,a.TRANSACTOR_FLAG


	from NZRRAP.BASEL_CCAR_PD_CURVE a , DB2RRAP.BASEL_AIRB_PRD_LKP b 

	WHERE a.mth_end_dt = "&mth_end_dt."d AND (a.CCAR_BASEL_PRD_TP_NM = b.CCAR_BASEL_PRD_TP_NM)
AND ("&yrmth" BETWEEN b.EFF_FROM_YR_MTH AND b.EFF_TO_YR_MTH)
AND (a.TOT_EXPSR_ABOVE_1500K_LMT_F NE 'Y' or missing(a.TOT_EXPSR_ABOVE_1500K_LMT_F))
order by 1,2,3;
quit;


/********************************************************************************************************************/



proc sql;
connect using nzrrap as nzcon;
execute(delete from &RRAP_DB..DT4_RT18_EST_CCAR_VARS where mth_tm_id = &mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
quit;

proc sql;
connect using nzrrap as nzcon;
execute(insert into &RRAP_DB..DT4_RT18_EST_CCAR_VARS  
select 
	 a.BASEL_ACCT_ID
	,a.MTH_TM_ID
	,a.SRC_SYS_CD
	,COALESCE(e.DT4_EXPSR_CL_KEY_VAL,f.DT4_EXPSR_CL_KEY_VAL) AS DT4_EXPSR_CL_KEY_VAL
	,a.CCAR_BASEL_PRD_TP_NM
	,a.BASEL_PRODUCT_TYPE
	,a.PERIOD_IND
	,a.PD_BAND

	,CASE 
 		WHEN REGEXP_LIKE(a.CCAR_BASEL_PRD_TP_NM, 'HELOC|OTHER_CON_CC|OTHER_CON_CL|REV_CON_CC|REV_CON_CL|REV_CON_SLR|OTHER_CON_SLR|OTHER_CON_SLT', 'i' ) = 1 THEN 1
 		ELSE NULL
 	 END AS EAD_INCL
	,c.BASEL_GRP
	,c.Pd_Value
	,d.RW_INSURER

	,coalesce(a.ADJUSTED_OS_BAL_AMT,0) as ADJUSTED_OS_BAL_AMT
	,coalesce(a.UNADJUSTED_ADD_ON_BAL_AMT,0) as UNADJUSTED_ADD_ON_BAL_AMT
	,coalesce(a.BEFORE_ZERO_NET_UNDRAWN_AMT,0) as BEFORE_ZERO_NET_UNDRAWN_AMT
	,coalesce(a.AF_ZERO_NET_UNDRAWN_AMT,0) as AF_ZERO_NET_UNDRAWN_AMT

	,a.PD_FINAL_RPTG_RTO
	,a.LGD_FINAL_RPTG_RTO
	,a.EAD_FINAL_RPTG_RTO

	,a.DLGD_RPTG_RTO
	
	,coalesce(a.EXPOSURE_DRAWN,0) as EXPOSURE_DRAWN
	,coalesce(a.UNDRAWN,0) as UNDRAWN
	,coalesce(a.EXPSR_DRAWN_PRIOR_SECUR,0) as EXPSR_DRAWN_PRIOR_SECUR

	,coalesce(a.ECL_DRAWN_3,0) as ECL_DRAWN_3
	,coalesce(a.ECL_UNDRAWN_3,0) as ECL_UNDRAWN_3
	,coalesce(a.ECL_DRAWN_POSTSEC_3,0) as ECL_DRAWN_POSTSEC_3
	
	,coalesce(a.ECL_POST_ADJ_DRAWN_3,b.ECL_POST_ADJ_DRAWN_POSTSEC_3,0) as ECL_POST_ADJ_DRAWN_3   /*hadi added b.ECL_POST_ADJ_DRAWN_POSTSEC_3 to fill in for student lines*/
	,coalesce(a.ECL_POST_ADJ_UNDRAWN_3,0) as ECL_POST_ADJ_UNDRAWN_3
	,coalesce(a.ECL_POST_ADJ_DRAWN_POSTSEC_3,b.ECL_POST_ADJ_DRAWN_POSTSEC_3,0) as ECL_POST_ADJ_DRAWN_POSTSEC_3 
	
	,now() as INSRT_PROCESS_TMSTMP
	,now() as UPDT_PROCESS_TMSTMP

FROM &RRAP_WRK..RT18_EST_03 a 
	left join &RRAP_WRK..RT18_EST_04 b
		on a.basel_acct_id = b.basel_acct_id
	LEFT JOIN &RRAP_WRK..RT18_EST_PRD_LKP_PD_CURVE c
		ON a.CCAR_BASEL_PRD_TP_NM = c.CCAR_BASEL_PRD_TP_NM AND a.PD_BAND = c.PD_BAND  
				and coalesce(upper(a.CMHC_F),'z') = coalesce(upper(c.CMHC_F),'z') and coalesce(upper(a.TRANSACTOR_FLAG_QRR),'z') = coalesce(upper(c.TRANSACTOR_FLAG),'z')
	LEFT JOIN &RRAP_DB..DT4_RW_INSURER d 
		ON substr(a.BASEL_PRODUCT_TYPE,1,4) = d.name and &yrmth. between cast(d.EFF_FROM_YR_MTH as integer) and cast(d.EFF_TO_YR_MTH as integer)
	LEFT JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS E 	
		ON a.MTH_TM_ID = e.MTH_TM_ID AND a.BASEL_ACCT_ID = e.BASEL_ACCT_ID
	LEFT JOIN &RRAP_DB..DT4_RPTG_DRVD_VARS F 	
		ON a.MTH_TM_ID -40 = f.MTH_TM_ID AND a.BASEL_ACCT_ID = f.BASEL_ACCT_ID and a.PERIOD_IND = 'PREV'
WHERE a.MTH_TM_ID = &mth_tm_id.;) by nzcon;
execute(commit;) by nzcon;
execute(CALL SYSPROC.ADMIN_CMD (%nrbquote('RUNSTATS ON TABLE &RRAP_DB..DT4_RT18_EST_CCAR_VARS on KEY COLUMNS and INDEXES ALL'));) by nzcon;
execute(COMMIT;) by nzcon;
quit;


