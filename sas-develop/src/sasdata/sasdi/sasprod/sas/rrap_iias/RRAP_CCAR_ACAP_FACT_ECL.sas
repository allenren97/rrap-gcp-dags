/* Changes: Feb 2023: Basel III changes */

/*DECLARE THE PATH TO DEFINE AUTOCALL MACRO*/
/*INITIALIZATION OF VARIABLES AND EXECUTE AUTOCALL MACRO*/

%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

%GLOBAL YEARMONTH;
%GLOBAL PRELOAD_DATA_COUNT;
%GLOBAL SRCCOUNT00;
%GLOBAL TGTCOUNT00;
%GLOBAL ALLBANK_COUNT;
%GLOBAL SUBSIDIARY_COUNT;
%LET INPUT00=EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT;
%LET TGT00=EDRRAPT.BASEL_CCAR_EXPSR_FACT;

%macro rrap_db2yearmonth_initialize;
proc sql noprint;
create table _temp_yearmonth as 
select clndr_yr, tm_yr_seq_num, '00' as charmonth, '000000' as yearmonth from 
EDRTLRT.TM_DIM where TM_ID = &MTH_TM_ID;
quit;
/*Convert the integer values to char values as needed*/
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
/*Save the string YYYYMM for the latest MTH_TM_ID fetched from TM_DIM into a 
variable*/
proc sql noprint;
select yearmonth into :yearmonth from _temp_yearmonth;
quit;
%mend rrap_db2yearmonth_initialize;


/*CLEANUP MACRO*/
/*THIS MACRO DELETES ALL TEMPORARY SAS DATASETS LISTED BY THE USER AT THE SPECIFIED PATH ON AIX PLATFORM*/
/*LIBREF SHOULD BE THE LIBREF DECLARED FOR A UNIX PATH IN THE PROGRAM*/
/*DATASETS SHOULD BE THE LIST OF DATASETS SEPARATED BY A SINGLE SPACE*/
%MACRO SAS_DATASET_CLEANUP (LIBREF=, DATASETS=);
/*DELETE TEMPORARY DATASETS*/
PROC DATASETS LIBRARY=&LIBREF;
DELETE &DATASETS;
RUN;
QUIT;
%MEND;

/*Macro for fetching the yearmonth in an integer format*/
%rrap_db2yearmonth_initialize;
%PUT YEARMONTH IS &YEARMONTH;

PROC SQL;
	SELECT TM_LVL_ST_DT FORMAT=YYMMN6. INTO :PREV_YEARMONTH FROM NZRRAP.TM_DIM WHERE TM_ID=&MTH_TM_ID-40 AND TM_LVL='Month';
QUIT;

%PUT &=PREV_YEARMONTH;

/*
data &INPATH..BASEL_SUBSIDIARY_ACL_LKP;
		SET &db..BASEL_SUBSIDIARY_ACL_LKP;
RUN;

data &INPATH..BASEL_CCAR_BUS_AGGRTD_FACT;
		SET &db..BASEL_CCAR_BUS_AGGRTD_FACT;
		WHERE mth_tm_id = (&MTH_TM_ID.);
RUN;
*/

PROC SQL;
	CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain="IIAS_Auth");
	EXECUTE(DROP TABLE IF EXISTS &net_db..RRAP_Rpt_Extract00_ECL_CURR)BY IIASCON;
	EXECUTE(
		CREATE table &net_db..RRAP_Rpt_Extract00_ECL_CURR as 
			(select b.basel_acct_id, 
				'CURR' as PERIOD_IND,
			(case when b.final_ecl_stage = 1 then b.final_ecl_cad_drawn else 0 end) as ECL_Drawn_1,
			(case when b.final_ecl_stage = 2 then b.final_ecl_cad_drawn else 0 end) as ECL_Drawn_2,
			(case when b.final_ecl_stage = 3 then b.final_ecl_cad_drawn else 0 end) as ECL_Drawn_3,				
			(case when b.final_ecl_stage = 1 then b.final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_1,
			(case when b.final_ecl_stage = 2 then b.final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_2,
			(case when b.final_ecl_stage = 3 then b.final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_3,
			(case when b.final_ecl_stage = 1 then b.final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_1,
			(case when b.final_ecl_stage = 2 then b.final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_2,
			(case when b.final_ecl_stage = 3 then b.final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_3
			FROM (SELECT CNTRY_CD,ACCT_NUM,BASEL_ACCT_ID,CPP_ENTITY_FOLIO_CD,CPP_PRD_FOLIO_CD,CPP_QUALI_SUB_CD,CPP_QUANTI_SUB_CD,PIT_STAT_CD,STG3_IND,OS_BAL_AMT,FINAL_ECL_STAGE,FINAL_ECL_CAD,FINAL_ECL_CAD_DRAWN,FINAL_ECL_CAD_UNDRAWN,CRNT_AUTH_LMT_AMT,UNDRAWN_AMT,SCORED_UNSCORED_IND,MTH_TM_ID,SRC_SYS_CD,FINAL_ECL_CAD_DRAWN_POSTSEC FROM
                        (SELECT *,ROWNUMBER() OVER (PARTITION BY CNTRY_CD,ACCT_NUM,BASEL_ACCT_ID,CPP_ENTITY_FOLIO_CD,CPP_PRD_FOLIO_CD,CPP_QUALI_SUB_CD,CPP_QUANTI_SUB_CD,PIT_STAT_CD,STG3_IND,OS_BAL_AMT,FINAL_ECL_STAGE,FINAL_ECL_CAD,FINAL_ECL_CAD_DRAWN,FINAL_ECL_CAD_UNDRAWN,CRNT_AUTH_LMT_AMT,UNDRAWN_AMT,SCORED_UNSCORED_IND,MTH_TM_ID,SRC_SYS_CD,FINAL_ECL_CAD_DRAWN_POSTSEC) AS RN
                        FROM &net_db..BASEL_IFRS9_ECL_PROFILE_FACT WHERE MTH_TM_ID=(&MTH_TM_ID-40)) AS A
                        WHERE RN =1) as b 
            INNER JOIN &net_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT as A
				ON b.basel_acct_id = a.basel_acct_id
			WHERE A.MTH_TM_ID=&MTH_TM_ID AND B.MTH_TM_ID=(&MTH_TM_ID-40))WITH DATA)BY IIASCON;
			
EXECUTE(DROP TABLE IF EXISTS &net_db..RRAP_Rpt_Extract00_ECL_Prev)BY IIASCON;
	EXECUTE(
		CREATE table &net_db..RRAP_Rpt_Extract00_ECL_Prev as 
			(select basel_acct_id,
				'PREV' as PERIOD_IND,
			(case when final_ecl_stage = 1 then final_ecl_cad_drawn else 0 end) as ECL_Drawn_1,
			(case when final_ecl_stage = 2 then final_ecl_cad_drawn else 0 end) as ECL_Drawn_2,
			(case when final_ecl_stage = 3 then final_ecl_cad_drawn else 0 end) as ECL_Drawn_3,				
			(case when final_ecl_stage = 1 then final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_1,
			(case when final_ecl_stage = 2 then final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_2,
			(case when final_ecl_stage = 3 then final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_3,
			(case when final_ecl_stage = 1 then final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_1,
			(case when final_ecl_stage = 2 then final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_2,
			(case when final_ecl_stage = 3 then final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_3
			FROM (SELECT CNTRY_CD,ACCT_NUM,BASEL_ACCT_ID,CPP_ENTITY_FOLIO_CD,CPP_PRD_FOLIO_CD,CPP_QUALI_SUB_CD,CPP_QUANTI_SUB_CD,PIT_STAT_CD,STG3_IND,OS_BAL_AMT,FINAL_ECL_STAGE,FINAL_ECL_CAD,FINAL_ECL_CAD_DRAWN,FINAL_ECL_CAD_UNDRAWN,CRNT_AUTH_LMT_AMT,UNDRAWN_AMT,SCORED_UNSCORED_IND,MTH_TM_ID,SRC_SYS_CD,FINAL_ECL_CAD_DRAWN_POSTSEC FROM
                        (SELECT *,ROWNUMBER() OVER (PARTITION BY CNTRY_CD,ACCT_NUM,BASEL_ACCT_ID,CPP_ENTITY_FOLIO_CD,CPP_PRD_FOLIO_CD,CPP_QUALI_SUB_CD,CPP_QUANTI_SUB_CD,PIT_STAT_CD,STG3_IND,OS_BAL_AMT,FINAL_ECL_STAGE,FINAL_ECL_CAD,FINAL_ECL_CAD_DRAWN,FINAL_ECL_CAD_UNDRAWN,CRNT_AUTH_LMT_AMT,UNDRAWN_AMT,SCORED_UNSCORED_IND,MTH_TM_ID,SRC_SYS_CD,FINAL_ECL_CAD_DRAWN_POSTSEC) AS RN
                        FROM &net_db..BASEL_IFRS9_ECL_PROFILE_FACT WHERE MTH_TM_ID=(&MTH_TM_ID-40)) AS A
                        WHERE RN =1) IFRS 
            WHERE 
				acct_num IN  (select b.acct_num from &net_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT a inner join &net_db..BASEL_ACCT_DIM b
				on a.basel_acct_id = b.basel_acct_id WHERE A.MTH_TM_ID=(&MTH_TM_ID-40)) and 
				acct_num not IN (select b.acct_num from &net_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT a inner join &net_db..BASEL_ACCT_DIM b
				on a.basel_acct_id = b.basel_acct_id WHERE A.MTH_TM_ID=&MTH_TM_ID) and 
				basel_acct_id in (select basel_acct_id from &net_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT WHERE MTH_TM_ID=(&MTH_TM_ID-40)) AND IFRS.MTH_TM_ID=(&MTH_TM_ID-40))WITH DATA)BY IIASCON;
	DISCONNECT FROM IIASCON;
QUIT;
	
PROC SQL;
	CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain="IIAS_Auth");
    EXECUTE(DROP TABLE IF EXISTS &net_db..RRAP_Rpt_Extract00_ECL)BY IIASCON;
	EXECUTE(
		CREATE table &net_db..RRAP_Rpt_Extract00_ECL as 
			(select a.BASEL_ACCT_ID ,
				a.ADJUSTED_OS_BAL_AMT,
				a.CCAR_BASEL_PRD_TP_NM,
				a.CONS_DFT_MTH_CNT,
				a.EAD_BASEL_SEG_ID,
				(case when a.EAD_BASEL_SEG_NUM IS NULL then -1 else a.EAD_BASEL_SEG_NUM end) as EAD_BASEL_SEG_NUM,
				a.EAD_FLRD_RPTG_RTO,
				a.LEGAL_ENTITY,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.LGD_BASEL_SEG_ID 
					when a.SRC_SYS_CD in ('SPL')  and a.LGD_BASEL_SEG_ID IS NULL THEN -1
					else a.LGD_BASEL_SEG_ID end) as  LGD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') and a.LGD_BASEL_SEG_NUM IS NULL then -1
					when a.SRC_SYS_CD in ('SPL') and a.LGD_BASEL_SEG_NUM IS NULL then -1
					else a.LGD_BASEL_SEG_NUM end) as LGD_BASEL_SEG_NUM,
				a.LGD_FLRD_RPTG_RTO,
				a.MTH_TM_ID,
				a.NCR_EXPSR_CL_KEY_VAL,
				COALESCE(c.pd_band,A.pd_band) AS pd_band,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.PD_BASEL_SEG_ID
					else a.PD_BASEL_SEG_ID end) as PD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.PD_BASEL_SEG_NUM
					else a.PD_BASEL_SEG_NUM end) as PD_BASEL_SEG_NUM,
				a.PD_FINAL_RPTG_RTO,
				a.SRC_SYS_CD,
				a.UNADJUSTED_ADD_ON_BAL_AMT,
				a.BEFORE_ZERO_NET_UNDRAWN_AMT,
				a.AF_ZERO_NET_UNDRAWN_AMT,
				a.INTR_ACCR_AMT,
				a.UNQ_ACCT_ID, A.SCRTY_TP_DESC ,A.BASEL_PRD_TP_CD,
				(case when A.SRC_SYS_CD = 'KS' AND A.INSUR_F='YES' THEN 'YES'
					when A.SRC_SYS_CD in ('MOR','TNG-MOR') AND A.SCRTY_TP_DESC='Insured' THEN 'YES' ELSE 'NO' end ) as INSURER_FLAG 
				,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, 
				a.DLGD_F, 
				a.LTV_BUCKET AS LTV_PERCENTAGE, /*adding for Basel III */				
				a.TRNST_NUM, 					/*adding for bcar_ccar combine file */
				a.EGL_DEPRTMNT, 				/*adding for bcar_ccar combine file */
				a.PRD_ID, 						/*adding for bcar_ccar combine file */
				a.RNTL_PRPTY_F, 				/*adding for Basel III */
				a.CURRENCY_MISMATCH_F, 			/*adding for Basel III */
				a.TOT_EXPSR_ABOVE_1500K_LMT_F, 	/*adding for Basel III */
				a.ORIG_AMT_LOAN, 				/*adding for Basel III */
				a.TRANSACTOR_FLAG_QRR, 				/*adding for Basel III */
				a.PD_90_DAY_F,					/*adding for Basel III */
				a.UNINSURED_FLRD_LGD_RTO,			/*adding for Basel III */
				a.UNINSURED_DLGD_RTO,			/*adding for Basel III */
				a.CLP_FLAG,
				b.PERIOD_IND,					
				b.ECL_Drawn_1,
				b.ECL_Drawn_2,
				b.ECL_Drawn_3,				
				b.ECL_Undrawn_1,
				b.ECL_Undrawn_2,
				b.ECL_Undrawn_3,
				b.ECL_Drawn_PostSec_1,
				b.ECL_Drawn_PostSec_2,
				b.ECL_Drawn_PostSec_3
FROM &net_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT as A LEFT JOIN &net_db..RRAP_Rpt_Extract00_ECL_CURR as B
 ON a.basel_acct_id = b.basel_acct_id
 LEFT JOIN &net_db..PD_BAND_DIM c on c.NCR_EXPSR_CL_KEY_VAL='0599' AND a.TOT_EXPSR_ABOVE_1500K_LMT_F = 'Y' and a.ASST_CL_NUM <> 1 and a.PD_FLRD_RPTG_RTO between c.PD_MIN_VAL and c.PD_MAX_VAL
 AND &YEARMONTH. between cast(c.eff_from_yr_mth AS integer) and cast(c.eff_to_yr_mth AS integer)
		where a.SRC_SYS_CD in ('KS','SPL','TNG-MOR', 'MOR') 
			and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N'
  			and a.PIT_STAT_CD in ('CUR','DEF')
			and a.TRNST_EXCLSN_F='N'
			and a.DLGD_F='N'
			AND A.MTH_TM_ID=&MTH_TM_ID
			AND (PD_BASEL_SEG_NUM IS NOT NULL AND LGD_BASEL_SEG_NUM IS NOT NULL))WITH DATA)BY IIASCON;
DISCONNECT FROM IIASCON;
QUIT;

PROC SQL;
	CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain="IIAS_Auth");
	EXECUTE(
		UPDATE &net_db..RRAP_Rpt_Extract00_ECL
			SET PERIOD_IND = 'CURR'
				WHERE PERIOD_IND IS NULL
					)BY IIASCON;
	DISCONNECT FROM IIASCON;
QUIT;
	
PROC SQL;
	CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain="IIAS_Auth");
	EXECUTE(INSERT INTO &net_db..RRAP_Rpt_Extract00_ECL
	select 	a.BASEL_ACCT_ID ,
				a.ADJUSTED_OS_BAL_AMT,
				a.CCAR_BASEL_PRD_TP_NM,
				a.CONS_DFT_MTH_CNT,
				a.EAD_BASEL_SEG_ID,
				(case when a.EAD_BASEL_SEG_NUM IS NULL then -1 else a.EAD_BASEL_SEG_NUM end ) as EAD_BASEL_SEG_NUM,
				a.EAD_FLRD_RPTG_RTO,
				a.LEGAL_ENTITY,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.LGD_BASEL_SEG_ID 
					when a.SRC_SYS_CD in ('SPL')  and a.LGD_BASEL_SEG_ID IS NULL THEN -1
					else a.LGD_BASEL_SEG_ID end) as  LGD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') and a.LGD_BASEL_SEG_NUM IS NULL then -1
					when a.SRC_SYS_CD in ('SPL') and a.LGD_BASEL_SEG_NUM IS NULL then -1
					else a.LGD_BASEL_SEG_NUM end) as LGD_BASEL_SEG_NUM,
				a.LGD_FLRD_RPTG_RTO,
				a.MTH_TM_ID ,
				a.NCR_EXPSR_CL_KEY_VAL,
				COALESCE(c.pd_band,A.pd_band) AS pd_band,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.PD_BASEL_SEG_ID
					else a.PD_BASEL_SEG_ID end) as PD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.PD_BASEL_SEG_NUM
					else a.PD_BASEL_SEG_NUM end) as PD_BASEL_SEG_NUM,
				a.PD_FINAL_RPTG_RTO,
				a.SRC_SYS_CD,
				a.UNADJUSTED_ADD_ON_BAL_AMT,
				a.BEFORE_ZERO_NET_UNDRAWN_AMT,
				a.AF_ZERO_NET_UNDRAWN_AMT,
				a.INTR_ACCR_AMT,
				a.UNQ_ACCT_ID, A.SCRTY_TP_DESC ,A.BASEL_PRD_TP_CD,
				(case when A.SRC_SYS_CD = 'KS' AND A.INSUR_F='YES' THEN 'YES'
					when A.SRC_SYS_CD in ('MOR','TNG-MOR') AND A.SCRTY_TP_DESC='Insured' THEN 'YES' ELSE 'NO' end ) as INSURER_FLAG 
				,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, 
				a.LTV_BUCKET AS LTV_PERCENTAGE,  		/*adding for Basel III */
				a.TRNST_NUM, 							/*adding for bcar_ccar combine file */
				a.EGL_DEPRTMNT, 						/*adding for bcar_ccar combine file */
				a.PRD_ID,       						/*adding for bcar_ccar combine file */
				a.RNTL_PRPTY_F,							/*adding for Basel III */
				a.CURRENCY_MISMATCH_F, 					/*adding for Basel III */
				a.TOT_EXPSR_ABOVE_1500K_LMT_F, 			/*adding for Basel III */
				a.ORIG_AMT_LOAN, 						/*adding for Basel III */
				a.TRANSACTOR_FLAG_QRR, 						/*adding for Basel III */
				a.PD_90_DAY_F,							/*adding for Basel III */
				a.UNINSURED_FLRD_LGD_RTO,					/*adding for Basel III */
				a.UNINSURED_DLGD_RTO,					/*adding for Basel III */
				a.CLP_FLAG,
				b.PERIOD_IND,
				b.ECL_Drawn_1,
				b.ECL_Drawn_2,
				b.ECL_Drawn_3,				
				b.ECL_Undrawn_1,
				b.ECL_Undrawn_2,
				b.ECL_Undrawn_3,
				b.ECL_Drawn_PostSec_1,
				b.ECL_Drawn_PostSec_2,
				b.ECL_Drawn_PostSec_3
FROM &net_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT as A inner JOIN &net_db..RRAP_Rpt_Extract00_ECL_Prev as B
 ON a.basel_acct_id = b.basel_acct_id
 LEFT JOIN &net_db..PD_BAND_DIM c on c.NCR_EXPSR_CL_KEY_VAL='0599' AND a.TOT_EXPSR_ABOVE_1500K_LMT_F = 'Y' and a.ASST_CL_NUM <> 1 and a.PD_FLRD_RPTG_RTO between c.PD_MIN_VAL and c.PD_MAX_VAL
 AND &PREV_YEARMONTH. between cast(c.eff_from_yr_mth AS integer) and cast(c.eff_to_yr_mth AS integer)
		where  a.SRC_SYS_CD in ('KS','SPL','TNG-MOR','MOR')
		and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N'
  		and a.PIT_STAT_CD in ('CUR','DEF')
		and a.TRNST_EXCLSN_F='N'
		and a.DLGD_F='N' 
		AND A.MTH_TM_ID=(&MTH_TM_ID-40))BY IIASCON;
	DISCONNECT FROM IIASCON;
QUIT;


/*Calculate first set of transformation rules using the base data.*/
proc sql noprint;
		create table &INPATH..RRAP_Rpt_Extract_AllBank_ECL as
			select 	
				'DOM-BANK-BNS' as LEGAL_ENTITY format $40.,
				PD_BAND,
				CCAR_BASEL_PRD_TP_NM,
				MTH_TM_ID,
				SRC_SYS_CD,
				COALESCE(LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM,
				COALESCE(EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM, 
				PD_BASEL_SEG_NUM, 
				PD_BASEL_SEG_ID, 
				LGD_BASEL_SEG_ID,
				EAD_BASEL_SEG_ID,
				INSURER_FLAG as INSUR_F,
				case when mean(PD_FINAL_RPTG_RTO) >=1 then 'T' else 'F' end as Default_F format $1.,
				sum(ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
				sum(case when ADJUSTED_OS_BAL_AMT <=0 then 0 else ADJUSTED_OS_BAL_AMT end) as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
				case when mean(PD_FINAL_RPTG_RTO) >=1 then max(WGHTD_DLGD_RTO) else . end as EXPCTD_LOSS_RTO format 28.8,
				case when SRC_SYS_CD='KS' then sum(BEFORE_ZERO_NET_UNDRAWN_AMT) else 0 end as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
				case when SRC_SYS_CD='KS' then sum(AF_ZERO_NET_UNDRAWN_AMT) else 0 end as AF_ZERO_NET_UNDRAWN_AMT format 17.3,
				0 as BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
				0 as AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
				max(LGD_FLRD_RPTG_RTO) as LGD_FLRD_RPTG_RTO format 28.8,
				case when SRC_SYS_CD = 'KS'
					then case
						when mean(PD_FINAL_RPTG_RTO) >=1 then 0 
						else max(EAD_FLRD_RPTG_RTO) 
						end
					else 0
				end
				as EAD_FLRD_RPTG_RTO format 28.8,
				'CAD' as CRNCY_CD format $10.,
				0 as PRTL_WRITE_OFF_AMT format 17.3,
				'TRUE' as UNCONDTNLY_CNCLBL format $10.,
				SUM(INTR_ACCR_AMT) as ACCR_INTR_AMT format 17.3,
				AVG(WGHTD_DLGD_RTO) as WGHTD_DLGD_RTO, 
				LTV_PERCENTAGE, 
				TRNST_NUM, EGL_DEPRTMNT, PRD_ID, /*adding for bcar_ccar combine file */
				count(1) as OBLIGORS,
				RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, /*adding for Basel III */
				SUM(ORIG_AMT_LOAN) AS ORIG_AMT_LOAN,								/*adding for Basel III */
				PD_90_DAY_F, UNINSURED_FLRD_LGD_RTO,	UNINSURED_DLGD_RTO,					/*adding for Basel III */
				CLP_FLAG,PERIOD_IND,
				sum(ECL_Drawn_1) as ECL_Drawn_1,
				sum(ECL_Drawn_2) as ECL_Drawn_2,
				sum(ECL_Drawn_3) as ECL_Drawn_3,	
				sum(ECL_Undrawn_1) as ECL_Undrawn_1,
				sum(ECL_Undrawn_2) as ECL_Undrawn_2,
				sum(ECL_Undrawn_3) as ECL_Undrawn_3,
				sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1,
				sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
				sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3,
				&SESSIONTIME AS INSRT_PROCESS_TMSTMP,
				&SESSIONTIME AS UPDT_PROCESS_TMSTMP
from NZRRAP.RRAP_Rpt_Extract00_ECL
where DLGD_F='N'
and ((SRC_SYS_CD in ('KS','SPL')) 
	or (SRC_SYS_CD in ('TNG-MOR', 'MOR') and 
		CCAR_BASEL_PRD_TP_NM NOT LIKE '%GENW%' AND CCAR_BASEL_PRD_TP_NM NOT LIKE '%GUAR%'))
group by PD_BAND, CCAR_BASEL_PRD_TP_NM, MTH_TM_ID, SRC_SYS_CD, 
	LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, PD_BASEL_SEG_NUM, 
	PD_BASEL_SEG_ID, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID ,INSURER_FLAG, LTV_PERCENTAGE, 
	TRNST_NUM,EGL_DEPRTMNT, PRD_ID, /*adding for bcar_ccar combine file */
	PERIOD_IND,
	RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, /*adding for Basel III */
	PD_90_DAY_F, UNINSURED_FLRD_LGD_RTO,	UNINSURED_DLGD_RTO,CLP_FLAG	/*adding for Basel III */
order by PD_BAND, CCAR_BASEL_PRD_TP_NM, MTH_TM_ID, SRC_SYS_CD, 
	LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, PD_BASEL_SEG_NUM, 
	PD_BASEL_SEG_ID, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID ,INSURER_FLAG, LTV_PERCENTAGE,
	TRNST_NUM,EGL_DEPRTMNT, PRD_ID, /*adding for bcar_ccar combine file */
	PERIOD_IND,
	RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, /*adding for Basel III */
	PD_90_DAY_F, UNINSURED_FLRD_LGD_RTO,	UNINSURED_DLGD_RTO,CLP_FLAG /*adding for Basel III */
	;
quit;


proc sort data= &INPATH..RRAP_Rpt_Extract_AllBank_ECL nodupkey; 
by 
MTH_TM_ID LEGAL_ENTITY CCAR_BASEL_PRD_TP_NM PD_BAND LGD_BASEL_SEG_NUM EAD_BASEL_SEG_NUM INSUR_F LTV_PERCENTAGE 
TRNST_NUM EGL_DEPRTMNT PRD_ID /*adding for bcar_ccar combine file */ 
RNTL_PRPTY_F CURRENCY_MISMATCH_F TOT_EXPSR_ABOVE_1500K_LMT_F TRANSACTOR_FLAG_QRR 
PD_90_DAY_F UNINSURED_FLRD_LGD_RTO UNINSURED_DLGD_RTO CLP_FLAG; 
quit;


PROC SQL ;
DROP TABLE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL; 
CREATE TABLE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL AS (
SELECT 
legal_entity format $40.
,PD_BAND
,CCAR_BASEL_PRD_TP_NM
,MTH_TM_ID
,LGD_BASEL_SEG_NUM
,EAD_BASEL_SEG_NUM
,PD_BASEL_SEG_NUM
,PD_BASEL_SEG_ID
,LGD_BASEL_SEG_ID
,EAD_BASEL_SEG_ID
,DEFAULT_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FLRD_RPTG_RTO
,EAD_FLRD_RPTG_RTO
,crncy_cd
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,TRIM(INSUR_F) as INSUR_F
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
,TRNST_NUM,EGL_DEPRTMNT,PRD_ID /*adding for bcar_ccar combine file */
,RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, ORIG_AMT_LOAN
,PD_90_DAY_F ,UNINSURED_FLRD_LGD_RTO, UNINSURED_DLGD_RTO ,CLP_FLAG
,OBLIGORS,
	PERIOD_IND,
	ECL_Drawn_1,
	ECL_Drawn_2,
	ECL_Drawn_3,				
	ECL_Undrawn_1,
	ECL_Undrawn_2,
	ECL_Undrawn_3,
	ECL_Drawn_PostSec_1,
	ECL_Drawn_PostSec_2,
	ECL_Drawn_PostSec_3
FROM &INPATH..RRAP_Rpt_Extract_AllBank_ECL);
QUIT;

/****UPDATE PREV MONTH'S VALUES TO ZERO ***/
PROC SQL NOPRINT;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET BEFR_ZERO_NET_ADJ_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE PERIOD_IND = 'PREV' ;
QUIT;

/************ PMI *************************/
proc sql noprint;
	create table &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI AS (
	SELECT 
	basel_acct_id
	,'DOM-BANK-BNS' as LEGAL_ENTITY format $40.
	,PD_BAND
	,CCAR_BASEL_PRD_TP_NM
	,MTH_TM_ID
	,COALESCE(LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM
	,COALESCE(EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM
	,PD_BASEL_SEG_NUM
	,PD_BASEL_SEG_ID
	,LGD_BASEL_SEG_ID
	,EAD_BASEL_SEG_ID
	,case when PD_FINAL_RPTG_RTO >=1 then 'T' else 'F' end as Default_F format $1.
	,ADJUSTED_OS_BAL_AMT AS BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8
	,case when ADJUSTED_OS_BAL_AMT <=0 then 0 else ADJUSTED_OS_BAL_AMT end as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8
	,case when PD_FINAL_RPTG_RTO >=1 then WGHTD_DLGD_RTO else . end as EXPCTD_LOSS_RTO format 28.8
	,0 AS BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3
	,0 AS AF_ZERO_NET_UNDRAWN_AMT format 17.3
	,0 AS BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3
	,0 AS AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3
	,LGD_FLRD_RPTG_RTO format 28.8
	,0 AS EAD_FLRD_RPTG_RTO format 28.8
	,'CAD' as CRNCY_CD format $10.
	,0 as PRTL_WRITE_OFF_AMT format 17.3
	,'TRUE' as UNCONDTNLY_CNCLBL format $10.
	,INTR_ACCR_AMT as ACCR_INTR_AMT format 17.3
	,&SESSIONTIME AS INSRT_PROCESS_TMSTMP
	,&SESSIONTIME AS UPDT_PROCESS_TMSTMP
	,TRIM(INSURER_FLAG) as INSUR_F
	,WGHTD_DLGD_RTO
	,LTV_PERCENTAGE
	,TRNST_NUM,EGL_DEPRTMNT,PRD_ID /*adding for bcar_ccar combine file */
	,RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, ORIG_AMT_LOAN
	,PD_90_DAY_F ,UNINSURED_FLRD_LGD_RTO, UNINSURED_DLGD_RTO ,CLP_FLAG
	,1 as OBLIGORS
	,PERIOD_IND,
	ECL_Drawn_1,
	ECL_Drawn_2,
	ECL_Drawn_3,				
	ECL_Undrawn_1,
	ECL_Undrawn_2,
	ECL_Undrawn_3,
	ECL_Drawn_PostSec_1,
	ECL_Drawn_PostSec_2,
	ECL_Drawn_PostSec_3
from NZRRAP.RRAP_Rpt_Extract00_ECL
where DLGD_F='N'
and SRC_SYS_CD in ('TNG-MOR', 'MOR') 
and (CCAR_BASEL_PRD_TP_NM LIKE '%GENW%' or CCAR_BASEL_PRD_TP_NM LIKE '%GUAR%')
);
QUIT;

/****UPDATE PREV MONTH'S VALUES TO ZERO ***/
PROC SQL NOPRINT;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET BEFR_ZERO_NET_ADJ_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE PERIOD_IND = 'PREV' ;
QUIT;


/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/

PROC SQL;
DROP TABLE &INPATH..RRAP_Rpt_Sub_Extract00_ECL;
CREATE table &INPATH..RRAP_Rpt_Sub_Extract00_ECL as 
select 
A.basel_acct_id ,
A.ADJUSTED_OS_BAL_AMT,
A.CCAR_BASEL_PRD_TP_NM,
A.CONS_DFT_MTH_CNT,
A.EAD_BASEL_SEG_ID,
(case
		when A.EAD_BASEL_SEG_NUM IS NULL then -1
		else A.EAD_BASEL_SEG_NUM end
) as EAD_BASEL_SEG_NUM,
A.EAD_FLRD_RPTG_RTO,
A.LEGAL_ENTITY,
A.LGD_BASEL_SEG_ID,
(case
		when A.LGD_BASEL_SEG_NUM IS NULL then -1
		else A.LGD_BASEL_SEG_NUM end
) as LGD_BASEL_SEG_NUM,
A.LGD_FLRD_RPTG_RTO,
A.MTH_TM_ID,
A.NCR_EXPSR_CL_KEY_VAL,
A.PD_BAND,
A.PD_BASEL_SEG_ID,
A.PD_BASEL_SEG_NUM,
A.PD_FINAL_RPTG_RTO,
A.SRC_SYS_CD,
A.UNADJUSTED_ADD_ON_BAL_AMT,
A.UNQ_ACCT_ID,
A.INTR_ACCR_AMT,
(case 	when A.SCRTY_TP_DESC = 'Insured' THEN  'YES' ELSE 'NO' end ) as INSURER_FLAG 
,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, 
				a.LTV_BUCKET AS LTV_PERCENTAGE,
				a.TRNST_NUM,a.EGL_DEPRTMNT,a.PRD_ID, /*adding for bcar_ccar combine file */
				a.RNTL_PRPTY_F,
				a.CURRENCY_MISMATCH_F,
				a.TOT_EXPSR_ABOVE_1500K_LMT_F,
				a.ORIG_AMT_LOAN,
				a.TRANSACTOR_FLAG_QRR,
				a.PD_90_DAY_F,							/*adding for Basel III */
				a.UNINSURED_FLRD_LGD_RTO,					/*adding for Basel III */
				a.UNINSURED_DLGD_RTO,					/*adding for Basel III */
				a.CLP_FLAG,
				b.PERIOD_IND,
				b.ECL_Drawn_1,
				b.ECL_Drawn_2,
				b.ECL_Drawn_3,				
				b.ECL_Undrawn_1,
				b.ECL_Undrawn_2,
				b.ECL_Undrawn_3,
				b.ECL_Drawn_PostSec_1,
				b.ECL_Drawn_PostSec_2,
				b.ECL_Drawn_PostSec_3
 FROM NZRRAP.BASEL_ANALYTCL_BL_INSTRMNT_FACT as A LEFT JOIN NZRRAP.RRAP_Rpt_Extract00_ECL_CURR as B
 ON a.basel_acct_id = b.basel_acct_id
where A.LEGAL_ENTITY in ('DOM-SUB-DUNDEE','DOM-SUB-MTCC','DOM-SUB-NT','DOM-SUB-MAPLE','DOM-SUB-SMC', 'DOM-SUB-TNG') 
	and A.SRC_SYS_CD in ('MOR', 'TNG-MOR') 
	and A.CONSM_PRD_TREATMNT_CD='A' and A.SML_BUS_F='N'
	and A.PIT_STAT_CD in ('CUR','DEF')
	and A.TRNST_EXCLSN_F='N'
	and A.DLGD_F='N'
	AND A.MTH_TM_ID=&MTH_TM_ID
	AND (A.PD_BASEL_SEG_NUM IS NOT MISSING AND A.LGD_BASEL_SEG_NUM IS NOT MISSING);
;
quit;


PROC SQL NOPRINT;
UPDATE &INPATH..RRAP_Rpt_Sub_Extract00_ECL SET PERIOD_IND = 'CURR' WHERE PERIOD_IND IS NULL ;
QUIT;

PROC SQL;
	INSERT INTO &INPATH..RRAP_Rpt_Sub_Extract00_ECL
	select 
A.basel_acct_id ,
A.ADJUSTED_OS_BAL_AMT,
A.CCAR_BASEL_PRD_TP_NM,
A.CONS_DFT_MTH_CNT,
A.EAD_BASEL_SEG_ID,
(case
		when A.EAD_BASEL_SEG_NUM IS NULL then -1
		else A.EAD_BASEL_SEG_NUM end
) as EAD_BASEL_SEG_NUM,
A.EAD_FLRD_RPTG_RTO,
A.LEGAL_ENTITY,
A.LGD_BASEL_SEG_ID,
(case
		when A.LGD_BASEL_SEG_NUM IS NULL then -1
		else A.LGD_BASEL_SEG_NUM end
) as LGD_BASEL_SEG_NUM,
A.LGD_FLRD_RPTG_RTO,
a.MTH_TM_ID ,
A.NCR_EXPSR_CL_KEY_VAL,
A.PD_BAND,
A.PD_BASEL_SEG_ID,
A.PD_BASEL_SEG_NUM,
A.PD_FINAL_RPTG_RTO,
A.SRC_SYS_CD,
A.UNADJUSTED_ADD_ON_BAL_AMT,
A.UNQ_ACCT_ID,
A.INTR_ACCR_AMT,
(case 	when A.SCRTY_TP_DESC = 'Insured' THEN  'YES' ELSE 'NO' end ) as INSURER_FLAG 
,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, 
a.LTV_BUCKET AS LTV_PERCENTAGE,  		/*adding for Basel III */
a.TRNST_NUM, 							/*adding for bcar_ccar combine file */
a.EGL_DEPRTMNT, 						/*adding for bcar_ccar combine file */
a.PRD_ID,       						/*adding for bcar_ccar combine file */
a.RNTL_PRPTY_F,							/*adding for Basel III */
a.CURRENCY_MISMATCH_F, 					/*adding for Basel III */
a.TOT_EXPSR_ABOVE_1500K_LMT_F, 			/*adding for Basel III */
a.ORIG_AMT_LOAN, 						/*adding for Basel III */
a.TRANSACTOR_FLAG_QRR, 						/*adding for Basel III */
a.PD_90_DAY_F,							/*adding for Basel III */
a.UNINSURED_FLRD_LGD_RTO,					/*adding for Basel III */
a.UNINSURED_DLGD_RTO,					/*adding for Basel III */
a.CLP_FLAG,
b.PERIOD_IND,
b.ECL_Drawn_1,
b.ECL_Drawn_2,
b.ECL_Drawn_3,				
b.ECL_Undrawn_1,
b.ECL_Undrawn_2,
b.ECL_Undrawn_3,
b.ECL_Drawn_PostSec_1,
b.ECL_Drawn_PostSec_2,
b.ECL_Drawn_PostSec_3
 FROM NZRRAP.BASEL_ANALYTCL_BL_INSTRMNT_FACT as A inner JOIN  NZRRAP.RRAP_Rpt_Extract00_ECL_Prev as B
 ON a.basel_acct_id = b.basel_acct_id
where  A.LEGAL_ENTITY in ('DOM-SUB-DUNDEE','DOM-SUB-MTCC','DOM-SUB-NT','DOM-SUB-MAPLE','DOM-SUB-SMC', 'DOM-SUB-TNG') 
	and A.SRC_SYS_CD in ('MOR', 'TNG-MOR') 
	and A.CONSM_PRD_TREATMNT_CD='A' and A.SML_BUS_F='N'
	and A.PIT_STAT_CD in ('CUR','DEF')
	and A.TRNST_EXCLSN_F='N'
	and A.DLGD_F='N'
	and a.MTH_TM_ID=(&MTH_TM_ID-40)
; 
QUIT;


/*Get base aggregated data;*/
proc sql noprint;
create table &INPATH..RRAP_Rpt_Extract_Subsidiary_ECL as
select LEGAL_ENTITY,
PD_BAND,
CCAR_BASEL_PRD_TP_NM,
MTH_TM_ID,
COALESCE(LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM,
COALESCE(EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM,
PD_BASEL_SEG_NUM,
PD_BASEL_SEG_ID, 
LGD_BASEL_SEG_ID,
EAD_BASEL_SEG_ID,
INSURER_FLAG as INSUR_F,
case when mean(PD_FINAL_RPTG_RTO) >=1 then 'T' else 'F' end as DEFAULT_F format $1.,
sum (ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
sum(case when ADJUSTED_OS_BAL_AMT <=0 then 0 else ADJUSTED_OS_BAL_AMT end) as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
case when mean(PD_FINAL_RPTG_RTO) >=1 then max(WGHTD_DLGD_RTO) else . end as EXPCTD_LOSS_RTO format 28.8,
0 as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
0 as AF_ZERO_NET_UNDRAWN_AMT format 17.3, 
0 as BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
0 as AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
max(LGD_FLRD_RPTG_RTO) as LGD_FLRD_RPTG_RTO format 28.8,
0 as EAD_FLRD_RPTG_RTO format 28.8,
'CAD' as CRNCY_CD format $10.,
0 as PRTL_WRITE_OFF_AMT format 17.3,
'TRUE' as UNCONDTNLY_CNCLBL format $10.,
SUM(INTR_ACCR_AMT) as ACCR_INTR_AMT format 17.3,
AVG(WGHTD_DLGD_RTO) as WGHTD_DLGD_RTO, 
LTV_PERCENTAGE, 
TRNST_NUM,EGL_DEPRTMNT,PRD_ID, /*adding for bcar_ccar combine file */
count(1) as OBLIGORS,
RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR,
SUM(ORIG_AMT_LOAN) AS ORIG_AMT_LOAN,
PD_90_DAY_F, UNINSURED_FLRD_LGD_RTO,	UNINSURED_DLGD_RTO, CLP_FLAG, /*adding for Basel III */
PERIOD_IND,
sum(ECL_Drawn_1) as ECL_Drawn_1,
sum(ECL_Drawn_2) as ECL_Drawn_2,
sum(ECL_Drawn_3) as ECL_Drawn_3,	
sum(ECL_Undrawn_1) as ECL_Undrawn_1,
sum(ECL_Undrawn_2) as  ECL_Undrawn_2,
sum(ECL_Undrawn_3) as ECL_Undrawn_3,
sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1,
sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3,
&SESSIONTIME AS INSRT_PROCESS_TMSTMP,
&SESSIONTIME AS UPDT_PROCESS_TMSTMP
from &INPATH..RRAP_Rpt_Sub_Extract00_ECL
where DLGD_F='N'
and CCAR_BASEL_PRD_TP_NM NOT LIKE '%GENW%' 
and CCAR_BASEL_PRD_TP_NM NOT LIKE '%GUAR%'
group by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, PD_BAND,
 		 LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID,PD_BASEL_SEG_NUM,
		PD_BASEL_SEG_ID,INSURER_FLAG, WGHTD_DLGD_RTO,LTV_PERCENTAGE,PERIOD_IND,
		TRNST_NUM,EGL_DEPRTMNT,PRD_ID, /*adding for bcar_ccar combine file */
		RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, /*adding for Basel III */
		PD_90_DAY_F, UNINSURED_FLRD_LGD_RTO,	UNINSURED_DLGD_RTO, CLP_FLAG	/*adding for Basel III */
order by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, PD_BAND, 
 		 LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID, WGHTD_DLGD_RTO,LTV_PERCENTAGE,PERIOD_IND,
		 TRNST_NUM,EGL_DEPRTMNT,PRD_ID, /*adding for bcar_ccar combine file */
		 RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, /*adding for Basel III */
		 PD_90_DAY_F, UNINSURED_FLRD_LGD_RTO,	UNINSURED_DLGD_RTO, CLP_FLAG	/*adding for Basel III */
;
quit;

PROC SQL NOPRINT;
INSERT INTO &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SELECT 
legal_entity
,PD_BAND
,CCAR_BASEL_PRD_TP_NM
,MTH_TM_ID
,LGD_BASEL_SEG_NUM
,EAD_BASEL_SEG_NUM
,PD_BASEL_SEG_NUM
,PD_BASEL_SEG_ID
,LGD_BASEL_SEG_ID
,EAD_BASEL_SEG_ID
,DEFAULT_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FLRD_RPTG_RTO
,EAD_FLRD_RPTG_RTO
,crncy_cd
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,TRIM(INSUR_F) as INSUR_F
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
,TRNST_NUM,EGL_DEPRTMNT,PRD_ID /*adding for bcar_ccar combine file */
,RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, ORIG_AMT_LOAN
,PD_90_DAY_F ,UNINSURED_FLRD_LGD_RTO, UNINSURED_DLGD_RTO, CLP_FLAG
,OBLIGORS,
PERIOD_IND,
ECL_Drawn_1,
ECL_Drawn_2,
ECL_Drawn_3,
ECL_Undrawn_1,
ECL_Undrawn_2,
ECL_Undrawn_3,
ECL_Drawn_PostSec_1,
ECL_Drawn_PostSec_2,
ECL_Drawn_PostSec_3
FROM &INPATH..RRAP_Rpt_Extract_Subsidiary_ECL;
QUIT;


/****UPDATE PREV MONTH'S VALUES TO ZERO ***/
PROC SQL NOPRINT;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT IS NULL AND CCAR_BASEL_PRD_TP_NM LIKE '%TNG%' AND MTH_TM_ID=&MTH_TM_ID;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET AF_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE AF_ZERO_NET_ALWBL_CR_LOSS_AMT IS NULL AND CCAR_BASEL_PRD_TP_NM LIKE '%TNG%' AND MTH_TM_ID=&MTH_TM_ID;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET BEFR_ZERO_NET_ADJ_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE PERIOD_IND = 'PREV' ;
QUIT;

/******************PMI SUBSIDIARY****************/
PROC SQL NOPRINT;
INSERT INTO &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI 
SELECT 
basel_acct_id
,legal_entity
,PD_BAND
	,CCAR_BASEL_PRD_TP_NM
	,MTH_TM_ID
	,COALESCE(LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM
	,COALESCE(EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM
	,PD_BASEL_SEG_NUM
	,PD_BASEL_SEG_ID
	,LGD_BASEL_SEG_ID
	,EAD_BASEL_SEG_ID
	,case when PD_FINAL_RPTG_RTO >=1 then 'T' else 'F' end as Default_F format $1.
	,ADJUSTED_OS_BAL_AMT AS BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8
	,case when ADJUSTED_OS_BAL_AMT <=0 then 0 else ADJUSTED_OS_BAL_AMT end as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8
	,case when PD_FINAL_RPTG_RTO >=1 then WGHTD_DLGD_RTO else . end as EXPCTD_LOSS_RTO format 28.8
	,0 AS BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3
	,0 AS AF_ZERO_NET_UNDRAWN_AMT format 17.3
	,0 AS BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3
	,0 AS AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3
	,LGD_FLRD_RPTG_RTO format 28.8
	,0 AS EAD_FLRD_RPTG_RTO format 28.8
	,'CAD' as CRNCY_CD format $10.
	,0 as PRTL_WRITE_OFF_AMT format 17.3
	,'TRUE' as UNCONDTNLY_CNCLBL format $10.
	,INTR_ACCR_AMT as ACCR_INTR_AMT format 17.3
	,&SESSIONTIME AS INSRT_PROCESS_TMSTMP
	,&SESSIONTIME AS INSRT_PROCESS_TMSTMP
	,TRIM(INSURER_FLAG) as INSUR_F
	,WGHTD_DLGD_RTO
	,LTV_PERCENTAGE
	,TRNST_NUM,EGL_DEPRTMNT,PRD_ID /*adding for bcar_ccar combine file */
	,RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, ORIG_AMT_LOAN
	,PD_90_DAY_F ,UNINSURED_FLRD_LGD_RTO, UNINSURED_DLGD_RTO, CLP_FLAG
	,1 as OBLIGORS
	,PERIOD_IND,
	ECL_Drawn_1,
	ECL_Drawn_2,
	ECL_Drawn_3,				
	ECL_Undrawn_1,
	ECL_Undrawn_2,
	ECL_Undrawn_3,
	ECL_Drawn_PostSec_1,
	ECL_Drawn_PostSec_2,
	ECL_Drawn_PostSec_3
from &INPATH..RRAP_Rpt_Sub_Extract00_ECL
where DLGD_F='N'
and (CCAR_BASEL_PRD_TP_NM LIKE '%GENW%' OR CCAR_BASEL_PRD_TP_NM LIKE '%GUAR%')
;
QUIT;

/****UPDATE PREV MONTH'S VALUES TO ZERO ***/
PROC SQL NOPRINT;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT IS NULL AND CCAR_BASEL_PRD_TP_NM LIKE '%TNG%' AND MTH_TM_ID=&MTH_TM_ID;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET AF_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE AF_ZERO_NET_ALWBL_CR_LOSS_AMT IS NULL AND CCAR_BASEL_PRD_TP_NM LIKE '%TNG%' AND MTH_TM_ID=&MTH_TM_ID;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET BEFR_ZERO_NET_ADJ_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE PERIOD_IND = 'PREV' ;
QUIT;


/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
	
	PROC SQL;
	CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain="IIAS_Auth");
	EXECUTE(DROP TABLE IF EXISTS &net_db..RRAP_Rpt_Extract00_ECL)BY IIASCON;
	EXECUTE(
		CREATE table &net_db..RRAP_Rpt_Extract00_ECL as 
			(
		select 	a.basel_acct_id ,
				a.ADJUSTED_OS_BAL_AMT,
				a.CCAR_BASEL_PRD_TP_NM,
				a.CONS_DFT_MTH_CNT,
				a.EAD_BASEL_SEG_ID,
				(case when a.EAD_BASEL_SEG_NUM IS NULL then -1 else a.EAD_BASEL_SEG_NUM end ) as EAD_BASEL_SEG_NUM,
				a.EAD_FLRD_RPTG_RTO,
				a.LEGAL_ENTITY,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.LGD_BASEL_SEG_ID 
				when a.SRC_SYS_CD in ('SPL')  and a.LGD_BASEL_SEG_ID IS NULL THEN -1
					else a.LGD_BASEL_SEG_ID end) as  LGD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') and a.LGD_BASEL_SEG_NUM IS NULL then -1
					when a.SRC_SYS_CD in ('SPL') and a.LGD_BASEL_SEG_NUM IS NULL then -1
					else a.LGD_BASEL_SEG_NUM end) as LGD_BASEL_SEG_NUM,
				a.LGD_FLRD_RPTG_RTO,
				a.MTH_TM_ID,
				a.NCR_EXPSR_CL_KEY_VAL,
				COALESCE(c.pd_band,A.pd_band) AS pd_band,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.PD_BASEL_SEG_ID
					else a.PD_BASEL_SEG_ID end) as PD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.PD_BASEL_SEG_NUM
					else a.PD_BASEL_SEG_NUM end) as PD_BASEL_SEG_NUM,
				a.PD_FINAL_RPTG_RTO,
				a.SRC_SYS_CD,
				a.UNADJUSTED_ADD_ON_BAL_AMT,
				a.BEFORE_ZERO_NET_UNDRAWN_AMT,
				a.AF_ZERO_NET_UNDRAWN_AMT,
				a.INTR_ACCR_AMT,
				a.UNQ_ACCT_ID, A.SCRTY_TP_DESC ,A.BASEL_PRD_TP_CD,
				(case when A.SRC_SYS_CD = 'KS' AND A.INSUR_F='YES' THEN 'YES'
					when A.SRC_SYS_CD in ('MOR','TNG-MOR') AND A.SCRTY_TP_DESC='Insured' THEN 'YES' ELSE 'NO' end ) as INSURER_FLAG 

				,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, 
				a.LTV_BUCKET AS LTV_PERCENTAGE, /*adding for Basel III */				
				a.TRNST_NUM, 					/*adding for bcar_ccar combine file */
				a.EGL_DEPRTMNT, 				/*adding for bcar_ccar combine file */
				a.PRD_ID, 						/*adding for bcar_ccar combine file */
				a.RNTL_PRPTY_F, 				/*adding for Basel III */
				a.CURRENCY_MISMATCH_F, 			/*adding for Basel III */
				a.TOT_EXPSR_ABOVE_1500K_LMT_F, 	/*adding for Basel III */
				a.ORIG_AMT_LOAN, 				/*adding for Basel III */
				a.TRANSACTOR_FLAG_QRR, 				/*adding for Basel III */
				a.PD_90_DAY_F,					/*adding for Basel III */
				a.UNINSURED_FLRD_LGD_RTO,			/*adding for Basel III */
				a.UNINSURED_DLGD_RTO,			
				a.CLP_FLAG,/*adding for Basel III */
				b.PERIOD_IND,
				b.ECL_Drawn_1,
				b.ECL_Drawn_2,
				b.ECL_Drawn_3,				
				b.ECL_Undrawn_1,
				b.ECL_Undrawn_2,
				b.ECL_Undrawn_3,
				b.ECL_Drawn_PostSec_1,
				b.ECL_Drawn_PostSec_2,
				b.ECL_Drawn_PostSec_3
FROM &net_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT as A LEFT JOIN  &net_db..RRAP_Rpt_Extract00_ECL_CURR as B
 ON a.basel_acct_id = b.basel_acct_id
 LEFT JOIN &net_db..PD_BAND_DIM c on c.NCR_EXPSR_CL_KEY_VAL='0599' AND a.TOT_EXPSR_ABOVE_1500K_LMT_F = 'Y' and a.ASST_CL_NUM <> 1 and a.PD_FLRD_RPTG_RTO between c.PD_MIN_VAL and c.PD_MAX_VAL
 AND &YEARMONTH. between cast(c.eff_from_yr_mth AS integer) and cast(c.eff_to_yr_mth AS integer)
		where a.SRC_SYS_CD in ('KS','SPL','TNG-MOR','MOR') 
			and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N'
			and a.PIT_STAT_CD in ('CUR','DEF')
			and a.TRNST_EXCLSN_F='N'
			and a.DLGD_F='Y' AND A.MTH_TM_ID=&MTH_TM_ID
AND (PD_BASEL_SEG_NUM IS NOT NULL AND LGD_BASEL_SEG_NUM IS NOT NULL))WITH DATA)BY IIASCON;
	DISCONNECT FROM IIASCON;
QUIT;

PROC SQL;
	CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain="IIAS_Auth");
	EXECUTE(
		UPDATE &net_db..RRAP_Rpt_Extract00_ECL
			SET PERIOD_IND = 'CURR'
				WHERE PERIOD_IND IS NULL
					)BY IIASCON;
	DISCONNECT FROM IIASCON;
QUIT;

PROC SQL;
	CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain="IIAS_Auth");
	EXECUTE(INSERT INTO &net_db..RRAP_Rpt_Extract00_ECL
			select 	a.BASEL_ACCT_ID,
				a.ADJUSTED_OS_BAL_AMT,
				a.CCAR_BASEL_PRD_TP_NM,
				a.CONS_DFT_MTH_CNT,
				a.EAD_BASEL_SEG_ID,
				(case when a.EAD_BASEL_SEG_NUM IS NULL then -1 else a.EAD_BASEL_SEG_NUM end ) as EAD_BASEL_SEG_NUM,
				a.EAD_FLRD_RPTG_RTO,
				a.LEGAL_ENTITY,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.LGD_BASEL_SEG_ID 
				when a.SRC_SYS_CD in ('SPL')  and a.LGD_BASEL_SEG_ID IS NULL THEN -1
					else a.LGD_BASEL_SEG_ID end) as  LGD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') and a.LGD_BASEL_SEG_NUM IS NULL then -1
					when a.SRC_SYS_CD in ('SPL') and a.LGD_BASEL_SEG_NUM IS NULL then -1
					else a.LGD_BASEL_SEG_NUM end) as LGD_BASEL_SEG_NUM,
				a.LGD_FLRD_RPTG_RTO,
				a.MTH_TM_ID ,
				a.NCR_EXPSR_CL_KEY_VAL,
				COALESCE(c.pd_band,A.pd_band) AS pd_band,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.PD_BASEL_SEG_ID
					else a.PD_BASEL_SEG_ID end) as PD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.PD_BASEL_SEG_NUM
					else a.PD_BASEL_SEG_NUM end) as PD_BASEL_SEG_NUM,
				a.PD_FINAL_RPTG_RTO,
				a.SRC_SYS_CD,
				a.UNADJUSTED_ADD_ON_BAL_AMT,
				a.BEFORE_ZERO_NET_UNDRAWN_AMT,
				a.AF_ZERO_NET_UNDRAWN_AMT,
				a.INTR_ACCR_AMT,
				a.UNQ_ACCT_ID, A.SCRTY_TP_DESC ,A.BASEL_PRD_TP_CD,
				(case when A.SRC_SYS_CD = 'KS' AND A.INSUR_F='YES' THEN 'YES'
					when A.SRC_SYS_CD in ('MOR','TNG-MOR') AND A.SCRTY_TP_DESC='Insured' THEN 'YES' ELSE 'NO' end ) as INSURER_FLAG 

				,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, 
				a.LTV_BUCKET AS LTV_PERCENTAGE, /*adding for Basel III */				
				a.TRNST_NUM, 					/*adding for bcar_ccar combine file */
				a.EGL_DEPRTMNT, 				/*adding for bcar_ccar combine file */
				a.PRD_ID, 						/*adding for bcar_ccar combine file */
				a.RNTL_PRPTY_F, 				/*adding for Basel III */
				a.CURRENCY_MISMATCH_F, 			/*adding for Basel III */
				a.TOT_EXPSR_ABOVE_1500K_LMT_F, 	/*adding for Basel III */
				a.ORIG_AMT_LOAN, 				/*adding for Basel III */
				a.TRANSACTOR_FLAG_QRR, 				/*adding for Basel III */
				a.PD_90_DAY_F,					/*adding for Basel III */
				a.UNINSURED_FLRD_LGD_RTO,			/*adding for Basel III */
				a.UNINSURED_DLGD_RTO,			/*adding for Basel III */		
				a.CLP_FLAG,
				b.PERIOD_IND,
				b.ECL_Drawn_1,
				b.ECL_Drawn_2,
				b.ECL_Drawn_3,				
				b.ECL_Undrawn_1,
				b.ECL_Undrawn_2,
				b.ECL_Undrawn_3,
				b.ECL_Drawn_PostSec_1,
				b.ECL_Drawn_PostSec_2,
				b.ECL_Drawn_PostSec_3
FROM &net_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT as A inner JOIN  &net_db..RRAP_Rpt_Extract00_ECL_Prev as B
 ON a.basel_acct_id = b.basel_acct_id
 LEFT JOIN &net_db..PD_BAND_DIM c on c.NCR_EXPSR_CL_KEY_VAL='0599' AND a.TOT_EXPSR_ABOVE_1500K_LMT_F = 'Y' and a.ASST_CL_NUM <> 1 and a.PD_FLRD_RPTG_RTO between c.PD_MIN_VAL and c.PD_MAX_VAL
 AND &PREV_YEARMONTH. between cast(c.eff_from_yr_mth AS integer) and cast(c.eff_to_yr_mth AS integer)
		where a.SRC_SYS_CD in ('KS','SPL','TNG-MOR','MOR') 
		and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N'
			and a.PIT_STAT_CD in ('CUR','DEF')
			and a.TRNST_EXCLSN_F='N'
			and a.DLGD_F='Y' AND A.MTH_TM_ID=(&MTH_TM_ID-40))BY IIASCON;
	DISCONNECT FROM IIASCON;
QUIT;



/*Calculate first set of transformation rules using the base data.*/
/*DLGD RECORDS USE DIFFERENT AGGREGATION.*/
	proc sql noprint;
		create table &INPATH..RRAP_Rpt_Extract_AllBank_ECL as
			select 
				'DOM-BANK-BNS' as LEGAL_ENTITY format $40.,
				PD_BAND,
				CCAR_BASEL_PRD_TP_NM,
				MTH_TM_ID,
				SRC_SYS_CD,
				COALESCE(LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM,
				COALESCE(EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM,
				INSURER_FLAG as INSUR_F,
				PD_BASEL_SEG_NUM, 
				PD_BASEL_SEG_ID, 
				LGD_BASEL_SEG_ID,
				EAD_BASEL_SEG_ID, 
				case when mean(PD_FINAL_RPTG_RTO) >=1 then 'T' else 'F' end as Default_F format $1.,
				sum(ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
				sum(case when ADJUSTED_OS_BAL_AMT <=0 then 0 else ADJUSTED_OS_BAL_AMT end) as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
				case when mean(PD_FINAL_RPTG_RTO) >=1 then max(WGHTD_DLGD_RTO) else . end as EXPCTD_LOSS_RTO format 28.8,
				case when SRC_SYS_CD='KS' then sum(BEFORE_ZERO_NET_UNDRAWN_AMT) else 0 end as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
				case when SRC_SYS_CD='KS' then sum(AF_ZERO_NET_UNDRAWN_AMT) else 0 end as AF_ZERO_NET_UNDRAWN_AMT format 17.3,
				0 as BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
				0 as AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
				max(LGD_FLRD_RPTG_RTO) AS LGD_FLRD_RPTG_RTO format 28.8,
				case when SRC_SYS_CD = 'KS'
					then case
						when mean(PD_FINAL_RPTG_RTO) >=1 then 0 
						else max(EAD_FLRD_RPTG_RTO) 
						end
					else 0
				end
				as EAD_FLRD_RPTG_RTO format 28.8,
			'CAD' as CRNCY_CD format $10.,
			0 as PRTL_WRITE_OFF_AMT format 17.3,
			'TRUE' as UNCONDTNLY_CNCLBL format $10.,
			SUM(INTR_ACCR_AMT) as ACCR_INTR_AMT format 17.3,
			WGHTD_DLGD_RTO, 
			LTV_PERCENTAGE, 
			TRNST_NUM,EGL_DEPRTMNT,PRD_ID,/*adding for bcar_ccar combine file */
			count(1) as OBLIGORS,
			RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR,
			SUM(ORIG_AMT_LOAN) AS ORIG_AMT_LOAN,
			PD_90_DAY_F, UNINSURED_FLRD_LGD_RTO,	UNINSURED_DLGD_RTO, CLP_FLAG,
			PERIOD_IND,
			sum(ECL_Drawn_1) as ECL_Drawn_1,
			sum(ECL_Drawn_2) as ECL_Drawn_2,
			sum(ECL_Drawn_3) as ECL_Drawn_3,	
			sum(ECL_Undrawn_1) as ECL_Undrawn_1,
			sum(ECL_Undrawn_2) as ECL_Undrawn_2,
			sum(ECL_Undrawn_3) as ECL_Undrawn_3,
			sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1 ,
			sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
			sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3,
			&SESSIONTIME AS INSRT_PROCESS_TMSTMP,
			&SESSIONTIME AS UPDT_PROCESS_TMSTMP
	from NZRRAP.RRAP_Rpt_Extract00_ECL
	where DLGD_F='Y'
	and ((SRC_SYS_CD in ('KS','SPL')) 
	or (SRC_SYS_CD in ('TNG-MOR', 'MOR') and 
		CCAR_BASEL_PRD_TP_NM NOT LIKE '%GENW%' AND CCAR_BASEL_PRD_TP_NM NOT LIKE '%GUAR%'))
		group by PD_BAND, CCAR_BASEL_PRD_TP_NM, MTH_TM_ID, SRC_SYS_CD, 
			LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, PD_BASEL_SEG_NUM, 
			PD_BASEL_SEG_ID, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID ,INSURER_FLAG, WGHTD_DLGD_RTO, LTV_PERCENTAGE,PERIOD_IND,
			TRNST_NUM,EGL_DEPRTMNT,PRD_ID,	/*adding for bcar_ccar combine file */
			RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, /*adding for Basel III */
			PD_90_DAY_F, UNINSURED_FLRD_LGD_RTO,	UNINSURED_DLGD_RTO, CLP_FLAG	/*adding for Basel III */
	order by PD_BAND, CCAR_BASEL_PRD_TP_NM, MTH_TM_ID, SRC_SYS_CD, 
		LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, PD_BASEL_SEG_NUM, 
		PD_BASEL_SEG_ID, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID ,INSURER_FLAG, WGHTD_DLGD_RTO, LTV_PERCENTAGE,PERIOD_IND,
		TRNST_NUM,EGL_DEPRTMNT,PRD_ID,/*adding for bcar_ccar combine file */
		RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, /*adding for Basel III */
		PD_90_DAY_F, UNINSURED_FLRD_LGD_RTO,	UNINSURED_DLGD_RTO, CLP_FLAG	/*adding for Basel III */
	;
	quit;
	

proc sort data= &INPATH..RRAP_Rpt_Extract_AllBank_ECL nodupkey; 
by 
MTH_TM_ID LEGAL_ENTITY CCAR_BASEL_PRD_TP_NM PD_BAND LGD_BASEL_SEG_NUM EAD_BASEL_SEG_NUM INSUR_F WGHTD_DLGD_RTO LTV_PERCENTAGE
TRNST_NUM EGL_DEPRTMNT PRD_ID /*adding for bcar_ccar combine file */
RNTL_PRPTY_F CURRENCY_MISMATCH_F TOT_EXPSR_ABOVE_1500K_LMT_F TRANSACTOR_FLAG_QRR PD_90_DAY_F UNINSURED_FLRD_LGD_RTO UNINSURED_DLGD_RTO CLP_FLAG; 
; 
quit;


PROC SQL NOPRINT;
INSERT INTO &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SELECT 
legal_entity
,PD_BAND
,CCAR_BASEL_PRD_TP_NM
,MTH_TM_ID
,LGD_BASEL_SEG_NUM
,EAD_BASEL_SEG_NUM
,PD_BASEL_SEG_NUM
,PD_BASEL_SEG_ID
,LGD_BASEL_SEG_ID
,EAD_BASEL_SEG_ID
,DEFAULT_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FLRD_RPTG_RTO
,EAD_FLRD_RPTG_RTO
,crncy_cd
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,TRIM(INSUR_F) as INSUR_F
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
,TRNST_NUM,EGL_DEPRTMNT,PRD_ID/*adding for bcar_ccar combine file */
,RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, ORIG_AMT_LOAN
,PD_90_DAY_F ,UNINSURED_FLRD_LGD_RTO, UNINSURED_DLGD_RTO, CLP_FLAG
,OBLIGORS,
PERIOD_IND,
ECL_Drawn_1,
ECL_Drawn_2,
ECL_Drawn_3,
ECL_Undrawn_1,
ECL_Undrawn_2,
ECL_Undrawn_3,
ECL_Drawn_PostSec_1,
ECL_Drawn_PostSec_2,
ECL_Drawn_PostSec_3
FROM &INPATH..RRAP_Rpt_Extract_AllBank_ECL;
QUIT;

/****UPDATE PREV MONTH'S VALUES TO ZERO ***/
PROC SQL NOPRINT;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET BEFR_ZERO_NET_ADJ_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE PERIOD_IND = 'PREV' ;
QUIT;

/************ PMI *************************/
proc sql noprint;
	INSERT INTO &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI 
	SELECT 
	basel_acct_id
	,'DOM-BANK-BNS' as LEGAL_ENTITY format $40.
	,PD_BAND
	,CCAR_BASEL_PRD_TP_NM
	,MTH_TM_ID
	,COALESCE(LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM
	,COALESCE(EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM
	,PD_BASEL_SEG_NUM
	,PD_BASEL_SEG_ID
	,LGD_BASEL_SEG_ID
	,EAD_BASEL_SEG_ID
	,case when PD_FINAL_RPTG_RTO >=1 then 'T' else 'F' end as Default_F format $1.
	,ADJUSTED_OS_BAL_AMT AS BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8
	,case when ADJUSTED_OS_BAL_AMT <=0 then 0 else ADJUSTED_OS_BAL_AMT end as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8
	,case when PD_FINAL_RPTG_RTO >=1 then WGHTD_DLGD_RTO else . end as EXPCTD_LOSS_RTO format 28.8
	,0 AS BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3
	,0 AS AF_ZERO_NET_UNDRAWN_AMT format 17.3
	,0 AS BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3
	,0 AS AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3
	,LGD_FLRD_RPTG_RTO format 28.8
	,0 AS EAD_FLRD_RPTG_RTO format 28.8
	,'CAD' as CRNCY_CD format $10.
	,0 as PRTL_WRITE_OFF_AMT format 17.3
	,'TRUE' as UNCONDTNLY_CNCLBL format $10.
	,INTR_ACCR_AMT as ACCR_INTR_AMT format 17.3
	,&SESSIONTIME AS INSRT_PROCESS_TMSTMP
	,&SESSIONTIME AS UPDT_PROCESS_TMSTMP
	,TRIM(INSURER_FLAG) as INSUR_F
	,WGHTD_DLGD_RTO
	,LTV_PERCENTAGE
	,TRNST_NUM,EGL_DEPRTMNT,PRD_ID /*adding for bcar_ccar combine file */
	,RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, ORIG_AMT_LOAN
	,PD_90_DAY_F ,UNINSURED_FLRD_LGD_RTO, UNINSURED_DLGD_RTO, CLP_FLAG
	,1 as OBLIGORS
	,PERIOD_IND,
	ECL_Drawn_1,
	ECL_Drawn_2,
	ECL_Drawn_3,				
	ECL_Undrawn_1,
	ECL_Undrawn_2,
	ECL_Undrawn_3,
	ECL_Drawn_PostSec_1,
	ECL_Drawn_PostSec_2,
	ECL_Drawn_PostSec_3
from NZRRAP.RRAP_Rpt_Extract00_ECL
where DLGD_F='Y'
and SRC_SYS_CD in ('TNG-MOR', 'MOR') 
and (CCAR_BASEL_PRD_TP_NM LIKE '%GENW%' or CCAR_BASEL_PRD_TP_NM LIKE '%GUAR%')
;
QUIT;

/****UPDATE PREV MONTH'S VALUES TO ZERO ***/
PROC SQL NOPRINT;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET BEFR_ZERO_NET_ADJ_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE PERIOD_IND = 'PREV' ;

QUIT;

/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/

proc sql noprint;
drop table &INPATH..RRAP_Rpt_Sub_Extract00_ECL;
    CREATE table &INPATH..RRAP_Rpt_Sub_Extract00_ECL as 
	select 
	A.BASEL_ACCT_ID ,
A.ADJUSTED_OS_BAL_AMT,
A.CCAR_BASEL_PRD_TP_NM,
A.CONS_DFT_MTH_CNT,
A.EAD_BASEL_SEG_ID,
(case
		when A.EAD_BASEL_SEG_NUM IS NULL then -1
		else A.EAD_BASEL_SEG_NUM end
) as EAD_BASEL_SEG_NUM,
A.EAD_FLRD_RPTG_RTO,
A.LEGAL_ENTITY,
A.LGD_BASEL_SEG_ID,
(case
		when A.LGD_BASEL_SEG_NUM IS NULL then -1
		else A.LGD_BASEL_SEG_NUM end
) as LGD_BASEL_SEG_NUM,
A.LGD_FLRD_RPTG_RTO,
A.MTH_TM_ID,
A.NCR_EXPSR_CL_KEY_VAL,
A.PD_BAND,
A.PD_BASEL_SEG_ID,
A.PD_BASEL_SEG_NUM,
A.PD_FINAL_RPTG_RTO,
A.SRC_SYS_CD,
A.UNADJUSTED_ADD_ON_BAL_AMT,
/*UNDRAWN_AMT,*/
A.UNQ_ACCT_ID,
A.INTR_ACCR_AMT,
(case 	when A.SCRTY_TP_DESC = 'Insured' THEN  'YES' ELSE 'NO' end ) as INSURER_FLAG 
,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, 
				a.LTV_BUCKET AS LTV_PERCENTAGE,
				a.TRNST_NUM,a.EGL_DEPRTMNT,a.PRD_ID,/*adding for bcar_ccar combine file */
				a.RNTL_PRPTY_F,
				a.CURRENCY_MISMATCH_F,
				a.TOT_EXPSR_ABOVE_1500K_LMT_F,
				a.ORIG_AMT_LOAN,
				a.TRANSACTOR_FLAG_QRR,
				a.PD_90_DAY_F,							/*adding for Basel III */
				a.UNINSURED_FLRD_LGD_RTO,					/*adding for Basel III */
				a.UNINSURED_DLGD_RTO,					/*adding for Basel III */
				a.CLP_FLAG,
				b.PERIOD_IND,
				b.ECL_Drawn_1,
				b.ECL_Drawn_2,
				b.ECL_Drawn_3,				
				b.ECL_Undrawn_1,
				b.ECL_Undrawn_2,
				b.ECL_Undrawn_3,
				b.ECL_Drawn_PostSec_1,
				b.ECL_Drawn_PostSec_2,
				b.ECL_Drawn_PostSec_3
	FROM NZRRAP.BASEL_ANALYTCL_BL_INSTRMNT_FACT as A LEFT JOIN  NZRRAP.RRAP_Rpt_Extract00_ECL_CURR as B
 ON a.basel_acct_id = b.basel_acct_id
where A.LEGAL_ENTITY in ('DOM-SUB-DUNDEE','DOM-SUB-MTCC','DOM-SUB-NT','DOM-SUB-MAPLE','DOM-SUB-SMC', 'DOM-SUB-TNG') 
	and A.SRC_SYS_CD in ('MOR', 'TNG-MOR') and A.CONSM_PRD_TREATMNT_CD='A' and A.SML_BUS_F='N'
	and A.PIT_STAT_CD in ('CUR','DEF')
	and A.TRNST_EXCLSN_F='N' 
	AND A.MTH_TM_ID=&MTH_TM_ID
			AND (A.PD_BASEL_SEG_NUM IS NOT MISSING AND A.LGD_BASEL_SEG_NUM IS NOT MISSING)
	;
quit;


PROC SQL NOPRINT;
UPDATE &INPATH..RRAP_Rpt_Sub_Extract00_ECL SET PERIOD_IND = 'CURR' WHERE PERIOD_IND IS NULL ;
QUIT;

PROC SQL;
	INSERT INTO &INPATH..RRAP_Rpt_Sub_Extract00_ECL
		select 
	A.BASEL_ACCT_ID ,
A.ADJUSTED_OS_BAL_AMT,
A.CCAR_BASEL_PRD_TP_NM,
A.CONS_DFT_MTH_CNT,
A.EAD_BASEL_SEG_ID,
(case
		when A.EAD_BASEL_SEG_NUM IS NULL then -1
		else A.EAD_BASEL_SEG_NUM end
) as EAD_BASEL_SEG_NUM,

A.EAD_FLRD_RPTG_RTO,
A.LEGAL_ENTITY,
A.LGD_BASEL_SEG_ID,
(case
		when A.LGD_BASEL_SEG_NUM IS NULL then -1
		else A.LGD_BASEL_SEG_NUM end
) as LGD_BASEL_SEG_NUM,

A.LGD_FLRD_RPTG_RTO,
a.MTH_TM_ID ,
A.NCR_EXPSR_CL_KEY_VAL,
A.PD_BAND,
A.PD_BASEL_SEG_ID,
A.PD_BASEL_SEG_NUM,
A.PD_FINAL_RPTG_RTO,
A.SRC_SYS_CD,
A.UNADJUSTED_ADD_ON_BAL_AMT,
/*UNDRAWN_AMT,*/
A.UNQ_ACCT_ID,
A.INTR_ACCR_AMT,
(case 	when A.SCRTY_TP_DESC = 'Insured' THEN  'YES' ELSE 'NO' end ) as INSURER_FLAG 
,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, 
a.LTV_BUCKET AS LTV_PERCENTAGE,  		/*adding for Basel III */
a.TRNST_NUM, 							/*adding for bcar_ccar combine file */
a.EGL_DEPRTMNT, 						/*adding for bcar_ccar combine file */
a.PRD_ID,       						/*adding for bcar_ccar combine file */
a.RNTL_PRPTY_F,							/*adding for Basel III */
a.CURRENCY_MISMATCH_F, 					/*adding for Basel III */
a.TOT_EXPSR_ABOVE_1500K_LMT_F, 			/*adding for Basel III */
a.ORIG_AMT_LOAN, 						/*adding for Basel III */
a.TRANSACTOR_FLAG_QRR, 						/*adding for Basel III */
a.PD_90_DAY_F,							/*adding for Basel III */
a.UNINSURED_FLRD_LGD_RTO,					/*adding for Basel III */
a.UNINSURED_DLGD_RTO,					/*adding for Basel III */
a.CLP_FLAG,
b.PERIOD_IND,
b.ECL_Drawn_1,
b.ECL_Drawn_2,
b.ECL_Drawn_3,				
b.ECL_Undrawn_1,
b.ECL_Undrawn_2,
b.ECL_Undrawn_3,
b.ECL_Drawn_PostSec_1,
b.ECL_Drawn_PostSec_2,
b.ECL_Drawn_PostSec_3
	FROM NZRRAP.BASEL_ANALYTCL_BL_INSTRMNT_FACT as A inner JOIN  NZRRAP.RRAP_Rpt_Extract00_ECL_Prev as B
 ON a.basel_acct_id = b.basel_acct_id
where A.LEGAL_ENTITY in ('DOM-SUB-DUNDEE','DOM-SUB-MTCC','DOM-SUB-NT','DOM-SUB-MAPLE','DOM-SUB-SMC', 'DOM-SUB-TNG') 
	and A.SRC_SYS_CD in ('MOR', 'TNG-MOR') and A.CONSM_PRD_TREATMNT_CD='A' and A.SML_BUS_F='N'
	and A.PIT_STAT_CD in ('CUR','DEF')
	and A.TRNST_EXCLSN_F='N' 
	AND A.MTH_TM_ID=&MTH_TM_ID-40;
QUIT;


/*Get base aggregated data;*/
proc sql noprint;
create table &INPATH..RRAP_Rpt_Extract_Subsidiary_ECL as
select LEGAL_ENTITY,
	PD_BAND,
	CCAR_BASEL_PRD_TP_NM,
	MTH_TM_ID,
	COALESCE(LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM,
	COALESCE(EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM,
	PD_BASEL_SEG_NUM,
	PD_BASEL_SEG_ID, 
	LGD_BASEL_SEG_ID,
	EAD_BASEL_SEG_ID,
	case when mean(PD_FINAL_RPTG_RTO) >=1 then 'T' else 'F' end as DEFAULT_F format $1.,
	INSURER_FLAG as INSUR_F,
	sum(ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
	sum(case when ADJUSTED_OS_BAL_AMT <=0 then 0 else ADJUSTED_OS_BAL_AMT end) as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
	case when mean(PD_FINAL_RPTG_RTO) >=1 then max(WGHTD_DLGD_RTO) else . end as EXPCTD_LOSS_RTO format 28.8,
	0 as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
	0 as AF_ZERO_NET_UNDRAWN_AMT format 17.3, 
	0 as BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
	0 as AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
	max(LGD_FLRD_RPTG_RTO) as LGD_FLRD_RPTG_RTO format 28.8,
	0 as EAD_FLRD_RPTG_RTO format 28.8,
	'CAD' as CRNCY_CD format $10.,
	0 as PRTL_WRITE_OFF_AMT format 17.3,
	'TRUE' as UNCONDTNLY_CNCLBL format $10.,
	SUM(INTR_ACCR_AMT) as ACCR_INTR_AMT format 17.3,
	WGHTD_DLGD_RTO, 
	LTV_PERCENTAGE,
	TRNST_NUM,EGL_DEPRTMNT,PRD_ID,/*adding for bcar_ccar combine file */
	count(1) as OBLIGORS,
	RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR,
	SUM(ORIG_AMT_LOAN) AS ORIG_AMT_LOAN,
	PD_90_DAY_F, UNINSURED_FLRD_LGD_RTO,	UNINSURED_DLGD_RTO, CLP_FLAG,
	PERIOD_IND,
	sum(ECL_Drawn_1) as ECL_Drawn_1,
	sum(ECL_Drawn_2) as ECL_Drawn_2,
	sum(ECL_Drawn_3) as ECL_Drawn_3,	
	sum(ECL_Undrawn_1) as ECL_Undrawn_1,
	sum(ECL_Undrawn_2) as ECL_Undrawn_2,
	sum(ECL_Undrawn_3) as ECL_Undrawn_3,
	sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1,
	sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
	sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3,
	&SESSIONTIME AS INSRT_PROCESS_TMSTMP,
	&SESSIONTIME AS UPDT_PROCESS_TMSTMP
from &INPATH..RRAP_Rpt_Sub_Extract00_ECL
where DLGD_F='Y'
and CCAR_BASEL_PRD_TP_NM NOT LIKE '%GENW%' 
and CCAR_BASEL_PRD_TP_NM NOT LIKE '%GUAR%'
group by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, PD_BAND,
 		 LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID,PD_BASEL_SEG_NUM,
		PD_BASEL_SEG_ID,INSURER_FLAG, WGHTD_DLGD_RTO, LTV_PERCENTAGE,PERIOD_IND,
		TRNST_NUM,EGL_DEPRTMNT,PRD_ID,/*adding for bcar_ccar combine file */
		RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR,
		PD_90_DAY_F, UNINSURED_FLRD_LGD_RTO,	UNINSURED_DLGD_RTO, CLP_FLAG	/*adding for Basel III */
order by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, PD_BAND, 
 		LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID, LTV_PERCENTAGE,
		TRNST_NUM,EGL_DEPRTMNT,PRD_ID,/*adding for bcar_ccar combine file */
		RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR,
		PD_90_DAY_F, UNINSURED_FLRD_LGD_RTO,	UNINSURED_DLGD_RTO, CLP_FLAG	/*adding for Basel III */
		 ;
quit;


PROC SQL NOPRINT;
INSERT INTO &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SELECT 
legal_entity
,PD_BAND
,CCAR_BASEL_PRD_TP_NM
,MTH_TM_ID
,LGD_BASEL_SEG_NUM
,EAD_BASEL_SEG_NUM
,PD_BASEL_SEG_NUM
,PD_BASEL_SEG_ID
,LGD_BASEL_SEG_ID
,EAD_BASEL_SEG_ID
,DEFAULT_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FLRD_RPTG_RTO
,EAD_FLRD_RPTG_RTO
,crncy_cd
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,TRIM(INSUR_F) as INSUR_F
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
,TRNST_NUM,EGL_DEPRTMNT,PRD_ID/*adding for bcar_ccar combine file */
,RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, ORIG_AMT_LOAN
,PD_90_DAY_F ,UNINSURED_FLRD_LGD_RTO, UNINSURED_DLGD_RTO, CLP_FLAG
,OBLIGORS,
PERIOD_IND,
ECL_Drawn_1,
ECL_Drawn_2,
ECL_Drawn_3,
ECL_Undrawn_1,
ECL_Undrawn_2,
ECL_Undrawn_3,
ECL_Drawn_PostSec_1,
ECL_Drawn_PostSec_2,
ECL_Drawn_PostSec_3
FROM &INPATH..RRAP_Rpt_Extract_Subsidiary_ECL;
QUIT;

/******************PMI SUBSIDIARY****************/
PROC SQL NOPRINT;
INSERT INTO &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI 
SELECT 
basel_acct_id
,legal_entity
,PD_BAND
	,CCAR_BASEL_PRD_TP_NM
	,MTH_TM_ID
	,COALESCE(LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM
	,COALESCE(EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM
	,PD_BASEL_SEG_NUM
	,PD_BASEL_SEG_ID
	,LGD_BASEL_SEG_ID
	,EAD_BASEL_SEG_ID
	,case when PD_FINAL_RPTG_RTO >=1 then 'T' else 'F' end as Default_F format $1.
	,ADJUSTED_OS_BAL_AMT AS BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8
	,case when ADJUSTED_OS_BAL_AMT <=0 then 0 else ADJUSTED_OS_BAL_AMT end as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8
	,case when PD_FINAL_RPTG_RTO >=1 then WGHTD_DLGD_RTO else . end as EXPCTD_LOSS_RTO format 28.8
	,0 AS BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3
	,0 AS AF_ZERO_NET_UNDRAWN_AMT format 17.3
	,0 AS BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3
	,0 AS AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3
	,LGD_FLRD_RPTG_RTO format 28.8
	,0 AS EAD_FLRD_RPTG_RTO format 28.8
	,'CAD' as CRNCY_CD format $10.
	,0 as PRTL_WRITE_OFF_AMT format 17.3
	,'TRUE' as UNCONDTNLY_CNCLBL format $10.
	,INTR_ACCR_AMT as ACCR_INTR_AMT format 17.3
	,&SESSIONTIME AS INSRT_PROCESS_TMSTMP
	,&SESSIONTIME AS INSRT_PROCESS_TMSTMP
	,TRIM(INSURER_FLAG) as INSUR_F
	,WGHTD_DLGD_RTO
	,LTV_PERCENTAGE
	,TRNST_NUM,EGL_DEPRTMNT,PRD_ID /*adding for bcar_ccar combine file */
	,RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, TRANSACTOR_FLAG_QRR, ORIG_AMT_LOAN
	,PD_90_DAY_F ,UNINSURED_FLRD_LGD_RTO, UNINSURED_DLGD_RTO, CLP_FLAG
	,1 as OBLIGORS
	,PERIOD_IND,
	ECL_Drawn_1,
	ECL_Drawn_2,
	ECL_Drawn_3,				
	ECL_Undrawn_1,
	ECL_Undrawn_2,
	ECL_Undrawn_3,
	ECL_Drawn_PostSec_1,
	ECL_Drawn_PostSec_2,
	ECL_Drawn_PostSec_3
from &INPATH..RRAP_Rpt_Sub_Extract00_ECL
where DLGD_F='Y'
and (CCAR_BASEL_PRD_TP_NM LIKE '%GENW%' or CCAR_BASEL_PRD_TP_NM LIKE '%GUAR%')
;
QUIT;


/****UPDATE PREV MONTH'S VALUES TO ZERO ***/
PROC SQL NOPRINT;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET BEFR_ZERO_NET_ADJ_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET AF_ZERO_NET_ADJUSTED_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET BEFORE_ZERO_NET_UNDRAWN_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET AF_ZERO_NET_UNDRAWN_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET PRTL_WRITE_OFF_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET ACCR_INTR_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET OBLIGORS=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL SET LEGAL_ENTITY = (TRIM(LEGAL_ENTITY) || '_PREV') WHERE PERIOD_IND = 'PREV';
QUIT;

/****UPDATE PREV MONTH'S VALUES TO ZERO ***/
PROC SQL NOPRINT;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET BEFR_ZERO_NET_ADJ_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET AF_ZERO_NET_ADJUSTED_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET BEFORE_ZERO_NET_UNDRAWN_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET AF_ZERO_NET_UNDRAWN_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET PRTL_WRITE_OFF_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET ACCR_INTR_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET OBLIGORS=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI SET LEGAL_ENTITY = (TRIM(LEGAL_ENTITY) || '_PREV') WHERE PERIOD_IND = 'PREV';
QUIT;

PROC SQL NOPRINT;
CONNECT USING NZRRAP AS NZCON;
EXECUTE(DELETE FROM &net_db..BASEL_CCAR_EXPSR_FACT_ACAP)BY NZCON  ;
quit;


proc sql;
connect using NZRRAP as nzcon;
INSERT INTO NZRRAP.BASEL_CCAR_EXPSR_FACT_ACAP 
(LEGAL_ENTITY,
PD_BAND,
CCAR_BASEL_PRD_TP_NM,
MTH_TM_ID,
LGD_BASEL_SEG_NUM,
EAD_BASEL_SEG_NUM,
PD_BASEL_SEG_NUM,
PD_BASEL_SEG_ID,
LGD_BASEL_SEG_ID,
EAD_BASEL_SEG_ID,
PD_90_DAY_F,
BEFR_ZERO_NET_ADJ_OS_BAL_AMT,
AF_ZERO_NET_ADJUSTED_OS_BAL_AMT,
EXPCTD_LOSS_RTO,
BEFORE_ZERO_NET_UNDRAWN_AMT,
AF_ZERO_NET_UNDRAWN_AMT,
BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT,
AF_ZERO_NET_ALWBL_CR_LOSS_AMT,
LGD_FLRD_RPTG_RTO,
EAD_FLRD_RPTG_RTO,
CRNCY_CD,
PRTL_WRITE_OFF_AMT,
UNCONDTNLY_CNCLBL,
ACCR_INTR_AMT,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP,
INSUR_F,
WGHTD_DLGD_RTO,
LTV_PERCENTAGE,
TRNST_NUM,EGL_DEPRTMNT,PRD_ID,/*adding for bcar_ccar combine file */
OBLIGORS,
PERIOD_IND,
ECL_Drawn_1,
ECL_Drawn_2,
ECL_Drawn_3,
ECL_Undrawn_1,
ECL_Undrawn_2,
ECL_Undrawn_3,
ECL_Drawn_PostSec_1,
ECL_Drawn_PostSec_2,
ECL_Drawn_PostSec_3,
RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, ORIG_AMT_LOAN, TRANSACTOR_FLAG_QRR,
DEFAULT_F, UNINSURED_FLRD_LGD_RTO, UNINSURED_DLGD_RTO, CLP_FLAG,
BASEL_ACCT_ID, PMI
)
select LEGAL_ENTITY,
PD_BAND,
CCAR_BASEL_PRD_TP_NM,
MTH_TM_ID,
LGD_BASEL_SEG_NUM,
EAD_BASEL_SEG_NUM,
PD_BASEL_SEG_NUM,
PD_BASEL_SEG_ID,
LGD_BASEL_SEG_ID,
EAD_BASEL_SEG_ID,
PD_90_DAY_F,
BEFR_ZERO_NET_ADJ_OS_BAL_AMT,
AF_ZERO_NET_ADJUSTED_OS_BAL_AMT,
EXPCTD_LOSS_RTO,
BEFORE_ZERO_NET_UNDRAWN_AMT,
AF_ZERO_NET_UNDRAWN_AMT,
BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT,
AF_ZERO_NET_ALWBL_CR_LOSS_AMT,
LGD_FLRD_RPTG_RTO,
EAD_FLRD_RPTG_RTO,
CRNCY_CD,
PRTL_WRITE_OFF_AMT,
UNCONDTNLY_CNCLBL,
ACCR_INTR_AMT,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP,
INSUR_F,
WGHTD_DLGD_RTO,
LTV_PERCENTAGE,
TRNST_NUM,EGL_DEPRTMNT,PRD_ID,/*adding for bcar_ccar combine file */
OBLIGORS,
PERIOD_IND,
ECL_Drawn_1,
ECL_Drawn_2,
ECL_Drawn_3,
ECL_Undrawn_1,
ECL_Undrawn_2,
ECL_Undrawn_3,
ECL_Drawn_PostSec_1,
ECL_Drawn_PostSec_2,
ECL_Drawn_PostSec_3,
RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, ORIG_AMT_LOAN, TRANSACTOR_FLAG_QRR,
DEFAULT_F, UNINSURED_FLRD_LGD_RTO, UNINSURED_DLGD_RTO, CLP_FLAG, 0, 'N'
 FROM &INPATH..BASEL_CCAR_EXPSR_FACT_ECL;
disconnect from nzcon;

quit;

/* add data from &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI to BASEL_CCAR_EXPSR_FACT_ACAP_B */
proc sql;
connect using NZRRAP as nzcon;
INSERT INTO NZRRAP.BASEL_CCAR_EXPSR_FACT_ACAP 
(LEGAL_ENTITY,
PD_BAND,
CCAR_BASEL_PRD_TP_NM,
MTH_TM_ID,
LGD_BASEL_SEG_NUM,
EAD_BASEL_SEG_NUM,
PD_BASEL_SEG_NUM,
PD_BASEL_SEG_ID,
LGD_BASEL_SEG_ID,
EAD_BASEL_SEG_ID,
PD_90_DAY_F,
BEFR_ZERO_NET_ADJ_OS_BAL_AMT,
AF_ZERO_NET_ADJUSTED_OS_BAL_AMT,
EXPCTD_LOSS_RTO,
BEFORE_ZERO_NET_UNDRAWN_AMT,
AF_ZERO_NET_UNDRAWN_AMT,
BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT,
AF_ZERO_NET_ALWBL_CR_LOSS_AMT,
LGD_FLRD_RPTG_RTO,
EAD_FLRD_RPTG_RTO,
CRNCY_CD,
PRTL_WRITE_OFF_AMT,
UNCONDTNLY_CNCLBL,
ACCR_INTR_AMT,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP,
INSUR_F,
WGHTD_DLGD_RTO,
LTV_PERCENTAGE,
TRNST_NUM,EGL_DEPRTMNT,PRD_ID,/*adding for bcar_ccar combine file */
OBLIGORS,
PERIOD_IND,
ECL_Drawn_1,
ECL_Drawn_2,
ECL_Drawn_3,
ECL_Undrawn_1,
ECL_Undrawn_2,
ECL_Undrawn_3,
ECL_Drawn_PostSec_1,
ECL_Drawn_PostSec_2,
ECL_Drawn_PostSec_3,
RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, ORIG_AMT_LOAN, TRANSACTOR_FLAG_QRR,
DEFAULT_F, UNINSURED_FLRD_LGD_RTO, UNINSURED_DLGD_RTO, CLP_FLAG,
BASEL_ACCT_ID, PMI
)
select LEGAL_ENTITY,
PD_BAND,
CCAR_BASEL_PRD_TP_NM,
MTH_TM_ID,
LGD_BASEL_SEG_NUM,
EAD_BASEL_SEG_NUM,
PD_BASEL_SEG_NUM,
PD_BASEL_SEG_ID,
LGD_BASEL_SEG_ID,
EAD_BASEL_SEG_ID,
PD_90_DAY_F,
BEFR_ZERO_NET_ADJ_OS_BAL_AMT,
AF_ZERO_NET_ADJUSTED_OS_BAL_AMT,
EXPCTD_LOSS_RTO,
BEFORE_ZERO_NET_UNDRAWN_AMT,
AF_ZERO_NET_UNDRAWN_AMT,
BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT,
AF_ZERO_NET_ALWBL_CR_LOSS_AMT,
LGD_FLRD_RPTG_RTO,
EAD_FLRD_RPTG_RTO,
CRNCY_CD,
PRTL_WRITE_OFF_AMT,
UNCONDTNLY_CNCLBL,
ACCR_INTR_AMT,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP,
INSUR_F,
WGHTD_DLGD_RTO,
LTV_PERCENTAGE,
TRNST_NUM,EGL_DEPRTMNT,PRD_ID,/*adding for bcar_ccar combine file */
OBLIGORS,
PERIOD_IND,
ECL_Drawn_1,
ECL_Drawn_2,
ECL_Drawn_3,
ECL_Undrawn_1,
ECL_Undrawn_2,
ECL_Undrawn_3,
ECL_Drawn_PostSec_1,
ECL_Drawn_PostSec_2,
ECL_Drawn_PostSec_3,
RNTL_PRPTY_F, CURRENCY_MISMATCH_F, TOT_EXPSR_ABOVE_1500K_LMT_F, ORIG_AMT_LOAN, TRANSACTOR_FLAG_QRR,
DEFAULT_F, UNINSURED_FLRD_LGD_RTO, UNINSURED_DLGD_RTO, CLP_FLAG, Basel_acct_id, 'Y'
 FROM &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_PMI;
disconnect from nzcon;

quit;

PROC SQL;
	CONNECT TO DB2 AS IIASCON(DATABASE=BLUDBPRD authdomain="IIAS_Auth");
	EXECUTE(DROP TABLE IF EXISTS &net_db..BASEL_CCAR_EXPSR_FACT_ACAP_temp)BY IIASCON;
QUIT;

data NZRRAP.BASEL_CCAR_EXPSR_FACT_ACAP_temp;
	set NZRRAP.BASEL_CCAR_EXPSR_FACT_ACAP;
	if PERIOD_IND = 'PREV' then LEGAL_ENTITY = (TRIM(LEGAL_ENTITY) || '_PREV');
	else LEGAL_ENTITY = LEGAL_ENTITY;
run;

proc sql;
	drop table NZRRAP.BASEL_CCAR_EXPSR_FACT_ACAP;
quit;

data NZRRAP.BASEL_CCAR_EXPSR_FACT_ACAP;
	set NZRRAP.BASEL_CCAR_EXPSR_FACT_ACAP_temp;
run;

proc sql;
	drop table NZRRAP.BASEL_CCAR_EXPSR_FACT_ACAP_temp;
quit;


%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Extract00_ECL RRAP_Rpt_Extract00_ECL1 RRAP_Rpt_Extract_KS_ECL RRAP_Rpt_Extract_MOR_ECL RRAP_Rpt_Extract_SPL_ECL _temp_aggr_base_ECL);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=_temp_aggr_NCR_balance_ECL _temp_dir_NCR_ECL _temp_acct_balance_ECL _temp_acct_prorated_loss_ECL _temp_aggr_ACL_ECL);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Extract_KS_Final_ECL RRAP_Rpt_Extract_MOR_Final_ECL RRAP_Rpt_Extract_SPL_Final_ECL RRAP_Rpt_Extract_AllBank_ECL);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Sub_Extract00_ECL RRAP_Rpt_Sub_Extract00_ECL1 _temp_aggr_base_ECL _temp_aggr_balance_ECL _temp_GL_ACL_balance_ECL _temp_acct_balance_ECL);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=_temp_acct_prorated_loss_ECL _temp_aggr_ACL_ECL RRAP_Rpt_Extract_Subsidiary_ECL BASEL_CCAR_EXPSR_FACT_ECL_MISS);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Extract_TNG_ECL RRAP_Rpt_Extract_TNG_Final_ECL);
