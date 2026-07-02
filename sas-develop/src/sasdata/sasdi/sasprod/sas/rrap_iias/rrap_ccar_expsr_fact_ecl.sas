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

/****************************************************************************************/
/****************************************************************************************/
/********************************Changes for ECL*****************************************/
/****************************************************************************************/
/****************************************************************************************/
/** needs to be changed to original table and check the condition**/
data &INPATH..BASEL_ACCT_DIM;
	SET NZRRAP.BASEL_ACCT_DIM;
	/*where 1 = 0;*/
	run;

data &INPATH..BASEL_SUBSIDIARY_ACL_LKP;
		SET &db..BASEL_SUBSIDIARY_ACL_LKP;
RUN;

data &INPATH..BASEL_CCAR_BUS_AGGRTD_FACT;
		SET &db..BASEL_CCAR_BUS_AGGRTD_FACT;
		WHERE mth_tm_id = (&MTH_TM_ID.);
RUN;

/* Get table from NZ into SAS */
data &INPATH..basel_ifrs9_ecl_profile_fact;
	SET NZRRAP.basel_ifrs9_ecl_profile_fact;
/*	where 1 = 0;*/
WHERE MTH_TM_ID = (&MTH_TM_ID.-40);
run;

/* Get last month's IF table */
data &lib..BL_INS_FACT_PREV_ECL;
set &DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT;
where mth_tm_id = (&MTH_TM_ID. - 40); 
RUN;

data &lib..BL_INS_FACT_CURR_ECL;
set &DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT;
	where mth_tm_id = (&MTH_TM_ID.); 
RUN;

data &lib..BL_INS_FACT_CURR;
set &DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT;
where mth_tm_id = (&MTH_TM_ID.) and (not missing(pd_basel_seg_num) and not missing(lgd_basel_seg_num));
RUN;


/*proc sql noprint;
drop table &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_MISS;
    CREATE table &INPATH..BASEL_CCAR_EXPSR_FACT_ECL_MISS as 
select 'DOM-BANK-BNS_MISS' as LEGAL_ENTITY , a.PD_BAND, a.CCAR_BASEL_PRD_TP_NM, a.MTH_TM_ID ,
0 as LGD_BASEL_SEG_NUM,
0 as EAD_BASEL_SEG_NUM,
0 as PD_BASEL_SEG_NUM,
0 as PD_BASEL_SEG_ID,
0 as LGD_BASEL_SEG_ID,
0 as EAD_BASEL_SEG_ID,
' ' as PD_90_DAY_F,
0 as BEFR_ZERO_NET_ADJ_OS_BAL_AMT,
0 as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT,
0 as EXPCTD_LOSS_RTO,
0 as BEFORE_ZERO_NET_UNDRAWN_AMT,
0 as AF_ZERO_NET_UNDRAWN_AMT,
0 as BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT,
0 as AF_ZERO_NET_ALWBL_CR_LOSS_AMT,
0 as LGD_FINAL_RPTG_RTO,
0 as EAD_FINAL_RPTG_RTO,
'CAD' as CRNCY_CD,
0 as PRTL_WRITE_OFF_AMT,
' ' as UNCONDTNLY_CNCLBL,
0 as ACCR_INTR_AMT,
&SESSIONTIME  as INSRT_PROCESS_TMSTMP,
&SESSIONTIME as UPDT_PROCESS_TMSTMP,
' ' as INSUR_F,
0 as WGHTD_DLGD_RTO,
' '  as  LTV_PERCENTAGE,
count(distinct c.acct_num) as OBLIGORS,
'MISS' as PERIOD_IND  ,
sum(case when c.final_ecl_stage = 1 then c.final_ecl_cad_drawn else 0 end) as ECL_Drawn_1,
sum(case when c.final_ecl_stage = 2 then c.final_ecl_cad_drawn else 0 end) as ECL_Drawn_2,
sum(case when c.final_ecl_stage = 3 then c.final_ecl_cad_drawn else 0 end) as ECL_Drawn_3,                           
sum(case when c.final_ecl_stage = 1 then c.final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_1,
sum(case when c.final_ecl_stage = 2 then c.final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_2,
sum(case when c.final_ecl_stage = 3 then c.final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_3,
sum(case when c.final_ecl_stage = 1 then c.final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_1,
sum(case when c.final_ecl_stage = 2 then c.final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_2,
sum(case when c.final_ecl_stage = 3 then c.final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_3
from (select * from &lib..BL_INS_FACT_CURR_ECL where substr(CCAR_BASEL_PRD_TP_NM,1,1) between 'A' and 'Z'
union
select * from &lib..BL_INS_FACT_PREV_ECL where substr(CCAR_BASEL_PRD_TP_NM,1,1) between 'A' and 'Z' and basel_acct_id in 
(select basel_acct_id from &lib..BL_INS_FACT_CURR_ECL where substr(CCAR_BASEL_PRD_TP_NM,1,1) not between 'A' and 'Z')) a 
inner join &INPATH..BASEL_ACCT_DIM b on a.basel_acct_id = b.basel_acct_id 
inner join ( select distinct ACCT_NUM,CPP_ENTITY_FOLIO_CD,CPP_PRD_FOLIO_CD,CPP_QUALI_SUB_CD,CPP_QUANTI_SUB_CD,
PIT_STAT_CD,STG3_IND,OS_BAL_AMT,FINAL_ECL_STAGE,FINAL_ECL_CAD,FINAL_ECL_CAD_DRAWN,FINAL_ECL_CAD_UNDRAWN,CRNT_AUTH_LMT_AMT,UNDRAWN_AMT,SCORED_UNSCORED_IND,MTH_TM_ID,SRC_SYS_CD,FINAL_ECL_CAD_DRAWN_POSTSEC
  from &INPATH..basel_ifrs9_ecl_profile_fact) c on b.ACCT_NUM = c.acct_num and b.acct_num in (
select distinct acct_num from  &INPATH..basel_ifrs9_ecl_profile_fact where acct_num not in (
select distinct b.acct_num as  acct_num from &lib..BL_INS_FACT_CURR a  inner join &INPATH..basel_ifrs9_ecl_profile_fact b
on a.basel_acct_id = b.basel_acct_id
where a.SRC_SYS_CD in ('TNG-MOR','KS','MOR','SPL') and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N'
                     and a.PIT_STAT_CD in ('CUR','DEF')
                     and a.TRNST_EXCLSN_F='N' 
                     and a.DLGD_F='N'
union
select distinct b.acct_num  as  acct_num from &lib..BL_INS_FACT_CURR  a  inner join &INPATH..basel_ifrs9_ecl_profile_fact b
on a.basel_acct_id = b.basel_acct_id
where a.SRC_SYS_CD in ('TNG-MOR','KS','MOR','SPL') and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N'
                    and a.PIT_STAT_CD in ('CUR','DEF')
                     and a.TRNST_EXCLSN_F='N' 
                     and a.DLGD_F='Y'))
group by a.LEGAL_ENTITY,a.PD_BAND,a.CCAR_BASEL_PRD_TP_NM, a.MTH_TM_ID , PERIOD_IND  ;
quit;*/



/* Get accounts from current month's Instrument Fact Table */
PROC SQL;
	DROP TABLE &INPATH..RRAP_Rpt_Extract00_ECL_CURR;
	CREATE table &INPATH..RRAP_Rpt_Extract00_ECL_CURR as 
		select 	b.basel_acct_id, 
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
 FROM &INPATH..basel_ifrs9_ecl_profile_fact as b INNER JOIN &lib..BL_INS_FACT_CURR_ECL as A
 ON b.basel_acct_id = a.basel_acct_id
;
QUIT;


/* Get accounts from previous month's Instrument Fact Table */
/************** Need to ZERO out the fields********************/

/*** Old Logic ******
PROC SQL;
	DROP TABLE &INPATH..RRAP_Rpt_Extract00_ECL_Prev;
	CREATE table &INPATH..RRAP_Rpt_Extract00_ECL_Prev as 
		select 	b.basel_acct_id,
				'PREV' as PERIOD_IND,
				(case when b.final_ecl_stage = 1 then b.final_ecl_cad_drawn else 0 end) as ECL_Drawn_1,
				(case when b.final_ecl_stage = 2 then b.final_ecl_cad_drawn else 0 end) as ECL_Drawn_2,
				(case when b.final_ecl_stage = 3 then b.final_ecl_cad_drawn else 0 end) as ECL_Drawn_3,				
				(case when b.final_ecl_stage = 1 then b.final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_1,
				(case when b.final_ecl_stage = 2 then b.final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_2,
				(case when b.final_ecl_stage = 3 then b.final_ecl_cad_undrawn else 0 end) as ECL_Undrawn_3,
				(case when b.final_ecl_stage = 1 then b.final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_1,
				(case when b.final_ecl_stage = 2 then b.final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_2,
				(case when b.final_ecl_stage = 3 then b.final_ecl_cad_drawn_postsec else 0 end) as ECL_Drawn_PostSec_3
 FROM &INPATH..basel_ifrs9_ecl_profile_fact as b WHERE  B.BASEL_ACCT_ID IN  (
SELECT basel_acct_id FROM &lib..BL_INS_FACT_PREV_ECL where  BASEL_ACCT_ID NOT IN 
 (SELECT BASEL_ACCT_ID FROM &lib..BL_INS_FACT_CURR_ECL)
)
;
QUIT;***/

PROC SQL;
	DROP TABLE &INPATH..RRAP_Rpt_Extract00_ECL_Prev;
	CREATE table &INPATH..RRAP_Rpt_Extract00_ECL_Prev as 
		select 	basel_acct_id,
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
 FROM &INPATH..basel_ifrs9_ecl_profile_fact  WHERE  
acct_num IN  (select b.acct_num from &lib..BL_INS_FACT_PREV_ECL a inner join &INPATH..BASEL_ACCT_DIM b
on a.basel_acct_id = b.basel_acct_id) and 
acct_num not IN (select b.acct_num from &lib..BL_INS_FACT_CURR_ECL a inner join &INPATH..BASEL_ACCT_DIM b
on a.basel_acct_id = b.basel_acct_id) and 
basel_acct_id in (select basel_acct_id from &lib..BL_INS_FACT_PREV_ECL)
;
QUIT;

PROC SQL;
	DROP TABLE &INPATH..RRAP_Rpt_Extract00_ECL;
	CREATE table &INPATH..RRAP_Rpt_Extract00_ECL as 
		select 	a.ACCT_NUM ,
				a.ADJUSTED_OS_BAL_AMT,
				a.CCAR_BASEL_PRD_TP_NM,
				a.CONS_DFT_MTH_CNT,
				a.EAD_BASEL_SEG_ID,
				(case when a.EAD_BASEL_SEG_NUM eq . then -1 else a.EAD_BASEL_SEG_NUM end ) as EAD_BASEL_SEG_NUM,
				a.EAD_FINAL_RPTG_RTO,
				a.LEGAL_ENTITY,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.LGD_BASEL_SEG_ID 
					when a.SRC_SYS_CD in ('SPL')  and a.LGD_BASEL_SEG_ID eq . THEN -1
					else a.LGD_BASEL_SEG_ID end) as  LGD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') and a.LGD_BASEL_SEG_NUM eq . then -1
					when a.SRC_SYS_CD in ('SPL') and a.LGD_BASEL_SEG_NUM eq . then -1
					else a.LGD_BASEL_SEG_NUM end) as LGD_BASEL_SEG_NUM,
				a.LGD_FINAL_RPTG_RTO,
				a.MTH_TM_ID,
				a.NCR_EXPSR_CL_KEY_VAL,
				a.PD_BAND,
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
				a.UNQ_ACCT_ID, A.SCRTY_TP_DESC ,A.BASEL_PRD_TP_CD,
				(case when A.SRC_SYS_CD = 'KS' AND A.INSUR_F='YES' THEN 'YES'
					when A.SRC_SYS_CD in ('MOR','TNG-MOR') AND A.SCRTY_TP_DESC='Insured' THEN 'YES' ELSE 'NO' end ) as INSURER_FLAG 
				,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, a.LTV_PERCENTAGE, 
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
 FROM &lib..BL_INS_FACT_CURR as A LEFT JOIN  &INPATH..RRAP_Rpt_Extract00_ECL_CURR as B
 ON a.basel_acct_id = b.basel_acct_id
		where a.SRC_SYS_CD in ('TNG-MOR','KS','MOR','SPL') and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N'
  			and a.PIT_STAT_CD in ('CUR','DEF')
			and a.TRNST_EXCLSN_F='N'
			and a.DLGD_F='N';
QUIT;

PROC SQL NOPRINT;
UPDATE &INPATH..RRAP_Rpt_Extract00_ECL 
SET PERIOD_IND = 'CURR' WHERE PERIOD_IND IS NULL ;
QUIT;

PROC SQL;
	INSERT INTO &INPATH..RRAP_Rpt_Extract00_ECL
select 	a.ACCT_NUM ,
				a.ADJUSTED_OS_BAL_AMT,
				a.CCAR_BASEL_PRD_TP_NM,
				a.CONS_DFT_MTH_CNT,
				a.EAD_BASEL_SEG_ID,
				(case when a.EAD_BASEL_SEG_NUM eq . then -1 else a.EAD_BASEL_SEG_NUM end ) as EAD_BASEL_SEG_NUM,
				a.EAD_FINAL_RPTG_RTO,
				a.LEGAL_ENTITY,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.LGD_BASEL_SEG_ID 
					when a.SRC_SYS_CD in ('SPL')  and a.LGD_BASEL_SEG_ID eq . THEN -1
					else a.LGD_BASEL_SEG_ID end) as  LGD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') and a.LGD_BASEL_SEG_NUM eq . then -1
					when a.SRC_SYS_CD in ('SPL') and a.LGD_BASEL_SEG_NUM eq . then -1
					else a.LGD_BASEL_SEG_NUM end) as LGD_BASEL_SEG_NUM,
				a.LGD_FINAL_RPTG_RTO,
				a.MTH_TM_ID ,
				a.NCR_EXPSR_CL_KEY_VAL,
				a.PD_BAND,
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
				a.UNQ_ACCT_ID, A.SCRTY_TP_DESC ,A.BASEL_PRD_TP_CD,
				(case when A.SRC_SYS_CD = 'KS' AND A.INSUR_F='YES' THEN 'YES'
					when A.SRC_SYS_CD in ('MOR','TNG-MOR') AND A.SCRTY_TP_DESC='Insured' THEN 'YES' ELSE 'NO' end ) as INSURER_FLAG 
				,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, a.LTV_PERCENTAGE, 
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
 FROM &lib..BL_INS_FACT_PREV_ECL as A inner JOIN  &INPATH..RRAP_Rpt_Extract00_ECL_Prev as B
 ON a.basel_acct_id = b.basel_acct_id
		where  a.SRC_SYS_CD in ('TNG-MOR','KS','MOR','SPL') and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N'
  			and a.PIT_STAT_CD in ('CUR','DEF')
			and a.TRNST_EXCLSN_F='N'
			and a.DLGD_F='N';
QUIT;

data &INPATH..RRAP_Rpt_Extract_KS_ECL
	&INPATH..RRAP_Rpt_Extract_MOR_ECL
	&INPATH..RRAP_Rpt_Extract_SPL_ECL
	&INPATH..RRAP_Rpt_Extract_TNG_ECL;
	set &INPATH..RRAP_Rpt_Extract00_ECL;


	if SRC_SYS_CD in ('KS') then output &INPATH..RRAP_Rpt_Extract_KS_ECL;
	else if SRC_SYS_CD in ('MOR') then output &INPATH..RRAP_Rpt_Extract_MOR_ECL;
	else if SRC_SYS_CD in ('SPL') then output &INPATH..RRAP_Rpt_Extract_SPL_ECL;
	else if SRC_SYS_CD in ('TNG-MOR') then output &INPATH..RRAP_Rpt_Extract_TNG_ECL;
	else delete;
run;

%macro getRRAPreportingExtract_ECL(SRC_SYS_CD=, indata=, NCRdata=, outdata=);
	/*Calculate first set of transformation rules using the base data.*/
	proc sql noprint;
		create table &INPATH.._temp_aggr_base_ECL as
			select 	PD_BAND,
				CCAR_BASEL_PRD_TP_NM,
				MTH_TM_ID,
				SRC_SYS_CD,
				LGD_BASEL_SEG_NUM,
				EAD_BASEL_SEG_NUM, 
				PD_BASEL_SEG_NUM, 
				PD_BASEL_SEG_ID, 
				LGD_BASEL_SEG_ID,
				EAD_BASEL_SEG_ID,
				INSURER_FLAG,
			case 
				when mean(PD_FINAL_RPTG_RTO) >=1 then 'T' 
				else 'F' 
			end 
		as PD_90_DAY_F 
			format $1.,
			%if &SRC_SYS_CD=KS or &SRC_SYS_CD=MOR or &SRC_SYS_CD=TNG %then

			%do;
				sum(ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
				sum
				(case 
					when ADJUSTED_OS_BAL_AMT <=0 then 0 
					else ADJUSTED_OS_BAL_AMT 
				end )
				as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
			%end;
%else %if &SRC_SYS_CD=SPL %then
	%do;
		sum(ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
		sum
		(case 
			when (ADJUSTED_OS_BAL_AMT) <=0 then 0 
			else (ADJUSTED_OS_BAL_AMT) 
		end)
		as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
	%end;

	case 
		when mean(PD_FINAL_RPTG_RTO) >=1 then max(WGHTD_DLGD_RTO) 
		else . 
	end 														
as EXPCTD_LOSS_RTO format 28.8,
	%if &SRC_SYS_CD=KS %then

		%do;
			sum(BEFORE_ZERO_NET_UNDRAWN_AMT) as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
			sum(AF_ZERO_NET_UNDRAWN_AMT) as AF_ZERO_NET_UNDRAWN_AMT format 17.3,
		%end;
%else %if &SRC_SYS_CD ne KS  %then
	%do;
		0 as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
		0 as AF_ZERO_NET_UNDRAWN_AMT format 17.3,
	%end;

	max(LGD_FINAL_RPTG_RTO) as LGD_FINAL_RPTG_RATIO format 28.8,
	%if &SRC_SYS_CD=KS %then

		%do;
			case 
				when mean(PD_FINAL_RPTG_RTO) >=1 then 0 
				else max(EAD_FINAL_RPTG_RTO) 
			end 
		as EAD_FINAL_RPTG_RATIO format 28.8
		%end;
%else
	%do;
		0 as EAD_FINAL_RPTG_RATIO format 28.8
	%end;

	,AVG(WGHTD_DLGD_RTO) as WGHTD_DLGD_RTO, LTV_PERCENTAGE, count(1) as OBLIGORS,
	PERIOD_IND,
	sum(ECL_Drawn_1) as ECL_Drawn_1,
	sum(ECL_Drawn_2) as ECL_Drawn_2,
	sum(ECL_Drawn_3) as ECL_Drawn_3,	
	sum(ECL_Undrawn_1) as ECL_Undrawn_1,
	sum(ECL_Undrawn_2) as ECL_Undrawn_2,
	sum(ECL_Undrawn_3) as ECL_Undrawn_3,
	sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1,
	sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
	sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3
	from &indata.
	where DLGD_F='N'
		group by PD_BAND, CCAR_BASEL_PRD_TP_NM, MTH_TM_ID, SRC_SYS_CD, 
			LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, PD_BASEL_SEG_NUM, 
			PD_BASEL_SEG_ID, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID ,INSURER_FLAG, LTV_PERCENTAGE, PERIOD_IND
	order by PD_BAND, CCAR_BASEL_PRD_TP_NM, MTH_TM_ID, SRC_SYS_CD, 
		LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, PD_BASEL_SEG_NUM, 
		PD_BASEL_SEG_ID, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID ,INSURER_FLAG, LTV_PERCENTAGE,PERIOD_IND
	;
	quit;


/*step 1 of last transformation*/
/*Get base NCR data - aggregate level irrespective of src_sys_code; */
/*So the source dataset is the primary dataset which ensures the same aggr_NCR_balance is available for all SRC_SYS_CD */
proc sql noprint;
create table &INPATH.._temp_aggr_NCR_balance_ECL as
select 	MTH_TM_ID,
NCR_EXPSR_CL_KEY_VAL,
/*INSURER_FLAG,*/
sum(
	case 	when (SRC_SYS_CD='KS' or SRC_SYS_CD='MOR') then ADJUSTED_OS_BAL_AMT
when SRC_SYS_CD='SPL' then ADJUSTED_OS_BAL_AMT
	end
)
as total_outstanding_balance,
sum(case when (SRC_SYS_CD='KS' or SRC_SYS_CD='MOR') THEN CASE WHEN ADJUSTED_OS_BAL_AMT < 0 then 0 ELSE ADJUSTED_OS_BAL_AMT END
 when SRC_SYS_CD='SPL' THEN CASE WHEN (ADJUSTED_OS_BAL_AMT) < 0 THEN 0 ELSE (ADJUSTED_OS_BAL_AMT) END
end
)
as total_outstanding_balance_net,
PERIOD_IND,
	sum(ECL_Drawn_1) as ECL_Drawn_1,
	sum(ECL_Drawn_2) as ECL_Drawn_2,
	sum(ECL_Drawn_3) as ECL_Drawn_3,	
	sum(ECL_Undrawn_1) as  ECL_Undrawn_1,
	sum(ECL_Undrawn_2) as ECL_Undrawn_2,
	sum(ECL_Undrawn_3) as ECL_Undrawn_3,
	sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1 ,
	sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
	sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3
from &INPATH..RRAP_Rpt_Extract00_ECL
where PD_FINAL_RPTG_RTO >=1
and SRC_SYS_CD in ('KS','MOR','SPL')
group by MTH_TM_ID, NCR_EXPSR_CL_KEY_VAL ,PERIOD_IND
/*,INSURER_FLAG*/
order by MTH_TM_ID, NCR_EXPSR_CL_KEY_VAL,PERIOD_IND
;
quit;

/*step 2a of last transformation*/
/*Get NCR data - NCR Key Value level;*/
proc sql noprint;
create table &INPATH.._temp_dir_NCR_ECL as
select	distinct	
MTH_TM_ID, 
NCR_EXPSR_CL_KEY,
CR_LOSS_ALWNC_AMT
from &NCRdata. where MTH_TM_ID=&MTH_TM_ID
;
quit;

/*step 2b and 2c of last transformation rule*/
/*Get balance data - account level;*/
proc sql noprint;
create table &INPATH.._temp_acct_balance_ECL as

select 	MTH_TM_ID,
SRC_SYS_CD,
CCAR_BASEL_PRD_TP_NM, 	
PD_BAND,
PD_BASEL_SEG_NUM, 
EAD_BASEL_SEG_NUM, 
LGD_BASEL_SEG_NUM, 
ADJUSTED_OS_BAL_AMT,
UNADJUSTED_ADD_ON_BAL_AMT,
CONS_DFT_MTH_CNT,
NCR_EXPSR_CL_KEY_VAL,
ACCT_NUM,
INSURER_FLAG, LTV_PERCENTAGE,
case 
	when SRC_SYS_CD in ('KS','MOR') then ADJUSTED_OS_BAL_AMT
	when SRC_SYS_CD in ('SPL') then ADJUSTED_OS_BAL_AMT
	else 0
end as outstanding_balance, 
case 
when SRC_SYS_CD in ('KS','MOR') and ADJUSTED_OS_BAL_AMT>0 then  
ADJUSTED_OS_BAL_AMT
when SRC_SYS_CD in ('KS','MOR') and ADJUSTED_OS_BAL_AMT<=0 then 0
when SRC_SYS_CD in ('SPL') and 
(ADJUSTED_OS_BAL_AMT)>0 then 
(ADJUSTED_OS_BAL_AMT)
when SRC_SYS_CD in ('SPL') and 
(ADJUSTED_OS_BAL_AMT)<=0 then 0
	else 0
end as outstanding_balance_after_net,
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
from &INPATH..RRAP_Rpt_Extract00_ECL
where PD_FINAL_RPTG_RTO >=1
order by MTH_TM_ID, SRC_SYS_CD,CCAR_BASEL_PRD_TP_NM, PD_BAND, 
PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM, NCR_EXPSR_CL_KEY_VAL, 
ACCT_NUM,PERIOD_IND
;
quit;

/*step 3 of last transformation rule*/
/*Combine account level PRORATED_CR_LOSS_ALLOW_AMT;*/
proc sql noprint;
	create table &INPATH.._temp_acct_prorated_loss_ECL as
		select 	a.*, 
			b.total_outstanding_balance,
			b.total_outstanding_balance_net,
			c.CR_LOSS_ALWNC_AMT,
			(a.outstanding_balance/b.total_outstanding_balance)*c.CR_LOSS_ALWNC_AMT as 
			PRORATED_CR_LOSS_ALLOW_AMT,
			(a.outstanding_balance_after_net/b.total_outstanding_balance_net)*c.CR_LOSS_ALWNC_AMT 
		as PRORATED_CR_LOSS_ALLOW_AMT_AFTER 
			from &INPATH.._temp_acct_balance_ECL as a 
				left join &INPATH.._temp_aggr_NCR_balance_ECL as b 
					on a.MTH_TM_ID=b.MTH_TM_ID and a.NCR_EXPSR_CL_KEY_VAL=b.NCR_EXPSR_CL_KEY_VAL 
				left join &INPATH.._temp_dir_NCR_ECL as c
					on a.MTH_TM_ID=c.MTH_TM_ID and a.NCR_EXPSR_CL_KEY_VAL=c.NCR_EXPSR_CL_KEY 
				order by MTH_TM_ID, SRC_SYS_CD, CCAR_BASEL_PRD_TP_NM, PD_BAND,
					PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM, NCR_EXPSR_CL_KEY_VAL, 
					ACCT_NUM
	;
quit;

/*step 4 of last transformation rule*/
/*Calculate ACL at CCAR aggregated level;*/
proc sql noprint;
create table &INPATH.._temp_aggr_ACL_ECL as
select  MTH_TM_ID,
SRC_SYS_CD,
CCAR_BASEL_PRD_TP_NM,
PD_BAND,
PD_BASEL_SEG_NUM, 
EAD_BASEL_SEG_NUM, 
LGD_BASEL_SEG_NUM,INSURER_FLAG,LTV_PERCENTAGE,
sum(PRORATED_CR_LOSS_ALLOW_AMT) as ACL,
sum(PRORATED_CR_LOSS_ALLOW_AMT_AFTER) as After_Netting_ACL ,
PERIOD_IND,
	sum(ECL_Drawn_1) as ECL_Drawn_1,
	sum(ECL_Drawn_2) as ECL_Drawn_2,
	sum(ECL_Drawn_3) as ECL_Drawn_3,	
	sum(ECL_Undrawn_1) as ECL_Undrawn_1 ,
	sum(ECL_Undrawn_2) as ECL_Undrawn_2,
	sum(ECL_Undrawn_3) as ECL_Undrawn_3,
	sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1,
	sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
	sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3
from &INPATH.._temp_acct_prorated_loss_ECL
group by MTH_TM_ID,SRC_SYS_CD, CCAR_BASEL_PRD_TP_NM, PD_BAND,INSURER_FLAG, LTV_PERCENTAGE,
PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM,PERIOD_IND
order by MTH_TM_ID, SRC_SYS_CD, CCAR_BASEL_PRD_TP_NM, PD_BAND, 
PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM,PERIOD_IND
;
quit;

/*Generate final output;*/
proc sql noprint;
create table &outdata. as
select 'DOM-BANK-BNS' as LEGAL_ENTITY format $40.,
a.PD_BAND,
a.CCAR_BASEL_PRD_TP_NM,
a.MTH_TM_ID,
a.SRC_SYS_CD,
COALESCE(a.LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM,
COALESCE(a.EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM,
a.INSURER_FLAG as INSUR_F,
a.PD_BASEL_SEG_NUM, 
a.PD_BASEL_SEG_ID, 
a.LGD_BASEL_SEG_ID,
a.EAD_BASEL_SEG_ID, 
a.PD_90_DAY_F,
a.BEFR_ZERO_NET_ADJ_OS_BAL_AMT,
a.AF_ZERO_NET_ADJUSTED_OS_BAL_AMT,
a.EXPCTD_LOSS_RTO,
a.BEFORE_ZERO_NET_UNDRAWN_AMT,
a.AF_ZERO_NET_UNDRAWN_AMT,
case when a.PD_90_DAY_F='T' then b.ACL else 0 end as BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
case when a.PD_90_DAY_F='T' then b.After_Netting_ACL else 0 end as 
AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
a.LGD_FINAL_RPTG_RATIO AS LGD_FINAL_RPTG_RTO,
a.EAD_FINAL_RPTG_RATIO AS EAD_FINAL_RPTG_RTO,
'CAD' as CRNCY_CD format $10.,
0 as PRTL_WRITE_OFF_AMT format 17.3,
'TRUE' as UNCONDTNLY_CNCLBL format $10.,
0 as ACCR_INTR_AMT format 17.3,
A.WGHTD_DLGD_RTO, a.LTV_PERCENTAGE, a.OBLIGORS,
	A.PERIOD_IND,
	A.ECL_Drawn_1,
	A.ECL_Drawn_2,
	A.ECL_Drawn_3,				
	A.ECL_Undrawn_1,
	A.ECL_Undrawn_2,
	A.ECL_Undrawn_3,
	A.ECL_Drawn_PostSec_1,
	A.ECL_Drawn_PostSec_2,
	A.ECL_Drawn_PostSec_3,
&SESSIONTIME AS INSRT_PROCESS_TMSTMP,
&SESSIONTIME AS UPDT_PROCESS_TMSTMP
from &INPATH.._temp_aggr_base_ECL as a 
left join &INPATH.._temp_aggr_ACL_ECL as b
	on a.MTH_TM_ID=b.MTH_TM_ID
	and a.SRC_SYS_CD=b.SRC_SYS_CD
	and a.CCAR_BASEL_PRD_TP_NM=b.CCAR_BASEL_PRD_TP_NM
	and a.PD_BAND=b.PD_BAND 
	and a.PD_BASEL_SEG_NUM=b.PD_BASEL_SEG_NUM
	and a.EAD_BASEL_SEG_NUM=b.EAD_BASEL_SEG_NUM
	and a.LGD_BASEL_SEG_NUM=b.LGD_BASEL_SEG_NUM
	and a.INSURER_FLAG=b.INSURER_FLAG
	and a.LTV_PERCENTAGE=b.LTV_PERCENTAGE
;
quit;
%mend getRRAPreportingExtract_ECL;

options mprint ;
/*STEP03 - Generate output datasets per SRC_SYS_CD;*/
%getRRAPreportingExtract_ECL(SRC_SYS_CD=KS,
	indata=&INPATH..RRAP_Rpt_Extract_KS_ECL,
	NCRdata=&INPATH..BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=&INPATH..RRAP_Rpt_Extract_KS_Final_ECL);
%getRRAPreportingExtract_ECL(SRC_SYS_CD=MOR,
	indata=&INPATH..RRAP_Rpt_Extract_MOR_ECL,
	NCRdata=&INPATH..BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=&INPATH..RRAP_Rpt_Extract_MOR_Final_ECL);
%getRRAPreportingExtract_ECL(SRC_SYS_CD=SPL,
	indata=&INPATH..RRAP_Rpt_Extract_SPL_ECL,
	NCRdata=&INPATH..BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=&INPATH..RRAP_Rpt_Extract_SPL_Final_ECL);

/*STEP04 - Concatenate output files for KS MOR SPL;*/
%getRRAPreportingExtract_ECL(SRC_SYS_CD=TNG,
	indata=&INPATH..RRAP_Rpt_Extract_TNG_ECL,
	NCRdata=&INPATH..BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=&INPATH..RRAP_Rpt_Extract_TNG_Final_ECL);
	

data &INPATH..RRAP_Rpt_Extract_AllBank_ECL;
format legal_entity $40. crncy_cd $10. UNCONDTNLY_CNCLBL $10.;
	set &INPATH..RRAP_Rpt_Extract_KS_Final_ECL
		&INPATH..RRAP_Rpt_Extract_MOR_Final_ECL
		&INPATH..RRAP_Rpt_Extract_SPL_Final_ECL
		&INPATH..RRAP_Rpt_Extract_TNG_Final_ECL;
		drop src_sys_cd;
run;

proc sort data= &INPATH..RRAP_Rpt_Extract_AllBank_ECL nodupkey; 
by 
MTH_TM_ID LEGAL_ENTITY CCAR_BASEL_PRD_TP_NM PD_BAND LGD_BASEL_SEG_NUM EAD_BASEL_SEG_NUM INSUR_F LTV_PERCENTAGE; 
quit;


PROC SQL ;
DROP TABLE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL; 
CREATE TABLE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL AS (
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
,PD_90_DAY_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FINAL_RPTG_RTO
,EAD_FINAL_RPTG_RTO
,crncy_cd
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,TRIM(INSUR_F) as INSUR_F
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
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
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET BEFR_ZERO_NET_ADJ_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE PERIOD_IND = 'PREV' ;
QUIT;

proc sql;
connect using NZRRAP as nzcon;
execute (truncate EDRTLRP1D.BASEL_CCAR_EXPSR_FACT_ECL1 Immediate) by nzcon;
disconnect from nzcon;
quit;

proc sql;
connect using NZRRAP as nzcon;
INSERT INTO NZRRAP.BASEL_CCAR_EXPSR_FACT_ECL1
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
LGD_FINAL_RPTG_RTO,
EAD_FINAL_RPTG_RTO,
CRNCY_CD,
PRTL_WRITE_OFF_AMT,
UNCONDTNLY_CNCLBL,
ACCR_INTR_AMT,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP,
INSUR_F,
WGHTD_DLGD_RTO,
LTV_PERCENTAGE,
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
ECL_Drawn_PostSec_3)
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
LGD_FINAL_RPTG_RTO,
EAD_FINAL_RPTG_RTO,
CRNCY_CD,
PRTL_WRITE_OFF_AMT,
UNCONDTNLY_CNCLBL,
ACCR_INTR_AMT,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP,
INSUR_F,
WGHTD_DLGD_RTO,
LTV_PERCENTAGE,
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
ECL_Drawn_PostSec_3 FROM &INPATH..BASEL_CCAR_EXPSR_FACT_ECL;
quit;


/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/

PROC SQL;
DROP TABLE &INPATH..RRAP_Rpt_Sub_Extract00_ECL;
CREATE table &INPATH..RRAP_Rpt_Sub_Extract00_ECL as 
select 
A.ACCT_NUM ,
A.ADJUSTED_OS_BAL_AMT,
A.CCAR_BASEL_PRD_TP_NM,
A.CONS_DFT_MTH_CNT,
A.EAD_BASEL_SEG_ID,
(case
		when A.EAD_BASEL_SEG_NUM eq . then -1
		else A.EAD_BASEL_SEG_NUM end
) as EAD_BASEL_SEG_NUM,
A.EAD_FINAL_RPTG_RTO,
A.LEGAL_ENTITY,
A.LGD_BASEL_SEG_ID,
(case
		when A.LGD_BASEL_SEG_NUM eq . then -1
		else A.LGD_BASEL_SEG_NUM end
) as LGD_BASEL_SEG_NUM,
A.LGD_FINAL_RPTG_RTO,
A.MTH_TM_ID,
A.NCR_EXPSR_CL_KEY_VAL,
A.PD_BAND,
A.PD_BASEL_SEG_ID,
A.PD_BASEL_SEG_NUM,
A.PD_FINAL_RPTG_RTO,
A.SRC_SYS_CD,
A.UNADJUSTED_ADD_ON_BAL_AMT,
A.UNQ_ACCT_ID,
(case 	when A.SCRTY_TP_DESC = 'Insured' THEN  'YES' ELSE 'NO' end ) as INSURER_FLAG 
,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, a.LTV_PERCENTAGE,
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
 FROM &lib..BL_INS_FACT_CURR as A LEFT JOIN  &INPATH..RRAP_Rpt_Extract00_ECL_CURR as B
 ON a.basel_acct_id = b.basel_acct_id
where A.LEGAL_ENTITY in ('DOM-SUB-DUNDEE','DOM-SUB-MTCC','DOM-SUB-NT','DOM-SUB-MAPLE','DOM-SUB-SMC', 'DOM-SUB-TNG') 
	and A.SRC_SYS_CD in ('MOR', 'TNG-MOR') and A.CONSM_PRD_TREATMNT_CD='A' and A.SML_BUS_F='N'
	and A.PIT_STAT_CD in ('CUR','DEF')
	and A.TRNST_EXCLSN_F='N'
;
quit;


PROC SQL NOPRINT;
UPDATE &INPATH..RRAP_Rpt_Sub_Extract00_ECL 
SET PERIOD_IND = 'CURR' WHERE PERIOD_IND IS NULL ;
QUIT;

PROC SQL;
	INSERT INTO &INPATH..RRAP_Rpt_Sub_Extract00_ECL
	select 
A.ACCT_NUM ,
A.ADJUSTED_OS_BAL_AMT,
A.CCAR_BASEL_PRD_TP_NM,
A.CONS_DFT_MTH_CNT,
A.EAD_BASEL_SEG_ID,
(case
		when A.EAD_BASEL_SEG_NUM eq . then -1
		else A.EAD_BASEL_SEG_NUM end
) as EAD_BASEL_SEG_NUM,
A.EAD_FINAL_RPTG_RTO,
A.LEGAL_ENTITY,
A.LGD_BASEL_SEG_ID,
(case
		when A.LGD_BASEL_SEG_NUM eq . then -1
		else A.LGD_BASEL_SEG_NUM end
) as LGD_BASEL_SEG_NUM,
A.LGD_FINAL_RPTG_RTO,
a.MTH_TM_ID ,
A.NCR_EXPSR_CL_KEY_VAL,
A.PD_BAND,
A.PD_BASEL_SEG_ID,
A.PD_BASEL_SEG_NUM,
A.PD_FINAL_RPTG_RTO,
A.SRC_SYS_CD,
A.UNADJUSTED_ADD_ON_BAL_AMT,
A.UNQ_ACCT_ID,
(case 	when A.SCRTY_TP_DESC = 'Insured' THEN  'YES' ELSE 'NO' end ) as INSURER_FLAG 
,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, a.LTV_PERCENTAGE,
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
 FROM &lib..BL_INS_FACT_PREV_ECL as A inner JOIN  &INPATH..RRAP_Rpt_Extract00_ECL_Prev as B
 ON a.basel_acct_id = b.basel_acct_id
where  A.LEGAL_ENTITY in ('DOM-SUB-DUNDEE','DOM-SUB-MTCC','DOM-SUB-NT','DOM-SUB-MAPLE','DOM-SUB-SMC', 'DOM-SUB-TNG') 
	and A.SRC_SYS_CD in ('MOR', 'TNG-MOR') and A.CONSM_PRD_TREATMNT_CD='A' and A.SML_BUS_F='N'
	and A.PIT_STAT_CD in ('CUR','DEF')
	and A.TRNST_EXCLSN_F='N'
; 
QUIT;

%macro getRRAPreportingExtractSubs_ECL(indata=, ACLlookup=, outdata=);

data &indata;
set &indata;
RUN;

/*Get base aggregated data;*/

proc sql noprint;
create table &INPATH.._temp_aggr_base_ECL as
select 	MTH_TM_ID,
		LEGAL_ENTITY,
		CCAR_BASEL_PRD_TP_NM,
		PD_BAND,
		PD_BASEL_SEG_NUM,
		PD_BASEL_SEG_ID,
		LGD_BASEL_SEG_NUM,
 		EAD_BASEL_SEG_NUM, 	
		LGD_BASEL_SEG_ID,
		EAD_BASEL_SEG_ID, 
		INSURER_FLAG,
	case 
			when mean(PD_FINAL_RPTG_RTO) >=1 then 'T'
			else 'F' 
		end as PD_90_DAY_F format $1.,
		sum (ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
		sum(
			case
				when ADJUSTED_OS_BAL_AMT <=0 then 0
				else ADJUSTED_OS_BAL_AMT
			end
			) as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
		max(LGD_FINAL_RPTG_RTO) as LGD_FINAL_RPTG_RATIO format 28.8,
		case 
			when mean(PD_FINAL_RPTG_RTO) >=1 then max(WGHTD_DLGD_RTO)
			else . 
		end as EXPCTD_LOSS_RTO format 28.8 
		,AVG(WGHTD_DLGD_RTO) as WGHTD_DLGD_RTO, LTV_PERCENTAGE, count(1) as OBLIGORS,
    PERIOD_IND,
	sum(ECL_Drawn_1) as ECL_Drawn_1,
	sum(ECL_Drawn_2) as ECL_Drawn_2,
	sum(ECL_Drawn_3) as ECL_Drawn_3,	
	sum(ECL_Undrawn_1) as ECL_Undrawn_1,
	sum(ECL_Undrawn_2) as  ECL_Undrawn_2,
	sum(ECL_Undrawn_3) as ECL_Undrawn_3,
	sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1,
	sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
	sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3
	from &indata.
where DLGD_F='N'
group by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, PD_BAND,
 		 LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID,PD_BASEL_SEG_NUM,
		PD_BASEL_SEG_ID,INSURER_FLAG, WGHTD_DLGD_RTO,LTV_PERCENTAGE,PERIOD_IND
order by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, PD_BAND, 
 		 LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID, WGHTD_DLGD_RTO,LTV_PERCENTAGE,PERIOD_IND ;
quit;


/*step 1 of last transformation rule*/
/*Get total outstanding balances - aggregate level by LEGAL_ENTITY;*/
proc sql noprint;
create table &INPATH.._temp_aggr_balance_ECL as
select 	MTH_TM_ID,
		LEGAL_ENTITY,
		sum(ADJUSTED_OS_BAL_AMT) as total_os_balance,
		sum(
			case
				when ADJUSTED_OS_BAL_AMT <=0 then 0
				else ADJUSTED_OS_BAL_AMT
			end
			) as total_os_balance_after_net ,
			   PERIOD_IND,
	sum(ECL_Drawn_1) as ECL_Drawn_1,
	sum(ECL_Drawn_2) as ECL_Drawn_2,
	sum(ECL_Drawn_3) as ECL_Drawn_3,	
	sum(ECL_Undrawn_1) as ECL_Undrawn_1,
	sum(ECL_Undrawn_2) as ECL_Undrawn_2,
	sum(ECL_Undrawn_3) as ECL_Undrawn_3,
	sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1,
	sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
	sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3
from &indata.
where PD_FINAL_RPTG_RTO >=1
group by MTH_TM_ID, LEGAL_ENTITY, PERIOD_IND
order by MTH_TM_ID, LEGAL_ENTITY ,PERIOD_IND
;
quit;

/*step 2a of last transformation rule*/
/*Get ACL Lookup data;*/
proc sql noprint;
    create table &INPATH.._temp_GL_ACL_balance_ECL as 
  	select &MTH_TM_ID as MTH_TM_ID,	
		LEGAL_ENTITY,
		EFF_FROM_YR_MTH,
		EFF_TO_YR_MTH,
		GENL_LEDGER_ENTITY_ACL_AMT
	from 	&ACLlookup.
where input(EFF_FROM_YR_MTH,6.) <= &yearmonth and &yearmonth <= 
input(EFF_TO_YR_MTH,6.) 
/*	and crnt_f='Y'*/
and LEGAL_ENTITY in ('DOM-SUB-DUNDEE','DOM-SUB-MTCC','DOM-SUB-NT',
'DOM-SUB-MAPLE','DOM-SUB-SMC');
quit;

/*step 2b and 2c of last transformation rule*/
/*Get Adjusted Outstanding Balance - account level;*/
proc sql noprint; 
create table &INPATH.._temp_acct_balance_ECL as
select 	MTH_TM_ID,
		LEGAL_ENTITY,
		CCAR_BASEL_PRD_TP_NM,
		PD_BAND,
		LGD_BASEL_SEG_NUM,
 		EAD_BASEL_SEG_NUM, 	
		ADJUSTED_OS_BAL_AMT,
		PD_FINAL_RPTG_RTO,
		case
			when ADJUSTED_OS_BAL_AMT>0 then ADJUSTED_OS_BAL_AMT
			when ADJUSTED_OS_BAL_AMT<=0 then 0
			else 0
			end as ADJUSTED_OS_BAL_AMT_after_net format 20.8, 
		CONS_DFT_MTH_CNT,
		ACCT_NUM, INSURER_FLAG, LTV_PERCENTAGE,
		PERIOD_IND,
	ECL_Drawn_1,
	ECL_Drawn_2,
	ECL_Drawn_3,	
	ECL_Undrawn_1 ,
	ECL_Undrawn_2,
	ECL_Undrawn_3,
	ECL_Drawn_PostSec_1 ,
	ECL_Drawn_PostSec_2,
	ECL_Drawn_PostSec_3
from &indata.  
where PD_FINAL_RPTG_RTO >=1
order by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM,
		PD_BAND, LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, ACCT_NUM;
quit;

/*step 3 of last transformation rule*/
/*Combine account level data with ACL GL and total outstanding balances;*/
proc sql noprint;
create table &INPATH.._temp_acct_prorated_loss_ECL as
select 	a.*,
		b.total_os_balance,
		b.total_os_balance_after_net,
		c.GENL_LEDGER_ENTITY_ACL_AMT,
(a.ADJUSTED_OS_BAL_AMT/b.total_os_balance)*c.GENL_LEDGER_ENTITY_ACL_AMT as 
PRORATED_CR_LOSS_ALLOW_AMT,
(a.ADJUSTED_OS_BAL_AMT_after_net/b.total_os_balance_after_net)*c.GENL_LEDGER_ENTITY_ACL_AMT 
as PRORATED_CR_LOSS_ALLOW_AMT_after
from 	&INPATH.._temp_acct_balance_ECL as a 
left join &INPATH.._temp_aggr_balance_ECL as b on a.MTH_TM_ID=b.MTH_TM_ID and 
a.LEGAL_ENTITY=b.LEGAL_ENTITY 
left join &INPATH.._temp_GL_ACL_balance_ECL as c on a.MTH_TM_ID=c.MTH_TM_ID and 
a.LEGAL_ENTITY=c.LEGAL_ENTITY
order by MTH_TM_ID,LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM,
		PD_BAND, LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, ACCT_NUM;
quit;

/*step 4 of last transformation rule*/
/*Calculate ACL at CCAR aggregated level;*/
proc sql noprint;
create table &INPATH.._temp_aggr_ACL_ECL as
select  MTH_TM_ID,
		LEGAL_ENTITY,
 		CCAR_BASEL_PRD_TP_NM,
		PD_BAND,	
 		EAD_BASEL_SEG_NUM, 
 		LGD_BASEL_SEG_NUM, 
		INSURER_FLAG, LTV_PERCENTAGE,
		sum(PRORATED_CR_LOSS_ALLOW_AMT) as ACL,
		sum(PRORATED_CR_LOSS_ALLOW_AMT_after) as After_Netting_ACL ,
		PERIOD_IND,
	sum(ECL_Drawn_1) as ECL_Drawn_1,
	sum(ECL_Drawn_2) as ECL_Drawn_2,
	sum(ECL_Drawn_3) as ECL_Drawn_3,	
	sum(ECL_Undrawn_1) as ECL_Undrawn_1,
	sum(ECL_Undrawn_2) as ECL_Undrawn_2,
	sum(ECL_Undrawn_3) as ECL_Undrawn_3,
	sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1 ,
	sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
	sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3
from 	&INPATH.._temp_acct_prorated_loss_ECL
group by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, INSURER_FLAG,LTV_PERCENTAGE,
		PD_BAND, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM,PERIOD_IND
order by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM,
		PD_BAND, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM
		;
quit;

/*Generate final output;*/
proc sql noprint; 
create table &outdata. as
select  
a.LEGAL_ENTITY,
a.PD_BAND,
a.CCAR_BASEL_PRD_TP_NM,
a.MTH_TM_ID,
COALESCE(a.LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM,
COALESCE(a.EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM,
a.PD_BASEL_SEG_NUM,
a.PD_BASEL_SEG_ID, 
a.LGD_BASEL_SEG_ID,
a.EAD_BASEL_SEG_ID,
a.PD_90_DAY_F,
a.INSURER_FLAG as INSUR_F,
a.BEFR_ZERO_NET_ADJ_OS_BAL_AMT,
a.AF_ZERO_NET_ADJUSTED_OS_BAL_AMT,
a.EXPCTD_LOSS_RTO,
0 as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
0 as AF_ZERO_NET_UNDRAWN_AMT format 17.3, 
case when a.PD_90_DAY_F='T' then b.ACL else 0 end as 
BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
case when a.PD_90_DAY_F='T' then b.After_Netting_ACL else 0 end as AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
a.LGD_FINAL_RPTG_RATIO as LGD_FINAL_RPTG_RTO,
0 as EAD_FINAL_RPTG_RTO format 28.8,
'CAD' as CRNCY_CD format $10.,
0 as PRTL_WRITE_OFF_AMT format 17.3,
'TRUE' as UNCONDTNLY_CNCLBL format $10.,
0 as ACCR_INTR_AMT format 17.3,
a.WGHTD_DLGD_RTO, a.LTV_PERCENTAGE, a.OBLIGORS,
a.PERIOD_IND,
	a.ECL_Drawn_1,
	a.ECL_Drawn_2,
	a.ECL_Drawn_3,				
	a.ECL_Undrawn_1,
	a.ECL_Undrawn_2,
	a.ECL_Undrawn_3,
	a.ECL_Drawn_PostSec_1,
	a.ECL_Drawn_PostSec_2,
	a.ECL_Drawn_PostSec_3,
&SESSIONTIME AS INSRT_PROCESS_TMSTMP,
&SESSIONTIME AS UPDT_PROCESS_TMSTMP
from &INPATH.._temp_aggr_base_ECL as A 
left join &INPATH.._temp_aggr_ACL_ECL as b
on	a.MTH_TM_ID=b.MTH_TM_ID
	and a.LEGAL_ENTITY=b.LEGAL_ENTITY
	and a.CCAR_BASEL_PRD_TP_NM=b.CCAR_BASEL_PRD_TP_NM
	and a.PD_BAND=b.PD_BAND 
	and a.EAD_BASEL_SEG_NUM=b.EAD_BASEL_SEG_NUM
	and a.LGD_BASEL_SEG_NUM=b.LGD_BASEL_SEG_NUM
	and a.INSURER_FLAG=b.INSURER_FLAG
	and a.LTV_PERCENTAGE=b.LTV_PERCENTAGE
;
quit; 
%mend getRRAPreportingExtractSubs_ECL;
 
 
/*Generate output datasets;*/
%getRRAPreportingExtractSubs_ECL(indata=&INPATH..RRAP_Rpt_Sub_Extract00_ECL,
							 ACLlookup=&INPATH..BASEL_SUBSIDIARY_ACL_LKP, 
							 outdata=&INPATH..RRAP_Rpt_Extract_Subsidiary_ECL);


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
,PD_90_DAY_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FINAL_RPTG_RTO
,EAD_FINAL_RPTG_RTO
,crncy_cd
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,TRIM(INSUR_F) as INSUR_F
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
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


/*UPDATE NULL VALUES TO 0 FOR TNG PRODUCTS*/
PROC SQL NOPRINT;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT IS NULL AND CCAR_BASEL_PRD_TP_NM LIKE '%TNG%'
AND MTH_TM_ID=&MTH_TM_ID;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL
SET AF_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE AF_ZERO_NET_ALWBL_CR_LOSS_AMT IS NULL AND CCAR_BASEL_PRD_TP_NM LIKE '%TNG%'
AND MTH_TM_ID=&MTH_TM_ID;
QUIT;


/****UPDATE PREV MONTH'S VALUES TO ZERO ***/
PROC SQL NOPRINT;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET BEFR_ZERO_NET_ADJ_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE PERIOD_IND = 'PREV' ;
QUIT;

proc sql;
connect using NZRRAP as nzcon;
execute (truncate EDRTLRP1D.BASEL_CCAR_EXPSR_FACT_ECL2 Immediate) by nzcon;
disconnect from nzcon;
quit;

proc sql;
connect using NZRRAP as nzcon;
INSERT INTO NZRRAP.BASEL_CCAR_EXPSR_FACT_ECL2 
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
LGD_FINAL_RPTG_RTO,
EAD_FINAL_RPTG_RTO,
CRNCY_CD,
PRTL_WRITE_OFF_AMT,
UNCONDTNLY_CNCLBL,
ACCR_INTR_AMT,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP,
INSUR_F,
WGHTD_DLGD_RTO,
LTV_PERCENTAGE,
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
ECL_Drawn_PostSec_3)
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
LGD_FINAL_RPTG_RTO,
EAD_FINAL_RPTG_RTO,
CRNCY_CD,
PRTL_WRITE_OFF_AMT,
UNCONDTNLY_CNCLBL,
ACCR_INTR_AMT,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP,
INSUR_F,
WGHTD_DLGD_RTO,
LTV_PERCENTAGE,
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
ECL_Drawn_PostSec_3 FROM &INPATH..BASEL_CCAR_EXPSR_FACT_ECL;
quit;


%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Extract00_ECL RRAP_Rpt_Extract00_ECL_NULL  RRAP_Rpt_Extract_KS_ECL RRAP_Rpt_Extract_MOR_ECL RRAP_Rpt_Extract_SPL_ECL _temp_aggr_base_ECL);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=_temp_aggr_NCR_balance_ECL _temp_dir_NCR_ECL _temp_acct_balance_ECL _temp_acct_prorated_loss_ECL _temp_aggr_ACL_ECL);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Extract_KS_Final_ECL RRAP_Rpt_Extract_MOR_Final_ECL RRAP_Rpt_Extract_SPL_Final_ECL RRAP_Rpt_Extract_AllBank_ECL);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Sub_Extract00_ECL RRAP_Rpt_Sub_Extract00_ECL1 RRAP_Rpt_Sub_Extract00_ECL_NULL RRAP_Rpt_Sub_Extract00_ECL_Prev  _temp_aggr_base_ECL _temp_aggr_balance_ECL _temp_GL_ACL_balance_ECL _temp_acct_balance_ECL);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=_temp_acct_prorated_loss_ECL _temp_aggr_ACL_ECL RRAP_Rpt_Extract_Subsidiary_ECL);



/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;




	proc sql noprint;
	DROP TABLE &INPATH..RRAP_Rpt_Extract00_ECL;
	CREATE table &INPATH..RRAP_Rpt_Extract00_ECL as 
		select 	a.ACCT_NUM ,
				a.ADJUSTED_OS_BAL_AMT,
				a.CCAR_BASEL_PRD_TP_NM,
				a.CONS_DFT_MTH_CNT,
				a.EAD_BASEL_SEG_ID,
				(case when a.EAD_BASEL_SEG_NUM eq . then -1 else a.EAD_BASEL_SEG_NUM end ) as EAD_BASEL_SEG_NUM,
				a.EAD_FINAL_RPTG_RTO,
				a.LEGAL_ENTITY,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.LGD_BASEL_SEG_ID 
				when a.SRC_SYS_CD in ('SPL')  and a.LGD_BASEL_SEG_ID eq . THEN -1
					else a.LGD_BASEL_SEG_ID end) as  LGD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') and a.LGD_BASEL_SEG_NUM eq . then -1
					when a.SRC_SYS_CD in ('SPL') and a.LGD_BASEL_SEG_NUM eq . then -1
					else a.LGD_BASEL_SEG_NUM end) as LGD_BASEL_SEG_NUM,
				a.LGD_FINAL_RPTG_RTO,
				a.MTH_TM_ID,
				a.NCR_EXPSR_CL_KEY_VAL,
				a.PD_BAND,
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
				a.UNQ_ACCT_ID, A.SCRTY_TP_DESC ,A.BASEL_PRD_TP_CD,
				(case when A.SRC_SYS_CD = 'KS' AND A.INSUR_F='YES' THEN 'YES'
					when A.SRC_SYS_CD in ('MOR','TNG-MOR') AND A.SCRTY_TP_DESC='Insured' THEN 'YES' ELSE 'NO' end ) as INSURER_FLAG 

				,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, a.LTV_PERCENTAGE,
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
 FROM &lib..BL_INS_FACT_CURR as A LEFT JOIN  &INPATH..RRAP_Rpt_Extract00_ECL_CURR as B
 ON a.basel_acct_id = b.basel_acct_id
		where a.SRC_SYS_CD in ('TNG-MOR','KS','MOR','SPL') and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N'
			and a.PIT_STAT_CD in ('CUR','DEF')
			and a.TRNST_EXCLSN_F='N'
			and a.DLGD_F='Y';
	quit;

PROC SQL NOPRINT;
UPDATE &INPATH..RRAP_Rpt_Extract00_ECL 
SET PERIOD_IND = 'CURR' WHERE PERIOD_IND IS NULL ;
QUIT;

PROC SQL;
	INSERT INTO &INPATH..RRAP_Rpt_Extract00_ECL
			select 	a.ACCT_NUM ,
				a.ADJUSTED_OS_BAL_AMT,
				a.CCAR_BASEL_PRD_TP_NM,
				a.CONS_DFT_MTH_CNT,
				a.EAD_BASEL_SEG_ID,
				(case when a.EAD_BASEL_SEG_NUM eq . then -1 else a.EAD_BASEL_SEG_NUM end ) as EAD_BASEL_SEG_NUM,
				a.EAD_FINAL_RPTG_RTO,
				a.LEGAL_ENTITY,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.LGD_BASEL_SEG_ID 
				when a.SRC_SYS_CD in ('SPL')  and a.LGD_BASEL_SEG_ID eq . THEN -1
					else a.LGD_BASEL_SEG_ID end) as  LGD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') and a.LGD_BASEL_SEG_NUM eq . then -1
					when a.SRC_SYS_CD in ('SPL') and a.LGD_BASEL_SEG_NUM eq . then -1
					else a.LGD_BASEL_SEG_NUM end) as LGD_BASEL_SEG_NUM,
				a.LGD_FINAL_RPTG_RTO,
				a.MTH_TM_ID ,
				a.NCR_EXPSR_CL_KEY_VAL,
				a.PD_BAND,
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
				a.UNQ_ACCT_ID, A.SCRTY_TP_DESC ,A.BASEL_PRD_TP_CD,
				(case when A.SRC_SYS_CD = 'KS' AND A.INSUR_F='YES' THEN 'YES'
					when A.SRC_SYS_CD in ('MOR','TNG-MOR') AND A.SCRTY_TP_DESC='Insured' THEN 'YES' ELSE 'NO' end ) as INSURER_FLAG 

				,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, a.LTV_PERCENTAGE,
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
 FROM &lib..BL_INS_FACT_PREV_ECL as A inner JOIN  &INPATH..RRAP_Rpt_Extract00_ECL_Prev as B
 ON a.basel_acct_id = b.basel_acct_id
		where a.SRC_SYS_CD in ('TNG-MOR','KS','MOR','SPL') and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N'
			and a.PIT_STAT_CD in ('CUR','DEF')
			and a.TRNST_EXCLSN_F='N'
			and a.DLGD_F='Y'; 
QUIT;


/*STEP02 - Split extract by source system code;*/
data &INPATH..RRAP_Rpt_Extract_KS_ECL
	&INPATH..RRAP_Rpt_Extract_MOR_ECL
	&INPATH..RRAP_Rpt_Extract_SPL_ECL
	&INPATH..RRAP_Rpt_Extract_TNG_ECL;
	set &INPATH..RRAP_Rpt_Extract00_ECL;
	
	
*** Split into 3 output files;
	if SRC_SYS_CD in ('KS') then output &INPATH..RRAP_Rpt_Extract_KS_ECL;
	else if SRC_SYS_CD in ('MOR') then output &INPATH..RRAP_Rpt_Extract_MOR_ECL;
	else if SRC_SYS_CD in ('SPL') then output &INPATH..RRAP_Rpt_Extract_SPL_ECL;
	else if SRC_SYS_CD in ('TNG-MOR') then output &INPATH..RRAP_Rpt_Extract_TNG_ECL;
	else delete;
run;

%macro getRRAPreportingExtract_ECL(SRC_SYS_CD=, indata=, NCRdata=, outdata=);
	/*Calculate first set of transformation rules using the base data.*/

	/*DLGD RECORDS USE DIFFERENT AGGREGATION.*/
	proc sql noprint;
		create table &INPATH.._temp_aggr_base_ECL as
			select 	PD_BAND,
				CCAR_BASEL_PRD_TP_NM,
				MTH_TM_ID,
				SRC_SYS_CD,
				LGD_BASEL_SEG_NUM,
				EAD_BASEL_SEG_NUM, 
				PD_BASEL_SEG_NUM, 
				PD_BASEL_SEG_ID, 
				LGD_BASEL_SEG_ID,
				EAD_BASEL_SEG_ID,
				INSURER_FLAG,
			case 
				when mean(PD_FINAL_RPTG_RTO) >=1 then 'T' 
				else 'F' 
			end 
		as PD_90_DAY_F 
			format $1.,
			%if &SRC_SYS_CD=KS or &SRC_SYS_CD=MOR or &SRC_SYS_CD=TNG %then

			%do;
				sum(ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
				sum
				(case 
					when ADJUSTED_OS_BAL_AMT <=0 then 0 
					else ADJUSTED_OS_BAL_AMT 
				end )
				as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
			%end;
%else %if &SRC_SYS_CD=SPL %then
	%do;
		sum(ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
		sum
		(case 
			when (ADJUSTED_OS_BAL_AMT) <=0 then 0 
			else (ADJUSTED_OS_BAL_AMT) 
		end)
		as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
	%end;

	case 
		when mean(PD_FINAL_RPTG_RTO) >=1 then max(WGHTD_DLGD_RTO) 
		else . 
	end 														
as EXPCTD_LOSS_RTO format 28.8,
	%if &SRC_SYS_CD=KS %then

		%do;
			sum(BEFORE_ZERO_NET_UNDRAWN_AMT) as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
			sum(AF_ZERO_NET_UNDRAWN_AMT) as AF_ZERO_NET_UNDRAWN_AMT format 17.3,
		%end;
%else %if &SRC_SYS_CD ne KS  %then
	%do;
		0 as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
		0 as AF_ZERO_NET_UNDRAWN_AMT format 17.3,
	%end;

	max(LGD_FINAL_RPTG_RTO) as LGD_FINAL_RPTG_RATIO format 28.8,
	%if &SRC_SYS_CD=KS %then

		%do;
			case 
				when mean(PD_FINAL_RPTG_RTO) >=1 then 0 
				else max(EAD_FINAL_RPTG_RTO) 
			end 
		as EAD_FINAL_RPTG_RATIO format 28.8
		%end;
%else
	%do;
		0 as EAD_FINAL_RPTG_RATIO format 28.8
	%end;

	,WGHTD_DLGD_RTO, LTV_PERCENTAGE, count(1) as OBLIGORS,
PERIOD_IND,
	sum(ECL_Drawn_1) as ECL_Drawn_1,
	sum(ECL_Drawn_2) as ECL_Drawn_2,
	sum(ECL_Drawn_3) as ECL_Drawn_3,	
	sum(ECL_Undrawn_1) as ECL_Undrawn_1,
	sum(ECL_Undrawn_2) as ECL_Undrawn_2,
	sum(ECL_Undrawn_3) as ECL_Undrawn_3,
	sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1 ,
	sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
	sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3
	from &indata.
	where DLGD_F='Y'
		group by PD_BAND, CCAR_BASEL_PRD_TP_NM, MTH_TM_ID, SRC_SYS_CD, 
			LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, PD_BASEL_SEG_NUM, 
			PD_BASEL_SEG_ID, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID ,INSURER_FLAG, WGHTD_DLGD_RTO, LTV_PERCENTAGE,PERIOD_IND
	order by PD_BAND, CCAR_BASEL_PRD_TP_NM, MTH_TM_ID, SRC_SYS_CD, 
		LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, PD_BASEL_SEG_NUM, 
		PD_BASEL_SEG_ID, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID ,INSURER_FLAG, WGHTD_DLGD_RTO, LTV_PERCENTAGE,PERIOD_IND
	;
	quit;
	

/*step 1 of last transformation*/
/*Get base NCR data - aggregate level irrespective of src_sys_code; */
/*So the source dataset is the primary dataset which ensures the same aggr_NCR_balance is available for all SRC_SYS_CD */
proc sql noprint;
	create table &INPATH.._temp_aggr_NCR_balance_ECL as
		select 	MTH_TM_ID,
			NCR_EXPSR_CL_KEY_VAL,
			/*INSURER_FLAG,*/
	sum
	(
	case 	
		when (SRC_SYS_CD='KS' or SRC_SYS_CD='MOR') then ADJUSTED_OS_BAL_AMT
		when SRC_SYS_CD='SPL' then ADJUSTED_OS_BAL_AMT
	end
		)
		as total_outstanding_balance,
			sum
			(case 
				when (SRC_SYS_CD='KS' or SRC_SYS_CD='MOR') THEN 
				CASE 
					WHEN ADJUSTED_OS_BAL_AMT < 0 then 0 
					ELSE ADJUSTED_OS_BAL_AMT 
				END
				when SRC_SYS_CD='SPL' THEN 
				CASE 
					WHEN (ADJUSTED_OS_BAL_AMT) < 0 THEN 0 
					ELSE (ADJUSTED_OS_BAL_AMT) 
				END
				end
				)
				as total_outstanding_balance_net,
				PERIOD_IND,
	sum(ECL_Drawn_1) as ECL_Drawn_1,
	sum(ECL_Drawn_2) as ECL_Drawn_2,
	sum(ECL_Drawn_3) as ECL_Drawn_3,	
	sum(ECL_Undrawn_1) as ECL_Undrawn_1,
	sum(ECL_Undrawn_2) as  ECL_Undrawn_2,
	sum(ECL_Undrawn_3) as ECL_Undrawn_3,
	sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1,
	sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
	sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3
					from &INPATH..RRAP_Rpt_Extract00_ECL
						where PD_FINAL_RPTG_RTO >=1
							and SRC_SYS_CD in ('KS','MOR','SPL')
							group by MTH_TM_ID, NCR_EXPSR_CL_KEY_VAL ,PERIOD_IND
								/*,INSURER_FLAG*/
	order by MTH_TM_ID, NCR_EXPSR_CL_KEY_VAL
	;
quit;

/*step 2a of last transformation*/
/*Get NCR data - NCR Key Value level;*/
proc sql noprint;
create table &INPATH.._temp_dir_NCR_ECL as
select	distinct	
MTH_TM_ID, 
NCR_EXPSR_CL_KEY,
CR_LOSS_ALWNC_AMT
from &NCRdata. where MTH_TM_ID=&MTH_TM_ID
;
quit;

/*step 2b and 2c of last transformation rule*/
/*Get balance data - account level;*/
proc sql noprint;
	create table &INPATH.._temp_acct_balance_ECL as
		select 	MTH_TM_ID,
			SRC_SYS_CD,
			CCAR_BASEL_PRD_TP_NM, 	
			PD_BAND,
			PD_BASEL_SEG_NUM, 
			EAD_BASEL_SEG_NUM, 
			LGD_BASEL_SEG_NUM, 
			ADJUSTED_OS_BAL_AMT,
			UNADJUSTED_ADD_ON_BAL_AMT,
			CONS_DFT_MTH_CNT,
			NCR_EXPSR_CL_KEY_VAL,
			ACCT_NUM,
			INSURER_FLAG,
			WGHTD_DLGD_RTO, LTV_PERCENTAGE,
		case 
			when SRC_SYS_CD in ('KS','MOR') then ADJUSTED_OS_BAL_AMT
			when SRC_SYS_CD in ('SPL') then ADJUSTED_OS_BAL_AMT
			else 0
		end 
	as outstanding_balance, 
		case 
			when SRC_SYS_CD in ('KS','MOR') and ADJUSTED_OS_BAL_AMT>0 then  
			ADJUSTED_OS_BAL_AMT
			when SRC_SYS_CD in ('KS','MOR') and ADJUSTED_OS_BAL_AMT<=0 then 0
			when SRC_SYS_CD in ('SPL') and 
			(ADJUSTED_OS_BAL_AMT)>0 then 
			(ADJUSTED_OS_BAL_AMT)
			when SRC_SYS_CD in ('SPL') and 
			(ADJUSTED_OS_BAL_AMT)<=0 then 0
			else 0
		end 
	as outstanding_balance_after_net ,
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
		from &INPATH..RRAP_Rpt_Extract00_ECL
			where PD_FINAL_RPTG_RTO >=1
				order by MTH_TM_ID, SRC_SYS_CD,CCAR_BASEL_PRD_TP_NM, PD_BAND, 
					PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM, NCR_EXPSR_CL_KEY_VAL, 
					ACCT_NUM
	;
quit;

/*step 3 of last transformation rule*/
/*Combine account level PRORATED_CR_LOSS_ALLOW_AMT;*/
proc sql noprint;
	create table &INPATH.._temp_acct_prorated_loss_ECL as
		select 	a.*, 
			b.total_outstanding_balance,
			b.total_outstanding_balance_net,
			c.CR_LOSS_ALWNC_AMT,
			(a.outstanding_balance/b.total_outstanding_balance)*c.CR_LOSS_ALWNC_AMT as 
			PRORATED_CR_LOSS_ALLOW_AMT,
			(a.outstanding_balance_after_net/b.total_outstanding_balance_net)*c.CR_LOSS_ALWNC_AMT 
		as PRORATED_CR_LOSS_ALLOW_AMT_AFTER 
			from &INPATH.._temp_acct_balance_ECL as a 
				left join &INPATH.._temp_aggr_NCR_balance_ECL as b 
					on a.MTH_TM_ID=b.MTH_TM_ID and a.NCR_EXPSR_CL_KEY_VAL=b.NCR_EXPSR_CL_KEY_VAL 
				left join &INPATH.._temp_dir_NCR_ECL as c
					on a.MTH_TM_ID=c.MTH_TM_ID and a.NCR_EXPSR_CL_KEY_VAL=c.NCR_EXPSR_CL_KEY 
				order by MTH_TM_ID, SRC_SYS_CD, CCAR_BASEL_PRD_TP_NM, PD_BAND,
					PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM, NCR_EXPSR_CL_KEY_VAL, 
					ACCT_NUM
	;
quit;

/*step 4 of last transformation rule*/
/*Calculate ACL at CCAR aggregated level;*/
proc sql noprint;
	create table &INPATH.._temp_aggr_ACL_ECL as
		select  MTH_TM_ID,
			SRC_SYS_CD,
			CCAR_BASEL_PRD_TP_NM,
			PD_BAND,
			PD_BASEL_SEG_NUM, 
			EAD_BASEL_SEG_NUM, 
			LGD_BASEL_SEG_NUM,INSURER_FLAG,WGHTD_DLGD_RTO,LTV_PERCENTAGE,
			sum(PRORATED_CR_LOSS_ALLOW_AMT) as ACL,
			sum(PRORATED_CR_LOSS_ALLOW_AMT_AFTER) as After_Netting_ACL ,
			PERIOD_IND,
	sum(ECL_Drawn_1) as ECL_Drawn_1,
	sum(ECL_Drawn_2) as ECL_Drawn_2,
	sum(ECL_Drawn_3) as ECL_Drawn_3,	
	sum(ECL_Undrawn_1) as ECL_Undrawn_1 ,
	sum(ECL_Undrawn_2)  as ECL_Undrawn_2,
	sum(ECL_Undrawn_3) as  ECL_Undrawn_3,
	sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1,
	sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
	sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3
		from &INPATH.._temp_acct_prorated_loss_ECL
			group by MTH_TM_ID,SRC_SYS_CD, CCAR_BASEL_PRD_TP_NM, PD_BAND,INSURER_FLAG, WGHTD_DLGD_RTO,LTV_PERCENTAGE,
				PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM,PERIOD_IND
			order by MTH_TM_ID, SRC_SYS_CD, CCAR_BASEL_PRD_TP_NM, PD_BAND, 
				PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM
	;
quit;
/*Generate final output;*/
proc sql noprint;
create table &outdata. as
select 'DOM-BANK-BNS' as LEGAL_ENTITY format $40.,
a.PD_BAND,
a.CCAR_BASEL_PRD_TP_NM,
a.MTH_TM_ID,
a.MTH_TM_ID,
a.SRC_SYS_CD,
COALESCE(a.LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM,
COALESCE(a.EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM,
a.INSURER_FLAG as INSUR_F,
a.PD_BASEL_SEG_NUM, 
a.PD_BASEL_SEG_ID, 
a.LGD_BASEL_SEG_ID,
a.EAD_BASEL_SEG_ID, 
a.PD_90_DAY_F,
a.BEFR_ZERO_NET_ADJ_OS_BAL_AMT,
a.AF_ZERO_NET_ADJUSTED_OS_BAL_AMT,
a.EXPCTD_LOSS_RTO,
a.BEFORE_ZERO_NET_UNDRAWN_AMT,
a.AF_ZERO_NET_UNDRAWN_AMT,
case when a.PD_90_DAY_F='T' then b.ACL else 0 end as BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
case when a.PD_90_DAY_F='T' then b.After_Netting_ACL else 0 end as 
AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
a.LGD_FINAL_RPTG_RATIO AS LGD_FINAL_RPTG_RTO,
a.EAD_FINAL_RPTG_RATIO AS EAD_FINAL_RPTG_RTO,
'CAD' as CRNCY_CD format $10.,
0 as PRTL_WRITE_OFF_AMT format 17.3,
'TRUE' as UNCONDTNLY_CNCLBL format $10.,
0 as ACCR_INTR_AMT format 17.3,
A.WGHTD_DLGD_RTO, a.LTV_PERCENTAGE, A.OBLIGORS,
			a.PERIOD_IND,
	a.ECL_Drawn_1,
	a.ECL_Drawn_2,
	a.ECL_Drawn_3,	
	a.ECL_Undrawn_1,
	a.ECL_Undrawn_2,
	a.ECL_Undrawn_3,
	a.ECL_Drawn_PostSec_1,
	a.ECL_Drawn_PostSec_2,
 	a.ECL_Drawn_PostSec_3,
&SESSIONTIME AS INSRT_PROCESS_TMSTMP,
&SESSIONTIME AS UPDT_PROCESS_TMSTMP
from &INPATH.._temp_aggr_base_ECL as a 
left join &INPATH.._temp_aggr_ACL_ECL as b
	on a.MTH_TM_ID=b.MTH_TM_ID
	and a.SRC_SYS_CD=b.SRC_SYS_CD
	and a.CCAR_BASEL_PRD_TP_NM=b.CCAR_BASEL_PRD_TP_NM
	and a.PD_BAND=b.PD_BAND 
	and a.PD_BASEL_SEG_NUM=b.PD_BASEL_SEG_NUM
	and a.EAD_BASEL_SEG_NUM=b.EAD_BASEL_SEG_NUM
	and a.LGD_BASEL_SEG_NUM=b.LGD_BASEL_SEG_NUM
	and a.INSURER_FLAG=b.INSURER_FLAG
	and a.WGHTD_DLGD_RTO = b.WGHTD_DLGD_RTO
	and a.LTV_PERCENTAGE = b.LTV_PERCENTAGE
;
quit;
%mend getRRAPreportingExtract_ECL;

/*STEP03 - Generate output datasets per SRC_SYS_CD;*/
%getRRAPreportingExtract_ECL(SRC_SYS_CD=KS,
	indata=&INPATH..RRAP_Rpt_Extract_KS_ECL,
	NCRdata=&INPATH..BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=&INPATH..RRAP_Rpt_Extract_KS_Final_ECL);
%getRRAPreportingExtract_ECL(SRC_SYS_CD=MOR,
	indata=&INPATH..RRAP_Rpt_Extract_MOR_ECL,
	NCRdata=&INPATH..BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=&INPATH..RRAP_Rpt_Extract_MOR_Final_ECL);
%getRRAPreportingExtract_ECL(SRC_SYS_CD=SPL,
	indata=&INPATH..RRAP_Rpt_Extract_SPL_ECL,
	NCRdata=&INPATH..BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=&INPATH..RRAP_Rpt_Extract_SPL_Final_ECL);

/*STEP04 - Concatenate output files for KS MOR SPL;*/
%getRRAPreportingExtract_ECL(SRC_SYS_CD=TNG,
	indata=&INPATH..RRAP_Rpt_Extract_TNG_ECL,
	NCRdata=&INPATH..BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=&INPATH..RRAP_Rpt_Extract_TNG_Final_ECL);


data &INPATH..RRAP_Rpt_Extract_AllBank_ECL;
format legal_entity $40. crncy_cd $10. UNCONDTNLY_CNCLBL $10.;
	set &INPATH..RRAP_Rpt_Extract_KS_Final_ECL
		&INPATH..RRAP_Rpt_Extract_MOR_Final_ECL
		&INPATH..RRAP_Rpt_Extract_SPL_Final_ECL
		&INPATH..RRAP_Rpt_Extract_TNG_Final_ECL;
		drop src_sys_cd;
run;

proc sort data= &INPATH..RRAP_Rpt_Extract_AllBank_ECL nodupkey; 
by 
MTH_TM_ID LEGAL_ENTITY CCAR_BASEL_PRD_TP_NM PD_BAND LGD_BASEL_SEG_NUM EAD_BASEL_SEG_NUM INSUR_F WGHTD_DLGD_RTO LTV_PERCENTAGE; 
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
,PD_90_DAY_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FINAL_RPTG_RTO
,EAD_FINAL_RPTG_RTO
,crncy_cd
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,TRIM(INSUR_F) as INSUR_F
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
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
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET BEFR_ZERO_NET_ADJ_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE PERIOD_IND = 'PREV' ;
QUIT;

proc sql;
connect using NZRRAP as nzcon;
execute (truncate EDRTLRP1D.BASEL_CCAR_EXPSR_FACT_ECL3 Immediate) by nzcon;
disconnect from nzcon;
quit;


proc sql;
connect using NZRRAP as nzcon;
INSERT INTO NZRRAP.BASEL_CCAR_EXPSR_FACT_ECL3 
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
LGD_FINAL_RPTG_RTO,
EAD_FINAL_RPTG_RTO,
CRNCY_CD,
PRTL_WRITE_OFF_AMT,
UNCONDTNLY_CNCLBL,
ACCR_INTR_AMT,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP,
INSUR_F,
WGHTD_DLGD_RTO,
LTV_PERCENTAGE,
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
ECL_Drawn_PostSec_3)
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
LGD_FINAL_RPTG_RTO,
EAD_FINAL_RPTG_RTO,
CRNCY_CD,
PRTL_WRITE_OFF_AMT,
UNCONDTNLY_CNCLBL,
ACCR_INTR_AMT,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP,
INSUR_F,
WGHTD_DLGD_RTO,
LTV_PERCENTAGE,
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
ECL_Drawn_PostSec_3 FROM &INPATH..BASEL_CCAR_EXPSR_FACT_ECL;
quit;


/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/

proc sql noprint;
drop table &INPATH..RRAP_Rpt_Sub_Extract00_ECL;
    CREATE table &INPATH..RRAP_Rpt_Sub_Extract00_ECL as 
	select 
	A.ACCT_NUM ,
A.ADJUSTED_OS_BAL_AMT,
A.CCAR_BASEL_PRD_TP_NM,
A.CONS_DFT_MTH_CNT,
A.EAD_BASEL_SEG_ID,
(case
		when A.EAD_BASEL_SEG_NUM eq . then -1
		else A.EAD_BASEL_SEG_NUM end
) as EAD_BASEL_SEG_NUM,

A.EAD_FINAL_RPTG_RTO,
A.LEGAL_ENTITY,
A.LGD_BASEL_SEG_ID,
(case
		when A.LGD_BASEL_SEG_NUM eq . then -1
		else A.LGD_BASEL_SEG_NUM end
) as LGD_BASEL_SEG_NUM,

A.LGD_FINAL_RPTG_RTO,
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
(case 	when A.SCRTY_TP_DESC = 'Insured' THEN  'YES' ELSE 'NO' end ) as INSURER_FLAG 
,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, a.LTV_PERCENTAGE,
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
 FROM &lib..BL_INS_FACT_CURR as A LEFT JOIN  &INPATH..RRAP_Rpt_Extract00_ECL_CURR as B
 ON a.basel_acct_id = b.basel_acct_id
where A.LEGAL_ENTITY in ('DOM-SUB-DUNDEE','DOM-SUB-MTCC','DOM-SUB-NT','DOM-SUB-MAPLE','DOM-SUB-SMC', 'DOM-SUB-TNG') 
	and A.SRC_SYS_CD in ('MOR', 'TNG-MOR') and A.CONSM_PRD_TREATMNT_CD='A' and A.SML_BUS_F='N'
	and A.PIT_STAT_CD in ('CUR','DEF')
	and A.TRNST_EXCLSN_F='N'
	;
quit;


PROC SQL NOPRINT;
UPDATE &INPATH..RRAP_Rpt_Sub_Extract00_ECL 
SET PERIOD_IND = 'CURR' WHERE PERIOD_IND IS NULL ;
QUIT;

PROC SQL;
	INSERT INTO &INPATH..RRAP_Rpt_Sub_Extract00_ECL
		select 
	A.ACCT_NUM ,
A.ADJUSTED_OS_BAL_AMT,
A.CCAR_BASEL_PRD_TP_NM,
A.CONS_DFT_MTH_CNT,
A.EAD_BASEL_SEG_ID,
(case
		when A.EAD_BASEL_SEG_NUM eq . then -1
		else A.EAD_BASEL_SEG_NUM end
) as EAD_BASEL_SEG_NUM,

A.EAD_FINAL_RPTG_RTO,
A.LEGAL_ENTITY,
A.LGD_BASEL_SEG_ID,
(case
		when A.LGD_BASEL_SEG_NUM eq . then -1
		else A.LGD_BASEL_SEG_NUM end
) as LGD_BASEL_SEG_NUM,

A.LGD_FINAL_RPTG_RTO,
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
(case 	when A.SCRTY_TP_DESC = 'Insured' THEN  'YES' ELSE 'NO' end ) as INSURER_FLAG 
,a.DLGD_RPTG_RTO as WGHTD_DLGD_RTO, a.DLGD_F, a.LTV_PERCENTAGE,
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
 FROM &lib..BL_INS_FACT_PREV_ECL as A inner JOIN  &INPATH..RRAP_Rpt_Extract00_ECL_Prev as B
 ON a.basel_acct_id = b.basel_acct_id
where A.LEGAL_ENTITY in ('DOM-SUB-DUNDEE','DOM-SUB-MTCC','DOM-SUB-NT','DOM-SUB-MAPLE','DOM-SUB-SMC', 'DOM-SUB-TNG') 
	and A.SRC_SYS_CD in ('MOR', 'TNG-MOR') and A.CONSM_PRD_TREATMNT_CD='A' and A.SML_BUS_F='N'
	and A.PIT_STAT_CD in ('CUR','DEF')
	and A.TRNST_EXCLSN_F='N'; 
QUIT;


/*Calculate first set of transformation rules using the base data.*/
%macro getRRAPreportingExtractSubs_ECL(indata=, ACLlookup=, outdata=);
/*Generate LGD Base Segment equal to blank for defaulters over 24 months;*/
data &indata;
set &indata;

RUN;

/*Get base aggregated data;*/
proc sql noprint;
create table &INPATH.._temp_aggr_base_ECL as
select 	MTH_TM_ID,
		LEGAL_ENTITY,
		CCAR_BASEL_PRD_TP_NM,
		PD_BAND,
		PD_BASEL_SEG_NUM,
		PD_BASEL_SEG_ID,
		LGD_BASEL_SEG_NUM,
 		EAD_BASEL_SEG_NUM, 	
		LGD_BASEL_SEG_ID,
		EAD_BASEL_SEG_ID, 
		INSURER_FLAG,
	case 
			when mean(PD_FINAL_RPTG_RTO) >=1 then 'T'
			else 'F' 
		end as PD_90_DAY_F format $1.,
		sum (ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
		sum(
			case
				when ADJUSTED_OS_BAL_AMT <=0 then 0
				else ADJUSTED_OS_BAL_AMT
			end
			) as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
		max(LGD_FINAL_RPTG_RTO) as LGD_FINAL_RPTG_RATIO format 28.8,
		case 
			when mean(PD_FINAL_RPTG_RTO) >=1 then max(WGHTD_DLGD_RTO)
			else . 
		end as EXPCTD_LOSS_RTO format 28.8 


		,WGHTD_DLGD_RTO, LTV_PERCENTAGE, count(1) as OBLIGORS,
PERIOD_IND,
	sum(ECL_Drawn_1) as ECL_Drawn_1,
	sum(ECL_Drawn_2) as ECL_Drawn_2,
	sum(ECL_Drawn_3) as ECL_Drawn_3,	
	sum(ECL_Undrawn_1) as ECL_Undrawn_1,
	sum(ECL_Undrawn_2) as ECL_Undrawn_2,
	sum(ECL_Undrawn_3) as ECL_Undrawn_3,
	sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1,
	sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
	sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3 
from &indata.
where DLGD_F='Y'
group by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, PD_BAND,
 		 LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID,PD_BASEL_SEG_NUM,
		PD_BASEL_SEG_ID,INSURER_FLAG, WGHTD_DLGD_RTO, LTV_PERCENTAGE,PERIOD_IND


order by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, PD_BAND, 
 		 LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID, LTV_PERCENTAGE

		 ;
quit;
/*step 1 of last transformation rule*/
/*Get total outstanding balances - aggregate level by LEGAL_ENTITY;*/
proc sql noprint;
create table &INPATH.._temp_aggr_balance_ECL as
select 	MTH_TM_ID,
		LEGAL_ENTITY,
		sum(ADJUSTED_OS_BAL_AMT) as total_os_balance,
		sum(
			case
				when ADJUSTED_OS_BAL_AMT <=0 then 0
				else ADJUSTED_OS_BAL_AMT
			end
			) as total_os_balance_after_net ,
			PERIOD_IND,
	sum(ECL_Drawn_1) as ECL_Drawn_1,
	sum(ECL_Drawn_2) as ECL_Drawn_2,
	sum(ECL_Drawn_3) as ECL_Drawn_3,	
	sum(ECL_Undrawn_1) as ECL_Undrawn_1,
	sum(ECL_Undrawn_2) as ECL_Undrawn_2,
	sum(ECL_Undrawn_3) as ECL_Undrawn_3 ,
	sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1,
	sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
	sum(ECL_Drawn_PostSec_3) as ECL_Drawn_PostSec_3
from &indata.
where PD_FINAL_RPTG_RTO >=1
group by MTH_TM_ID, LEGAL_ENTITY
order by MTH_TM_ID, LEGAL_ENTITY 
;
quit;

/*step 2a of last transformation rule*/
/*Get ACL Lookup data;*/
proc sql noprint;
    create table &INPATH.._temp_GL_ACL_balance_ECL as 
  	select &MTH_TM_ID as MTH_TM_ID,	
		LEGAL_ENTITY,
		EFF_FROM_YR_MTH,
		EFF_TO_YR_MTH,
		GENL_LEDGER_ENTITY_ACL_AMT
	from 	&ACLlookup.
where input(EFF_FROM_YR_MTH,6.) <= &yearmonth and &yearmonth <= 
input(EFF_TO_YR_MTH,6.) 
/*	and crnt_f='Y'*/
and LEGAL_ENTITY in ('DOM-SUB-DUNDEE','DOM-SUB-MTCC','DOM-SUB-NT',
'DOM-SUB-MAPLE','DOM-SUB-SMC');
quit;

/*step 2b and 2c of last transformation rule*/
/*Get Adjusted Outstanding Balance - account level;*/
proc sql noprint; 
create table &INPATH.._temp_acct_balance_ECL as
select 	MTH_TM_ID,
		LEGAL_ENTITY,
		CCAR_BASEL_PRD_TP_NM,
		PD_BAND,
		LGD_BASEL_SEG_NUM,
 		EAD_BASEL_SEG_NUM, 	
		ADJUSTED_OS_BAL_AMT,
		PD_FINAL_RPTG_RTO,
		case
			when ADJUSTED_OS_BAL_AMT>0 then ADJUSTED_OS_BAL_AMT
			when ADJUSTED_OS_BAL_AMT<=0 then 0
			else 0
			end as ADJUSTED_OS_BAL_AMT_after_net format 20.8, 
		CONS_DFT_MTH_CNT,
		ACCT_NUM, INSURER_FLAG, WGHTD_DLGD_RTO, LTV_PERCENTAGE,
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
from &indata.  
where PD_FINAL_RPTG_RTO >=1
order by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM,
		PD_BAND, LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, ACCT_NUM;
quit;

/*step 3 of last transformation rule*/
/*Combine account level data with ACL GL and total outstanding balances;*/
proc sql noprint;
	create table &INPATH.._temp_acct_prorated_loss_ECL as
		select 	a.*,
			b.total_os_balance,
			b.total_os_balance_after_net,
			c.GENL_LEDGER_ENTITY_ACL_AMT,
			(a.ADJUSTED_OS_BAL_AMT/b.total_os_balance)*c.GENL_LEDGER_ENTITY_ACL_AMT as 
			PRORATED_CR_LOSS_ALLOW_AMT,
			(a.ADJUSTED_OS_BAL_AMT_after_net/b.total_os_balance_after_net)*c.GENL_LEDGER_ENTITY_ACL_AMT 
		as PRORATED_CR_LOSS_ALLOW_AMT_after
			from 	&INPATH.._temp_acct_balance_ECL as a 
				left join &INPATH.._temp_aggr_balance_ECL as b on a.MTH_TM_ID=b.MTH_TM_ID and 
					a.LEGAL_ENTITY=b.LEGAL_ENTITY 
				left join &INPATH.._temp_GL_ACL_balance_ECL as c on a.MTH_TM_ID=c.MTH_TM_ID and 
					a.LEGAL_ENTITY=c.LEGAL_ENTITY
				order by MTH_TM_ID,LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM,
					PD_BAND, LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, ACCT_NUM;
quit;
/*step 4 of last transformation rule*/
/*Calculate ACL at CCAR aggregated level;*/
proc sql noprint;
create table &INPATH.._temp_aggr_ACL_ECL as
select  MTH_TM_ID,
		LEGAL_ENTITY,
 		CCAR_BASEL_PRD_TP_NM,
		PD_BAND,	
 		EAD_BASEL_SEG_NUM, 
 		LGD_BASEL_SEG_NUM, 
		INSURER_FLAG, /*ADDED AS PART OF Q4*/
		WGHTD_DLGD_RTO,LTV_PERCENTAGE,
		sum(PRORATED_CR_LOSS_ALLOW_AMT) as ACL,
		sum(PRORATED_CR_LOSS_ALLOW_AMT_after) as After_Netting_ACL ,
		PERIOD_IND,
	sum(ECL_Drawn_1) as ECL_Drawn_1,
	sum(ECL_Drawn_2) as ECL_Drawn_2,
	sum(ECL_Drawn_3) as ECL_Drawn_3,	
	sum(ECL_Undrawn_1) as ECL_Undrawn_1,
	sum(ECL_Undrawn_2) as ECL_Undrawn_2,
	sum(ECL_Undrawn_3) as ECL_Undrawn_3,
	sum(ECL_Drawn_PostSec_1) as ECL_Drawn_PostSec_1,
	sum(ECL_Drawn_PostSec_2) as ECL_Drawn_PostSec_2,
	sum(ECL_Drawn_PostSec_3) as  ECL_Drawn_PostSec_3
from 	&INPATH.._temp_acct_prorated_loss_ECL
group by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, INSURER_FLAG,WGHTD_DLGD_RTO,LTV_PERCENTAGE,
		PD_BAND, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM,PERIOD_IND
order by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM,
		PD_BAND, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM
		;
quit;

/*Generate final output;*/
proc sql noprint; 
create table &outdata. as
select  
a.LEGAL_ENTITY,
a.PD_BAND,
a.CCAR_BASEL_PRD_TP_NM,
a.MTH_TM_ID,
COALESCE(a.LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM,
COALESCE(a.EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM,
a.PD_BASEL_SEG_NUM,
a.PD_BASEL_SEG_ID, 
a.LGD_BASEL_SEG_ID,
a.EAD_BASEL_SEG_ID,
a.PD_90_DAY_F,
a.INSURER_FLAG as INSUR_F,
a.BEFR_ZERO_NET_ADJ_OS_BAL_AMT,
a.AF_ZERO_NET_ADJUSTED_OS_BAL_AMT,
a.EXPCTD_LOSS_RTO,
0 as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
0 as AF_ZERO_NET_UNDRAWN_AMT format 17.3, 
case when a.PD_90_DAY_F='T' then b.ACL else 0 end as 
BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
case when a.PD_90_DAY_F='T' then b.After_Netting_ACL else 0 end as AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
a.LGD_FINAL_RPTG_RATIO as LGD_FINAL_RPTG_RTO,
0 as EAD_FINAL_RPTG_RTO format 28.8,
'CAD' as CRNCY_CD format $10.,
0 as PRTL_WRITE_OFF_AMT format 17.3,
'TRUE' as UNCONDTNLY_CNCLBL format $10.,
0 as ACCR_INTR_AMT format 17.3,
a.WGHTD_DLGD_RTO, a.LTV_PERCENTAGE, A.OBLIGORS,
a.PERIOD_IND,
a.ECL_Drawn_1,
a.ECL_Drawn_2,
a.ECL_Drawn_3,
a.ECL_Undrawn_1,
a.ECL_Undrawn_2,
a.ECL_Undrawn_3,
a.ECL_Drawn_PostSec_1,
a.ECL_Drawn_PostSec_2,
a.ECL_Drawn_PostSec_3,
&SESSIONTIME AS INSRT_PROCESS_TMSTMP,
&SESSIONTIME AS UPDT_PROCESS_TMSTMP
from &INPATH.._temp_aggr_base_ECL as a 
left join &INPATH.._temp_aggr_ACL_ECL as b
on	a.MTH_TM_ID=b.MTH_TM_ID
	and a.LEGAL_ENTITY=b.LEGAL_ENTITY
	and a.CCAR_BASEL_PRD_TP_NM=b.CCAR_BASEL_PRD_TP_NM
	and a.PD_BAND=b.PD_BAND 
	and a.EAD_BASEL_SEG_NUM=b.EAD_BASEL_SEG_NUM
	and a.LGD_BASEL_SEG_NUM=b.LGD_BASEL_SEG_NUM
	and a.INSURER_FLAG=b.INSURER_FLAG
	and a.WGHTD_DLGD_RTO=b.WGHTD_DLGD_RTO
	and a.LTV_PERCENTAGE=b.LTV_PERCENTAGE
;
quit; 
%mend getRRAPreportingExtractSubs_ECL;
 
/*Generate output datasets;*/
%getRRAPreportingExtractSubs_ECL(indata=&INPATH..RRAP_Rpt_Sub_Extract00_ECL,
							 ACLlookup=&INPATH..BASEL_SUBSIDIARY_ACL_LKP, 
							 outdata=&INPATH..RRAP_Rpt_Extract_Subsidiary_ECL);


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
,PD_90_DAY_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FINAL_RPTG_RTO
,EAD_FINAL_RPTG_RTO
,crncy_cd
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,TRIM(INSUR_F) as INSUR_F
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
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


/*UPDATE NULL VALUES TO 0 FOR TNG PRODUCTS*/
PROC SQL NOPRINT;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT IS NULL AND CCAR_BASEL_PRD_TP_NM LIKE '%TNG%'
AND MTH_TM_ID=&MTH_TM_ID;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL
SET AF_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE AF_ZERO_NET_ALWBL_CR_LOSS_AMT IS NULL AND CCAR_BASEL_PRD_TP_NM LIKE '%TNG%'
AND MTH_TM_ID=&MTH_TM_ID;
QUIT;

/****UPDATE PREV MONTH'S VALUES TO ZERO ***/
PROC SQL NOPRINT;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET BEFR_ZERO_NET_ADJ_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET AF_ZERO_NET_ADJUSTED_OS_BAL_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET BEFORE_ZERO_NET_UNDRAWN_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET AF_ZERO_NET_UNDRAWN_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET AF_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL  
SET PRTL_WRITE_OFF_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET ACCR_INTR_AMT=0 WHERE PERIOD_IND = 'PREV' ;
UPDATE &INPATH..BASEL_CCAR_EXPSR_FACT_ECL 
SET OBLIGORS=0 WHERE PERIOD_IND = 'PREV' ;
QUIT;


PROC SQL NOPRINT;
CONNECT USING NZRRAP AS NZCON;
EXECUTE(DELETE FROM EDRTLRP1D.BASEL_CCAR_EXPSR_FACT_ECL)BY NZCON  ;
quit;


proc sql;
connect using NZRRAP as nzcon;
INSERT INTO NZRRAP.BASEL_CCAR_EXPSR_FACT_ECL 
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
LGD_FINAL_RPTG_RTO,
EAD_FINAL_RPTG_RTO,
CRNCY_CD,
PRTL_WRITE_OFF_AMT,
UNCONDTNLY_CNCLBL,
ACCR_INTR_AMT,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP,
INSUR_F,
WGHTD_DLGD_RTO,
LTV_PERCENTAGE,
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
ECL_Drawn_PostSec_3)
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
LGD_FINAL_RPTG_RTO,
EAD_FINAL_RPTG_RTO,
CRNCY_CD,
PRTL_WRITE_OFF_AMT,
UNCONDTNLY_CNCLBL,
ACCR_INTR_AMT,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP,
INSUR_F,
WGHTD_DLGD_RTO,
LTV_PERCENTAGE,
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
ECL_Drawn_PostSec_3 FROM &INPATH..BASEL_CCAR_EXPSR_FACT_ECL;
disconnect from nzcon;

quit;

proc delete data=NZRRAP.BASEL_CCAR_EXPSR_FACT_ECL_temp;
run;

data NZRRAP.BASEL_CCAR_EXPSR_FACT_ECL_temp;
	set NZRRAP.BASEL_CCAR_EXPSR_FACT_ECL;
	if PERIOD_IND = 'PREV' then LEGAL_ENTITY = (TRIM(LEGAL_ENTITY) || '_PREV');
	else LEGAL_ENTITY = LEGAL_ENTITY;
run;

proc sql;
	drop table NZRRAP.BASEL_CCAR_EXPSR_FACT_ECL;
quit;

data NZRRAP.BASEL_CCAR_EXPSR_FACT_ECL;
	set NZRRAP.BASEL_CCAR_EXPSR_FACT_ECL_temp;
run;

proc sql;
	drop table NZRRAP.BASEL_CCAR_EXPSR_FACT_ECL_temp;
quit;

/*proc sql;
connect using NZUAT as nzcon;
UPDATE NZUAT.BASEL_CCAR_EXPSR_FACT_ECL 
SET LEGAL_ENTITY = (TRIM(LEGAL_ENTITY) || '_PREV')
WHERE PERIOD_IND = 'PREV';
disconnect from nzcon;
quit;*/



/*proc sql;
connect using NZUAT as nzcon;
INSERT INTO NZUAT.BASEL_CCAR_EXPSR_FACT_ECL 
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
LGD_FINAL_RPTG_RTO,
EAD_FINAL_RPTG_RTO,
CRNCY_CD,
PRTL_WRITE_OFF_AMT,
UNCONDTNLY_CNCLBL,
ACCR_INTR_AMT,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP,
INSUR_F,
WGHTD_DLGD_RTO,
LTV_PERCENTAGE,
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
ECL_Drawn_PostSec_3)
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
LGD_FINAL_RPTG_RTO,
EAD_FINAL_RPTG_RTO,
CRNCY_CD,
PRTL_WRITE_OFF_AMT,
UNCONDTNLY_CNCLBL,
ACCR_INTR_AMT,
INSRT_PROCESS_TMSTMP,
UPDT_PROCESS_TMSTMP,
INSUR_F,
WGHTD_DLGD_RTO,
LTV_PERCENTAGE,
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
ECL_Drawn_PostSec_3 FROM INPATH.BASEL_CCAR_EXPSR_FACT_ECL_MISS;
disconnect from nzcon;

quit;*/

%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Extract00_ECL RRAP_Rpt_Extract00_ECL1 RRAP_Rpt_Extract_KS_ECL RRAP_Rpt_Extract_MOR_ECL RRAP_Rpt_Extract_SPL_ECL _temp_aggr_base_ECL);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=_temp_aggr_NCR_balance_ECL _temp_dir_NCR_ECL _temp_acct_balance_ECL _temp_acct_prorated_loss_ECL _temp_aggr_ACL_ECL);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Extract_KS_Final_ECL RRAP_Rpt_Extract_MOR_Final_ECL RRAP_Rpt_Extract_SPL_Final_ECL RRAP_Rpt_Extract_AllBank_ECL);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Sub_Extract00_ECL RRAP_Rpt_Sub_Extract00_ECL1 _temp_aggr_base_ECL _temp_aggr_balance_ECL _temp_GL_ACL_balance_ECL _temp_acct_balance_ECL);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=_temp_acct_prorated_loss_ECL _temp_aggr_ACL_ECL RRAP_Rpt_Extract_Subsidiary_ECL BASEL_CCAR_EXPSR_FACT_ECL_MISS);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Extract_TNG_ECL RRAP_Rpt_Extract_TNG_Final_ECL);
