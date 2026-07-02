%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);
%global YYYYMMDD;
%LET OUTLANDING=&OUTPATH;
%let datetime_start = %sysfunc(TIME());
%put >>> START TIME: %sysfunc(datetime(),datetime14.);
options validvarname = any source source2;
%LET INPUT00=&LIB..BASEL_ECONMC_CAPTL_EXTR;
%PUT YEARMONTH IS &YEARMONTH;
%PUT MTH_TM_ID IS &MTH_TM_ID;

/*Hadi Dimashkieh - 19JUL2016 - Used for replacement of CRNT_F with EFF_FROM and EFF_TO dates*/
PROC SQL NOPRINT;
	select TM_LVL_ST_DT format=yymmn6. into :mth_tm_id_yrmth from EDRTLRT.tm_dim where tm_id = &mth_tm_id and tm_lvl='Month';
QUIT;

/*CREATE A DATASET WITH YYYYMMDD FORMAT FOR THE PROCESSING DATE*/
DATA _CHARDATE_;
	TODAYDATE=today();
	FORMAT TODAYDATE YYMMDD10.;
	TODAYYEAR=PUT(YEAR(TODAYDATE),4.);
	FORMAT TODAYYEAR $4.;

	IF MONTH(TODAYDATE)<10 THEN
		DO;
			TODAYMONTH='0'||PUT(MONTH(TODAYDATE),1.);
			FORMAT TODAYMONTH $2.;
		END;
	ELSE
		DO;
			TODAYMONTH=PUT(MONTH(TODAYDATE),2.);
			FORMAT TODAYMONTH $2.;
		END;

	IF DAY(TODAYDATE)<10 THEN
		DO;
			TODAYDAY='0'||PUT(DAY(TODAYDATE),1.);
			FORMAT TODAYDAY $2.;
		END;
	ELSE
		DO;
			TODAYDAY=PUT(DAY(TODAYDATE),2.);
			FORMAT TODAYDAY $2.;
		END;

	CHAR_PROCESSING_DATE=TODAYYEAR||TODAYMONTH||TODAYDAY;
RUN;

/*STORE THE ABOVE CREATED PROCESSING DATE INTO A MACRO VARIABLE*/
PROC SQL NOPRINT;
	SELECT CHAR_PROCESSING_DATE INTO :CHAR_PROCESSING_DATE FROM _CHARDATE_;
QUIT;

/*CREATE A DATASET WITH YYYYMMDD FORMAT FOR CURRENT DATE OR SYSTEM DATE*/
DATA _CURRENTDATE_;
	TODAYDATE=DATE();
	FORMAT TODAYDATE YYMMDD10.;
	TODAYYEAR=PUT(YEAR(TODAYDATE),4.);
	FORMAT TODAYYEAR $4.;

	IF MONTH(TODAYDATE)<10 THEN
		DO;
			TODAYMONTH='0'||PUT(MONTH(TODAYDATE),1.);
			FORMAT TODAYMONTH $2.;
		END;
	ELSE
		DO;
			TODAYMONTH=PUT(MONTH(TODAYDATE),2.);
			FORMAT TODAYMONTH $2.;
		END;

	IF DAY(TODAYDATE)<10 THEN
		DO;
			TODAYDAY='0'||PUT(DAY(TODAYDATE),1.);
			FORMAT TODAYDAY $2.;
		END;
	ELSE
		DO;
			TODAYDAY=PUT(DAY(TODAYDATE),2.);
			FORMAT TODAYDAY $2.;
		END;

	CURRENTDATE=TODAYYEAR||TODAYMONTH||TODAYDAY;
RUN;

/*STORE THE ABOVE CREATED SYSTEM DATE INTO A MACRO VARIABLE*/
PROC SQL NOPRINT;
	SELECT CURRENTDATE INTO :CURRENTDATE FROM _CURRENTDATE_;
QUIT;

/*CREATE THE FINAL DATASET BEFORE EXPORTING*/
%PUT >> Processing_Month_Time_ID = &Processing_Month_Time_ID.;
%global mth_tm_id;
%global tm_lvl_st_dt;
%global tm_lvl_end_dt;
%global dtime;

proc sql noprint;
	select tm_id as mth_tm_id, tm_lvl_st_dt, tm_lvl_end_dt , datetime() as dtime format datetime25.
		into :mth_tm_id, :tm_lvl_st_dt, :tm_lvl_end_dt, :dtime from  EDRTLRT.TM_DIM
			where tm_id= &Processing_Month_Time_ID.;
quit;

%put &mth_tm_id, &tm_lvl_st_dt, &tm_lvl_end_dt, &dtime;
%GLOBAL OW_FTP;
%LET OW_FTP = %SYSGET(OW_FTP); /* SAS gets environment variable*/

/*** prod landing source path ***/
%let nobsaf=0;
%let nobsbf=0;
%let WITHZERONET=; /**BEF_NEG_NET_;**/

/** DB2 I/O Peformance Issues needed to copy DB2 files to local servers first  **/
data work.BASEL_CCAR_EXPSR_EXTR_ECL;
	set NZRRAP.BASEL_CCAR_EXPSR_EXTR_ECL (rename=(PD_BAND=PDx));
	PDn=round(PDx);
	PD_BAND =PDn;
	drop pdx pdn;
	where mth_end_dt="&tm_lvl_end_dt"d;
run;

/* JIRA Ticket: RRMSS-694: Remove undrawn for defaulted accounts */
data work.BASEL_CCAR_EXPSR_EXTR_ECL;
	set work.BASEL_CCAR_EXPSR_EXTR_ECL;
	 if PD_BAND = 26 then AF_ZERO_NET_UNDRAWN_AMT = 0;
     if PD_BAND = 26 then BEFORE_ZERO_NET_UNDRAWN_AMT = 0;
run;

/* Modified - Added for Securitization to add new columns for Halifax and Trillium 
	Jira ticket: ENRRAP-213
*/

proc sql;
CONNECT USING &DB AS DB2CON;
create table &lib..BASEL_CCAR_EXPSR_EXTR_ECL as 
SELECT expsr.*,
		case when d.secrtztn_flag ne "" then a.expsr_drawn_prior_secur  else . end as expsr_drawn_prior_secur format=22.2,
		case when d.secrtztn_flag ne "" and expsr_drawn_no_secur eq . then 0 
			when d.secrtztn_flag ne "" then b.expsr_drawn_no_secur  
			else . 
		end as  expsr_drawn_no_secur format=22.2,
		case when d.secrtztn_flag ne "" then d.expsr_drawn_with_secur  else . end as expsr_drawn_with_secur format=22.2,
		case when d.secrtztn_flag ne "" then c.secur_amt  else . end as secur_amt format=22.2,        
		case when d.secrtztn_flag ne "" then d.expsr_drawn_reduced  else . end as expsr_drawn_reduced format=22.2,
		d.secrtztn_flag
FROM work.BASEL_CCAR_EXPSR_EXTR_ECL expsr
LEFT JOIN 
	(select * from connection to DB2CON 
/*-- 'Exposures (Drawn) Prior SECRTZTN'n*/
(select CCAR_BASEL_PRD_TP_NM, sum(expsr_drawn_prior_secur) as expsr_drawn_prior_secur
	from 
	(SELECT CCAR_BASEL_PRD_TP_NM, src_sys_cd, CONSM_PRD_TREATMNT_CD, TRNST_EXCLSN_F, prd_id,PIT_STAT_CD,
		case 
			when SRC_SYS_CD = 'KS' then sum(OS_BAL_AMT + GENL_LEDGER_BALCNG_ADJ_AMT) 
			when SRC_SYS_CD = 'SPL' and PIT_STAT_CD in ('CUR') and CONSM_PRD_TREATMNT_CD='A' and TRNST_EXCLSN_F='N' and prd_id = 'S09' then sum(OS_BAL_AMT + GENL_LEDGER_BALCNG_ADJ_AMT)
			else sum(OS_BAL_AMT) end			
		as expsr_drawn_prior_secur
	FROM EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT
	WHERE MTH_TM_ID = &mth_tm_id
		 AND ((
					SRC_SYS_CD = 'KS'
                	AND (CCAR_BASEL_PRD_TP_NM like 'REV_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'REV_CON_CL%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CL%')
                	AND OS_BAL_AMT > 0)
					or
					(SRC_SYS_CD = 'SPL'
                	AND (CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_REG%')
                	AND ADJUSTED_OS_BAL_AMT >= 0)
			)
	group by CCAR_BASEL_PRD_TP_NM, src_sys_cd,CONSM_PRD_TREATMNT_CD, TRNST_EXCLSN_F, prd_id,PIT_STAT_CD) intmed
	group by CCAR_BASEL_PRD_TP_NM
)) as a
on (EXPSR.CCAR_BASEL_PRD_TP_NM=a.CCAR_BASEL_PRD_TP_NM 
	AND expsr.MTH_END_DT="&tm_lvl_end_dt"d)
LEFT JOIN
	(select * from connection to DB2CON 
/*-- 'Exposures (Drawn) no SECRTZTN'n*/
(	SELECT CCAR_BASEL_PRD_TP_NM, sum(expsr_drawn_no_secur) as expsr_drawn_no_secur
	from 
	(SELECT CCAR_BASEL_PRD_TP_NM, src_sys_cd, CONSM_PRD_TREATMNT_CD, TRNST_EXCLSN_F, prd_id,PIT_STAT_CD,
		case 
			when SRC_SYS_CD = 'KS' then sum(OS_BAL_AMT + GENL_LEDGER_BALCNG_ADJ_AMT) 
			when SRC_SYS_CD = 'SPL' and PIT_STAT_CD in ('CUR') and CONSM_PRD_TREATMNT_CD='A' and TRNST_EXCLSN_F='N' and prd_id = 'S09' then sum(OS_BAL_AMT + GENL_LEDGER_BALCNG_ADJ_AMT)
			else sum(OS_BAL_AMT) end			
		as expsr_drawn_no_secur 
	FROM EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT
	WHERE MTH_TM_ID = &mth_tm_id
		 AND
				(( SRC_SYS_CD = 'KS'
					AND OS_BAL_AMT > 0
                	AND (CCAR_BASEL_PRD_TP_NM like 'REV_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'REV_CON_CL%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CL%')
                	AND ADJUSTED_OS_BAL_AMT = BEFORE_ZERO_NET_DRAWN_AMT )
                or
				(SRC_SYS_CD = 'SPL'
					AND ADJUSTED_OS_BAL_AMT >= 0
                	AND (CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_REG%')
                	AND ADJUSTED_OS_BAL_AMT = BEFORE_ZERO_NET_DRAWN_AMT)
				)
	group by CCAR_BASEL_PRD_TP_NM, src_sys_cd, CONSM_PRD_TREATMNT_CD, TRNST_EXCLSN_F, prd_id,PIT_STAT_CD) intmed
	group by CCAR_BASEL_PRD_TP_NM
)) as b
on (EXPSR.CCAR_BASEL_PRD_TP_NM=b.CCAR_BASEL_PRD_TP_NM
	AND expsr.MTH_END_DT="&tm_lvl_end_dt"d)
LEFT JOIN
	(select * from connection to DB2CON 
/*-- 'SECRTZTN Amount'n*/
(	SELECT CCAR_BASEL_PRD_TP_NM, sum(BEFORE_ZERO_NET_DRAWN_AMT - ADJUSTED_OS_BAL_AMT) as secur_amt
	FROM EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT
	WHERE MTH_TM_ID = &mth_tm_id
	 AND 
			((SRC_SYS_CD = 'KS'
               AND (CCAR_BASEL_PRD_TP_NM like 'REV_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'REV_CON_CL%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CL%'))
			or
			(SRC_SYS_CD = 'SPL'
                AND (CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_REG%'))
			)	
	group by CCAR_BASEL_PRD_TP_NM
)) as c
on (EXPSR.CCAR_BASEL_PRD_TP_NM=c.CCAR_BASEL_PRD_TP_NM
	AND expsr.MTH_END_DT="&tm_lvl_end_dt"d)
LEFT JOIN
	(select * from connection to DB2CON 
/* -- 'Exposures (Drawn) with SECRTZTN'n*/
/* -- and 'Exposures (Drawn) Reduced'n*/
/* -- and 'SECRTZTN Flag'n*/
(	SELECT CCAR_BASEL_PRD_TP_NM, sum(expsr_drawn_with_secur) as expsr_drawn_with_secur, sum(expsr_drawn_reduced) as expsr_drawn_reduced, 


	(CASE 
			WHEN CCAR_BASEL_PRD_TP_NM like 'REV_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CC%' THEN 'Trillium (CC)'
			WHEN CCAR_BASEL_PRD_TP_NM like 'REV_CON_CL%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CL%' THEN 'Halifax (CL)'
			WHEN CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_REG%' THEN 'START'
			else ''
		END) as SECRTZTN_FLAG
	from 
	(SELECT CCAR_BASEL_PRD_TP_NM, src_sys_cd, CONSM_PRD_TREATMNT_CD, TRNST_EXCLSN_F, prd_id,PIT_STAT_CD,
		case 
			when SRC_SYS_CD = 'KS' then sum(OS_BAL_AMT + GENL_LEDGER_BALCNG_ADJ_AMT) 
			when SRC_SYS_CD = 'SPL' and PIT_STAT_CD in ('CUR') and CONSM_PRD_TREATMNT_CD='A' and TRNST_EXCLSN_F='N' and prd_id = 'S09' then sum(OS_BAL_AMT + GENL_LEDGER_BALCNG_ADJ_AMT)
			else sum(OS_BAL_AMT) end			
		as expsr_drawn_with_secur, 
		sum(ADJUSTED_OS_BAL_AMT) as expsr_drawn_reduced
		
	FROM EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT
	WHERE MTH_TM_ID = &mth_tm_id
		AND ADJUSTED_OS_BAL_AMT <> BEFORE_ZERO_NET_DRAWN_AMT
		AND ((SRC_SYS_CD = 'KS'
                AND (CCAR_BASEL_PRD_TP_NM like 'REV_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'REV_CON_CL%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CL%'))
            or
			(SRC_SYS_CD = 'SPL'
              AND (CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_REG%'))
			)	
	group by CCAR_BASEL_PRD_TP_NM, src_sys_cd, CONSM_PRD_TREATMNT_CD, TRNST_EXCLSN_F, prd_id, PIT_STAT_CD) intmed
	group by CCAR_BASEL_PRD_TP_NM
)) as d
on (EXPSR.CCAR_BASEL_PRD_TP_NM=d.CCAR_BASEL_PRD_TP_NM
	AND expsr.MTH_END_DT="&tm_lvl_end_dt"d);
DISCONNECT FROM DB2CON;
quit;

proc sort data=&lib..BASEL_CCAR_EXPSR_EXTR_ECL;
	by UNQ_RECD_ID ; /*legal_entity;*/
run;

/*HADI*/
data &lib..Basel_ccar_pd_curve_extr;
	set &DB..Basel_ccar_pd_curve_extr (rename=(PD_BAND=PDx));
	PDn=round(PDx);
	PD_BAND =PDn;
	drop pdx pdn;
	where mth_end_dt="&tm_lvl_end_dt"d;
run;

DATA &LIB..BASEL_AIRB_GUARNT_LKP;
	SET &DB..BASEL_AIRB_GUARNT_LKP;

	/*	WHERE CRNT_F='Y';*/
	WHERE "&mth_tm_id_yrmth." BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH;
run;

data &lib..BASEL_AIRB_PRD_LKP;
	set &DB..BASEL_AIRB_PRD_LKP;

	/*	WHERE CRNT_F='Y';*/
	WHERE "&mth_tm_id_yrmth." BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH;
run;

options validvarname=any;

/**************************************************************************************************************************************************/
/**************************************************************************************************************************************************/
/**************************************************************************************************************************************************/
/**********************************************************ATTRIBUTION + STUDENT LINES*************************************************************/
/**************************************************************************************************************************************************/
/*********************************************************************START************************************************************************/
/**************************************************************************************************************************************************/



/**************************************************************************************************************************************************/
/**************************************************************************************************************************************************/
/**************************************************************************************************************************************************/
/**********************************************************ATTRIBUTION + STUDENT LINES*************************************************************/
/**************************************************************************************************************************************************/
/*********************************************************************END**************************************************************************/
/**************************************************************************************************************************************************/

%macro CCAR_FILES_GENERATE(WITHZERONET=);
	options validvarname=any;

PROC IMPORT DATAFILE="&outlanding/CCAR_PROVISIONS_MAP.csv"
OUT=&INPATH..CCAR_PROVISIONS_MAPPING(rename=('Provisions Product'n=Provisions_Product 'Basel Product Type'n=Basel_Product_Type 'Security Type Desc'n=Security_Type_Desc))
DBMS=CSV
REPLACE;
GETNAMES=YES;
RUN;

PROC IMPORT DATAFILE="&outlanding/Provisions_Post_ECL_RRAP_2019_Q4.csv"
OUT=&INPATH..POST_ECL_PROVISIONS(rename=('Security Type Desc'n=Security_Type_Desc 'ECL Drawn 1 Adj'n=ECL_Drawn_1_Adj 'ECL Drawn 2 Adj'n=ECL_Drawn_2_Adj 'ECL Drawn 3 Adj'n=ECL_Drawn_3_Adj 'ECL Undrawn 1 Adj'n=ECL_Undrawn_1_Adj 'ECL Undrawn 2 Adj'n=ECL_Undrawn_2_Adj 'ECL Undrawn 3 Adj'n=ECL_Undrawn_3_Adj ))
DBMS=CSV
REPLACE;
GETNAMES=YES;
RUN;



	%macro Attrib(Indata=,outdata=);
PROC SQL;
   CREATE TABLE &INPATH..exposer_01 AS 
   SELECT t1.'Unique Identifier'n, 
          t1.'Basel Product Type'n, 
          t1.'PD BAND'n, 
          t1.'Legal Entity'n, 
          t1.'90-Day-Past-Due-Flag'n, 
          t1.'Uncond Canc'n, 
          t1.'Exposures (Drawn)'n, 
          t1.'EAD %'n, 
          t1.Currency, 
          t1.EL, 
          t1.Undrawn, 
          t1.'Accrued Interest'n, 
          t1.ACL, 
          t1.'Partial WO'n, 
          t1.LGD, 
          t1.EADF, 
          t1.INSURER_F, 
          t1.DLGD, 
          t1.LTV_PERCENTAGE, 
          t1.OBLIGORS, 
          t1.'Exposures (Drawn) Prior SECRTZTN'n, 
          t1.'Exposures (Drawn) no SECRTZTN'n, 
          t1.'Exposures (Drawn) with SECRTZTN'n, 
          t1.'SECRTZTN Amount'n, 
          t1.'Exposures (Drawn) Reduced'n, 
          t1.'SECRTZTN Flag'n, 
          t1.'ECL Drawn 1'n, 
          t1.'ECL Drawn 2'n, 
          t1.'ECL Drawn 3'n, 
          t1.'ECL Undrawn 1'n, 
          t1.'ECL Undrawn 2'n, 
          t1.'ECL Undrawn 3'n, 
          t1.'ECL Drawn PostSec 1'n, 
          t1.'ECL Drawn PostSec 2'n, 
          t1.'ECL Drawn PostSec 3'n
      FROM &Indata t1;
QUIT;


PROC SQL;
   CREATE TABLE &INPATH..Post_Adj_values AS 
   SELECT t1.Provisions_Product, 
          t1.Basel_Product_Type, 
          t1.Security_Type_Desc, 
          t2.ECL_Drawn_1_Adj, 
          t2.ECL_Drawn_2_Adj, 
          t2.ECL_Drawn_3_Adj, 
          t2.ECL_Undrawn_1_Adj, 
          t2.ECL_Undrawn_2_Adj, 
          t2.ECL_Undrawn_3_Adj
      FROM &INPATH..CCAR_PROVISIONS_MAPPING t1
           LEFT JOIN &INPATH..POST_ECL_PROVISIONS t2 ON (t1.Security_Type_Desc = t2.Security_Type_Desc) AND 
          (t1.Provisions_Product = t2.Products);
QUIT;

PROC SQL;
   CREATE TABLE WORK.pre_CCAR_Mapping AS 
   SELECT t1.'Unique Identifier'n, 
          t1.'Basel Product Type'n,
		  t2.provisions_product, 
          t1.'PD BAND'n, 
          t1.'Legal Entity'n, 
          t1.'90-Day-Past-Due-Flag'n, 
          t1.'Uncond Canc'n, 
          t1.'Exposures (Drawn)'n, 
          t1.'EAD %'n, 
          t1.Currency, 
          t1.EL, 
          t1.Undrawn, 
          t1.'Accrued Interest'n, 
          t1.ACL, 
          t1.'Partial WO'n, 
          t1.LGD, 
          t1.EADF, 
          t1.INSURER_F, 
          t1.DLGD, 
          t1.LTV_PERCENTAGE, 
          t1.OBLIGORS, 
          t1.'Exposures (Drawn) Prior SECRTZTN'n, 
          t1.'Exposures (Drawn) no SECRTZTN'n, 
          t1.'Exposures (Drawn) with SECRTZTN'n, 
          t1.'SECRTZTN Amount'n, 
          t1.'Exposures (Drawn) Reduced'n, 
          t1.'SECRTZTN Flag'n, 
          t1.'ECL Drawn 1'n, 
          t1.'ECL Drawn 2'n, 
          t1.'ECL Drawn 3'n, 
          t1.'ECL Undrawn 1'n, 
          t1.'ECL Undrawn 2'n, 
          t1.'ECL Undrawn 3'n, 
          t1.'ECL Drawn PostSec 1'n, 
          t1.'ECL Drawn PostSec 2'n, 
          t1.'ECL Drawn PostSec 3'n
      FROM &indata t1
           LEFT JOIN &INPATH..CCAR_PROVISIONS_MAPPING t2 ON ((prxchange('s/\_\d+//',-1,compress(t1.'Basel Product Type'n))) = t2.Basel_Product_Type);
QUIT;

PROC SQL;
   CREATE TABLE &INPATH..Pre_Adj_Sum AS 
   SELECT t1.Provisions_Product, 
          /* SUM_of_ECL Drawn 1 */
            (SUM(t1.'ECL Drawn 1'n)) FORMAT=30.2 AS 'SUM_of_ECL Drawn 1'n, 
          /* SUM_of_ECL Drawn 2 */
            (SUM(t1.'ECL Drawn 2'n)) FORMAT=30.2 AS 'SUM_of_ECL Drawn 2'n, 
          /* SUM_of_ECL Drawn 3 */
            (SUM(t1.'ECL Drawn 3'n)) FORMAT=30.2 AS 'SUM_of_ECL Drawn 3'n, 
          /* SUM_of_ECL Undrawn 1 */
            (SUM(t1.'ECL Undrawn 1'n)) FORMAT=30.2 AS 'SUM_of_ECL Undrawn 1'n, 
          /* SUM_of_ECL_Undrawn_2 */
            (SUM(t1.'ECL Undrawn 2'n)) FORMAT=30.2 AS SUM_of_ECL_Undrawn_2, 
          /* SUM_of_ECL_Undrawn_3 */
            (SUM(t1.'ECL Undrawn 3'n)) FORMAT=30.2 AS SUM_of_ECL_Undrawn_3, 
          /* SUM_of_ECL Drawn PostSec 1 */
            (SUM(t1.'ECL Drawn PostSec 1'n)) FORMAT=30.2 AS 'SUM_of_ECL Drawn PostSec 1'n, 
          /* SUM_of_ECL Drawn PostSec 2 */
            (SUM(t1.'ECL Drawn PostSec 2'n)) FORMAT=30.2 AS 'SUM_of_ECL Drawn PostSec 2'n, 
          /* SUM_of_ECL Drawn PostSec 3 */
            (SUM(t1.'ECL Drawn PostSec 3'n)) FORMAT=30.2 AS 'SUM_of_ECL Drawn PostSec 3'n
      FROM WORK.PRE_CCAR_MAPPING t1
      GROUP BY t1.Provisions_Product;
QUIT;

PROC SQL;
   CREATE TABLE &INPATH..exposer_02 AS 
   SELECT t1.'Unique Identifier'n, 
          t1.'Basel Product Type'n, 
          t1.'PD BAND'n, 
          t1.'Legal Entity'n, 
          t1.'90-Day-Past-Due-Flag'n, 
          t1.'Uncond Canc'n, 
          t1.'Exposures (Drawn)'n, 
          t1.'EAD %'n, 
          t1.Currency, 
          t1.EL, 
          t1.Undrawn, 
          t1.'Accrued Interest'n, 
          t1.ACL, 
          t1.'Partial WO'n, 
          t1.LGD, 
          t1.EADF, 
          t1.INSURER_F, 
          t1.DLGD, 
          t1.LTV_PERCENTAGE, 
          t1.OBLIGORS, 
          t1.'Exposures (Drawn) Prior SECRTZTN'n, 
          t1.'Exposures (Drawn) no SECRTZTN'n, 
          t1.'Exposures (Drawn) with SECRTZTN'n, 
          t1.'SECRTZTN Amount'n, 
          t1.'Exposures (Drawn) Reduced'n, 
          t1.'SECRTZTN Flag'n,  
          t1.'ECL Drawn 1'n format 12.2, 
          t1.'ECL Drawn 2'n format 12.2, 
          t1.'ECL Drawn 3'n format 12.2, 
          t1.'ECL Undrawn 1'n format 12.2, 
          t1.'ECL Undrawn 2'n format 12.2, 
          t1.'ECL Undrawn 3'n format 12.2, 
          t1.'ECL Drawn PostSec 1'n format 12.2, 
          t1.'ECL Drawn PostSec 2'n format 12.2, 
          t1.'ECL Drawn PostSec 3'n format 12.2, 
          /* ECL_Drawn_1_Post_Adj */
            ((t2.ECL_Drawn_1_Adj / t3.'SUM_of_ECL Drawn 1'n) * t1.'ECL Drawn 1'n) format=30.2 AS 'ECL Post Adj Drawn 1'n, 
          /* ECL_DRWAN_2_POST_ADJ */
            ((t2.ECL_Drawn_2_Adj / t3.'SUM_of_ECL Drawn 2'n) * t1.'ECL Drawn 2'n) format=30.2 AS 'ECL Post Adj Drawn 2'n, 
          /* ECL_DRAWN_3_POST_ADJ */
            ((t2.ECL_Drawn_3_Adj / t3.'SUM_of_ECL Drawn 3'n) * t1.'ECL Drawn 3'n) format=30.2 AS 'ECL Post Adj Drawn 3'n, 
          /* ECL_UNDRAWN_1_POST_ADJ */
            ((t2.ECL_Undrawn_1_Adj / t3.'SUM_of_ECL Undrawn 1'n) * t1.'ECL Undrawn 1'n) format=30.2 AS 'ECL Post Adj Undrawn 1'n, 
          /* ECL_UNDRAWN_2_POST_ADJ */
            ((t2.ECL_Undrawn_2_Adj / t3.SUM_of_ECL_Undrawn_2) * t1.'ECL Undrawn 2'n) format=30.2 AS 'ECL Post Adj Undrawn 2'n, 
          /* ECL_UNDRAWN_3_POST_ADJ */
            ((t2.ECL_Undrawn_3_Adj / t3.SUM_of_ECL_Undrawn_3)*t1.'ECL Undrawn 3'n) format=30.2 AS 'ECL Post Adj Undrawn 3'n, 
          /* ECL_DRAWN_POSTSEC_ADJ_1 */
           (calculated 'ECL Post Adj Drawn 1'n/t1.'ECL Drawn 1'n)*t1.'ECL Drawn PostSec 1'n format=30.2 AS 'ECL Post Adj Drawn PostSec 1'n, 
          /* ECL_DRAWN_POSTSEC_ADJ_2 */
            (calculated 'ECL Post Adj Drawn 2'n/t1.'ECL Drawn 2'n)*t1.'ECL Drawn PostSec 2'n format=30.2 AS 'ECL Post Adj Drawn PostSec 2'n, 
          /* ECL_DRAWN_POSTSEC_ADJ_3 */
            (calculated 'ECL Post Adj Drawn 3'n/t1.'ECL Drawn 3'n)*t1.'ECL Drawn PostSec 3'n format=30.2 AS 'ECL Post Adj Drawn PostSec 3'n
      FROM &indata t1
           LEFT JOIN (&INPATH..POST_ADJ_VALUES t2
           LEFT JOIN &INPATH..PRE_ADJ_SUM t3 ON (t2.Provisions_Product = t3.Provisions_Product)) ON ((prxchange('s/\_\d+//',-1,compress(t1.'Basel Product Type'n))) = t2.BASEL_PRODUCT_TYPE);
QUIT;

data test;
	set &INPATH..exposer_02;
run;
proc sql;
	delete from test t1
	where (prxchange('s/\_\d+//',-1,compress(t1.'Basel Product Type'n))) in ('OTHER_CON_SLR',
            'OTHER_CON_SLT','REV_CON_SLR');
quit;

proc sql;
	create table work.spsp_temp1 as
	select t1.'Unique Identifier'n, 
          t1.'Basel Product Type'n, 
          t1.'PD BAND'n, 
          t1.'Legal Entity'n, 
          t1.'90-Day-Past-Due-Flag'n, 
          t1.'Uncond Canc'n, 
          t1.'Exposures (Drawn)'n, 
          t1.'EAD %'n, 
          t1.Currency, 
          t1.EL, 
          t1.Undrawn, 
          t1.'Accrued Interest'n, 
          t1.ACL, 
          t1.'Partial WO'n, 
          t1.LGD, 
          t1.EADF, 
          t1.INSURER_F, 
          t1.DLGD, 
          t1.LTV_PERCENTAGE, 
          t1.OBLIGORS, 
          t1.'Exposures (Drawn) Prior SECRTZTN'n, 
          t1.'Exposures (Drawn) no SECRTZTN'n, 
          t1.'Exposures (Drawn) with SECRTZTN'n, 
          t1.'SECRTZTN Amount'n, 
          t1.'Exposures (Drawn) Reduced'n, 
          t1.'SECRTZTN Flag'n, 
          t1.'ECL Drawn 1'n, 
          t1.'ECL Drawn 2'n, 
          t1.'ECL Drawn 3'n, 
          t1.'ECL Undrawn 1'n, 
          t1.'ECL Undrawn 2'n, 
          t1.'ECL Undrawn 3'n, 
          t1.'ECL Drawn PostSec 1'n, 
          t1.'ECL Drawn PostSec 2'n, 
          t1.'ECL Drawn PostSec 3'n, 
          /* Product_Type */
            (case when (prxchange('s/\_\d+//',-1,compress(t1.'Basel Product Type'n))) in ('OTHER_CON_SLR',
            'OTHER_CON_SLT','REV_CON_SLR') then 'Student Lines' end) AS Product_Type
	 FROM &INPATH..exposer_01 t1
			where (prxchange('s/\_\d+//',-1,compress(t1.'Basel Product Type'n))) in ('OTHER_CON_SLR','OTHER_CON_SLT','REV_CON_SLR');
	QUIT;

proc sql;
	create table spsp_temp2 as
	select t1.*, t2.ECL_Drawn_1_Adj,t2.ECL_Drawn_2_Adj,t2.ECL_Drawn_3_Adj
	FROM spsp_temp1 t1
		LEFT JOIN &INPATH..POST_ECL_PROVISIONS t2 ON (t1.Product_Type = t2.Products);
quit;

proc sql;
	create table spsp_temp3_26 as
	select *,(SUM(t1.'Exposures (Drawn)'n)) FORMAT=22.2 AS SUM_EXPSR_DRAWN_SPSP, 
          /* SUM_EXPSR_UNDRAWN_SPSP */
            (SUM(t1.Undrawn)) FORMAT=19.2 AS SUM_EXPSR_UNDRAWN_SPSP, 
          /* TOTAL_SPSP_PRE */
            ((SUM(t1.'Exposures (Drawn)'n)) + (SUM(t1.Undrawn))) AS TOTAL_SPSP_PRE_26,
			((('Exposures (Drawn)'n + Undrawn)/calculated TOTAL_SPSP_PRE_26)*ECL_Drawn_3_Adj) as ECL_DRAWN_3_Post
	from spsp_temp2 t1
/*	where t1.'PD BAND'n='26'; */
	where t1.'PD BAND'n=26;
quit;

Proc sql;
	create table spsp_temp3_26_filter as 
	select * from spsp_temp3_26 where 'Basel Product Type'n is not missing;
quit;

proc sql;
	create table spsp_temp3_NE26 as
	select *,             (SUM(t1.'Exposures (Drawn)'n)) FORMAT=22.2 AS SUM_EXPSR_DRAWN_SPSP, 
          /* SUM_EXPSR_UNDRAWN_SPSP */
            (SUM(t1.Undrawn)) FORMAT=19.2 AS SUM_EXPSR_UNDRAWN_SPSP, 
          /* TOTAL_SPSP_PRE */
            ((SUM(t1.'Exposures (Drawn)'n)) + (SUM(t1.Undrawn))) AS TOTAL_SPSP_PRE_NE26,

			((('Exposures (Drawn)'n + Undrawn)/calculated TOTAL_SPSP_PRE_NE26)*ECL_Drawn_1_Adj) as ECL_DRAWN_1_Post,
((('Exposures (Drawn)'n + Undrawn)/calculated TOTAL_SPSP_PRE_NE26)*ECL_Drawn_2_Adj) as ECL_DRAWN_2_Post
	from spsp_temp2 t1
/*	where t1.'PD BAND'n not eq '26'; */
	where t1.'PD BAND'n not eq 26;
quit;

Proc sql;
	create table spsp_temp3_NE26_filter as 
	select * from spsp_temp3_NE26 where 'Basel Product Type'n is not missing;
quit;

data spsp_total;
	set spsp_temp3_26_filter(rename=(TOTAL_SPSP_PRE_26=TOTAL_SPSP_PRE)) spsp_temp3_NE26_filter(rename=(TOTAL_SPSP_PRE_NE26=TOTAL_SPSP_PRE));
run;

proc sql;
	create table append_attrib_SPSP as
	select  t1.'Unique Identifier'n, 
          t1.'Basel Product Type'n, 
          t1.'PD BAND'n, 
          t1.'Legal Entity'n, 
          t1.'90-Day-Past-Due-Flag'n, 
          t1.'Uncond Canc'n, 
          t1.'Exposures (Drawn)'n, 
          t1.'EAD %'n, 
          t1.Currency, 
          t1.EL, 
          t1.Undrawn, 
          t1.'Accrued Interest'n, 
          t1.ACL, 
          t1.'Partial WO'n, 
          t1.LGD, 
          t1.EADF, 
          t1.INSURER_F, 
          t1.DLGD, 
          t1.LTV_PERCENTAGE, 
          t1.OBLIGORS, 
          t1.'Exposures (Drawn) Prior SECRTZTN'n, 
          t1.'Exposures (Drawn) no SECRTZTN'n, 
          t1.'Exposures (Drawn) with SECRTZTN'n, 
          t1.'SECRTZTN Amount'n, 
          t1.'Exposures (Drawn) Reduced'n, 
          t1.'SECRTZTN Flag'n, 
          t1.'ECL Drawn 1'n, 
          t1.'ECL Drawn 2'n, 
          t1.'ECL Drawn 3'n, 
          t1.'ECL Undrawn 1'n, 
          t1.'ECL Undrawn 2'n, 
          t1.'ECL Undrawn 3'n, 
          t1.'ECL Drawn PostSec 1'n, 
          t1.'ECL Drawn PostSec 2'n, 
          t1.'ECL Drawn PostSec 3'n,
		  t1.ECL_DRAWN_1_Post as 'ECL Post Adj Drawn 1'n,
		  t1.ECL_DRAWN_2_Post as 'ECL Post Adj Drawn 2'n,
		  t1.ECL_DRAWN_3_Post as 'ECL Post Adj Drawn 3'n,
		  . as 'ECL Post Adj Undrawn 1'n,
		  . as 'ECL Post Adj Undrawn 2'n,
		  . as 'ECL Post Adj Undrawn 3'n,
		  t1.ECL_Drawn_1_post as 'ECL Post Adj Drawn PostSec 1'n,
		  t1.ECL_Drawn_2_post as 'ECL Post Adj Drawn PostSec 2'n,
		  t1.ECL_Drawn_3_post as 'ECL Post Adj Drawn PostSec 3'n
		from spsp_total t1;
	quit;

	proc sql;
		insert into test
		select * from append_attrib_SPSP;
	quit;

proc sort data=test out=&outdata;
	by 'Unique Identifier'n;
run;
%mend Attrib;

	%if &WITHZERONET ne %then
		%do;
			/** BEFORE ZERO NETTING **/
			%let reqvars=
				UNQ_RECD_ID
				CCAR_BASEL_PRD_TP_NM
				PD_BAND
				LEGAL_ENTITY
				PD_90_DAY_F
				UNCONDTNLY_CNCLBL
				BEFR_ZERO_NET_ADJ_OS_BAL_AMT
				EAD_PC
				CRNCY_CD
				EXPCTD_LOSS_RTO_TEXT
				BEFORE_ZERO_NET_UNDRAWN_AMT
				ACCR_INTR_AMT
				BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
				PRTL_WRITE_OFF_AMT
				LGD_FINAL_RPTG_RTO_TEXT
				EAD_FINAL_RPTG_RTO_TEXT
				INSUR_F
				WGHTD_DLGD_RTO
				LTV_PERCENTAGE
				OBLIGORS
				/* Added for securitization */
				expsr_drawn_prior_secur
				expsr_drawn_no_secur
				expsr_drawn_with_secur
				secur_amt
				expsr_drawn_reduced
				secrtztn_flag
				ECL_Drawn_1
				ECL_Drawn_2
				ECL_Drawn_3
				ECL_Undrawn_1
				ECL_Undrawn_2
				ECL_Undrawn_3
				ECL_Drawn_PostSec_1
				ECL_Drawn_PostSec_2
				ECL_Drawn_PostSec_3;

			data BASEL_CCAR_EXPSR_EXTR_ECL_TMP0;
				retain  &reqvars.;
				set &LIB..BASEL_CCAR_EXPSR_EXTR_ECL
					(keep= mth_end_dt  &reqvars. ) nobs = lastobs;					
				/* where(BEFR_ZERO_NET_ADJ_OS_BAL_AMT ne 0 or BEFORE_ZERO_NET_UNDRAWN_AMT ne 0);*/
				 dt = compress(put(mth_end_dt,yymmdd10.),'-');
				rundt = compress(put(today(),yymmdd10.),'-');
				tm = put(time(),tod8.);
				if expsr_drawn_prior_secur = . then
					expsr_drawn_prior_secur = BEFR_ZERO_NET_ADJ_OS_BAL_AMT;
				call symput('YYYYMMDD',compress(dt));
				call symput('rundt',compress(rundt));
				format WGHTD_DLGD_RTO percent14.6;

				/*call symput('nobsbf',_n_);*/
				/*drop mth_end_dt;*/
			run;

			options validvarname = any;

			data &lib..EXP1_&WITHZERONET.
				&lib..SUB_DUNDEE_EXP2_&WITHZERONET.
				&lib..SUB_TNG_EXP3_&WITHZERONET.
				&lib..SUB_MTCC_EXP4_&WITHZERONET.
				&lib..SUB_NT_EXP5_&WITHZERONET.
				&lib..SUB_SMC_MAPLE_EXP6_&WITHZERONET.
			;
				set BASEL_CCAR_EXPSR_EXTR_ECL_TMP0;
				array cvar 
					$ EXPCTD_LOSS_RTO_TEXT  LGD_FINAL_RPTG_RTO_TEXT EAD_FINAL_RPTG_RTO_TEXT EAD_PC;

				/*do over cvar;*/
				/*if indexc(cvar,'123456789') = 0 then cvar = ''; */
				/*else cvar=trim(compress(cvar,'%'))||'0000%';*/
				/*end;*/
				if Legal_Entity IN ('DOM-SUB-SMC', 'DOM-SUB-MAPLE', 'DOM-SUB-NT','DOM-SUB-MTCC','DOM-SUB-DUNDEE') then
					EAD_FINAL_RPTG_RTO_TEXT = '100.00000%';
				keep &reqvars.;

				if Legal_Entity = 'DOM-BANK-ALONE' then
					output &lib..EXP1_&WITHZERONET.;
				else if Legal_Entity = 'DOM-SUB-DUNDEE' then
					output &lib..SUB_DUNDEE_EXP2_&WITHZERONET.;
				else if Legal_Entity = 'DOM-SUB-TNG' then
					output &lib..SUB_TNG_EXP3_&WITHZERONET.;
				else if Legal_Entity = 'DOM-SUB-MTCC' then
					output &lib..SUB_MTCC_EXP4_&WITHZERONET.;
				else if Legal_Entity = 'DOM-SUB-NT' then
					output &lib..SUB_NT_EXP5_&WITHZERONET.;
				else if Legal_Entity IN ('DOM-SUB-SMC', 'DOM-SUB-MAPLE') then
					output &lib..SUB_SMC_MAPLE_EXP6_&WITHZERONET.;
				rename
					UNQ_RECD_ID = 'Unique Identifier'n
					CCAR_BASEL_PRD_TP_NM = 'Basel Product Type'n
					PD_BAND = 'PD BAND'n
					LEGAL_ENTITY = 'Legal Entity'n
					PD_90_DAY_F = '90-Day-Past-Due-Flag'n
					UNCONDTNLY_CNCLBL = 'Uncond Canc'n
					BEFR_ZERO_NET_ADJ_OS_BAL_AMT = 'Exposures (Drawn)'n
					EAD_PC = 'EAD %'n
					CRNCY_CD = Currency
					EXPCTD_LOSS_RTO_TEXT = EL
					BEFORE_ZERO_NET_UNDRAWN_AMT = Undrawn
					ACCR_INTR_AMT = 'Accrued Interest'n
					BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT = ACL
					PRTL_WRITE_OFF_AMT = 'Partial WO'n
					LGD_FINAL_RPTG_RTO_TEXT = LGD
					EAD_FINAL_RPTG_RTO_TEXT = EADF
					INSUR_F= INSURER_F
					WGHTD_DLGD_RTO=DLGD
					OBLIGORS=OBLIGORS
				/* Added for securitization */
					expsr_drawn_prior_secur='Exposures (Drawn) Prior SECRTZTN'n
					expsr_drawn_no_secur='Exposures (Drawn) no SECRTZTN'n
					expsr_drawn_with_secur='Exposures (Drawn) with SECRTZTN'n
					secur_amt='SECRTZTN Amount'n
					expsr_drawn_reduced='Exposures (Drawn) Reduced'n
					secrtztn_flag='SECRTZTN Flag'n
					ECL_Drawn_1='ECL Drawn 1'n
					ECL_Drawn_2='ECL Drawn 2'n
					ECL_Drawn_3='ECL Drawn 3'n
					ECL_Undrawn_1='ECL Undrawn 1'n
					ECL_Undrawn_2='ECL Undrawn 2'n
					ECL_Undrawn_3='ECL Undrawn 3'n
					ECL_Drawn_PostSec_1='ECL Drawn PostSec 1'n
					ECL_Drawn_PostSec_2='ECL Drawn PostSec 2'n
					ECL_Drawn_PostSec_3='ECL Drawn PostSec 3'n;
			RUN;

%attrib(indata=&lib..EXP1_&WITHZERONET., outdata=&lib..EXPSR_&WITHZERONET.)
%attrib(indata=&lib..SUB_DUNDEE_EXP2_&WITHZERONET., outdata=&lib..SUB_DUNDEE_EXPSR_&WITHZERONET.)
%attrib(indata=&lib..SUB_TNG_EXP3_&WITHZERONET., outdata=&lib..SUB_TNG_EXPSR_&WITHZERONET.)
%attrib(indata=&lib..SUB_MTCC_EXP4_&WITHZERONET., outdata=&lib..SUB_MTCC_EXPSR_&WITHZERONET.)
%attrib(indata=&lib..SUB_NT_EXP5_&WITHZERONET., outdata=&lib..SUB_NT_EXPSR_&WITHZERONET.)
%attrib(indata=&lib..SUB_SMC_MAPLE_EXP6_&WITHZERONET., outdata=&lib..SUB_SMC_MAPLE_EXPSR_&WITHZERONET.)


data &lib..EXPSR_&WITHZERONET.;
				SET &lib..EXPSR_&WITHZERONET.;;
				call symput('nobsbf',_n_);

				if ACL = . then
					ACL = 0;
			run;

			/** producing BEFORE ZERO control CTL table **/
			/*** calculating HAS_TOTAL BEFORE ZERO for control file **/
			proc sql threads noprint;
				select
					/*mth_end_dt*/

					/*,legal_entity*/
					sum('Exposures (Drawn)'n) format=25.2 as hash_total_bf
					into :hashtotbf
						from &lib..EXPSR_&WITHZERONET.
							/*group by*/

					/*mth_end_dt*/
					/*,legal_entity*/
					;
			quit;

			%put >>> BEFR_ZERO hash_total = &hashtotbf.;

			DATA &lib..EXPSR_&WITHZERONET.&YYYYMMDD._ctl;
				length 
					YYYYMMDD 
					rundt $10
					nobs $6
					hashtot $15;
				file "&outlanding./cmf/outgoing/DR-AIRB-EXPOSURES-&WITHZERONET.&YYYYMMDD..ctl";

				**lrecl=203 
				recfm=V;
				YYYYMMDD = "&YYYYMMDD.,";
				rundt = "&rundt.,";
				nobs = left(put(&nobsbf. ,6.));
				n2= &hashtotbf.;
				hashtot = left(put(n2 , 25.2));

				/*hashtot = "&hashtotbf.";*/
				DLM=",";
				mtxtx=compress((rundt||nobs||DLM||hashtot),' ');
				put
					@1 YYYYMMDD
					@10 mtxtx
				;

				/*@20 nobs*/
				/*@29 DLM*/
				/*@30 hashtot*/
				/*;*/
				output;
			run;

%macro exportcsv_befznet(dsn=DR-AIRB-EXPOSURES-&WITHZERONET.);

	data tmp_name;
		dsname1 = trim(substr(translate("&dsn.",'_','-'),9));
		dsname2 = trim(tranwrd(dsname1,'EXPOSURES','EXPSR'));
		call symput ('dsname',trim(dsname2));
	run;

	%put >>> dsname = &dsname.;

	data &dsname;
		set &lib..&dsname;
		pd_bandn = input('PD BAND'n,8.);

	proc sort out= &lib..&dsname (drop= PD_BANDn )force;
		by 'Unique Identifier'n
  		   'Basel Product Type'n
			PD_BANDn;
	run;

	proc export data = &lib..&dsname  outfile="&outlanding./cmf/outgoing/&dsn.&YYYYMMDD..csv"
		dbms=csv replace;
	run;

	/*x 'sed s/\"//g &outlanding./&dsn.&YYYYMMDD..csv';*/
%mend exportcsv_befznet;

%exportcsv_befznet(dsn=DR-AIRB-EXPOSURES-&WITHZERONET.);
%exportcsv_befznet(dsn=DR-AIRB-SUB-DUNDEE-EXPOSURES-&WITHZERONET.);
%exportcsv_befznet(dsn=DR-AIRB-SUB-TNG-EXPOSURES-&WITHZERONET.);
%exportcsv_befznet(dsn=DR-AIRB-SUB-MTCC-EXPOSURES-&WITHZERONET.);
%exportcsv_befznet(dsn=DR-AIRB-SUB-NT-EXPOSURES-&WITHZERONET.);
%exportcsv_befznet(dsn=DR-AIRB-SUB-SMC-MAPLE-EXPOSURES-&WITHZERONET.);
%end;
%else %if &WITHZERONET = %then
	%do;
		/** AFTER ZERO NETTING *************************************/
		%let reqvars=
			UNQ_RECD_ID
			CCAR_BASEL_PRD_TP_NM
			PD_BAND
			LEGAL_ENTITY
			PD_90_DAY_F
			UNCONDTNLY_CNCLBL
			AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
			EAD_PC
			CRNCY_CD
			EXPCTD_LOSS_RTO_TEXT
			AF_ZERO_NET_UNDRAWN_AMT
			ACCR_INTR_AMT
			AF_ZERO_NET_ALWBL_CR_LOSS_AMT
			PRTL_WRITE_OFF_AMT
			LGD_FINAL_RPTG_RTO_TEXT
			EAD_FINAL_RPTG_RTO_TEXT
			INSUR_F
			WGHTD_DLGD_RTO
			LTV_PERCENTAGE
			OBLIGORS
		/* Added for securitization */
			expsr_drawn_prior_secur
			expsr_drawn_no_secur
			expsr_drawn_with_secur
			secur_amt
			expsr_drawn_reduced
			secrtztn_flag
			ECL_Drawn_1
			ECL_Drawn_2
			ECL_Drawn_3
			ECL_Undrawn_1
			ECL_Undrawn_2
			ECL_Undrawn_3
			ECL_Drawn_PostSec_1
			ECL_Drawn_PostSec_2
			ECL_Drawn_PostSec_3;

		data BASEL_CCAR_EXPSR_EXTR_ECL_TMP0;
			retain  &reqvars.;
			set &LIB..BASEL_CCAR_EXPSR_EXTR_ECL
				(keep= mth_end_dt  &reqvars. ) nobs = lastobs;
			/*	where AF_ZERO_NET_ADJUSTED_OS_BAL_AMT ne 0  OR AF_ZERO_NET_UNDRAWN_AMT ne 0;*/
			dt = compress(put(mth_end_dt,yymmdd10.),'-');
			rundt = compress(put(today(),yymmdd10.),'-');
			tm = put(time(),tod8.);

			if EAD_FINAL_RPTG_RTO_TEXT = '' then
				EAD_FINAL_RPTG_RTO_TEXT = '0.000000%';
			if expsr_drawn_prior_secur = . then
				expsr_drawn_prior_secur = AF_ZERO_NET_ADJUSTED_OS_BAL_AMT;
			call symput('YYYYMMDD',compress(dt));
			call symput('rundt',compress(rundt));
			format WGHTD_DLGD_RTO percent14.6;

			/*call symput('nobs',compress(lastobs));*/
			/*call symput('nobsaf',_n_);*/
			/*drop mth_end_dt;*/
		run;

		data &lib..EXPSR1_&WITHZERONET.&YYYYMMDD.
			&lib..SUB_DUNDEE_EXPSR2_&WITHZERONET.&YYYYMMDD.
			&lib..SUB_TNG_EXPSR3_&WITHZERONET.&YYYYMMDD.
			&lib..SUB_MTCC_EXPSR4_&WITHZERONET.&YYYYMMDD.
			&lib..SUB_NT_EXPSR5_&WITHZERONET.&YYYYMMDD.
			&lib..SUB_SMC_MAPLE_EXPSR6_&WITHZERONET.&YYYYMMDD.
		;
			set BASEL_CCAR_EXPSR_EXTR_ECL_TMP0;
			array cvar 
				$ EXPCTD_LOSS_RTO_TEXT  LGD_FINAL_RPTG_RTO_TEXT EAD_FINAL_RPTG_RTO_TEXT EAD_PC;

			/*do over cvar;*/
			/*if indexc(cvar,'123456789') = 0 then cvar = ''; */
			/*else cvar=trim(compress(cvar,'%'))||'0000%';*/
			/*end;*/
			if Legal_Entity IN ('DOM-SUB-SMC', 'DOM-SUB-MAPLE', 'DOM-SUB-NT','DOM-SUB-MTCC','DOM-SUB-DUNDEE') then
				EAD_FINAL_RPTG_RTO_TEXT = '100.00000%';
			keep &reqvars.;

			if Legal_Entity = 'DOM-BANK-ALONE' then
				output &lib..EXPSR1_&WITHZERONET.&YYYYMMDD.;
			else if Legal_Entity = 'DOM-SUB-DUNDEE' then
				output &lib..SUB_DUNDEE_EXPSR2_&WITHZERONET.&YYYYMMDD.;
			else if Legal_Entity = 'DOM-SUB-TNG' then
				output &lib..SUB_TNG_EXPSR3_&WITHZERONET.&YYYYMMDD.;
			else if Legal_Entity = 'DOM-SUB-MTCC' then
				output &lib..SUB_MTCC_EXPSR4_&WITHZERONET.&YYYYMMDD.;
			else if Legal_Entity = 'DOM-SUB-NT' then
				output &lib..SUB_NT_EXPSR5_&WITHZERONET.&YYYYMMDD.;
			else if Legal_Entity IN ('DOM-SUB-SMC', 'DOM-SUB-MAPLE') then
				output &lib..SUB_SMC_MAPLE_EXPSR6_&WITHZERONET.&YYYYMMDD.;

			/*call symput('nobsaf',_n_);*/
			rename
				UNQ_RECD_ID = 'Unique Identifier'n
				CCAR_BASEL_PRD_TP_NM = 'Basel Product Type'n
				PD_BAND = 'PD BAND'n
				LEGAL_ENTITY = 'Legal Entity'n
				PD_90_DAY_F = '90-Day-Past-Due-Flag'n
				UNCONDTNLY_CNCLBL = 'Uncond Canc'n
				AF_ZERO_NET_ADJUSTED_OS_BAL_AMT = 'Exposures (Drawn)'n
				EAD_PC = 'EAD %'n
				CRNCY_CD = Currency
				EXPCTD_LOSS_RTO_TEXT = EL
				AF_ZERO_NET_UNDRAWN_AMT = Undrawn
				ACCR_INTR_AMT = 'Accrued Interest'n
				AF_ZERO_NET_ALWBL_CR_LOSS_AMT = ACL
				PRTL_WRITE_OFF_AMT = 'Partial WO'n
				LGD_FINAL_RPTG_RTO_TEXT = LGD
				EAD_FINAL_RPTG_RTO_TEXT = EADF
				INSUR_F=INSURER_F
				WGHTD_DLGD_RTO=DLGD
				OBLIGORS=OBLIGORS
			/* Added for securitization */
				expsr_drawn_prior_secur='Exposures (Drawn) Prior SECRTZTN'n
				expsr_drawn_no_secur='Exposures (Drawn) no SECRTZTN'n
				expsr_drawn_with_secur='Exposures (Drawn) with SECRTZTN'n
				secur_amt='SECRTZTN Amount'n
				expsr_drawn_reduced='Exposures (Drawn) Reduced'n
				secrtztn_flag='SECRTZTN Flag'n
				ECL_Drawn_1='ECL Drawn 1'n
				ECL_Drawn_2='ECL Drawn 2'n
				ECL_Drawn_3='ECL Drawn 3'n
				ECL_Undrawn_1='ECL Undrawn 1'n
				ECL_Undrawn_2='ECL Undrawn 2'n
				ECL_Undrawn_3='ECL Undrawn 3'n
				ECL_Drawn_PostSec_1='ECL Drawn PostSec 1'n
				ECL_Drawn_PostSec_2='ECL Drawn PostSec 2'n
				ECL_Drawn_PostSec_3='ECL Drawn PostSec 3'n
				;
		run;


%attrib(indata=&lib..EXPSR1_&WITHZERONET.&YYYYMMDD., outdata=&lib..EXPSR_&WITHZERONET.&YYYYMMDD.)
%attrib(indata=&lib..SUB_DUNDEE_EXPSR2_&WITHZERONET.&YYYYMMDD., outdata=&lib..SUB_DUNDEE_EXPSR_&WITHZERONET.&YYYYMMDD.)
%attrib(indata=&lib..SUB_TNG_EXPSR3_&WITHZERONET.&YYYYMMDD., outdata=&lib..SUB_TNG_EXPSR_&WITHZERONET.&YYYYMMDD.)
%attrib(indata=&lib..SUB_MTCC_EXPSR4_&WITHZERONET.&YYYYMMDD., outdata=&lib..SUB_MTCC_EXPSR_&WITHZERONET.&YYYYMMDD.)
%attrib(indata=&lib..SUB_NT_EXPSR5_&WITHZERONET.&YYYYMMDD., outdata=&lib..SUB_NT_EXPSR_&WITHZERONET.&YYYYMMDD.)
%attrib(indata=&lib..SUB_SMC_MAPLE_EXPSR6_&WITHZERONET.&YYYYMMDD., outdata=&lib..SUB_SMC_MAPLE_EXPSR_&WITHZERONET.&YYYYMMDD.)


data &lib..EXPSR_&WITHZERONET.&YYYYMMDD.;
			SET &lib..EXPSR_&WITHZERONET.&YYYYMMDD.;
			call symput('nobsaf',_n_);

			if ACL = . then
				ACL = 0;
		run;

		%put >>> AF_ZERO NOBS_AF = &nobsaf.;

		/** producing control AFTER ZERO CTL table **/
		/*** calculating HASh_TOTAL for AFTER ZERO control file **/
		proc sql threads noprint;
			select 

				/*sai updated code due to data problems for aug 19th run with inputs from Min*/

			/*sum(input(EADF,percent8.6)) format=25.2 as hash_total_af*/
			sum('Exposures (Drawn)'n) format 25.2 as hash_total_af into :hashtotaf
			from &lib..EXPSR_&WITHZERONET.&YYYYMMDD.
			;
		quit;

		%put >>> AF_ZERO hash_total = &hashtotaf.;

		DATA &lib..EXPSR_&WITHZERONET.&YYYYMMDD._ctl;
			length 
				YYYYMMDD 
				rundt $10
				nobs $6
				hashtot $15 n1 8;
			file "&outlanding./cmf/outgoing/DR-AIRB-EXPOSURES-&WITHZERONET.&YYYYMMDD..ctl" ;/*lrecl=203 recfm=V ;*/
			YYYYMMDD = "&YYYYMMDD.,";
			rundt = "&rundt.,";
			nobs = left(put(&nobsaf. ,6.));
			n1= &hashtotaf.;
			hashtot = left(put(n1 , 25.2));

			/*hashtot = "&hashtotaf.";*/
			DLM=",";
			mtxtx=compress((rundt||nobs||DLM||hashtot),' ');
			put
				@1 YYYYMMDD
				@10 mtxtx
			;

			/*@20 nobs*/
			/*@29 DLM*/
			/*@30 hashtot*/
			/*;*/
			output;
		run;

		/************
		DR-AIRB-EXPOSURES-YYYYMMDD.csv  Legal_Entity = ''DOM-BANK-ALONE'
		DR-AIRB-SUB-DUNDEE-EXPOSURES-YYYYMMDD.csv       Legal_Entity = 'DOM-SUB-DUNDEE'
		DR-AIRB-SUB-TNG-EXPOSURES-YYYYMMDD.csv  Legal_Entity = 'DOM-SUB-TNG'
		DR-AIRB-SUB-MTCC-EXPOSURES-YYYYMMDD.csv Legal_Entity = 'DOM-SUB-MTCC'
		DR-AIRB-SUB-NT-EXPOSURES-YYYYMMDD.csv   Legal_Entity = 'DOM-SUB-NT'
		DR-AIRB-SUB-SMC-MAPLE-EXPOSURES-YYYYMMDD.csv Legal_Entity IN ('DOM-SUB-SMC', 
		'DOM-SUB-MAPLE')
		***************/
%macro exportcsv(dsn=DR-AIRB-EXPOSURES-&WITHZERONET.&YYYYMMDD.);

	data tmp_name;
		dsname1 = trim(substr(translate("&dsn.",'_','-'),9,50));
		dsname2 = trim(tranwrd(dsname1,'EXPOSURES','EXPSR'));
		call symput ('dsname',trim(dsname2));
	run;

	%put >>> dsname = &dsname.;

	data &dsname;
		set &lib..&dsname;
		pd_bandn = input('PD BAND'n,8.);

	proc sort out= &lib..&dsname (drop= PD_BANDn )force;
		by   'Unique Identifier'n 
             'Basel Product Type'n
			PD_BANDn;
	run;

	proc export data = &lib..&dsname  outfile="&outlanding./cmf/outgoing/&dsn..csv"
		dbms=csv replace;
	run;

	/*x 'sed s/\"//g &outlanding./&dsn.&YYYYMMDD..csv';*/
%mend exportcsv;

%exportcsv(dsn=DR-AIRB-EXPOSURES-&WITHZERONET.&YYYYMMDD.);
%exportcsv(dsn=DR-AIRB-SUB-DUNDEE-EXPOSURES-&WITHZERONET.&YYYYMMDD.);
%exportcsv(dsn=DR-AIRB-SUB-TNG-EXPOSURES-&WITHZERONET.&YYYYMMDD.);
%exportcsv(dsn=DR-AIRB-SUB-MTCC-EXPOSURES-&WITHZERONET.&YYYYMMDD.);
%exportcsv(dsn=DR-AIRB-SUB-NT-EXPOSURES-&WITHZERONET.&YYYYMMDD.);
%exportcsv(dsn=DR-AIRB-SUB-SMC-MAPLE-EXPOSURES-&WITHZERONET.&YYYYMMDD.);
%end;
%mend CCAR_FILEs_GENERATE;

%CCAR_FILES_GENERATE(WITHZERONET= );
%CCAR_FILES_GENERATE(WITHZERONET=BEF_NEG_NET_);

/********************DELETED BELOW PD-CURVE LOGIC*************************/

%PUT MTH_END_DT = "&MTH_END_DT"d;

DATA &LIB..Basel_ccar_pd_curve_extr;
	SET &DBNAME..BASEL_CCAR_PD_CURVE_EXTR;
	WHERE MTH_END_DT = "&MTH_END_DT"d;
RUN;

%macro CCAR_PD_CURVE_FILES_GENERATE(WITHZERONET=);
	/** Before zero Netting **/
	%let reqvars=
		CCAR_BASEL_PRD_TP_NM
		PD_BAND
		PD_VAL
		PD_MIN_VAL
		PD_MAX_VAL
	;

	PROC SQL NOPRINT;
		SELECT MAX(MTH_END_DT) INTO :MTH_END_DT FROM &lib..Basel_ccar_pd_curve_extr;
		CREATE TABLE Basel_ccar_pd_curve_extr AS SELECT * FROM &LIB..Basel_ccar_pd_curve_extr WHERE MTH_END_DT = &MTH_END_DT.;
	QUIT;

	data BASEL_CCAR_PD_CURVE_EXTR_TMP0;
		retain  &reqvars.;
		set Basel_ccar_pd_curve_extr
			(keep= mth_end_dt &reqvars. 
			rename=(PD_VAL=pdval PD_MIN_VAL = PDMINVAL PD_MAX_VAL = PDMAXVAL)) nobs = 
			lastobs;
		dt = compress(put(mth_end_dt,yymmdd10.),'-');
		rundt = compress(put(today(),yymmdd10.),'-');
		tm = put(time(),tod8.);
		PD_VAL =put(pdval*100,12.6)||"%";
		PD_MIN_VAL =put(pdMINval*100,12.4);
		PD_MAX_VAL =put(pdMAXval*100,12.4);
		call symput('YYYYMMDD',compress(dt));
		call symput('rundt',compress(rundt));
		call symput('nobs',_n_);

		/*call symput('nobs',compress(lastobs));*/
		/*drop mth_end_dt;*/
		/*drop pdval;*/
		;
	run;

	%put pd_curve >> YYYYMMDD = &YYYYMMDD.;

	/*** calculating HAS_TOTAL for control file **/
	proc sql threads noprint;
		select distinct
			/*mth_end_dt,*/

			/*CCAR_BASEL_PRD_TP_NM,*/
			sum(
			%if &WITHZERONET= %then

				%do;
					PDVAL
				%end;
%else
	%do;
		PDVAL
	%end;

	) as hash_total_pd_Curve into :hashtot from BASEL_CCAR_PD_CURVE_EXTR_TMP0 /*group by */
	/*mth_end_dt*/
	/*,CCAR_BASEL_PRD_TP_NM*/
	;
	quit;

	%put >>> pd_Curve hash_total = &hashtot.;
	%put >>> pd_curve nobs = &nobs.;

	DATA BASEL_CCAR_PD_CURVE_EXTR_TMP0;
		SET BASEL_CCAR_PD_CURVE_EXTR_TMP0;
		DROP PDVAL;
		keep &reqvars.;
	RUN;

	/** producing PD Curve control CTL table **/
	DATA &lib..DR_AIRB_PD_CURVE_&WITHZERONET.&YYYYMMDD._ctl;
		length 
			YYYYMMDD 
			rundt $10
			nobs $6
			hashtot $15 n1 8;
		file "&outlanding./cmf/outgoing/DR-AIRB-PD-CURVE-&WITHZERONET.&YYYYMMDD..ctl" ; /*lrecl=203 recfm=V ;*/
		YYYYMMDD = "&YYYYMMDD.,";
		rundt = "&rundt. ,";
		nobs = left(put(&nobs. ,6.));
		n1= 100*&hashtot.;
		hashtot = left(put(n1, 15.2));
		DLM=",";
		mtxtx=compress((rundt||nobs||DLM||hashtot),' ');
		put
			@1 YYYYMMDD
			@10 mtxtx
		;

		/*@20 nobs*/
		/*@29 DLM*/
		/*@30 hashtot*/
		/*;*/
		output;
	run;

	data &lib..DR_AIRB_PD_CURVE_&YYYYMMDD.;
		set BASEL_CCAR_PD_CURVE_EXTR_TMP0;
		drop PDMINVAL PDMAXVAL;
		rename
			CCAR_BASEL_PRD_TP_NM = 'Basel Product Type'n
			PD_BAND = 'PD Band'n
			PD_VAL  = 'Pd Value'n
			PD_MIN_VAL = 'PD Min'n
			PD_MAX_VAL = 'PD Max'n
		;
	run;

%macro exportcsv(dsn=DR-AIRB-PD-CURVE-&WITHZERONET.&YYYYMMDD.);

	data tmp_name;
		dsname2 = trim(substr(translate("&dsn.",'_','-'),1,50));
		call symput ('dsname',trim(dsname2));
	run;

	%put >>> dsname = &dsname.;

	data &dsname;
		set &lib..&dsname;
		pd_bandn = input('PD Band'n,8.);

	proc sort out= &lib..&dsname (drop= PD_BANDn )force;
		by 'Basel Product Type'n
			PD_BANDn;
	run;

	run;

	proc export data = &lib..&dsname  outfile="&outlanding./cmf/outgoing/&dsn..csv" dbms=csv replace;
	run;

	/*x 'sed s/\"//g &outlanding./&dsn.&YYYYMMDD..csv';*/
%mend exportcsv;

%exportcsv(dsn=DR-AIRB-PD-CURVE-&WITHZERONET.&YYYYMMDD.);
%mend CCAR_PD_CURVE_FILES_GENERATE;

%CCAR_PD_CURVE_FILES_GENERATE(WITHZERONET=);

/**** To create LOOKUP flat files ***/
%macro CCAR_LOOKUP_FILES_GENERATE(WITHZERONET=);
	/** Before zero Netting **/
	%let reqvars=
		CCAR_BASEL_PRD_TP_NM
		PRD_SHT_NM
		EXPSR_SUB_TP_CD
		BASEL_1_ASST_TP_CD
		REGULATORY_PRD_TP_CD
		PD_BAND_CD
	;

	data BASEL_CCAR_LOOKUP_EXTR_TMP0;
		retain  &reqvars.;
		set &DB..BASEL_AIRB_PRD_LKP
			(keep= CRNT_F EFF_FROM_YR_MTH EFF_TO_YR_MTH &reqvars. ) nobs = lastobs;

		/*WHERE CRNT_F = 'Y';*/
		WHERE "&mth_tm_id_yrmth." BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH;
		rundt = compress(put(today(),yymmdd10.),'-');
		tm = put(time(),tod8.);
		call symput('rundt',compress(rundt));
		call symput('nobs',compress(lastobs));

		/*drop mth_end_dt;*/
	run;

	data &lib..DRL_AIRB_Product_Lookup;
		set BASEL_CCAR_LOOKUP_EXTR_TMP0;
		keep &reqvars.;
	run;

	data &lib..DRL_AIRB_Product_Lookup;
		set  &lib..DRL_AIRB_Product_Lookup;
		rename
			CCAR_BASEL_PRD_TP_NM = 'Basel Product Type'n
			PRD_SHT_NM = 'Short Name'n
			EXPSR_SUB_TP_CD = 'Exposure Sub Type'n
			BASEL_1_ASST_TP_CD = 'Basel 1 Asset Type'n
			REGULATORY_PRD_TP_CD = 'Regulatory Product Type'n
			PD_BAND_CD = 'PD Band'n
		;
	run;

	/***Basel Product Type,Short Name,Exposure Sub Type,Basel 1 Asset Type,Regulatory Product Type,PD Band ***/
	DATA &LIB..DRL_AIRB_Guarantee_Lookup;
		SET &DB..BASEL_AIRB_GUARNT_LKP;

		/*WHERE CRNT_F = 'Y';*/
		WHERE "&mth_tm_id_yrmth." BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH;
		GUARNT_PARTCPTN_PC = round(GUARNT_PARTCPTN_PC);
		format GUARNT_PARTCPTN_PC 8.;
		KEEP 
			CCAR_BASEL_PRD_TP_NM
			GUARNT_PARTCPTN_PC
		;
		rename
			CCAR_BASEL_PRD_TP_NM = 'Basel Product'n
			GUARNT_PARTCPTN_PC   = 'Guarantee Participation %'n
		;
	RUN;

%macro exportcsv(dsn=DRL-AIRB-Product-Lookup);

	data tmp_name;
		dsname2 = trim(substr(translate("&dsn.",'_','-'),1,25));
		call symput ('dsname',trim(dsname2));
	run;

	%put >>> dsname = &dsname.;

	proc export data = &lib..&dsname  outfile="&outlanding./cmf/outgoing/&dsn..csv" dbms=csv replace;
	run;

	/*x 'sed s/\"//g &outlanding./&dsn.&YYYYMMDD..csv';*/
%mend exportcsv;

%exportcsv(dsn=DRL-AIRB-Product-Lookup);
%exportcsv(dsn=DRL-AIRB-Guarantee-Lookup);
%mend CCAR_LOOKUP_FILES_GENERATE;

%CCAR_LOOKUP_FILES_GENERATE(WITHZERONET=);