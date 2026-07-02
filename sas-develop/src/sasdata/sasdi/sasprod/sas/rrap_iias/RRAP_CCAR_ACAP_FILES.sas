

/*CCAR*/
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);
options mprint errorabend;
%global YYYYMMDD;
%LET OUTLANDING=&OUTPATH;
%let datetime_start = %sysfunc(TIME());
%put >>> START TIME: %sysfunc(datetime(),datetime14.);
options validvarname = any source source2;
%PUT YEARMONTH IS &YEARMONTH;
%PUT MTH_TM_ID IS &MTH_TM_ID;

/*Hadi Dimashkieh - 19JUL2016 - Used for replacement of CRNT_F with EFF_FROM and EFF_TO dates*/
PROC SQL NOPRINT;
	select TM_LVL_ST_DT format=yymmn6. into :mth_tm_id_yrmth from EDRTLRT.tm_dim where tm_id = &mth_tm_id and tm_lvl='Month';
QUIT;

/*Zoran Jovicic - 20DEC2021 - For RRMSS-915 PD Curve Fix, to get PREV_MTH_END_DT */
PROC SQL NOPRINT;
   select TM_LVL_END_DT into :PREV_MTH_END_DT from EDRTLRT.tm_dim where tm_id = &mth_tm_id - 40 ;
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
	set NZRRAP.BASEL_CCAR_EXPSR_EXTR_ACAP (rename=(PD_BAND=PDx));
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
CONNECT USING NZRRAP AS DB2CON;
create table &lib..BASEL_CCAR_EXPSR_EXTR_ECL as 
SELECT expsr.*,
		case when e.secrtztn_flag ne "" then a.expsr_drawn_prior_secur  else . end as expsr_drawn_prior_secur format=22.2,
		case when e.secrtztn_flag ne "" and b.expsr_drawn_no_secur eq . then 0 
			when e.secrtztn_flag ne "" then b.expsr_drawn_no_secur  
			else . 
		end as  expsr_drawn_no_secur format=22.2,
		case when e.secrtztn_flag ne "" then d.expsr_drawn_with_secur  else . end as expsr_drawn_with_secur format=22.2,
		case when e.secrtztn_flag ne "" then c.secur_amt  else . end as secur_amt format=22.2,        
		case when e.secrtztn_flag ne "" then d.expsr_drawn_reduced  else . end as expsr_drawn_reduced format=22.2,
		e.secrtztn_flag
FROM work.BASEL_CCAR_EXPSR_EXTR_ECL expsr
LEFT JOIN 
	(select * from connection to DB2CON 
/*-- 'Exposures_Prior_Securitization'n*/
(select CCAR_BASEL_PRD_TP_NM, sum(expsr_drawn_prior_secur) as expsr_drawn_prior_secur, prd_id,TRNST_NUM,EGL_DEPRTMNT,
	RNTL_PRPTY_F , LTV_BUCKET, CURRENCY_MISMATCH_F ,TOT_EXPSR_ABOVE_1500K_LMT_F ,TRANSACTOR_FLAG_QRR,PD_90_DAY_F
	from 
	(SELECT CCAR_BASEL_PRD_TP_NM, src_sys_cd, CONSM_PRD_TREATMNT_CD, TRNST_EXCLSN_F, prd_id,TRNST_NUM,EGL_DEPRTMNT,PIT_STAT_CD,
		RNTL_PRPTY_F , LTV_BUCKET, CURRENCY_MISMATCH_F ,TOT_EXPSR_ABOVE_1500K_LMT_F ,TRANSACTOR_FLAG_QRR,PD_90_DAY_F,
		case 
			when SRC_SYS_CD = 'KS' then sum(OS_BAL_AMT + GENL_LEDGER_BALCNG_ADJ_AMT) 
            when SRC_SYS_CD = 'SPL' and PIT_STAT_CD in ('CUR') and CONSM_PRD_TREATMNT_CD='A' and TRNST_EXCLSN_F='N' and prd_id in ('S09', 'S10') then sum(OS_BAL_AMT + GENL_LEDGER_BALCNG_ADJ_AMT)
			else sum(OS_BAL_AMT) end			
		as expsr_drawn_prior_secur
	FROM &NET_DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT
	WHERE MTH_TM_ID = &mth_tm_id
		 AND (
					( SRC_SYS_CD = 'KS'
                    	or
                      (SRC_SYS_CD = 'SPL'
                       AND (CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_REG%' or CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_RS%')
                       AND ADJUSTED_OS_BAL_AMT >= 0)
                    )
                	AND (CCAR_BASEL_PRD_TP_NM like 'REV_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'REV_CON_CL%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CL%')
                	AND OS_BAL_AMT > 0)
	group by CCAR_BASEL_PRD_TP_NM, src_sys_cd,CONSM_PRD_TREATMNT_CD, TRNST_EXCLSN_F, prd_id,TRNST_NUM,EGL_DEPRTMNT,PIT_STAT_CD,
				RNTL_PRPTY_F, LTV_BUCKET, CURRENCY_MISMATCH_F,TOT_EXPSR_ABOVE_1500K_LMT_F ,TRANSACTOR_FLAG_QRR,PD_90_DAY_F
	) intmed
	group by CCAR_BASEL_PRD_TP_NM, prd_id,TRNST_NUM,EGL_DEPRTMNT,
		RNTL_PRPTY_F , LTV_BUCKET, CURRENCY_MISMATCH_F ,TOT_EXPSR_ABOVE_1500K_LMT_F ,TRANSACTOR_FLAG_QRR,PD_90_DAY_F

)) as a
on (EXPSR.CCAR_BASEL_PRD_TP_NM=a.CCAR_BASEL_PRD_TP_NM 
	AND expsr.prd_id=a.prd_id
	AND expsr.TRNST_NUM=a.TRNST_NUM
	AND expsr.EGL_DEPRTMNT=a.EGL_DEPRTMNT
	AND EXPSR.RNTL_PRPTY_F=a.RNTL_PRPTY_F
	AND EXPSR.CURRENCY_MISMATCH_F=a.CURRENCY_MISMATCH_F
	AND EXPSR.TOT_EXPSR_ABOVE_1500K_LMT_F=a.TOT_EXPSR_ABOVE_1500K_LMT_F
	AND EXPSR.LTV_PERCENTAGE=a.LTV_BUCKET
	AND EXPSR.TRANSACTOR_FLAG_QRR=a.TRANSACTOR_FLAG_QRR
	AND substr(EXPSR.PD_90_DAY_F,1,1)=substr(a.PD_90_DAY_F,1,1)
	AND expsr.MTH_END_DT="&tm_lvl_end_dt"d)
LEFT JOIN
	(select * from connection to DB2CON 
/*-- 'Exposures_No_Securitization'n*/
(	SELECT CCAR_BASEL_PRD_TP_NM, sum(expsr_drawn_no_secur) as expsr_drawn_no_secur, prd_id,TRNST_NUM,EGL_DEPRTMNT,
		RNTL_PRPTY_F, LTV_BUCKET, CURRENCY_MISMATCH_F,TOT_EXPSR_ABOVE_1500K_LMT_F ,TRANSACTOR_FLAG_QRR,PD_90_DAY_F
	from 
	(SELECT CCAR_BASEL_PRD_TP_NM, src_sys_cd, CONSM_PRD_TREATMNT_CD, TRNST_EXCLSN_F, prd_id,TRNST_NUM,EGL_DEPRTMNT,PIT_STAT_CD,
		RNTL_PRPTY_F, LTV_BUCKET, CURRENCY_MISMATCH_F,TOT_EXPSR_ABOVE_1500K_LMT_F ,TRANSACTOR_FLAG_QRR,PD_90_DAY_F,
		case 
			when SRC_SYS_CD = 'KS' then sum(OS_BAL_AMT + GENL_LEDGER_BALCNG_ADJ_AMT) 
            when SRC_SYS_CD = 'SPL' and PIT_STAT_CD in ('CUR') and CONSM_PRD_TREATMNT_CD='A' and TRNST_EXCLSN_F='N' and prd_id in ('S09', 'S10') then sum(OS_BAL_AMT + GENL_LEDGER_BALCNG_ADJ_AMT)
			else sum(OS_BAL_AMT) end			
		as expsr_drawn_no_secur 
	FROM &NET_DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT
	WHERE MTH_TM_ID = &mth_tm_id
		 AND (
				( SRC_SYS_CD = 'KS'
					AND OS_BAL_AMT > 0
                	AND (CCAR_BASEL_PRD_TP_NM like 'REV_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'REV_CON_CL%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CL%')
                	AND ADJUSTED_OS_BAL_AMT = BEFORE_ZERO_NET_DRAWN_AMT )
                  or
                  (SRC_SYS_CD = 'SPL'
                      AND ADJUSTED_OS_BAL_AMT >= 0
                      AND (CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_REG%' or CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_RS%')
                      AND ADJUSTED_OS_BAL_AMT = BEFORE_ZERO_NET_DRAWN_AMT)
                  )
	group by CCAR_BASEL_PRD_TP_NM, src_sys_cd, CONSM_PRD_TREATMNT_CD, TRNST_EXCLSN_F, prd_id,TRNST_NUM,EGL_DEPRTMNT,PIT_STAT_CD,
		RNTL_PRPTY_F, LTV_BUCKET, CURRENCY_MISMATCH_F,TOT_EXPSR_ABOVE_1500K_LMT_F ,TRANSACTOR_FLAG_QRR,PD_90_DAY_F
	) intmed
	group by CCAR_BASEL_PRD_TP_NM, prd_id,TRNST_NUM,EGL_DEPRTMNT,
	RNTL_PRPTY_F, LTV_BUCKET, CURRENCY_MISMATCH_F,TOT_EXPSR_ABOVE_1500K_LMT_F ,TRANSACTOR_FLAG_QRR,PD_90_DAY_F
)) as b
on (EXPSR.CCAR_BASEL_PRD_TP_NM=b.CCAR_BASEL_PRD_TP_NM
	AND expsr.prd_id=b.prd_id
	AND expsr.TRNST_NUM=b.TRNST_NUM
	AND expsr.EGL_DEPRTMNT=b.EGL_DEPRTMNT
	AND EXPSR.RNTL_PRPTY_F=b.RNTL_PRPTY_F
	AND EXPSR.CURRENCY_MISMATCH_F=b.CURRENCY_MISMATCH_F
	AND EXPSR.TOT_EXPSR_ABOVE_1500K_LMT_F=b.TOT_EXPSR_ABOVE_1500K_LMT_F
	AND EXPSR.LTV_PERCENTAGE=b.LTV_BUCKET
	AND EXPSR.TRANSACTOR_FLAG_QRR=b.TRANSACTOR_FLAG_QRR
	AND substr(EXPSR.PD_90_DAY_F,1,1)=substr(b.PD_90_DAY_F,1,1)
	AND expsr.MTH_END_DT="&tm_lvl_end_dt"d)
LEFT JOIN
	(select * from connection to DB2CON 
/*-- 'Securitization_Amount'n*/
(	SELECT CCAR_BASEL_PRD_TP_NM, sum(BEFORE_ZERO_NET_DRAWN_AMT - ADJUSTED_OS_BAL_AMT) as secur_amt, prd_id,TRNST_NUM,EGL_DEPRTMNT,
		RNTL_PRPTY_F, LTV_BUCKET, CURRENCY_MISMATCH_F,TOT_EXPSR_ABOVE_1500K_LMT_F ,TRANSACTOR_FLAG_QRR,PD_90_DAY_F
	FROM &NET_DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT
	WHERE MTH_TM_ID = &mth_tm_id
	 AND (
			(SRC_SYS_CD = 'KS'
             AND (CCAR_BASEL_PRD_TP_NM like 'REV_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'REV_CON_CL%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CL%'))
            or
            (SRC_SYS_CD = 'SPL'
             AND (CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_REG%' or CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_RS%'))
         )
	group by CCAR_BASEL_PRD_TP_NM, prd_id,TRNST_NUM,EGL_DEPRTMNT,
	RNTL_PRPTY_F, LTV_BUCKET, CURRENCY_MISMATCH_F,TOT_EXPSR_ABOVE_1500K_LMT_F ,TRANSACTOR_FLAG_QRR,PD_90_DAY_F
)) as c
on (EXPSR.CCAR_BASEL_PRD_TP_NM=c.CCAR_BASEL_PRD_TP_NM
	AND expsr.prd_id=c.prd_id
	AND expsr.TRNST_NUM=c.TRNST_NUM
	AND expsr.EGL_DEPRTMNT=c.EGL_DEPRTMNT
	AND EXPSR.RNTL_PRPTY_F=c.RNTL_PRPTY_F
	AND EXPSR.CURRENCY_MISMATCH_F=c.CURRENCY_MISMATCH_F
	AND EXPSR.TOT_EXPSR_ABOVE_1500K_LMT_F=c.TOT_EXPSR_ABOVE_1500K_LMT_F
	AND EXPSR.LTV_PERCENTAGE=c.LTV_BUCKET
	AND EXPSR.TRANSACTOR_FLAG_QRR=c.TRANSACTOR_FLAG_QRR
	AND substr(EXPSR.PD_90_DAY_F,1,1)=substr(c.PD_90_DAY_F,1,1)
	AND expsr.MTH_END_DT="&tm_lvl_end_dt"d)
LEFT JOIN
	(select * from connection to DB2CON 
/* -- 'Exposures_With_Securitization'n*/
/* -- and 'Exposures_Reduced'n*/
(	SELECT CCAR_BASEL_PRD_TP_NM, sum(expsr_drawn_with_secur) as expsr_drawn_with_secur, sum(expsr_drawn_reduced) as expsr_drawn_reduced, prd_id,TRNST_NUM,EGL_DEPRTMNT,
	RNTL_PRPTY_F, LTV_BUCKET, CURRENCY_MISMATCH_F,TOT_EXPSR_ABOVE_1500K_LMT_F ,TRANSACTOR_FLAG_QRR,PD_90_DAY_F
	from 
	(SELECT CCAR_BASEL_PRD_TP_NM, src_sys_cd, CONSM_PRD_TREATMNT_CD, TRNST_EXCLSN_F, prd_id,TRNST_NUM,EGL_DEPRTMNT,PIT_STAT_CD,
		RNTL_PRPTY_F, LTV_BUCKET, CURRENCY_MISMATCH_F,TOT_EXPSR_ABOVE_1500K_LMT_F ,TRANSACTOR_FLAG_QRR,PD_90_DAY_F, 
		case 
			when SRC_SYS_CD = 'KS' then sum(OS_BAL_AMT + GENL_LEDGER_BALCNG_ADJ_AMT) 
            when SRC_SYS_CD = 'SPL' and PIT_STAT_CD in ('CUR') and CONSM_PRD_TREATMNT_CD='A' and TRNST_EXCLSN_F='N' and prd_id in ('S09', 'S10') then sum(OS_BAL_AMT + GENL_LEDGER_BALCNG_ADJ_AMT)
			else sum(OS_BAL_AMT) end			
		as expsr_drawn_with_secur, 
		sum(ADJUSTED_OS_BAL_AMT) as expsr_drawn_reduced
		
	FROM &NET_DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT
	WHERE MTH_TM_ID = &mth_tm_id
		AND ADJUSTED_OS_BAL_AMT <> BEFORE_ZERO_NET_DRAWN_AMT
		AND (
        	  (SRC_SYS_CD = 'KS'
               AND (CCAR_BASEL_PRD_TP_NM like 'REV_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'REV_CON_CL%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CL%'))
			  or
              (SRC_SYS_CD = 'SPL'
               AND (CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_REG%' or CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_RS%'))
            )	
	group by CCAR_BASEL_PRD_TP_NM, src_sys_cd, CONSM_PRD_TREATMNT_CD, TRNST_EXCLSN_F, prd_id,TRNST_NUM, EGL_DEPRTMNT,PIT_STAT_CD,
	RNTL_PRPTY_F, LTV_BUCKET, CURRENCY_MISMATCH_F,TOT_EXPSR_ABOVE_1500K_LMT_F ,TRANSACTOR_FLAG_QRR,PD_90_DAY_F
) intmed
	group by CCAR_BASEL_PRD_TP_NM, prd_id,TRNST_NUM,EGL_DEPRTMNT,
	RNTL_PRPTY_F, LTV_BUCKET, CURRENCY_MISMATCH_F,TOT_EXPSR_ABOVE_1500K_LMT_F ,TRANSACTOR_FLAG_QRR,PD_90_DAY_F
)) as d
on (EXPSR.CCAR_BASEL_PRD_TP_NM=d.CCAR_BASEL_PRD_TP_NM
	AND expsr.prd_id=d.prd_id
	AND expsr.TRNST_NUM=d.TRNST_NUM
	AND expsr.EGL_DEPRTMNT=d.EGL_DEPRTMNT
	AND EXPSR.RNTL_PRPTY_F=d.RNTL_PRPTY_F
	AND EXPSR.CURRENCY_MISMATCH_F=d.CURRENCY_MISMATCH_F
	AND EXPSR.TOT_EXPSR_ABOVE_1500K_LMT_F=d.TOT_EXPSR_ABOVE_1500K_LMT_F
	AND EXPSR.LTV_PERCENTAGE=d.LTV_BUCKET
	AND EXPSR.TRANSACTOR_FLAG_QRR=d.TRANSACTOR_FLAG_QRR
	AND substr(EXPSR.PD_90_DAY_F,1,1)=substr(d.PD_90_DAY_F,1,1)
	AND expsr.MTH_END_DT="&tm_lvl_end_dt"d)
LEFT JOIN 
	(select * from connection to DB2CON 
/* -- 'Securitization_Name'n*/
/* move this part out of section d to make sure all subgroup (prd_id, transt_num, EGL_Deprtmnt) within the same CCAR_BASEL_PRD_TP_NM group will have the same SECRTZTN_FLAG, which impact the logic for expsr_drawn_no_secur  */
(	SELECT CCAR_BASEL_PRD_TP_NM, 
	(CASE 
			WHEN CCAR_BASEL_PRD_TP_NM like 'REV_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CC%' THEN 'Trillium (CC)'
			WHEN CCAR_BASEL_PRD_TP_NM like 'REV_CON_CL%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CL%' THEN 'Halifax (CL)'
            WHEN CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_REG%' or CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_RS%' THEN 'START'
			else ''
		END) as SECRTZTN_FLAG
	FROM &NET_DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT
	WHERE MTH_TM_ID = &mth_tm_id 
		AND ADJUSTED_OS_BAL_AMT <> BEFORE_ZERO_NET_DRAWN_AMT
		AND (
        	  (SRC_SYS_CD = 'KS'
               AND (CCAR_BASEL_PRD_TP_NM like 'REV_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CC%' or CCAR_BASEL_PRD_TP_NM like 'REV_CON_CL%' or CCAR_BASEL_PRD_TP_NM like 'OTHER_CON_CL%'))
              or
              (SRC_SYS_CD = 'SPL'
               AND (CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_REG%' or CCAR_BASEL_PRD_TP_NM like 'ITL_AUTO_RS%'))
            )
	group by CCAR_BASEL_PRD_TP_NM
)) as e
on (EXPSR.CCAR_BASEL_PRD_TP_NM=e.CCAR_BASEL_PRD_TP_NM
	AND expsr.MTH_END_DT="&tm_lvl_end_dt"d);
DISCONNECT FROM DB2CON;
quit;

/* JIRA Ticket: RRMSS-850: Remove securitization fields for previous month */
proc sql;
update &lib..BASEL_CCAR_EXPSR_EXTR_ECL
set expsr_drawn_prior_secur = null,
	expsr_drawn_no_secur = null,
	expsr_drawn_with_secur = null,
	secur_amt = null,
	expsr_drawn_reduced = null
where period_ind = 'PREV';
quit;

proc sort data=&lib..BASEL_CCAR_EXPSR_EXTR_ECL;
	by UNQ_RECD_ID ; /*legal_entity;*/
run;

/*
DATA &LIB..BASEL_AIRB_GUARNT_LKP;
	SET &DB..BASEL_AIRB_GUARNT_LKP;
	WHERE "&mth_tm_id_yrmth." BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH;
run;

data &lib..BASEL_AIRB_PRD_LKP;
	set &DB..BASEL_AIRB_PRD_LKP;
	WHERE "&mth_tm_id_yrmth." BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH;
run;
*/

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

PROC SQL;
	CREATE TABLE &INPATH..CCAR_PROVISIONS_MAPPING AS
	SELECT PROVISIONS_PRODUCT,BASEL_PRODUCT_TYPE,SECURITY_TYPE_DESC 
	FROM NZRRAP.CCAR_PROVISIONS_MAPPING
	WHERE CRNT_F='Y' AND 
		INPUT(EFF_TO_YR_MTH,6.)>(SELECT INPUT(compress(TM_DESC, '/', 'a'),6.) AS TM_DESC 
									FROM NZRRAP.TM_DIM
									WHERE TM_ID=&MTH_TM_ID.);
QUIT;

PROC SQL;
	SELECT MAX(MTH_TM_ID) INTO:QTR_MTH FROM NZRRAP.Post_ECL_Provisions;
QUIT;

%PUT &=QTR_MTH;

PROC SQL;
	CREATE TABLE &INPATH..POST_ECL_PROVISIONS AS
	SELECT PRODUCTS,
		   SECURITY_TYPE_DESC ,
		   ECL_DRAWN_1_ADJ ,
		   ECL_DRAWN_2_ADJ ,
		   ECL_DRAWN_3_ADJ ,
		   ECL_UNDRAWN_1_ADJ ,
		   ECL_UNDRAWN_2_ADJ ,
		   ECL_UNDRAWN_3_ADJ 
	FROM NZRRAP.Post_ECL_Provisions
	WHERE MTH_TM_ID=&QTR_MTH.;
QUIT;



	%macro Attrib(Indata=,outdata=);
PROC SQL;
   CREATE TABLE &INPATH..exposer_01 AS 
   SELECT t1.'Unique_Identifier'n, 
          t1.'Basel_Product_Name'n, 
          t1.'ProbabilityDefault_Band'n, 
          t1.'Legal_Entity'n, 
          t1.'Day90_PastDue_Flag'n, 
          t1.'Unconditionally_Cancelable_Flag'n, 
          t1.'Drawn_Amount'n, 
          t1.'Exposures_at_Default'n, 
          t1.Currency, 
          t1.'Expected_Loss'n, 
          t1.'Undrawn_Amount'n, 
          t1.'Accrued_Interest'n, 
          /* t1.ACL, */
          t1.'Partial_Write_Off'n, 
          t1.'Loss_Given_Default'n, 
          t1.'EAD_Factor'n, 
          t1.'Insurance_Flag'n, 
          t1.'Downturn_LGD'n, 
          t1.'LTV_Bucket'n, 
          t1.'Obligors'n, 	  
          t1.'Exposures_Prior_Securitization'n, 
          t1.'Exposures_No_Securitization'n, 
          t1.'Exposures_With_Securitization'n, 
          t1.'Securitization_Amount'n, 
          t1.'Exposures_Reduced'n, 
          t1.'Securitization_Name'n, 
          t1.'ECL_Drawn1'n, 
          t1.'ECL_Drawn2'n, 
          t1.'ECL_Drawn3'n, 
          t1.'ECL_Undrawn1'n, 
          t1.'ECL_Undrawn2'n, 
          t1.'ECL_Undrawn3'n, 
          t1.'ECL_Drawn_PostSec1'n, 
          t1.'ECL_Drawn_PostSec2'n, 
          t1.'ECL_Drawn_PostSec3'n,
		  t1.'Transit_Number'n,		/*adding for bcar_ccar combine file */
		  t1.'Product_Id'n,			/*adding for bcar_ccar combine file */
		  t1.'EGL_Department_Id'n,	/*adding for bcar_ccar combine file */
		  t1.'Defaulted_Exposure_Flag'n,				
		  t1.'Uninsured_LGD'n,
		  t1.'Uninsured_DLGD'n,
		t1.'Rental_Income_Flag'n,
		t1.'Currency_Mismatch_Flag'n,
		t1.'Total_Exposure_Above_Limit'n,
		t1.'Orig_Loan_Amt_as_of_Insur_Date'n,
		t1.'Transactor_Flag'n,
		t1.'Clp_Flag'n
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
   SELECT t1.'Unique_Identifier'n, 
          t1.'Basel_Product_Name'n,
		  t2.provisions_product, 
          t1.'ProbabilityDefault_Band'n, 
          t1.'Legal_Entity'n, 
          t1.'Day90_PastDue_Flag'n, 
          t1.'Unconditionally_Cancelable_Flag'n, 
          t1.'Drawn_Amount'n, 
          t1.'Exposures_at_Default'n, 
          t1.Currency, 
          t1.'Expected_Loss'n, 
          t1.'Undrawn_Amount'n, 
          t1.'Accrued_Interest'n, 
          /* t1.ACL, */
          t1.'Partial_Write_Off'n, 
          t1.'Loss_Given_Default'n, 
          t1.'EAD_Factor'n, 
          t1.'Insurance_Flag'n, 
          t1.'Downturn_LGD'n, 
          t1.'LTV_Bucket'n, 
          t1.'Obligors'n, 		  
          t1.'Exposures_Prior_Securitization'n, 
          t1.'Exposures_No_Securitization'n, 
          t1.'Exposures_With_Securitization'n, 
          t1.'Securitization_Amount'n, 
          t1.'Exposures_Reduced'n, 
          t1.'Securitization_Name'n, 
          t1.'ECL_Drawn1'n, 
          t1.'ECL_Drawn2'n, 
          t1.'ECL_Drawn3'n, 
          t1.'ECL_Undrawn1'n, 
          t1.'ECL_Undrawn2'n, 
          t1.'ECL_Undrawn3'n, 
          t1.'ECL_Drawn_PostSec1'n, 
          t1.'ECL_Drawn_PostSec2'n, 
          t1.'ECL_Drawn_PostSec3'n,
		  t1.'Transit_Number'n,		/*adding for bcar_ccar combine file */
		  t1.'Product_Id'n,			/*adding for bcar_ccar combine file */
		  t1.'EGL_Department_Id'n,	/*adding for bcar_ccar combine file */
		  t1.'Defaulted_Exposure_Flag'n,				
		  t1.'Uninsured_LGD'n,
		  t1.'Uninsured_DLGD'n,
		t1.'Rental_Income_Flag'n,
		t1.'Currency_Mismatch_Flag'n,
		t1.'Total_Exposure_Above_Limit'n,
		t1.'Orig_Loan_Amt_as_of_Insur_Date'n,
		t1.'Transactor_Flag'n,
		t1.'Clp_Flag'n
      FROM &indata t1
           LEFT JOIN &INPATH..CCAR_PROVISIONS_MAPPING t2 ON ((prxchange('s/\_\d+//',-1,compress(t1.'Basel_Product_Name'n))) = t2.Basel_Product_Type);
QUIT;

PROC SQL;
   CREATE TABLE &INPATH..Pre_Adj_Sum AS 
   SELECT t1.Provisions_Product, 
          /* SUM_of_ECL Drawn 1 */
            (SUM(t1.'ECL_Drawn1'n)) FORMAT=30.8 AS 'SUM_of_ECL Drawn 1'n, 
          /* SUM_of_ECL Drawn 2 */
            (SUM(t1.'ECL_Drawn2'n)) FORMAT=30.8 AS 'SUM_of_ECL Drawn 2'n, 
          /* SUM_of_ECL Drawn 3 */
            (SUM(t1.'ECL_Drawn3'n)) FORMAT=30.8 AS 'SUM_of_ECL Drawn 3'n, 
          /* SUM_of_ECL Undrawn 1 */
            (SUM(t1.'ECL_Undrawn1'n)) FORMAT=30.8 AS 'SUM_of_ECL Undrawn 1'n, 
          /* SUM_of_ECL_Undrawn_2 */
            (SUM(t1.'ECL_Undrawn2'n)) FORMAT=30.8 AS SUM_of_ECL_Undrawn_2, 
          /* SUM_of_ECL_Undrawn_3 */
            (SUM(t1.'ECL_Undrawn3'n)) FORMAT=30.8 AS SUM_of_ECL_Undrawn_3, 
          /* SUM_of_ECL Drawn PostSec 1 */
            (SUM(t1.'ECL_Drawn_PostSec1'n)) FORMAT=30.8 AS 'SUM_of_ECL Drawn PostSec 1'n, 
          /* SUM_of_ECL Drawn PostSec 2 */
            (SUM(t1.'ECL_Drawn_PostSec2'n)) FORMAT=30.8 AS 'SUM_of_ECL Drawn PostSec 2'n, 
          /* SUM_of_ECL Drawn PostSec 3 */
            (SUM(t1.'ECL_Drawn_PostSec3'n)) FORMAT=30.8 AS 'SUM_of_ECL Drawn PostSec 3'n
      FROM WORK.PRE_CCAR_MAPPING t1
      GROUP BY t1.Provisions_Product;
QUIT;

PROC SQL;
   CREATE TABLE &INPATH..exposer_02 AS 
   SELECT t1.'Unique_Identifier'n, 
          t1.'Basel_Product_Name'n, 
          t1.'ProbabilityDefault_Band'n, 
          t1.'Legal_Entity'n, 
		  t1.'Defaulted_Exposure_Flag'n,				
          t1.'Day90_PastDue_Flag'n, 
          t1.'Unconditionally_Cancelable_Flag'n, 
          t1.'Drawn_Amount'n, 
          t1.'Exposures_at_Default'n, 
          t1.Currency, 
          t1.'Expected_Loss'n, 
          t1.'Undrawn_Amount'n, 
          t1.'Accrued_Interest'n, 
          /* t1.ACL, */
          t1.'Partial_Write_Off'n, 
          t1.'Loss_Given_Default'n, 
          t1.'EAD_Factor'n, 
          t1.'Insurance_Flag'n, 
          t1.'Downturn_LGD'n, 
		  t1.'Uninsured_LGD'n,
		  t1.'Uninsured_DLGD'n,
          t1.'LTV_Bucket'n, 
          t1.'Obligors'n, 	
		  t1.'Rental_Income_Flag'n,
		 t1.'Currency_Mismatch_Flag'n,
		 t1.'Total_Exposure_Above_Limit'n,
		t1.'Orig_Loan_Amt_as_of_Insur_Date'n,
		t1.'Transactor_Flag'n,
		t1.'Clp_Flag'n,		  
          t1.'Exposures_Prior_Securitization'n, 
          t1.'Exposures_No_Securitization'n, 
          t1.'Exposures_With_Securitization'n, 
          t1.'Securitization_Amount'n, 
          t1.'Exposures_Reduced'n, 
          t1.'Securitization_Name'n,  
          t1.'ECL_Drawn1'n format 30.8, 
          t1.'ECL_Drawn2'n format 30.8, 
          t1.'ECL_Drawn3'n format 30.8, 
          t1.'ECL_Undrawn1'n format 30.8, 
          t1.'ECL_Undrawn2'n format 30.8, 
          t1.'ECL_Undrawn3'n format 30.8, 
          t1.'ECL_Drawn_PostSec1'n format 30.8, 
          t1.'ECL_Drawn_PostSec2'n format 30.8, 
          t1.'ECL_Drawn_PostSec3'n format 30.8, 
          /* ECL_Drawn_1_Post_Adj */
            ((t2.ECL_Drawn_1_Adj / t3.'SUM_of_ECL Drawn 1'n) * t1.'ECL_Drawn1'n) format=30.8 AS 'ECL_Post_Adj_Drawn1'n, 
          /* ECL_DRWAN_2_POST_ADJ */
            ((t2.ECL_Drawn_2_Adj / t3.'SUM_of_ECL Drawn 2'n) * t1.'ECL_Drawn2'n) format=30.8 AS 'ECL_Post_Adj_Drawn2'n, 
          /* ECL_DRAWN_3_POST_ADJ */
            ((t2.ECL_Drawn_3_Adj / t3.'SUM_of_ECL Drawn 3'n) * t1.'ECL_Drawn3'n) format=30.8 AS 'ECL_Post_Adj_Drawn3'n, 
          /* ECL_UNDRAWN_1_POST_ADJ */
            ((t2.ECL_Undrawn_1_Adj / t3.'SUM_of_ECL Undrawn 1'n) * t1.'ECL_Undrawn1'n) format=30.8 AS 'ECL_Post_Adj_Undrawn1'n, 
          /* ECL_UNDRAWN_2_POST_ADJ */
            ((t2.ECL_Undrawn_2_Adj / t3.SUM_of_ECL_Undrawn_2) * t1.'ECL_Undrawn2'n) format=30.8 AS 'ECL_Post_Adj_Undrawn2'n, 
          /* ECL_UNDRAWN_3_POST_ADJ */
            ((t2.ECL_Undrawn_3_Adj / t3.SUM_of_ECL_Undrawn_3)*t1.'ECL_Undrawn3'n) format=30.8 AS 'ECL_Post_Adj_Undrawn3'n, 
          /* ECL_DRAWN_POSTSEC_ADJ_1 */
           (calculated 'ECL_Post_Adj_Drawn1'n/t1.'ECL_Drawn1'n)*t1.'ECL_Drawn_PostSec1'n format=30.8 AS 'ECL_Post_Adj_Drawn_PostSec1'n, 
          /* ECL_DRAWN_POSTSEC_ADJ_2 */
            (calculated 'ECL_Post_Adj_Drawn2'n/t1.'ECL_Drawn2'n)*t1.'ECL_Drawn_PostSec2'n format=30.8 AS 'ECL_Post_Adj_Drawn_PostSec2'n, 
          /* ECL_DRAWN_POSTSEC_ADJ_3 */
            (calculated 'ECL_Post_Adj_Drawn3'n/t1.'ECL_Drawn3'n)*t1.'ECL_Drawn_PostSec3'n format=30.8 AS 'ECL_Post_Adj_Drawn_PostSec3'n,
			t1.'Transit_Number'n,		/*adding for bcar_ccar combine file */
		  	t1.'Product_Id'n,			/*adding for bcar_ccar combine file */
		  	t1.'EGL_Department_Id'n		/*adding for bcar_ccar combine file */
      FROM &indata t1
           LEFT JOIN (&INPATH..POST_ADJ_VALUES t2
           LEFT JOIN &INPATH..PRE_ADJ_SUM t3 ON (t2.Provisions_Product = t3.Provisions_Product)) ON ((prxchange('s/\_\d+//',-1,compress(t1.'Basel_Product_Name'n))) = t2.BASEL_PRODUCT_TYPE);
QUIT;

data test;
	set &INPATH..exposer_02;
run;
proc sql;
	delete from test t1
	where (prxchange('s/\_\d+//',-1,compress(t1.'Basel_Product_Name'n))) in ('OTHER_CON_SLR',
            'OTHER_CON_SLT','REV_CON_SLR');
quit;

proc sql;
	create table work.spsp_temp1 as
	select t1.'Unique_Identifier'n, 
          t1.'Basel_Product_Name'n, 
          t1.'ProbabilityDefault_Band'n, 
          t1.'Legal_Entity'n, 
          t1.'Day90_PastDue_Flag'n, 
          t1.'Unconditionally_Cancelable_Flag'n, 
          t1.'Drawn_Amount'n, 
          t1.'Exposures_at_Default'n, 
          t1.Currency, 
          t1.'Expected_Loss'n, 
          t1.'Undrawn_Amount'n, 
          t1.'Accrued_Interest'n, 
          /* t1.ACL, */
          t1.'Partial_Write_Off'n, 
          t1.'Loss_Given_Default'n, 
          t1.'EAD_Factor'n, 
          t1.'Insurance_Flag'n, 
          t1.'Downturn_LGD'n, 
          t1.'LTV_Bucket'n, 
          t1.'Obligors'n, 		  
          t1.'Exposures_Prior_Securitization'n, 
          t1.'Exposures_No_Securitization'n, 
          t1.'Exposures_With_Securitization'n, 
          t1.'Securitization_Amount'n, 
          t1.'Exposures_Reduced'n, 
          t1.'Securitization_Name'n, 
          t1.'ECL_Drawn1'n, 
          t1.'ECL_Drawn2'n, 
          t1.'ECL_Drawn3'n, 
          t1.'ECL_Undrawn1'n, 
          t1.'ECL_Undrawn2'n, 
          t1.'ECL_Undrawn3'n, 
          t1.'ECL_Drawn_PostSec1'n, 
          t1.'ECL_Drawn_PostSec2'n, 
          t1.'ECL_Drawn_PostSec3'n, 
		  t1.'Transit_Number'n,		/*adding for bcar_ccar combine file */
		  t1.'Product_Id'n,			/*adding for bcar_ccar combine file */
		  t1.'EGL_Department_Id'n,	/*adding for bcar_ccar combine file */
		  t1.'Defaulted_Exposure_Flag'n,				
		  t1.'Uninsured_LGD'n,
		  t1.'Uninsured_DLGD'n,
		t1.'Rental_Income_Flag'n,
		t1.'Currency_Mismatch_Flag'n,
		t1.'Total_Exposure_Above_Limit'n,
		t1.'Orig_Loan_Amt_as_of_Insur_Date'n,
		t1.'Transactor_Flag'n,
		t1.'Clp_Flag'n,
          /* Product_Type */
            (case when (prxchange('s/\_\d+//',-1,compress(t1.'Basel_Product_Name'n))) in ('OTHER_CON_SLR',
            'OTHER_CON_SLT','REV_CON_SLR') then 'Student Lines' end) AS Product_Type
	 FROM &INPATH..exposer_01 t1
			where (prxchange('s/\_\d+//',-1,compress(t1.'Basel_Product_Name'n))) in ('OTHER_CON_SLR','OTHER_CON_SLT','REV_CON_SLR');
	QUIT;

proc sql;
	create table spsp_temp2 as
	select t1.*, t2.ECL_Drawn_1_Adj,t2.ECL_Drawn_2_Adj,t2.ECL_Drawn_3_Adj
	FROM spsp_temp1 t1
		LEFT JOIN &INPATH..POST_ECL_PROVISIONS t2 ON (t1.Product_Type = t2.Products);
quit;

proc sql;
	create table spsp_temp3_26 as
	select *,(SUM(t1.'Drawn_Amount'n)) FORMAT=22.2 AS SUM_EXPSR_DRAWN_SPSP, 
          /* SUM_EXPSR_UNDRAWN_SPSP */
            (SUM(t1.'Undrawn_Amount'n)) FORMAT=19.2 AS SUM_EXPSR_UNDRAWN_SPSP, 
          /* TOTAL_SPSP_PRE */
            ((SUM(t1.'Drawn_Amount'n)) + (SUM(t1.'Undrawn_Amount'n))) AS TOTAL_SPSP_PRE_26,
			((('Drawn_Amount'n + 'Undrawn_Amount'n)/calculated TOTAL_SPSP_PRE_26)*ECL_Drawn_3_Adj) as ECL_DRAWN_3_Post
	from spsp_temp2 t1
/*	where t1.'ProbabilityDefault_Band'n='26'; */
	where t1.'ProbabilityDefault_Band'n=26;
quit;

Proc sql;
	create table spsp_temp3_26_filter as 
	select * from spsp_temp3_26 where 'Basel_Product_Name'n is not missing;
quit;

proc sql;
	create table spsp_temp3_NE26 as
	select *,             (SUM(t1.'Drawn_Amount'n)) FORMAT=22.2 AS SUM_EXPSR_DRAWN_SPSP, 
          /* SUM_EXPSR_UNDRAWN_SPSP */
            (SUM(t1.'Undrawn_Amount'n)) FORMAT=19.2 AS SUM_EXPSR_UNDRAWN_SPSP, 
          /* TOTAL_SPSP_PRE */
            ((SUM(t1.'Drawn_Amount'n)) + (SUM(t1.'Undrawn_Amount'n))) AS TOTAL_SPSP_PRE_NE26,

			((('Drawn_Amount'n + 'Undrawn_Amount'n)/calculated TOTAL_SPSP_PRE_NE26)*ECL_Drawn_1_Adj) as ECL_DRAWN_1_Post,
((('Drawn_Amount'n + 'Undrawn_Amount'n)/calculated TOTAL_SPSP_PRE_NE26)*ECL_Drawn_2_Adj) as ECL_DRAWN_2_Post
	from spsp_temp2 t1
/*	where t1.'ProbabilityDefault_Band'n not eq '26'; */
	where t1.'ProbabilityDefault_Band'n not eq 26;
quit;

Proc sql;
	create table spsp_temp3_NE26_filter as 
	select * from spsp_temp3_NE26 where 'Basel_Product_Name'n is not missing;
quit;

data spsp_total;
	set spsp_temp3_26_filter(rename=(TOTAL_SPSP_PRE_26=TOTAL_SPSP_PRE)) spsp_temp3_NE26_filter(rename=(TOTAL_SPSP_PRE_NE26=TOTAL_SPSP_PRE));
run;

proc sql;
	create table append_attrib_SPSP as
	select  t1.'Unique_Identifier'n, 
          t1.'Basel_Product_Name'n, 
          t1.'ProbabilityDefault_Band'n, 
          t1.'Legal_Entity'n, 
		  t1.'Defaulted_Exposure_Flag'n,				
          t1.'Day90_PastDue_Flag'n, 
          t1.'Unconditionally_Cancelable_Flag'n, 
          t1.'Drawn_Amount'n, 
          t1.'Exposures_at_Default'n, 
          t1.Currency, 
          t1.'Expected_Loss'n, 
          t1.'Undrawn_Amount'n, 
          t1.'Accrued_Interest'n, 
          /* t1.ACL, */
          t1.'Partial_Write_Off'n, 
          t1.'Loss_Given_Default'n, 
          t1.'EAD_Factor'n, 
          t1.'Insurance_Flag'n, 
          t1.'Downturn_LGD'n, 
		  t1.'Uninsured_LGD'n,
		  t1.'Uninsured_DLGD'n,
          t1.'LTV_Bucket'n, 
          t1.'Obligors'n, 
		  t1.'Rental_Income_Flag'n,
		t1.'Currency_Mismatch_Flag'n,
		t1.'Total_Exposure_Above_Limit'n,
		t1.'Orig_Loan_Amt_as_of_Insur_Date'n,
		t1.'Transactor_Flag'n,
		t1.'Clp_Flag'n,
          t1.'Exposures_Prior_Securitization'n, 
          t1.'Exposures_No_Securitization'n, 
          t1.'Exposures_With_Securitization'n, 
          t1.'Securitization_Amount'n, 
          t1.'Exposures_Reduced'n, 
          t1.'Securitization_Name'n, 
          t1.'ECL_Drawn1'n, 
          t1.'ECL_Drawn2'n, 
          t1.'ECL_Drawn3'n, 
          t1.'ECL_Undrawn1'n, 
          t1.'ECL_Undrawn2'n, 
          t1.'ECL_Undrawn3'n, 
          t1.'ECL_Drawn_PostSec1'n, 
          t1.'ECL_Drawn_PostSec2'n, 
          t1.'ECL_Drawn_PostSec3'n,
		  t1.ECL_DRAWN_1_Post as 'ECL_Post_Adj_Drawn1'n,
		  t1.ECL_DRAWN_2_Post as 'ECL_Post_Adj_Drawn2'n,
		  t1.ECL_DRAWN_3_Post as 'ECL_Post_Adj_Drawn3'n,
		  . as 'ECL_Post_Adj_Undrawn1'n,
		  . as 'ECL_Post_Adj_Undrawn2'n,
		  . as 'ECL_Post_Adj_Undrawn3'n,
		  t1.ECL_Drawn_1_post as 'ECL_Post_Adj_Drawn_PostSec1'n,
		  t1.ECL_Drawn_2_post as 'ECL_Post_Adj_Drawn_PostSec2'n,
		  t1.ECL_Drawn_3_post as 'ECL_Post_Adj_Drawn_PostSec3'n,
		  t1.'Transit_Number'n,		/*adding for bcar_ccar combine file */
		  t1.'Product_Id'n,			/*adding for bcar_ccar combine file */
		  t1.'EGL_Department_Id'n	/*adding for bcar_ccar combine file */
		from spsp_total t1;
	quit;

	proc sql;
		insert into test
		select * from append_attrib_SPSP;
	quit;

proc sort data=test out=&outdata;
	by 'Unique_Identifier'n;
run;
%mend Attrib;

	%if &WITHZERONET ne %then
		%do;
			/** BEFORE ZERO NETTING **/
			%let reqvars=
				UNQ_RECD_ID
				CCAR_BASEL_PRD_TP_NM
				PD_BAND
				Legal_Entity
				PD_90_DAY_F
				UNCONDTNLY_CNCLBL
				BEFR_ZERO_NET_ADJ_OS_BAL_AMT
				EAD_PC
				CRNCY_CD
				EXPCTD_LOSS_RTO_TEXT
				BEFORE_ZERO_NET_UNDRAWN_AMT
				ACCR_INTR_AMT
			/*	BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT */
				PRTL_WRITE_OFF_AMT
				LGD_FINAL_RPTG_RTO_TEXT
				EAD_FINAL_RPTG_RTO_TEXT
				INSUR_F
				WGHTD_DLGD_RTO
				LTV_Percentage
				Obligors
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
				ECL_Drawn_PostSec_3
				TRNST_NUM
				PRD_ID
				EGL_DEPRTMNT
				RNTL_PRPTY_F 
				CURRENCY_MISMATCH_F 
				TOT_EXPSR_ABOVE_1500K_LMT_F 
				ORIG_AMT_LOAN 
				TRANSACTOR_FLAG_QRR
				CLP_FLAG
				DEFAULT_F
				UNINSURED_FLRD_LGD_RTO
				UNINSURED_DLGD_RTO
;

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
				format UNINSURED_DLGD_RTO percent14.6;

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
					UNQ_RECD_ID = 'Unique_Identifier'n
					CCAR_BASEL_PRD_TP_NM = 'Basel_Product_Name'n
					PD_BAND = 'ProbabilityDefault_Band'n
					Legal_Entity = 'Legal_Entity'n
					DEFAULT_F = 'Defaulted_Exposure_Flag'n
					PD_90_DAY_F = 'Day90_PastDue_Flag'n					
					UNCONDTNLY_CNCLBL = 'Unconditionally_Cancelable_Flag'n
					BEFR_ZERO_NET_ADJ_OS_BAL_AMT = 'Drawn_Amount'n
					EAD_PC = 'Exposures_at_Default'n
					CRNCY_CD = Currency
					EXPCTD_LOSS_RTO_TEXT = 'Expected_Loss'n
					BEFORE_ZERO_NET_UNDRAWN_AMT = 'Undrawn_Amount'n
					ACCR_INTR_AMT = 'Accrued_Interest'n
				/*	BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT = ACL */
					PRTL_WRITE_OFF_AMT = 'Partial_Write_Off'n
					LGD_FINAL_RPTG_RTO_TEXT = 'Loss_Given_Default'n
					EAD_FINAL_RPTG_RTO_TEXT = 'EAD_Factor'n
					INSUR_F= 'Insurance_Flag'n
					WGHTD_DLGD_RTO='Downturn_LGD'n
					UNINSURED_FLRD_LGD_RTO = 'Uninsured_LGD'n
					UNINSURED_DLGD_RTO = 'Uninsured_DLGD'n
					Obligors='Obligors'n
					LTV_PERCENTAGE='LTV_Bucket'n
					RNTL_PRPTY_F = 'Rental_Income_Flag'n
					CURRENCY_MISMATCH_F = 'Currency_Mismatch_Flag'n
					TOT_EXPSR_ABOVE_1500K_LMT_F = 'Total_Exposure_Above_Limit'n
					ORIG_AMT_LOAN = 'Orig_Loan_Amt_as_of_Insur_Date'n
					TRANSACTOR_FLAG_QRR = 'Transactor_Flag'n
					CLP_FLAG = 'Clp_Flag'n
				/* Added for securitization */
					expsr_drawn_prior_secur='Exposures_Prior_Securitization'n
					expsr_drawn_no_secur='Exposures_No_Securitization'n
					expsr_drawn_with_secur='Exposures_With_Securitization'n
					secur_amt='Securitization_Amount'n
					expsr_drawn_reduced='Exposures_Reduced'n
					secrtztn_flag='Securitization_Name'n
					ECL_Drawn_1='ECL_Drawn1'n
					ECL_Drawn_2='ECL_Drawn2'n
					ECL_Drawn_3='ECL_Drawn3'n
					ECL_Undrawn_1='ECL_Undrawn1'n
					ECL_Undrawn_2='ECL_Undrawn2'n
					ECL_Undrawn_3='ECL_Undrawn3'n
					ECL_Drawn_PostSec_1='ECL_Drawn_PostSec1'n
					ECL_Drawn_PostSec_2='ECL_Drawn_PostSec2'n
					ECL_Drawn_PostSec_3='ECL_Drawn_PostSec3'n
				/* ACAP */
					TRNST_NUM='Transit_Number'n
					PRD_ID='Product_Id'n
					EGL_DEPRTMNT='EGL_Department_Id'n;
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

/*				if ACL = . then*/
/*					ACL = 0;*/
			run;

			/** producing BEFORE ZERO control CTL table **/
			/*** calculating HAS_TOTAL BEFORE ZERO for control file **/
			proc sql threads noprint;
				select
					/*mth_end_dt*/

					/*,legal_entity*/
					sum('Drawn_Amount'n) format=25.2 as hash_total_bf
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
				file "&outlanding./cmf/outgoing/CCAR_ACAP/DR-AIRB-EXPOSURES-&WITHZERONET.&YYYYMMDD..ctl";

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
		pd_bandn = input('ProbabilityDefault_Band'n,8.);

	proc sort out= &lib..&dsname (drop= PD_BANDn )force;
		by 'Unique_Identifier'n
  		   'Basel_Product_Name'n
			PD_BANDn;
	run;

	 data _null_;
      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
      %let _EFIREC_ = 0;     /* clear export record count macro variable */
      file "&outlanding./cmf/outgoing/CCAR_ACAP/&dsn.&YYYYMMDD..csv" delimiter=',' DSD DROPOVER lrecl=32767;
      if _n_ = 1 then        /* write column names or labels */
       do;
         put
           "Unique_Identifier" ','
           "Basel_Product_Name" ','
           "ProbabilityDefault_Band" ','
           "Legal_Entity" ','
           "Defaulted_Exposure_Flag" ','
           "Day90_PastDue_Flag" ','
           "Unconditionally_Cancelable_Flag" ','
           "Drawn_Amount" ','
           "Exposures_at_Default" ','
           "Currency" ','
           "Expected_Loss" ','
           "Undrawn_Amount" ','
           "Accrued_Interest" ','
           "Partial_Write_Off" ','
           "Loss_Given_Default" ','
           "EAD_Factor" ','
           "Insurance_Flag" ','
           "Downturn_LGD" ','
           "Uninsured_LGD" ','
           "Uninsured_DLGD" ','
           "LTV_Bucket" ','
           "Obligors" ','
           "Exposure_Materially_Dependent_Rental_Income_Flag" ','
           "Currency_Mismatch_Flag" ','
           "Total_Exposure_Above_Limit" ','
           "Orig_Loan_Amt_as_of_Insur_Date" ','
           "Transactor_Flag" ','
	     "Clp_Flag" ','
           "Exposures_Prior_Securitization" ','
           "Exposures_No_Securitization" ','
           "Exposures_With_Securitization" ','
           "Securitization_Amount" ','
           "Exposures_Reduced" ','
           "Securitization_Name" ','
           "ECL_Drawn1" ','
           "ECL_Drawn2" ','
           "ECL_Drawn3" ','
           "ECL_Undrawn1" ','
           "ECL_Undrawn2" ','
           "ECL_Undrawn3" ','
           "ECL_Drawn_PostSec1" ','
           "ECL_Drawn_PostSec2" ','
           "ECL_Drawn_PostSec3" ','
           "ECL_Post_Adj_Drawn1" ','
           "ECL_Post_Adj_Drawn2" ','
           "ECL_Post_Adj_Drawn3" ','
           "ECL_Post_Adj_Undrawn1" ','
           "ECL_Post_Adj_Undrawn2" ','
           "ECL_Post_Adj_Undrawn3" ','
           "ECL_Post_Adj_Drawn_PostSec1" ','
           "ECL_Post_Adj_Drawn_PostSec2" ','
           "ECL_Post_Adj_Drawn_PostSec3" ','
           "Transit_Number" ','
           "Product_Id" ','
           "EGL_Department_Id"
         ;
       end;
     set  &lib..&dsname   end=EFIEOD;
         format Unique_Identifier $20. ;
         format Basel_Product_Name $50. ;
         format ProbabilityDefault_Band best12. ;
         format Legal_Entity $40. ;
         format Defaulted_Exposure_Flag $5. ;
         format Day90_PastDue_Flag $5. ;
         format Unconditionally_Cancelable_Flag $10. ;
         format Drawn_Amount 25.2 ;
         format Exposures_at_Default $15. ;
         format Currency $10. ;
         format Expected_Loss $22. ;
         format Undrawn_Amount 22.2 ;
         format Accrued_Interest 22.2 ;
         format Partial_Write_Off 22.2 ;
         format Loss_Given_Default $22. ;
         format EAD_Factor $22. ;
         format Insurance_Flag $10. ;
         format Downturn_LGD percent14.6 ;
         format Uninsured_LGD $22. ;
         format Uninsured_DLGD percent14.6 ;
         format LTV_Bucket $30. ;
         format Obligors 22. ;
         format Rental_Income_Flag $1. ;
         format Currency_Mismatch_Flag $1. ;
         format Total_Exposure_Above_Limit $1. ;
         format Orig_Loan_Amt_as_of_Insur_Date 19.3 ;
         format Transactor_Flag $1. ;
	   format Clp_Flag $1. ;
         format Exposures_Prior_Securitization 22.2 ;
         format Exposures_No_Securitization 22.2 ;
         format Exposures_With_Securitization 22.2 ;
         format Securitization_Amount 22.2 ;
         format Exposures_Reduced 22.2 ;
         format Securitization_Name $13. ;
         format ECL_Drawn1 30.2 ;
         format ECL_Drawn2 30.2 ;
         format ECL_Drawn3 30.2 ;
         format ECL_Undrawn1 30.2 ;
         format ECL_Undrawn2 30.2 ;
         format ECL_Undrawn3 30.2 ;
         format ECL_Drawn_PostSec1 30.2 ;
         format ECL_Drawn_PostSec2 30.2 ;
         format ECL_Drawn_PostSec3 30.2 ;
         format ECL_Post_Adj_Drawn1 30.2 ;
         format ECL_Post_Adj_Drawn2 30.2 ;
         format ECL_Post_Adj_Drawn3 30.2 ;
         format ECL_Post_Adj_Undrawn1 30.2 ;
         format ECL_Post_Adj_Undrawn2 30.2 ;
         format ECL_Post_Adj_Undrawn3 30.2 ;
         format ECL_Post_Adj_Drawn_PostSec1 30.2 ;
         format ECL_Post_Adj_Drawn_PostSec2 30.2 ;
         format ECL_Post_Adj_Drawn_PostSec3 30.2 ;
         format Transit_Number $5. ;
         format Product_Id $10. ;
         format EGL_Department_Id $5. ;
       do;
         EFIOUT + 1;
         put Unique_Identifier $ @;
         put Basel_Product_Name $ @;
         put ProbabilityDefault_Band @;
         put Legal_Entity $ @;
         put Defaulted_Exposure_Flag $ @;
         put Day90_PastDue_Flag $ @;
         put Unconditionally_Cancelable_Flag $ @;
         put Drawn_Amount @;
         put Exposures_at_Default $ @;
         put Currency $ @;
         put Expected_Loss $ @;
         put Undrawn_Amount @;
         put Accrued_Interest @;
         put Partial_Write_Off @;
         put Loss_Given_Default $ @;
         put EAD_Factor $ @;
         put Insurance_Flag $ @;
         put Downturn_LGD @;
         put Uninsured_LGD $ @;
         put Uninsured_DLGD @;
         put LTV_Bucket $ @;
         put Obligors @;
         put Rental_Income_Flag $ @;
         put Currency_Mismatch_Flag $ @;
         put Total_Exposure_Above_Limit $ @;
         put Orig_Loan_Amt_as_of_Insur_Date @;
         put Transactor_Flag $ @;
	   put Clp_Flag $ @;
         put Exposures_Prior_Securitization @;
         put Exposures_No_Securitization @;
         put Exposures_With_Securitization @;
         put Securitization_Amount @;
         put Exposures_Reduced @;
         put Securitization_Name $ @;
         put ECL_Drawn1 @;
         put ECL_Drawn2 @;
         put ECL_Drawn3 @;
         put ECL_Undrawn1 @;
         put ECL_Undrawn2 @;
         put ECL_Undrawn3 @;
         put ECL_Drawn_PostSec1 @;
         put ECL_Drawn_PostSec2 @;
         put ECL_Drawn_PostSec3 @;
         put ECL_Post_Adj_Drawn1 @;
         put ECL_Post_Adj_Drawn2 @;
         put ECL_Post_Adj_Drawn3 @;
         put ECL_Post_Adj_Undrawn1 @;
         put ECL_Post_Adj_Undrawn2 @;
         put ECL_Post_Adj_Undrawn3 @;
         put ECL_Post_Adj_Drawn_PostSec1 @;
         put ECL_Post_Adj_Drawn_PostSec2 @;
         put ECL_Post_Adj_Drawn_PostSec3 @;
         put Transit_Number $ @;
         put Product_Id $ @;
         put EGL_Department_Id $ ;
         ;
       end;
      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
      if EFIEOD then call symputx('_EFIREC_',EFIOUT);
      run;

	DATA _NULL_;
		CALL SYSTEM("openssl dgst -sha256 &outlanding./cmf/outgoing/CCAR_ACAP/&dsn.&YYYYMMDD..csv | sed -e 's/^.*= //g' >> &outlanding./cmf/outgoing/CCAR_ACAP/&dsn.&YYYYMMDD._chk.ctl");
	run;
	/*
	proc export data = &lib..&dsname  outfile="&outlanding./cmf/&dsn.&YYYYMMDD..csv"
		dbms=csv replace;
	run;
	*/

	/*x 'sed s/\"//g &outlanding./cmf/&dsn.&YYYYMMDD..csv';*/
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
			Legal_Entity
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
			LTV_Percentage
			Obligors
			RNTL_PRPTY_F 
			CURRENCY_MISMATCH_F 
			TOT_EXPSR_ABOVE_1500K_LMT_F 
			ORIG_AMT_LOAN 
			TRANSACTOR_FLAG_QRR
			CLP_FLAG
			DEFAULT_F
			UNINSURED_FLRD_LGD_RTO
			UNINSURED_DLGD_RTO
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
			ECL_Drawn_PostSec_3
		/* ACAP */
			TRNST_NUM
			PRD_ID
			EGL_DEPRTMNT
			;

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
			format UNINSURED_DLGD_RTO percent14.6;

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
				UNQ_RECD_ID = 'Unique_Identifier'n
				CCAR_BASEL_PRD_TP_NM = 'Basel_Product_Name'n
				PD_BAND = 'ProbabilityDefault_Band'n
				Legal_Entity = 'Legal_Entity'n
				DEFAULT_F = 'Defaulted_Exposure_Flag'n
				PD_90_DAY_F = 'Day90_PastDue_Flag'n
				UNCONDTNLY_CNCLBL = 'Unconditionally_Cancelable_Flag'n
				AF_ZERO_NET_ADJUSTED_OS_BAL_AMT = 'Drawn_Amount'n
				EAD_PC = 'Exposures_at_Default'n
				CRNCY_CD = Currency
				EXPCTD_LOSS_RTO_TEXT = 'Expected_Loss'n
				AF_ZERO_NET_UNDRAWN_AMT = 'Undrawn_Amount'n
				ACCR_INTR_AMT = 'Accrued_Interest'n
				/*AF_ZERO_NET_ALWBL_CR_LOSS_AMT = ACL*/
				PRTL_WRITE_OFF_AMT = 'Partial_Write_Off'n
				LGD_FINAL_RPTG_RTO_TEXT = 'Loss_Given_Default'n
				EAD_FINAL_RPTG_RTO_TEXT = 'EAD_Factor'n
				INSUR_F='Insurance_Flag'n
				WGHTD_DLGD_RTO='Downturn_LGD'n
				UNINSURED_FLRD_LGD_RTO = 'Uninsured_LGD'n
				UNINSURED_DLGD_RTO = 'Uninsured_DLGD'n
				Obligors='Obligors'n
			/* Added for securitization */
				expsr_drawn_prior_secur='Exposures_Prior_Securitization'n
				expsr_drawn_no_secur='Exposures_No_Securitization'n
				expsr_drawn_with_secur='Exposures_With_Securitization'n
				secur_amt='Securitization_Amount'n
				expsr_drawn_reduced='Exposures_Reduced'n
				secrtztn_flag='Securitization_Name'n
				ECL_Drawn_1='ECL_Drawn1'n
				ECL_Drawn_2='ECL_Drawn2'n
				ECL_Drawn_3='ECL_Drawn3'n
				ECL_Undrawn_1='ECL_Undrawn1'n
				ECL_Undrawn_2='ECL_Undrawn2'n
				ECL_Undrawn_3='ECL_Undrawn3'n
				ECL_Drawn_PostSec_1='ECL_Drawn_PostSec1'n
				ECL_Drawn_PostSec_2='ECL_Drawn_PostSec2'n
				ECL_Drawn_PostSec_3='ECL_Drawn_PostSec3'n
			/* ACAP */
				TRNST_NUM='Transit_Number'n
				PRD_ID='Product_Id'n
				EGL_DEPRTMNT='EGL_Department_Id'n
				LTV_PERCENTAGE='LTV_Bucket'n
				RNTL_PRPTY_F = 'Rental_Income_Flag'n
				CURRENCY_MISMATCH_F = 'Currency_Mismatch_Flag'n
				TOT_EXPSR_ABOVE_1500K_LMT_F = 'Total_Exposure_Above_Limit'n
				ORIG_AMT_LOAN = 'Orig_Loan_Amt_as_of_Insur_Date'n
				TRANSACTOR_FLAG_QRR = 'Transactor_Flag'n
				CLP_FLAG = 'Clp_Flag'n ;
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

/*			if ACL = . then*/
/*				ACL = 0;*/
		run;

		%put >>> AF_ZERO NOBS_AF = &nobsaf.;

		/** producing control AFTER ZERO CTL table **/
		/*** calculating HASh_TOTAL for AFTER ZERO control file **/
		proc sql threads noprint;
			select 

				/*sai updated code due to data problems for aug 19th run with inputs from Min*/

			/*sum(input('EAD_Factor'n,percent8.6)) format=25.2 as hash_total_af*/
			sum('Drawn_Amount'n) format 25.2 as hash_total_af into :hashtotaf
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
			file "&outlanding./cmf/outgoing/CCAR_ACAP/DR-AIRB-EXPOSURES-&WITHZERONET.&YYYYMMDD..ctl" ;/*lrecl=203 recfm=V ;*/
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
		pd_bandn = input('ProbabilityDefault_Band'n,8.);

	proc sort out= &lib..&dsname (drop= PD_BANDn )force;
		by   'Unique_Identifier'n 
             'Basel_Product_Name'n
			PD_BANDn;
	run;

	 data ccar_output;
      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
      %let _EFIREC_ = 0;     /* clear export record count macro variable */
      file "&outlanding./cmf/outgoing/CCAR_ACAP/&dsn..csv" delimiter=',' DSD DROPOVER lrecl=32767;
      if _n_ = 1 then        /* write column names or labels */
       do;
         put
           "Unique_Identifier" ','
           "Basel_Product_Name" ','
           "ProbabilityDefault_Band" ','
           "Legal_Entity" ','
           "Defaulted_Exposure_Flag" ','
           "Day90_PastDue_Flag" ','
           "Unconditionally_Cancelable_Flag" ','
           "Drawn_Amount" ','
           "Exposures_at_Default" ','
           "Currency" ','
           "Expected_Loss" ','
           "Undrawn_Amount" ','
           "Accrued_Interest" ','
           "Partial_Write_Off" ','
           "Loss_Given_Default" ','
           "EAD_Factor" ','
           "Insurance_Flag" ','
           "Downturn_LGD" ','
           "Uninsured_LGD" ','
           "Uninsured_DLGD" ','
           "LTV_Bucket" ','
           "Obligors" ','
           "Exposure_Materially_Dependent_Rental_Income_Flag" ','
           "Currency_Mismatch_Flag" ','
           "Total_Exposure_Above_Limit" ','
           "Orig_Loan_Amt_as_of_Insur_Date" ','
           "Transactor_Flag" ','
	     "Clp_Flag" ','
           "Exposures_Prior_Securitization" ','
           "Exposures_No_Securitization" ','
           "Exposures_With_Securitization" ','
           "Securitization_Amount" ','
           "Exposures_Reduced" ','
           "Securitization_Name" ','
           "ECL_Drawn1" ','
           "ECL_Drawn2" ','
           "ECL_Drawn3" ','
           "ECL_Undrawn1" ','
           "ECL_Undrawn2" ','
           "ECL_Undrawn3" ','
           "ECL_Drawn_PostSec1" ','
           "ECL_Drawn_PostSec2" ','
           "ECL_Drawn_PostSec3" ','
           "ECL_Post_Adj_Drawn1" ','
           "ECL_Post_Adj_Drawn2" ','
           "ECL_Post_Adj_Drawn3" ','
           "ECL_Post_Adj_Undrawn1" ','
           "ECL_Post_Adj_Undrawn2" ','
           "ECL_Post_Adj_Undrawn3" ','
           "ECL_Post_Adj_Drawn_PostSec1" ','
           "ECL_Post_Adj_Drawn_PostSec2" ','
           "ECL_Post_Adj_Drawn_PostSec3" ','
           "Transit_Number" ','
           "Product_Id" ','
           "EGL_Department_Id"
         ;
       end;
     set  &lib..&dsname   end=EFIEOD;
         format Unique_Identifier $20. ;
         format Basel_Product_Name $50. ;
         format ProbabilityDefault_Band best12. ;
         format Legal_Entity $40. ;
         format Defaulted_Exposure_Flag $5. ;
         format Day90_PastDue_Flag $5. ;
         format Unconditionally_Cancelable_Flag $10. ;
         format Drawn_Amount 25.2 ;
         format Exposures_at_Default $15. ;
         format Currency $10. ;
         format Expected_Loss $22. ;
         format Undrawn_Amount 22.2 ;
         format Accrued_Interest 22.2 ;
         format Partial_Write_Off 22.2 ;
         format Loss_Given_Default $22. ;
         format EAD_Factor $22. ;
         format Insurance_Flag $10. ;
         format Downturn_LGD percent14.6 ;
         format Uninsured_LGD $22. ;
         format Uninsured_DLGD percent14.6 ;
         format LTV_Bucket $30. ;
         format Obligors 22. ;
         format Rental_Income_Flag $1. ;
         format Currency_Mismatch_Flag $1. ;
         format Total_Exposure_Above_Limit $1. ;
         format Orig_Loan_Amt_as_of_Insur_Date 19.3 ;
         format Transactor_Flag $1. ;
	   format Clp_Flag $1. ;
         format Exposures_Prior_Securitization 22.2 ;
         format Exposures_No_Securitization 22.2 ;
         format Exposures_With_Securitization 22.2 ;
         format Securitization_Amount 22.2 ;
         format Exposures_Reduced 22.2 ;
         format Securitization_Name $13. ;
         format ECL_Drawn1 30.2 ;
         format ECL_Drawn2 30.2 ;
         format ECL_Drawn3 30.2 ;
         format ECL_Undrawn1 30.2 ;
         format ECL_Undrawn2 30.2 ;
         format ECL_Undrawn3 30.2 ;
         format ECL_Drawn_PostSec1 30.2 ;
         format ECL_Drawn_PostSec2 30.2 ;
         format ECL_Drawn_PostSec3 30.2 ;
         format ECL_Post_Adj_Drawn1 30.2 ;
         format ECL_Post_Adj_Drawn2 30.2 ;
         format ECL_Post_Adj_Drawn3 30.2 ;
         format ECL_Post_Adj_Undrawn1 30.2 ;
         format ECL_Post_Adj_Undrawn2 30.2 ;
         format ECL_Post_Adj_Undrawn3 30.2 ;
         format ECL_Post_Adj_Drawn_PostSec1 30.2 ;
         format ECL_Post_Adj_Drawn_PostSec2 30.2 ;
         format ECL_Post_Adj_Drawn_PostSec3 30.2 ;
         format Transit_Number $5. ;
         format Product_Id $10. ;
         format EGL_Department_Id $5. ;
       do;
         EFIOUT + 1;
         put Unique_Identifier $ @;
         put Basel_Product_Name $ @;
         put ProbabilityDefault_Band @;
         put Legal_Entity $ @;
         put Defaulted_Exposure_Flag $ @;
         put Day90_PastDue_Flag $ @;
         put Unconditionally_Cancelable_Flag $ @;
         put Drawn_Amount @;
         put Exposures_at_Default $ @;
         put Currency $ @;
         put Expected_Loss $ @;
         put Undrawn_Amount @;
         put Accrued_Interest @;
         put Partial_Write_Off @;
         put Loss_Given_Default $ @;
         put EAD_Factor $ @;
         put Insurance_Flag $ @;
         put Downturn_LGD  @;
         put Uninsured_LGD $ @;
         put Uninsured_DLGD @;
         put LTV_Bucket $ @;
         put Obligors @;
         put Rental_Income_Flag $ @;
         put Currency_Mismatch_Flag $ @;
         put Total_Exposure_Above_Limit $ @;
         put Orig_Loan_Amt_as_of_Insur_Date @;
         put Transactor_Flag $ @;
	   put Clp_Flag $ @;
         put Exposures_Prior_Securitization @;
         put Exposures_No_Securitization @;
         put Exposures_With_Securitization @;
         put Securitization_Amount @;
         put Exposures_Reduced @;
         put Securitization_Name $ @;
         put ECL_Drawn1 @;
         put ECL_Drawn2 @;
         put ECL_Drawn3 @;
         put ECL_Undrawn1 @;
         put ECL_Undrawn2 @;
         put ECL_Undrawn3 @;
         put ECL_Drawn_PostSec1 @;
         put ECL_Drawn_PostSec2 @;
         put ECL_Drawn_PostSec3 @;
         put ECL_Post_Adj_Drawn1 @;
         put ECL_Post_Adj_Drawn2 @;
         put ECL_Post_Adj_Drawn3 @;
         put ECL_Post_Adj_Undrawn1 @;
         put ECL_Post_Adj_Undrawn2 @;
         put ECL_Post_Adj_Undrawn3 @;
         put ECL_Post_Adj_Drawn_PostSec1 @;
         put ECL_Post_Adj_Drawn_PostSec2 @;
         put ECL_Post_Adj_Drawn_PostSec3 @;
         put Transit_Number $ @;
         put Product_Id $ @;
         put EGL_Department_Id $ ;
         ;
       end;
      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
      if EFIEOD then call symputx('_EFIREC_',EFIOUT);
      run;


%if &dsname. eq EXPSR_&YYYYMMDD. %then %do;

	proc sql;
		create table rrap_ccar_output as select 
			&mth_tm_id as mth_tm_id
			,Unique_Identifier
			,Basel_Product_Name
			,ProbabilityDefault_Band
			,Legal_Entity
			,Defaulted_Exposure_Flag
			,Day90_PastDue_Flag
			,Unconditionally_Cancelable_Flag
			,Drawn_Amount
			,Exposures_at_Default
			,Currency
			,Expected_Loss
			,Undrawn_Amount
			,Accrued_Interest
			,Partial_Write_Off
			,Loss_Given_Default
			,EAD_Factor
			,Insurance_Flag
			,case when missing(Downturn_LGD) then '' else compress(put(Downturn_LGD,percent14.6)) end as Downturn_LGD
			,Uninsured_LGD
			,case when missing(Uninsured_DLGD) then '' else compress(put(Uninsured_DLGD,percent14.6)) end  as Uninsured_DLGD
			,LTV_Bucket
			,Obligors
			,Rental_Income_Flag
			,Currency_Mismatch_Flag
			,Total_Exposure_Above_Limit
			,Orig_Loan_Amt_as_of_Insur_Date
			,Transactor_Flag
			,Clp_Flag
			,Exposures_Prior_Securitization
			,Exposures_No_Securitization
			,Exposures_With_Securitization
			,Securitization_Amount
			,Exposures_Reduced
			,Securitization_Name
			,ECL_Drawn1
			,ECL_Drawn2
			,ECL_Drawn3
			,ECL_Undrawn1
			,ECL_Undrawn2
			,ECL_Undrawn3
			,ECL_Drawn_PostSec1
			,ECL_Drawn_PostSec2
			,ECL_Drawn_PostSec3
			,ECL_Post_Adj_Drawn1
			,ECL_Post_Adj_Drawn2
			,ECL_Post_Adj_Drawn3
			,ECL_Post_Adj_Undrawn1
			,ECL_Post_Adj_Undrawn2
			,ECL_Post_Adj_Undrawn3
			,ECL_Post_Adj_Drawn_PostSec1
			,ECL_Post_Adj_Drawn_PostSec2
			,ECL_Post_Adj_Drawn_PostSec3
			,Transit_Number
			,Product_Id
			,EGL_Department_Id 
			,datetime() format datetime25. as INSRT_PROCESS_TMSTMP
			,datetime() format datetime25. as UPDT_PROCESS_TMSTMP
		from ccar_output;
	quit;



	proc sql;
	connect using nzrrap as nzcon;
	execute(delete from &net_db..RRAP_CCAR_ACAP_FILES where mth_tm_id=&mth_tm_id.; commit;) by nzcon;
	quit;

	proc append base=nzrrap.RRAP_CCAR_ACAP_FILES(bulkload=yes bl_method=cliload) data=rrap_ccar_output force nowarn; run;
%end;

DATA _NULL_;
	CALL SYSTEM("openssl dgst -sha256 &outlanding./cmf/outgoing/CCAR_ACAP/&dsn..csv | sed -e 's/^.*= //g' >> &outlanding./cmf/outgoing/CCAR_ACAP/&dsn._chk.ctl");
run;
	/*
	proc export data = &lib..&dsname  outfile="&outlanding./&dsn..csv"
		dbms=csv replace;
	run;
*/
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



/**** To create LOOKUP flat files ***/
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

	proc export data = &lib..&dsname  outfile="&outlanding./cmf/outgoing/CCAR_ACAP/&dsn..csv" dbms=csv replace;
	run;

	DATA _NULL_;
		CALL SYSTEM("openssl dgst -sha256 &outlanding./cmf/outgoing/CCAR_ACAP/&dsn..csv | sed -e 's/^.*= //g' >> &outlanding./cmf/outgoing/CCAR_ACAP/&dsn._chk.ctl");
	run;

	/*x 'sed s/\"//g &outlanding./&dsn.&YYYYMMDD..csv';*/
%mend exportcsv;

%exportcsv(dsn=DRL-AIRB-Product-Lookup);
%exportcsv(dsn=DRL-AIRB-Guarantee-Lookup);
%mend CCAR_LOOKUP_FILES_GENERATE;

%CCAR_LOOKUP_FILES_GENERATE(WITHZERONET=);
