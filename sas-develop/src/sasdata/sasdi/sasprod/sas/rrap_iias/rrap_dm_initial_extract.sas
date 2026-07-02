/*
		2022/1/30 Ganesh Patro: RRMSS-1597 - Change schema/table from DB2  to IIAS 
		for  EDRRAPT.BASEL_NCR_PD_BAND_DIM --> EDRTLRP1D.PD_BAND_DIM_NCR

*/

%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

%let datetime_start = %sysfunc(TIME()) ;
%put >>> START TIME: %sysfunc(datetime(),datetime14.);

options mprint source2 symbolgen;
%PUT _GLOBAL_;
%LET p_mth_tm_id=&MTH_TM_ID;
%LET LIB=DRAPT;
%LET DIM_NOT=;

PROC DATASETS LIB=WORK KILL NOLIST;
RUN;
QUIT;


data &LIB..BASEL_SCRTY_TP_LKP; set &DBNAME..BASEL_SCRTY_TP_LKP;;run; 

OPTIONS THREADS;
options compress =y;


data &lib..BASEL_ANALYTCL_BL_INSTRMNT_FACT;
set &DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT;
where mth_tm_id = &MTH_TM_ID. and (not missing(pd_basel_seg_num) and not missing(lgd_basel_seg_num));
RUN;

PROC SQL noprint THREADS ;
connect USING EDRRAPT as dbcon ;
create table &LIB..BASEL_ANALYTCL_BL as 
 select     *
 FROM connection to dbcon(
select 
MTH_TM_ID ,SRC_SYS_CD,PD_FINAL_RPTG_RTO,UTLTN_RTO,LOAN_TO_VAL_RTO,NCR_DLQNT_BCKT_KEY_VAL,
CONSM_PRD_TREATMNT_CD, PIT_STAT_CD, SML_BUS_F, 
       TRNST_EXCLSN_F
,NCR_EXPSR_CL_KEY_VAL,NCR_EXPSR_SIZE_KEY_VAL,NCR_GEO_KEY_VAL,NCR_LTV_KEY_VAL,NCR_PD_BAND_KEY_VAL,NCR_RT_KEY_VAL,NCR_RT_SYS_KEY_VAL
,sum(ADJUSTED_OS_BAL_AMT) as ADJUSTED_OS_BAL_AMT
,sum(UNADJUSTED_ADD_ON_BAL_AMT) as UNADJUSTED_ADD_ON_BAL_AMT
,count(distinct BASEL_ACCT_ID) as BASEL_ACCT_ID
,count(*) as BL_ROW_CNT
,sum(OS_BAL_AMT) as OS_BAL_AMT,sum(AUTH_AMT) as AUTH_AMT
from edrrapt.BASEL_ANALYTCL_BL_INSTRMNT_FACT
WHERE CONSM_PRD_TREATMNT_CD = 'A' AND
SML_BUS_F = 'N' AND
PIT_STAT_CD IN ('CUR', 'DEF') AND
TRNST_EXCLSN_F = 'N' AND
MTH_TM_ID= &MTH_TM_ID.
and (pd_basel_seg_num is not null and lgd_basel_seg_num is not null)
group by 
MTH_TM_ID ,SRC_SYS_CD,PD_FINAL_RPTG_RTO,UTLTN_RTO,LOAN_TO_VAL_RTO,NCR_DLQNT_BCKT_KEY_VAL,
CONSM_PRD_TREATMNT_CD, PIT_STAT_CD, SML_BUS_F, 
       TRNST_EXCLSN_F
,NCR_EXPSR_CL_KEY_VAL,NCR_EXPSR_SIZE_KEY_VAL,NCR_GEO_KEY_VAL,NCR_LTV_KEY_VAL,NCR_PD_BAND_KEY_VAL,NCR_RT_KEY_VAL,NCR_RT_SYS_KEY_VAL
);
DISCONNECT FROM DBCON;
quit;


proc datasets nolist library=&LIB.;
      modify BASEL_ANALYTCL_BL;
     index create MTH_TM_ID;
      index create CONSM_PRD_TREATMNT_CD;
      index create NCR_EXPSR_CL_KEY_VAL;
      index create PIT_STAT_CD;
      index create SML_BUS_F;
index create TRNST_EXCLSN_F;

quit;

proc datasets nolist library=&LIB.;
      modify BASEL_ANALYTCL_BL_INSTRMNT_FACT;
     index create MTH_TM_ID;
      index create CONSM_PRD_TREATMNT_CD;
      index create NCR_EXPSR_CL_KEY_VAL;
      index create PIT_STAT_CD;
      index create SML_BUS_F;
index create TRNST_EXCLSN_F;

quit;



data &LIB..BASEL_REVLVNG_CR_RPTG_DRVD_VAR; 
set &DBNAME..BASEL_REVLVNG_CR_RPTG_DRVD_VAR (BULKLOAD=YES BL_METHOD=CLILOAD); 
where mth_tm_id =&MTH_TM_ID.; run;

data &LIB..BASEL_SUBSIDIARY_ACL_LKP; set &DBNAME..BASEL_SUBSIDIARY_ACL_LKP;run;
data &LIB..BASEL_CCAR_BUS_AGGRTD_FACT; set &DBNAME..BASEL_CCAR_BUS_AGGRTD_FACT (BULKLOAD=YES BL_METHOD=CLILOAD);  where mth_tm_id =&MTH_TM_ID.; run;
data &LIB..BASEL_RPTG_BL_AGGRTD_NCR_BD_FACT; set &DBNAME..BASEL_RPTG_BL_AGGRTD_NCR_BD_FACT (BULKLOAD=YES BL_METHOD=CLILOAD);  where mth_tm_id =&MTH_TM_ID.; run;
data &LIB..BASEL_RPTG_BL_AGGRTD_NCR_BE_FACT; set &DBNAME..BASEL_RPTG_BL_AGGRTD_NCR_BE_FACT (BULKLOAD=YES BL_METHOD=CLILOAD);  where mth_tm_id =&MTH_TM_ID.; run;
data &LIB..TM_DIM; set EDRTLRT.TM_DIM; run;
data &LIB..BASEL_NCR_BD_RECD_TP_DIM; set &DBNAME..BASEL_NCR_BD_RECD_TP_DIM ; run;
data &LIB..BASEL_NCR_BUS_AGGRTD_FACT; set &DBNAME..BASEL_NCR_BUS_AGGRTD_FACT (BULKLOAD=YES BL_METHOD=CLILOAD);  where mth_tm_id =&MTH_TM_ID.; run;
data &LIB..BASEL_NCR_EAD_SEG_DIM; set &DBNAME..BASEL_NCR_EAD_SEG_DIM; run;
data &LIB..BASEL_NCR_EXPSR_SIZE_DIM; set &DBNAME..BASEL_NCR_EXPSR_SIZE_DIM; run;
data &LIB..Basel_expsr_cl_dim; set &DBNAME..Basel_expsr_cl_dim; run;
data &LIB..Dlqnt_dim; set &DBNAME..Dlqnt_dim; run;
data &LIB..BASEL_NCR_GEO_DIM; set &DBNAME..BASEL_NCR_GEO_DIM; run;
data &LIB..BASEL_NCR_HIERARCHY_LKP; set &DBNAME..BASEL_NCR_HIERARCHY_LKP; run;
data &LIB..BASEL_NCR_LGD_SEG_DIM; set &DBNAME..BASEL_NCR_LGD_SEG_DIM; run;
data &LIB..BASEL_NCR_LTV_DIM; set &DBNAME..BASEL_NCR_LTV_DIM; run;
data &LIB..PD_BAND_DIM; set NZRRAP.PD_BAND_DIM_NCR (where=(CRNT_F='Y')); run; /*RRMSS-1597 Repointing to IIAS*/
data &LIB..BASEL_NCR_PD_SEG_DIM; set &DBNAME..BASEL_NCR_PD_SEG_DIM; run;
data &LIB..BASEL_NCR_RISK_RT_SYS_DIM; set &DBNAME..BASEL_NCR_RISK_RT_SYS_DIM; run;
data &LIB..BASEL_NCR_RT_DIM; set &DBNAME..BASEL_NCR_RT_DIM; run;
data &LIB..BASEL_NCR_SECRTZTN_DIM; set &DBNAME..BASEL_NCR_SECRTZTN_DIM; run;
%put >>> END TIME: %sysfunc(datetime(),datetime14.);
%put >>> Benchmarking PROCESSING TIME:  %sysfunc(putn(%sysevalf(%sysfunc(TIME())-&datetime_start.),mmss.)) (mm:ss) ;
