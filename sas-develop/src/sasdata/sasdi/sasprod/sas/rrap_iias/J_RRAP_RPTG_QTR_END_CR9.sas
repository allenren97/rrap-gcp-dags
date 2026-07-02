/* RRMSS-3141 - Kalind Patel - Convert CR9 code to read from IIAS Instrument fact instead of DB2 */
/* RRMSS-3142 - CR9 Q4 2024 Change requirements (STORY TICKET) */

options mprint compress=yes;

%rrap_autoexec;

%let cr9path=&rrap_dir./data/rrap/reporting/cr9;
libname cr9 "&cr9path.";


%let rundate=&sysdate9.;

data _null_;
     call symputx('_rundate',put(intnx('QTR.2',"&rundate."d,-1,'e'),date9.));
run;

%put ******* Generating the CR9 report for Quarter Ending &_rundate.;


%let pd_scale_transn=
case 
           when actual_final_rto >= 000.00/100 and actual_final_rto < 000.15/100 then '0 to 0.15'
           when actual_final_rto >= 000.15/100 and actual_final_rto < 000.25/100 then '0.15 to 0.25'
           when actual_final_rto >= 000.25/100 and actual_final_rto < 000.50/100 then '0.25 to 0.50'
           when actual_final_rto >= 000.50/100 and actual_final_rto < 000.75/100 then '0.50 to 0.75'
           when actual_final_rto >= 000.75/100 and actual_final_rto < 002.50/100 then '0.75 to 2.50'
           when actual_final_rto >= 002.50/100 and actual_final_rto < 010.00/100 then '2.50 to 10.00'
           when actual_final_rto >= 010.00/100 and actual_final_rto < 100.00/100 then '10.00 to 100.00'
           when actual_final_rto = 100.00/100 then 'DEFAULT'
end;

%let pd_scale=
case 
           when PD_FLRD_RPTG_RTO>= 000.00/100 and PD_FLRD_RPTG_RTO< 000.15/100 then '0 to 0.15'
           when PD_FLRD_RPTG_RTO>= 000.15/100 and PD_FLRD_RPTG_RTO< 000.25/100 then '0.15 to 0.25'
           when PD_FLRD_RPTG_RTO>= 000.25/100 and PD_FLRD_RPTG_RTO< 000.50/100 then '0.25 to 0.50'
           when PD_FLRD_RPTG_RTO>= 000.50/100 and PD_FLRD_RPTG_RTO< 000.75/100 then '0.50 to 0.75'
           when PD_FLRD_RPTG_RTO>= 000.75/100 and PD_FLRD_RPTG_RTO< 002.50/100 then '0.75 to 2.50'
           when PD_FLRD_RPTG_RTO>= 002.50/100 and PD_FLRD_RPTG_RTO< 010.00/100 then '2.50 to 10.00'
           when PD_FLRD_RPTG_RTO>= 010.00/100 and PD_FLRD_RPTG_RTO< 100.00/100 then '10.00 to 100.00'
           when PD_FLRD_RPTG_RTO= 100.00/100 then 'DEFAULT'

end;

%let bcar_sched=
case 
     when bcar_sched_num in ('30','31') and SCRTY_TP_DESC='Insured' then '30/31 Insured'
     when bcar_sched_num in ('30','31') and SCRTY_TP_DESC <> 'Insured' then '30/31 Uninsured'
     else bcar_sched_num
end;


data _null_;
     
     call symputx('_rundate', (put(intnx('month', "&_rundate"d, 0, 'e'), date9.)));
     call symputx('nz_rundate', (put(intnx('month', "&_rundate"d, 0, 'e'), yymmdd10.)));

     call symputx("min_12_mon_period", (put(intnx('month', "&_rundate"d, -12, 'e'), date9.)));
     call symputx("nz_min_12_mon_period", (put(intnx('month', "&_rundate"d, -12, 'e'), yymmdd10.)));

     declare hash h0(dataset: "nzrrap.TM_DIM(keep=TM_LVL_END_DT TM_ID TM_LVL where=(TM_LVL='Month')"); 
           h0.defineKey("TM_LVL_END_DT");
           h0.defineData("TM_ID","TM_LVL_END_DT");
           h0.defineDone();
     call missing (TM_LVL_END_DT, TM_ID); 

     rc=h0.find(key:"&_rundate."d);

     call symputx('mth_tm_id',tm_id);
     call symputx('yymm',"'"||put(tm_lvl_end_dt,yymmn4.)||"'");
     call symputx('yyyymm',"'"||put(tm_lvl_end_dt,yymmn6.)||"'");

     rc1=h0.find(key:(intnx('month', "&_rundate"d, -12, 'e')));

     call symputx('mth_tm_id_12',tm_id);

run;

%put NOTE: mth_tm_id              = &mth_tm_id.;
%put NOTE: _rundate               = &_rundate.;
%put NOTE: nz_rundate             = &nz_rundate.;

%put NOTE: min_12_mon_period      = &min_12_mon_period.;
%put NOTE: nz_min_12_mon_period   = &nz_min_12_mon_period.;
%put NOTE: mth_tm_id_12           = &mth_tm_id_12.;

%put NOTE: yymm                   = &yymm.;
%put NOTE: yyyymm                 = &yyyymm.;

/******************************************************************************/
/*                           DERIVE COLUMNS                                   */
/******************************************************************************/
     
/* As per the requirement of CR9 */
/* New code for calcualtion of WEIGHTED_AVG_PD, ARITHMETIC_AVG_PD, OBLIGORS, AV_HIST_ANN_DEF_RT  */

/* RRMSS-3141 - Kalind Patel - Convert CR9 code to read from IIAS Instrument fact instead of DB2 */
/* RRMSS-3142 - Kalind Patel - CR9 Q4 2024 Change requirements (STORY TICKET) */

proc sql;
connect using NZRRAP as dbcon;
create table cr9.EAD_CR9 as select * from connection to dbcon(
    select     
              a.bcar_sched_num
             ,SCRTY_TP_DESC
             ,a.pd_flrd_rptg_rto
             ,a.src_sys_cd
             ,a.pd_unadjusted_rptg_rto
             ,a.EAD_FLRD_RPTG_RTO
             ,(NETEAD_DRAWN+NETEAD_UNDRAWN) AS EAD_After_CRM           
   
           from (   select TRNST_EXCLSN_F,PIT_STAT_CD,SML_BUS_F,CONSM_PRD_TREATMNT_CD,mth_tm_id,bcar_sched_num,pd_flrd_rptg_rto,src_sys_cd,pd_unadjusted_rptg_rto,EAD_FLRD_RPTG_RTO,BASEL_ACCT_ID,SCRTY_TP_DESC  
           FROM EDRTLRP1D.BASEL_ANALYTCL_BL_INSTRMNT_KS where BASEL_ACCT_ID > 0 
UNION ALL
 select TRNST_EXCLSN_F,PIT_STAT_CD,SML_BUS_F,CONSM_PRD_TREATMNT_CD,mth_tm_id,bcar_sched_num,pd_flrd_rptg_rto,src_sys_cd,pd_unadjusted_rptg_rto,EAD_FLRD_RPTG_RTO,BASEL_ACCT_ID,SCRTY_TP_DESC 
 FROM FRG_USER_DATA.BASEL_ANLYT_BL_INST_FCT_BNS_DLGD WHERE ((MORT_NUM <> '' AND BASEL_ACCT_ID IS NULL ) OR   (MORT_NUM <> '' AND BASEL_ACCT_ID !=-1))   
UNION ALL
   SELECT TRNST_EXCLSN_F,PIT_STAT_CD,SML_BUS_F,CONSM_PRD_TREATMNT_CD,mth_tm_id,bcar_sched_num,pd_flrd_rptg_rto,src_sys_cd,pd_unadjusted_rptg_rto,EAD_FLRD_RPTG_RTO,BASEL_ACCT_ID,SCRTY_TP_DESC 
   FROM FRG_USER_DATA.BASEL_ANLYT_BL_INST_FCT_TNG_DLGD  WHERE ((MORT_NUM <> '' AND BASEL_ACCT_ID IS NULL ) OR   (MORT_NUM <> '' AND BASEL_ACCT_ID !=-1))
UNION ALL
   SELECT TRNST_EXCLSN_F,PIT_STAT_CD,SML_BUS_F,CONSM_PRD_TREATMNT_CD,mth_tm_id,bcar_sched_num,pd_flrd_rptg_rto,src_sys_cd,pd_unadjusted_rptg_rto,EAD_FLRD_RPTG_RTO,BASEL_ACCT_ID,SCRTY_TP_DESC 
   FROM EDRTLRP1D.BASEL_PSNL_LN_ANL_BL_INST_FACT where BASEL_ACCT_ID > 0) a
   
   LEFT JOIN EDRTLRP1D.DT4_RT18_EST_ER_VARS b ON a.BASEL_ACCT_ID = b.BASEL_ACCT_ID AND A.MTH_TM_ID = B.MTH_TM_ID
           where a.mth_tm_id = &mth_tm_id. and a.pd_flrd_rptg_rto is not null and a.bcar_sched_num is not null and a.bcar_sched_num <> '.'
           and a.SRC_SYS_CD in ('TNG-MOR','KS','MOR','SPL') and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N' and a.PIT_STAT_CD in ('CUR','DEF') and a.TRNST_EXCLSN_F='N'
   
); quit;

/* RRMSS-3141 - Kalind Patel - Convert CR9 code to read from IIAS Instrument fact instead of DB2 */
/* RRMSS-3142 - Kalind Patel - CR9 Q4 2024 Change requirements (STORY TICKET) */
/* Code for calcualtion of WEIGHTED_AVG_PD, ARITHMETIC_AVG_PD, OBLIGORS, AV_HIST_ANN_DEF_RT  */
proc sql;
create table cr9.d_e_f2_i as
     select 
            &bcar_sched. as BCAR_SCHED
           ,&pd_scale. as pd_scale
           ,sum(PD_FLRD_RPTG_RTO*100*EAD_AFTER_CRM) / sum(EAD_AFTER_CRM) as weighted_avg_pd format=12.11
           ,sum(PD_FLRD_RPTG_RTO*100)/count(1) as arithmetic_avg_pd format=12.11
           ,count(1) as obligors format=comma12.
     from cr9.EAD_CR9
     group by 1,2
     order by 1,2;
quit;

/* RRMSS-3141 - Kalind Patel - Convert CR9 code to read from IIAS Instrument fact instead of DB2 */
/* RRMSS-3142 - Kalind Patel - CR9 Q4 2024 Change requirements (STORY TICKET) */
/* New code for calcualtion of OBLIGORS_PREV_YR */

%put "Calculating OBLIGORS_PREV_YR with the PD_FLRD_RPTG_RTO";
proc sql;
     connect using NZRRAP as dbcon;
           create table cr9.f1 as 
                select 
                      BCAR_SCHED, pd_scale, obligors_prev_yr format=comma12.
                from connection to dbcon(
                      select 
                            &bcar_sched. as BCAR_SCHED
                           ,&pd_scale. as pd_scale
                           ,count(1) as obligors_prev_yr
                from (   select TRNST_EXCLSN_F,PIT_STAT_CD,SML_BUS_F,CONSM_PRD_TREATMNT_CD,mth_tm_id,bcar_sched_num,pd_flrd_rptg_rto,src_sys_cd,BASEL_ACCT_ID,SCRTY_TP_DESC  
           FROM EDRTLRP1D.BASEL_ANALYTCL_BL_INSTRMNT_KS where BASEL_ACCT_ID > 0 
UNION ALL
 select TRNST_EXCLSN_F,PIT_STAT_CD,SML_BUS_F,CONSM_PRD_TREATMNT_CD,mth_tm_id,bcar_sched_num,pd_flrd_rptg_rto,src_sys_cd,BASEL_ACCT_ID,SCRTY_TP_DESC 
 FROM FRG_USER_DATA.BASEL_ANLYT_BL_INST_FCT_BNS_DLGD WHERE ((MORT_NUM <> '' AND BASEL_ACCT_ID IS NULL ) OR   (MORT_NUM <> '' AND BASEL_ACCT_ID !=-1))   
UNION ALL
   SELECT TRNST_EXCLSN_F,PIT_STAT_CD,SML_BUS_F,CONSM_PRD_TREATMNT_CD,mth_tm_id,bcar_sched_num,pd_flrd_rptg_rto,src_sys_cd,BASEL_ACCT_ID,SCRTY_TP_DESC 
   FROM FRG_USER_DATA.BASEL_ANLYT_BL_INST_FCT_TNG_DLGD  WHERE ((MORT_NUM <> '' AND BASEL_ACCT_ID IS NULL ) OR   (MORT_NUM <> '' AND BASEL_ACCT_ID !=-1))
UNION ALL
   SELECT TRNST_EXCLSN_F,PIT_STAT_CD,SML_BUS_F,CONSM_PRD_TREATMNT_CD,mth_tm_id,bcar_sched_num,pd_flrd_rptg_rto,src_sys_cd,BASEL_ACCT_ID,SCRTY_TP_DESC 
   FROM EDRTLRP1D.BASEL_PSNL_LN_ANL_BL_INST_FACT where BASEL_ACCT_ID > 0)
                      where mth_tm_id= &mth_tm_id_12. and pd_flrd_rptg_rto is not null and bcar_sched_num is not null and bcar_sched_num <> '.'
                      and SRC_SYS_CD in ('TNG-MOR','KS','MOR','SPL') and CONSM_PRD_TREATMNT_CD='A' and SML_BUS_F='N' and PIT_STAT_CD in ('CUR','DEF') and TRNST_EXCLSN_F='N' 
                           group by &bcar_sched. ,&pd_scale. 
                           order by &bcar_sched. ,&pd_scale.
                );
quit;

/* RRMSS-3141 - Kalind Patel - Convert CR9 code to read from IIAS Instrument fact instead of DB2 */
/* RRMSS-3142 - Kalind Patel - CR9 Q4 2024 Change requirements (STORY TICKET) */
/*****************************************************************************/
/*                           PIT STATUS                                      */
/*****************************************************************************/


/* Updated the old calcualtion code of PIT_STATUS */

/* Code for calculation of PIT_STATUS - SPL */
proc sql ;
connect using NZRRAP as dbcon;
create table cr9.pit_status_spl as select * from connection to dbcon(

select 
		 mth_tm_id
		,src_sys_cd
		,basel_acct_id
		,bcar_sched
		,pd_scale
		,PD_BASEL_SEG_ID
		,a.pd_basel_seg_id as basel_seg_id
		,status
		,process_date
		,b.final_rto 
        FROM (
with ORIGINATIONS as
(
select 
     mth_tm_id
     ,src_sys_cd
     ,basel_acct_id
     ,bcar_sched
     ,pd_scale 
	 ,PD_BASEL_SEG_ID

from 
(
     select 
           mth_tm_id
           ,src_sys_cd
           ,basel_acct_id
           ,bcar_sched_num
           ,&bcar_sched. as bcar_sched
           ,&pd_scale. as pd_scale
		   ,PD_BASEL_SEG_ID
           ,(rownumber() over (partition by basel_acct_id order by basel_acct_id, mth_tm_id)) as rn 

     from 
     EDRTLRP1D.BASEL_PSNL_LN_ANL_BL_INST_FACT 
     where mth_tm_id  <= &mth_tm_id. and mth_tm_id >= &mth_tm_id_12. and bcar_sched_num is not null and bcar_sched_num <> '.' and PD_FLRD_RPTG_RTO is not null
     and SRC_SYS_CD in ('SPL') and CONSM_PRD_TREATMNT_CD='A' and SML_BUS_F='N' and PIT_STAT_CD in ('CUR','DEF') and TRNST_EXCLSN_F='N' and BASEL_ACCT_ID > 0
)
     where rn=1
     order by mth_tm_id, basel_acct_id
)
, PIT_STATUS as 
(
   SELECT 
a.process_date,a.BASEL_ACCT_ID,a.STATUS,b.BASEL_SEG_ID AS PD_BASEL_SEG_ID,a.PD_BASEL_SEG_NUM

FROM (
     select 
            b.tm_lvl_end_dt as process_date
           ,basel_acct_id
           ,PIT_STATUS_V2 as status
		   ,a.PD_BASEL_SEG_ID
		   ,a.PD_BASEL_SEG_NUM
		   ,a.MTH_TM_ID 

           from (SELECT a.mth_tm_id,a.bcar_sched_num,a.PD_FLRD_RPTG_RTO,a.SRC_SYS_CD,a.TRNST_EXCLSN_F,a.BASEL_ACCT_ID,b.PIT_STATUS_V2,a.PD_BASEL_SEG_ID,a.PD_BASEL_SEG_NUM,a.CONSM_PRD_TREATMNT_CD,a.SML_BUS_F FROM EDRTLRP1D.BASEL_PSNL_LN_ANL_BL_INST_FACT a
LEFT JOIN EDRTLRP1D.BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2 b
ON a.basel_acct_id = b.BASEL_ACCT_ID AND a.MTH_TM_ID = b.MTH_TM_ID 
) a, EDRTLRP1D.tm_dim b
     where 
a.mth_tm_id=b.tm_id and b.tm_lvl='Month' and 
a.mth_tm_id  <= &mth_tm_id. and a.mth_tm_id >= &mth_tm_id_12. and bcar_sched_num is not null and bcar_sched_num <> '.' and PD_FLRD_RPTG_RTO is not null
     and SRC_SYS_CD in ('SPL') and CONSM_PRD_TREATMNT_CD='A' and SML_BUS_F='N' and PIT_STATUS_V2 in ('CUR','DEF') and TRNST_EXCLSN_F='N' and BASEL_ACCT_ID > 0 ) a
     LEFT JOIN EDRTLRP1D.BASEL_PNL_LN_PD_SEG_ACCT_XREF b
ON a.basel_acct_id = b.BASEL_ACCT_ID AND a.MTH_TM_ID = b.MTH_TM_ID 

)

select 
      a.mth_tm_id
     ,a.src_sys_cd
     ,a.basel_acct_id
     ,a.bcar_sched
     ,a.pd_scale
	 ,b.PD_BASEL_SEG_ID
	 ,b.PD_BASEL_SEG_NUM
     ,b.status
     ,b.process_date

from ORIGINATIONS a left join PIT_STATUS b
on a.basel_acct_id=b.basel_acct_id 
order by BCAR_SCHED, pd_scale, BASEL_ACCT_ID, process_date) a left join EDRTLRP1D.BASEL_SEG_RPTG_PARM b
    on a.pd_basel_seg_id=b.BASEL_SEG_ID where b.CRNT_F='Y'

); 
quit;

/* Code for calculation of PIT_STATUS - MOR & TNG */
proc sql ;
connect using NZRRAP as dbcon;
create table cr9.pit_status_mor_tng as select * from connection to dbcon(
SELECT * FROM (
with MOR_ORIGINATIONS as
(
select 
     mth_tm_id
     ,src_sys_cd
     ,basel_acct_id
     ,bcar_sched
     ,pd_scale 
	
	 ,PD_BASEL_SEG_NUM
	 ,PD_FLRD_RPTG_RTO

from 
(
      select 
           mth_tm_id
           ,src_sys_cd
           ,basel_acct_id
           ,bcar_sched_num
           ,&bcar_sched. as bcar_sched
           ,&pd_scale. as pd_scale
		   
		   ,PD_BASEL_SEG_NUM
		   ,PD_FLRD_RPTG_RTO 
           ,(rownumber() over (partition by basel_acct_id order by basel_acct_id, mth_tm_id)) as rn 

     FROM 
     
                     (  
select PD_BASEL_SEG_NUM,TRNST_EXCLSN_F,PIT_STAT_CD,SML_BUS_F,CONSM_PRD_TREATMNT_CD,mth_tm_id,bcar_sched_num,pd_flrd_rptg_rto,src_sys_cd,BASEL_ACCT_ID,SCRTY_TP_DESC 
 FROM FRG_USER_DATA.BASEL_ANLYT_BL_INST_FCT_BNS_DLGD WHERE ((MORT_NUM <> '' AND BASEL_ACCT_ID IS NULL ) OR   (MORT_NUM <> '' AND BASEL_ACCT_ID !=-1))   
UNION ALL
   SELECT PD_BASEL_SEG_NUM,TRNST_EXCLSN_F,PIT_STAT_CD,SML_BUS_F,CONSM_PRD_TREATMNT_CD,mth_tm_id,bcar_sched_num,pd_flrd_rptg_rto,src_sys_cd,BASEL_ACCT_ID,SCRTY_TP_DESC 
   FROM FRG_USER_DATA.BASEL_ANLYT_BL_INST_FCT_TNG_DLGD  WHERE ((MORT_NUM <> '' AND BASEL_ACCT_ID IS NULL ) OR   (MORT_NUM <> '' AND BASEL_ACCT_ID !=-1))
)
     where mth_tm_id  <= &mth_tm_id. and mth_tm_id >= &mth_tm_id_12. and bcar_sched_num is not null and bcar_sched_num <> '.' and PD_FLRD_RPTG_RTO is not null
     and SRC_SYS_CD in ('MOR') and CONSM_PRD_TREATMNT_CD='A' and SML_BUS_F='N' and PIT_STAT_CD in ('CUR','DEF') and TRNST_EXCLSN_F='N' 
)
     where rn=1
     order by mth_tm_id, basel_acct_id
)
, MOR_PIT_STATUS as 
(
 select 
            b.tm_lvl_end_dt as process_date
           ,basel_acct_id
           ,STATUS1 as status

		   ,a.PD_BASEL_SEG_NUM

           from (


SELECT a.mth_tm_id,a.bcar_sched_num,a.PD_FLRD_RPTG_RTO,a.SRC_SYS_CD,a.TRNST_EXCLSN_F,a.BASEL_ACCT_ID,b.STATUS1,a.CONSM_PRD_TREATMNT_CD,a.SML_BUS_F,a.MORT_NUM,
case when SRC_SYS_CD = 'MOR' and PD_BASEL_SEG_NUM = 98 then 98
when SRC_SYS_CD = 'MOR' and PD_BASEL_SEG_NUM <> 98 and PD_BASEL_SEG_NUM  is not NULL AND STATUS1 = 'DEF' then 99
ELSE NODE END AS PD_BASEL_SEG_NUM
FROM FRG_USER_DATA.BASEL_ANLYT_BL_INST_FCT_BNS_DLGD a
LEFT JOIN (SELECT b.TM_ID AS MTH_TM_ID ,a.* FROM  FRG_USER_DATA.SCORED_SEGMENTED_ACCTS_ANTQ a LEFT JOIN EDRTLRP1D.tm_dim b
ON a.PROCESS_DATE = b.TM_LVL_END_DT WHERE TM_LVL= 'Month') b
ON a.MORT_NUM = b.MORTGAGE_NO AND A.MTH_TM_ID = B.MTH_TM_ID
WHERE ((a.MORT_NUM <> '' AND a.BASEL_ACCT_ID IS NULL ) OR   (a.MORT_NUM <> '' AND a.BASEL_ACCT_ID !=-1))   
) a, EDRTLRP1D.tm_dim b
     where 
mth_tm_id=b.tm_id and b.tm_lvl='Month' and 
mth_tm_id  <= &mth_tm_id. and mth_tm_id >= &mth_tm_id_12. and bcar_sched_num is not null and bcar_sched_num <> '.' and PD_FLRD_RPTG_RTO is not null
     and SRC_SYS_CD in ('MOR') and CONSM_PRD_TREATMNT_CD='A' and SML_BUS_F='N' and STATUS1 in ('CUR','DEF') and TRNST_EXCLSN_F='N'
)

select 
      a.mth_tm_id
     ,a.src_sys_cd
     ,a.basel_acct_id
     ,a.bcar_sched
     ,a.pd_scale
	,case 
           when SRC_SYS_CD = 'MOR' then 'MOR PD'
	end as BASEL_SEG_NM_PS
	 ,a.PD_FLRD_RPTG_RTO
	 /* ,b.PD_BASEL_SEG_ID */
	 ,b.PD_BASEL_SEG_NUM
     ,b.status
     ,b.process_date

from MOR_ORIGINATIONS a left join MOR_PIT_STATUS b
on a.basel_acct_id=b.basel_acct_id
order by BCAR_SCHED, pd_scale, BASEL_ACCT_ID, process_date 
)

 UNION ALL

SELECT * FROM (

with TNG_ORIGINATIONS as
(
select 
     mth_tm_id
     ,src_sys_cd
     ,basel_acct_id
     ,bcar_sched
     ,pd_scale 
	 /* ,a.PD_BASEL_SEG_ID */
	 ,PD_BASEL_SEG_NUM
	 ,PD_FLRD_RPTG_RTO

from 
(
      select 
           mth_tm_id
           ,src_sys_cd
           ,basel_acct_id
           ,bcar_sched_num
           ,&bcar_sched. as bcar_sched
           ,&pd_scale. as pd_scale
		   ,PD_BASEL_SEG_NUM
		   ,PD_FLRD_RPTG_RTO 
           ,(rownumber() over (partition by basel_acct_id order by basel_acct_id, mth_tm_id)) as rn 

     FROM 
     
                     (  
    select PD_BASEL_SEG_NUM,TRNST_EXCLSN_F,PIT_STAT_CD,SML_BUS_F,CONSM_PRD_TREATMNT_CD,mth_tm_id,bcar_sched_num,pd_flrd_rptg_rto,src_sys_cd,BASEL_ACCT_ID,SCRTY_TP_DESC 
  FROM FRG_USER_DATA.BASEL_ANLYT_BL_INST_FCT_TNG_DLGD  WHERE ((MORT_NUM <> '' AND BASEL_ACCT_ID IS NULL ) OR   (MORT_NUM <> '' AND BASEL_ACCT_ID !=-1))
)
     where mth_tm_id  <= &mth_tm_id. and mth_tm_id >= &mth_tm_id_12. and bcar_sched_num is not null and bcar_sched_num <> '.' and PD_FLRD_RPTG_RTO is not null
     and SRC_SYS_CD in ('TNG-MOR') and CONSM_PRD_TREATMNT_CD='A' and SML_BUS_F='N' and PIT_STAT_CD in ('CUR','DEF') and TRNST_EXCLSN_F='N' 
)
     where rn=1
     order by mth_tm_id, basel_acct_id
)
, TNG_PIT_STATUS as 
(
     select 
            b.tm_lvl_end_dt as process_date
           ,basel_acct_id
           ,pit_stat_cd as status
		   ,a.PD_BASEL_SEG_NUM

           FROM (  
   select PD_BASEL_SEG_NUM,TRNST_EXCLSN_F,PIT_STAT_CD,SML_BUS_F,CONSM_PRD_TREATMNT_CD,mth_tm_id,bcar_sched_num,pd_flrd_rptg_rto,src_sys_cd,BASEL_ACCT_ID,SCRTY_TP_DESC 
  FROM FRG_USER_DATA.BASEL_ANLYT_BL_INST_FCT_TNG_DLGD  WHERE ((MORT_NUM <> '' AND BASEL_ACCT_ID IS NULL ) OR   (MORT_NUM <> '' AND BASEL_ACCT_ID !=-1))
)a , EDRTLRP1D.tm_dim b
           
     where 
mth_tm_id=b.tm_id and b.tm_lvl='Month' and 
mth_tm_id  <= &mth_tm_id. and mth_tm_id >= &mth_tm_id_12. and bcar_sched_num is not null and bcar_sched_num <> '.' and PD_FLRD_RPTG_RTO is not null
     and SRC_SYS_CD in ('TNG-MOR') and CONSM_PRD_TREATMNT_CD='A' and SML_BUS_F='N' and PIT_STAT_CD in ('CUR','DEF') and TRNST_EXCLSN_F='N'
)

select 
      a.mth_tm_id
     ,a.src_sys_cd
     ,a.basel_acct_id
     ,a.bcar_sched
     ,a.pd_scale
	,case 
           when SRC_SYS_CD = 'TNG-MOR' then 'TNG-MOR PD'
	end as BASEL_SEG_NM_PS
	 ,a.PD_FLRD_RPTG_RTO
	 
	 ,b.PD_BASEL_SEG_NUM
     ,b.status
     ,b.process_date

from TNG_ORIGINATIONS a left join TNG_PIT_STATUS b
on a.basel_acct_id=b.basel_acct_id

)
order by BCAR_SCHED, pd_scale, BASEL_ACCT_ID, process_date );
quit;



proc sort data=cr9.pit_status_spl;
by mth_tm_id BASEL_ACCT_ID;
run;

/* Code for actual_final_rto calculation of PIT_STATUS - SPL */

data cr9.pit_status_spl_2 (compress=no);
  set cr9.pit_status_spl;
  length actual_final_rto 8;
  format actual_final_rto 30.8;
BY mth_tm_id BASEL_ACCT_ID;
  if first.BASEL_ACCT_ID then actual_final_rto=final_rto;
retain actual_final_rto;
run;

/* Code for pd_scale calculation of PIT_STATUS - SPL */
proc sql ;
       create table cr9.pit_status_spl_3 (compress=no) as 
 select 
		 mth_tm_id
		,src_sys_cd
		,basel_acct_id
		,bcar_sched
        ,&pd_scale_transn. as pd_scale_new 
		,basel_seg_id
		,status
		,process_date
        from cr9.pit_status_spl_2
            order by BCAR_SCHED, pd_scale, BASEL_ACCT_ID, process_date;
	quit;

/* Code for basel_seg_id calculation of PIT_STATUS - MOR & TNG */
proc sql ;
       create table cr9.pit_status_mor_tng_1 (compress=no) as 
 select 
		 mth_tm_id
		,a.src_sys_cd
		,basel_acct_id
		,bcar_sched
		,b.basel_seg_id as basel_seg_id
		,PD_BASEL_SEG_NUM
		,status
		,process_date 
        from cr9.pit_status_mor_tng a left join NZRRAP.BASEL_SEG b
    on a.PD_BASEL_SEG_NUM=SEG_NUM and a.BASEL_SEG_NM_PS=b.BASEL_SEG_NM 
    order by BCAR_SCHED, BASEL_ACCT_ID, process_date;
	quit;

/* Code for actual_final_rto calculation of PIT_STATUS - MOR & TNG */     
proc sql ;
       create table cr9.pit_status_mor_tng_2 (compress=no) as 
 select 
		 mth_tm_id
		,src_sys_cd
		,basel_acct_id
		,bcar_sched 
		,a.basel_seg_id
		,PD_BASEL_SEG_NUM
		,status
		,process_date
		,b.final_rto 
        from cr9.pit_status_mor_tng_1 a left join NZRRAP.BASEL_SEG_RPTG_PARM b
    on a.BASEL_SEG_ID=b.BASEL_SEG_ID where b.CRNT_F='Y'
    order by BCAR_SCHED, BASEL_ACCT_ID, process_date;
	quit;

proc sort data=cr9.pit_status_mor_tng_2;
by mth_tm_id BASEL_ACCT_ID;
run;

/* Code for actual_final_rto calculation of PIT_STATUS - MOR & TNG */  
data cr9.pit_status_mor_tng_3 (compress=no);
  set cr9.pit_status_mor_tng_2;
  length actual_final_rto 8;
  format actual_final_rto 30.8;
BY mth_tm_id BASEL_ACCT_ID;
  if first.BASEL_ACCT_ID then actual_final_rto=final_rto;
retain actual_final_rto;
run;

/* Code for pd_scale calculation of PIT_STATUS - MOR & TNG */
proc sql ;
       create table cr9.pit_status_mor_tng_4 as 
 select 
		 mth_tm_id
		,src_sys_cd
		,basel_acct_id
		,bcar_sched
		,&pd_scale_transn. as pd_scale_new  
		,status
		,process_date		
        from cr9.pit_status_mor_tng_3    
    order by BCAR_SCHED, pd_scale_new, BASEL_ACCT_ID, process_date;
	quit;



/* Part -1  Code for calculation of PIT_STATUS - KS */
proc sql ;
    connect using NZRRAP as dbcon;
          create table cr9.pit_status_ks_part1 as select * from connection to dbcon(
        
        with ORIGINATIONS as
        (
        select 
             mth_tm_id
             ,src_sys_cd
             ,basel_acct_id
             ,bcar_sched
             ,pd_scale 
        
        from 
        (
             select 
                   mth_tm_id
                   ,src_sys_cd
                   ,basel_acct_id
                   ,bcar_sched_num
                   ,&bcar_sched. as bcar_sched
                   ,&pd_scale. as pd_scale
                   ,(rownumber() over (partition by basel_acct_id order by basel_acct_id, mth_tm_id)) as rn 
        
             from EDRTLRP1D.BASEL_ANALYTCL_BL_INSTRMNT_KS
             where mth_tm_id <= &mth_tm_id. and mth_tm_id >= &mth_tm_id_12. and bcar_sched_num is not null and bcar_sched_num <> '.' and PD_FLRD_RPTG_RTO is not null
             and SRC_SYS_CD ='KS' and CONSM_PRD_TREATMNT_CD='A' and SML_BUS_F='N' and PIT_STAT_CD in ('CUR','DEF') and TRNST_EXCLSN_F='N' and basel_acct_id > 0
     )
             where rn=1
             order by mth_tm_id, basel_acct_id
       
       )
       
       select 
       mth_tm_id
       ,src_sys_cd
       ,basel_acct_id
       ,bcar_sched
       ,pd_scale 
    
       from ORIGINATIONS 
    order by BCAR_SCHED, pd_scale, BASEL_ACCT_ID
    );
        quit;

/* Part -2  Code for calculation of PIT_STATUS - KS */
proc sql ;
       create table cr9.pit_status_ks_part2 (compress=no) as 
		select
				a.MTH_TM_ID,
				a.basel_acct_id,
               a.PIT_STAT_VER_2_CD as status,
			   b.tm_lvl_end_dt as process_date
    
               from NZRRAP.BASEL_REVLVNG_CR_BASE_DRVD_VARS a,nzrrap.tm_dim b
         where 
		   mth_tm_id=b.tm_id and b.tm_lvl='Month' and 
a.mth_tm_id <= &mth_tm_id. and a.mth_tm_id >= &mth_tm_id_12.
and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N'
     and a.PIT_STAT_VER_2_CD in ('CUR','DEF') and a.TRNST_EXCLSN_F='N'  and basel_acct_id > 0
  order by BASEL_ACCT_ID, process_date;
   quit;

proc sql ;
       create table cr9.pit_status_ks_final (compress=no) as
 select 
          a.mth_tm_id
         ,a.src_sys_cd
         ,a.basel_acct_id
         ,a.bcar_sched
         ,a.pd_scale
         ,b.status
		 ,b.process_date    
    
    from cr9.pit_status_ks_part1 a left join cr9.pit_status_ks_part2 b
    on a.basel_acct_id=b.basel_acct_id 
    order by BCAR_SCHED, pd_scale, BASEL_ACCT_ID, process_date;
	quit;

	proc sql noprint ;
  select distinct mth_tm_id into :ks_mnth_tm_ids separated by ',' from cr9.pit_status_ks_part1 ;
quit;

/* Code for calculation of PIT_STATUS - KS */
/* Used EDRTLRP1D.PD_SEG_ACCT_XREF to retrive BASEL_SEG_ID as per the requirement */

proc sql ;
    connect using NZRRAP as dbcon;
          create table cr9.temp_PD_SEG_ACCT_XREF (compress=no) as  select * from connection to dbcon(
        
        with PD_SEG_ACCT_XREF as
        (
 select 
		basel_seg_id,
		basel_acct_id,
		mth_tm_id

        from EDRTLRP1D.PD_SEG_ACCT_XREF   where mth_tm_id in (&ks_mnth_tm_ids)
       
       )
       
       select 
      	basel_seg_id,
		basel_acct_id,
		mth_tm_id
    
    from PD_SEG_ACCT_XREF 
    );
        quit;

		proc sql ;
       create table cr9.pit_status_ks_2 (compress=no) as 
 select 
		 a.mth_tm_id
		,src_sys_cd
		,a.basel_acct_id
		,bcar_sched
		,pd_scale 
		,b.basel_seg_id
		,status
		,process_date
		from cr9.pit_status_ks_final a left join cr9.temp_PD_SEG_ACCT_XREF  b
    on a.basel_acct_id=b.basel_acct_id and a.mth_tm_id=b.mth_tm_id;
	quit;

    
/* Added the following calcualtion code for calculation of PIT_STATUS specific to "KS" */
/* Used BASEL_SEG_RPTG_PARM to retrive final_rto as per the requirement */
	
proc sql ;
       create table cr9.pit_status_ks_3 (compress=no) as 
 select 
		 a.mth_tm_id
		,src_sys_cd
		,a.basel_acct_id
		,bcar_sched
		,pd_scale 
		,b.basel_seg_id
		,status
		,process_date
		,b.final_rto 
        from cr9.pit_status_ks_2 a left join NZRRAP.BASEL_SEG_RPTG_PARM b
    on a.BASEL_SEG_ID=b.BASEL_SEG_ID where b.CRNT_F='Y'
     order by BCAR_SCHED, pd_scale, BASEL_ACCT_ID, process_date;
	quit;

proc sort data=cr9.pit_status_ks_3 ;
by mth_tm_id BASEL_ACCT_ID;
run;
data cr9.pit_status_ks_3 (compress=no);
  set cr9.pit_status_ks_3;
  length actual_final_rto 8;
  format actual_final_rto 30.8;
BY mth_tm_id BASEL_ACCT_ID;
  if first.BASEL_ACCT_ID then actual_final_rto=final_rto;
retain actual_final_rto;
run;

/* Code for pd_scale calculation of PIT_STATUS - KS */
proc sql ;
       create table cr9.pit_status_ks_4 (compress=no) as 
 select 
		 mth_tm_id
		,src_sys_cd
		,basel_acct_id
		,bcar_sched
        ,&pd_scale_transn. as pd_scale
		,status
		,process_date
        from cr9.pit_status_ks_3    
    order by BCAR_SCHED, pd_scale, BASEL_ACCT_ID, process_date;
	quit;

/* Code for appending different portfolio specific PIT_STATUS tables into one for */
proc append base=cr9.pit_status_ks_4(keep=MTH_TM_ID SRC_SYS_CD BASEL_ACCT_ID BCAR_SCHED PD_SCALE status process_date) data=cr9.pit_status_mor_tng_4(rename=pd_scale_new=pd_scale) force; run;

proc append base=cr9.pit_status_ks_4 data=cr9.pit_status_spl_3(rename=pd_scale_new=pd_scale) force; run; 

proc sort data=cr9.pit_status_ks_4 out=cr9.pit_status; by BCAR_SCHED pd_scale BASEL_ACCT_ID process_date; run;

/* Craete indexes of final PIT_STATUS table for faste processing */
proc datasets library=cr9;
modify pit_status;
index create account=(BASEL_ACCT_ID process_date) / unique;
run;

/*****************************************************************************/
/*                           DEFAULTERS COUNT                                */
/*****************************************************************************/

/* Code for calculating DEF_OBLIGORS column */
data cr9.G (compress=no);
     set cr9.pit_status end=last;
     where not missing(status);
     by BCAR_SCHED pd_scale BASEL_ACCT_ID process_date;
     retain def_flg already_def_flg DEF_acnt def_cnt;
     if first.pd_scale then def_cnt=0;
     if first.BASEL_ACCT_ID then do;
           def_flg=0; def_acnt=0;already_def_flg=0; 
     end;
     if status = 'DEF'  then def_flg=1;
     if first.BASEL_ACCT_ID and status='DEF' then already_def_flg=1;
     if last.BASEL_ACCT_ID and def_flg=1 and already_def_flg=0 then DEF_acnt=1;
     def_cnt+def_acnt;
     if last.pd_scale;
     keep BCAR_SCHED pd_scale def_cnt;
run;

/* Code for calculating NEW_DEF_OBLIGORS column */
proc sql ;
connect using NZRRAP as dbcon;
create table pit_status_new_obg (compress=no) as select * from connection to dbcon(
select 
     basel_acct_id,
	 NEW_OBG
from 
(    select 
           basel_acct_id,
		   case when basel_acct_id > 0 then 1 else 0 end as NEW_OBG
                from          
                (   select mth_tm_id,src_sys_cd,pd_unadjusted_rptg_rto,BASEL_ACCT_ID  
           FROM EDRTLRP1D.BASEL_ANALYTCL_BL_INSTRMNT_KS where BASEL_ACCT_ID > 0 
UNION ALL
 select mth_tm_id,src_sys_cd,pd_unadjusted_rptg_rto,BASEL_ACCT_ID 
 FROM FRG_USER_DATA.BASEL_ANLYT_BL_INST_FCT_BNS_DLGD WHERE ((MORT_NUM <> '' AND BASEL_ACCT_ID IS NULL ) OR   (MORT_NUM <> '' AND BASEL_ACCT_ID !=-1))   
UNION ALL
   SELECT mth_tm_id,src_sys_cd,pd_unadjusted_rptg_rto,BASEL_ACCT_ID 
   FROM FRG_USER_DATA.BASEL_ANLYT_BL_INST_FCT_TNG_DLGD  WHERE ((MORT_NUM <> '' AND BASEL_ACCT_ID IS NULL ) OR   (MORT_NUM <> '' AND BASEL_ACCT_ID !=-1))
UNION ALL
   SELECT mth_tm_id,src_sys_cd,pd_unadjusted_rptg_rto,BASEL_ACCT_ID 
   FROM EDRTLRP1D.BASEL_PSNL_LN_ANL_BL_INST_FACT where BASEL_ACCT_ID > 0)
     where mth_tm_id  <= &mth_tm_id. and mth_tm_id >= &mth_tm_id_12. and SRC_SYS_CD in ('TNG-MOR','KS','MOR','SPL')
	 group by basel_acct_id
having max(mth_tm_id=&mth_tm_id_12.) = 0 
) order by basel_acct_id); 
quit;

PROC SQL;
create table CALC_ACCT_ODT (compress=no) as select a.*,b.NEW_OBG from cr9.pit_status a left join pit_status_new_obg b on a.basel_acct_id=b.basel_acct_id
order by BCAR_SCHED, pd_scale, BASEL_ACCT_ID, process_date;
quit;

data cr9.H (compress=no);
     set CALC_ACCT_ODT end=last;
     where not missing(status) and NEW_OBG =1;
     by BCAR_SCHED pd_scale BASEL_ACCT_ID process_date;
     retain def_flg already_def_flg DEF_acnt def_cnt;
     if first.pd_scale then def_cnt=0;
     if first.BASEL_ACCT_ID then do;
           def_flg=0; def_acnt=0;already_def_flg=0; 
     end;
     if status = 'DEF'  then def_flg=1;
     if first.BASEL_ACCT_ID and status='DEF' then already_def_flg=1;
     if last.BASEL_ACCT_ID and def_flg=1 and already_def_flg=0 then DEF_acnt=1;
     def_cnt+def_acnt;
     if last.pd_scale;
     keep BCAR_SCHED pd_scale def_cnt;
run;

/* Code to generate report template */
data CR9template;
input BCAR_SCHED $ 1-15 PD_SCALE $ 16-31;
datalines4;
30/31 Insured   0 to 0.15
30/31 Insured   0.15 to 0.25
30/31 Insured   0.25 to 0.50
30/31 Insured   0.50 to 0.75
30/31 Insured   0.75 to 2.50
30/31 Insured   2.50 to 10.00
30/31 Insured   10.00 to 100.00
30/31 Insured   DEFAULT
30/31 Uninsured 0 to 0.15
30/31 Uninsured 0.15 to 0.25
30/31 Uninsured 0.25 to 0.50
30/31 Uninsured 0.50 to 0.75
30/31 Uninsured 0.75 to 2.50
30/31 Uninsured 2.50 to 10.00
30/31 Uninsured 10.00 to 100.00
30/31 Uninsured DEFAULT
32              0 to 0.15
32              0.15 to 0.25
32              0.25 to 0.50
32              0.50 to 0.75
32              0.75 to 2.50
32              2.50 to 10.00
32              10.00 to 100.00
32              DEFAULT
33              0 to 0.15
33              0.15 to 0.25
33              0.25 to 0.50
33              0.50 to 0.75
33              0.75 to 2.50
33              2.50 to 10.00
33              10.00 to 100.00
33              DEFAULT
;;;;
run;
proc sort data=CR9template; by BCAR_SCHED pd_scale; run;

/* Code to create final data for report */
proc sql noprint;
	select FNCL_YR into :CURR_FNCL_YR
	from nzrrap.tm_dim
	where tm_lvl='Month' and tm_id = &mth_tm_id.;
quit;
%put Current Finanicial Year is &CURR_FNCL_YR;

data CR9_temp;
retain BCAR_SCHED PD_SCALE WEIGHTED_AVG_PD ARITHMETIC_AVG_PD OBLIGORS_PREV_YR OBLIGORS DEF_OBLIGORS NEW_DEF_OBLIGORS;
merge CR9template cr9.G(rename=(def_cnt=DEF_OBLIGORS)) cr9.H(rename=(def_cnt=NEW_DEF_OBLIGORS)) cr9.d_e_f2_i cr9.f1;
by BCAR_SCHED pd_scale;
           if pd_scale='DEFAULT' then do;
                DEF_OBLIGORS=0; NEW_DEF_OBLIGORS=0; OBLIGORS_PREV_YR=0; OBLIGORS=0;
           end;
if pd_scale = '0 to 0.15' then order=1;
else if pd_scale = '0.15 to 0.25' then order=2;
else if pd_scale = '0.25 to 0.50'  then order=3;
else if pd_scale = '0.50 to 0.75' then order=4;
else if pd_scale = '0.75 to 2.50' then order=5;
else if pd_scale = '10.00 to 100.00' then order=7;
else if pd_scale = '2.50 to 10.00' then order=6;
else if pd_scale = 'DEFAULT' then order=8;
annual_default_rate=(DEF_OBLIGORS-NEW_DEF_OBLIGORS)/OBLIGORS_PREV_YR;
hist_year=&CURR_FNCL_YR.;
run;

/* CR9.CR9_HISTORY table will be used to calculate AV_HIST_ANN_DEF_RT using annual_default_rate from 2019 till current year */

PROC SQL;
delete from CR9.CR9_HISTORY where hist_year=&CURR_FNCL_YR;
insert into CR9.CR9_HISTORY select * from CR9_temp;
quit;


/* RRMSS-3142 - CR9 Q4 2024 Change requirements (STORY TICKET) */
PROC SQL;
CREATE TABLE cr9_avg_hist_temp as 
select BCAR_SCHED,PD_SCALE, avg(annual_default_rate)*100 as  AV_HIST_ANN_DEF_RT from CR9.CR9_HISTORY group by BCAR_SCHED,PD_SCALE;
quit;

data CR9.CR9;
merge CR9_temp cr9_avg_hist_temp;
if pd_scale='DEFAULT' then AV_HIST_ANN_DEF_RT=100;
array chkmissing _numeric_;
        do over chkmissing;
            if chkmissing=. then chkmissing=0;
        end;
run;

proc sort data=CR9.CR9 out=CR9.CR9(drop=order hist_year annual_default_rate); by bcar_sched order; run;

/* Generate report in CSV format */
proc export data=CR9.CR9 dbms=csv outfile="&cr9path./cr9_&_rundate..csv" replace; run;

/* Delete temp data */
proc delete data=cr9.pit_status_mor_tng_test cr9.pit_status_spl cr9.pit_status_mor_tng cr9.pit_status_spl_2 
cr9.pit_status_spl_3 cr9.pit_status_spl_4 cr9.pit_status_mor_tng_1 cr9.pit_status_mor_tng_2 cr9.pit_status_mor_tng_3 cr9.pit_status_mor_tng_4 
cr9.temp_pd_seg_acct_xref cr9.pit_status_ks_2 cr9.pit_status_ks_3 cr9.pit_status_ks_4;
run;