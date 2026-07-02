UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*10510*/
select distinct CCAR_BASEL_PRD_TP_NM LOG_NM ,
0 LOG_CRNT_VAL,
0 LOG_BASLN_VAL ,
0 LOG_VARNC_VAL,
  I.CCAR_BASEL_PRD_TP_NM
  ||''~''|| nvl(I.src_sys_cd, '' '')
 ||''~''|| nvl(I.PD_UNADJUSTED_RPTG_RTO, ''0'')
 ||''~''|| nvl(I.PD_FINAL_RPTG_RTO, ''0'') LOG_CMB_RSLT
 from EDRTLRP1D.BASEL_ANALYTCL_BL_INSTRMNT_FACT I
 where MTH_TM_ID = &mth_tm_id and CONSM_PRD_TREATMNT_CD = ''A''  and SML_BUS_F = ''N'' 
and PIT_STAT_CD in (''CUR'',''DEF'') and TRNST_EXCLSN_F = ''N'' 
order by 1'
WHERE AUDIT_CNFGRTN_ID=10510;





UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*10530*/
select distinct
  CCAR_BASEL_PRD_TP_NM LOG_NM
     ,EAD_FINAL_RPTG_RTO LOG_CRNT_VAL
     ,LGD_FINAL_RPTG_RTO LOG_BASLN_VAL
     ,0  LOG_VARNC_VAL
     ,NVL(EAD_MODEL_NM, '' '')||''~''||NVL(LGD_MODEL_NM,'' '') LOG_CMB_RSLT
from EDRTLRP1D.BASEL_ANALYTCL_BL_INSTRMNT_FACT
where mth_tm_id = &mth_tm_id
and CONSM_PRD_TREATMNT_CD = ''A''  and SML_BUS_F = ''N'' 
and PIT_STAT_CD in (''CUR'',''DEF'') and TRNST_EXCLSN_F = ''N''
order by 1'
WHERE AUDIT_CNFGRTN_ID=10530;





UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*18140*/
select Product LOG_NM
, 0 LOG_CRNT_VAL
, 0 LOG_BASLN_VAL
, cnt LOG_VARNC_VAL 
, cast(rnk  as varchar (50)) LOG_CMB_RSLT
from (
/* --1*/
  select F.CCAR_BASEL_PRD_TP_NM Product, count(1) cnt, rank() over (order by count(1) DESC ) Rnk
  from EDRTLRP1D.BASEL_ANALYTCL_BL_INSTRMNT_FACT F
  left outer join EDRTLRP1D.BASEL_RPTG_PRD_LKP L 
     on rtrim(L.CCAR_BASEL_PRD_TP_NM) = substr(F.CCAR_BASEL_PRD_TP_NM,1,length(F.CCAR_BASEL_PRD_TP_NM)-9) AND
     L.PRD_ID=F.PRD_ID
  where F.MTH_TM_ID = &mth_tm_id
     and L.CCAR_BASEL_PRD_TP_NM is null
  group by F.CCAR_BASEL_PRD_TP_NM
/* --1*/
) R'
WHERE AUDIT_CNFGRTN_ID=18140;





UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*19130*/
select 
  Product LOG_NM
  ,Curr  LOG_CRNT_VAL
 ,Base  LOG_BASLN_VAL
  ,ABS((Curr - Base)*100)/cast(Base as float) LOG_VARNC_VAL
 ,''''  LOG_CMB_RSLT
 from ( 
 /*-- 2*/
  select Product
   , sum(case when Value_Type=''Base_Value'' then AFTR_OS_BAL_AMT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then AFTR_OS_BAL_AMT else 0 end) Curr 
  from ( 
  /*-- 1*/
   SELECT  
     substr(CCAR_BASEL_PRD_TP_NM,1,Length(CCAR_BASEL_PRD_TP_NM)-9) Product
     , MTH_TM_ID
     , SUM(CASE WHEN (ADJUSTED_OS_BAL_AMT/1000000 ) <= 0 THEN 0 
                ELSE (ADJUSTED_OS_BAL_AMT/1000000 ) END) AS AFTR_OS_BAL_AMT
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End Value_Type
   from EDRTLRP1D.BASEL_ANALYTCL_BL_INSTRMNT_FACT
   where MTH_TM_ID in ( &mth_tm_id, &mth_tm_id-40) and CONSM_PRD_TREATMNT_CD = ''A''  and SML_BUS_F = ''N'' 
                  and PIT_STAT_CD in (''CUR'',''DEF'') and TRNST_EXCLSN_F = ''N''
          and src_sys_cd = ''MOR''
   group by CCAR_BASEL_PRD_TP_NM,MTH_TM_ID
   order by 1,2
  /*-- 1*/
  ) A 
  group by Product 
  order by 1
 /*-- 2*/
) p'
WHERE AUDIT_CNFGRTN_ID=19130;





UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*19140*/
select 
  Product LOG_NM
  ,Curr  LOG_CRNT_VAL
 ,Base  LOG_BASLN_VAL
  ,ABS((Curr - Base)*100)/cast(Base as float) LOG_VARNC_VAL
 ,''''  LOG_CMB_RSLT
 from ( 
 /*-- 2*/
  select Product
   , sum(case when Value_Type=''Base_Value'' then AFTR_OS_BAL_AMT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then AFTR_OS_BAL_AMT else 0 end) Curr 
  from ( 
  /*-- 1*/
   SELECT  
     substr(CCAR_BASEL_PRD_TP_NM,1,Length(CCAR_BASEL_PRD_TP_NM)-9) Product
     , MTH_TM_ID
     , SUM(CASE WHEN (ADJUSTED_OS_BAL_AMT/1000000 ) <= 0 THEN 0 
                ELSE (ADJUSTED_OS_BAL_AMT/1000000 ) END) AS AFTR_OS_BAL_AMT
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End Value_Type
   from EDRTLRP1D.BASEL_ANALYTCL_BL_INSTRMNT_FACT
   where MTH_TM_ID in ( &mth_tm_id, &mth_tm_id-40) and CONSM_PRD_TREATMNT_CD = ''A''  and SML_BUS_F = ''N'' 
                  and PIT_STAT_CD in (''CUR'',''DEF'') and TRNST_EXCLSN_F = ''N''
          and src_sys_cd = ''TNG-MOR''
   group by CCAR_BASEL_PRD_TP_NM,MTH_TM_ID
   order by 1,2
  /*-- 1*/
  ) A 
  group by Product 
  order by 1
 /*-- 2*/
) p'
WHERE AUDIT_CNFGRTN_ID=19140;





UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*19150*/
select 
  Product LOG_NM
  ,Curr  LOG_CRNT_VAL
 ,Base  LOG_BASLN_VAL
  ,ABS((Curr - Base)*100)/cast(Base as float) LOG_VARNC_VAL
 ,''''  LOG_CMB_RSLT
 from ( 
 /*-- 2*/
  select Product
   , sum(case when Value_Type=''Base_Value'' then AFTR_OS_BAL_AMT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then AFTR_OS_BAL_AMT else 0 end) Curr 
  from ( 
 /*-- 1*/
   SELECT  
     substr(CCAR_BASEL_PRD_TP_NM,1,Length(CCAR_BASEL_PRD_TP_NM)-9) Product
     , MTH_TM_ID
     , SUM(CASE WHEN (ADJUSTED_OS_BAL_AMT/1000000 ) <= 0 THEN 0 
                ELSE (ADJUSTED_OS_BAL_AMT/1000000 ) END) AS AFTR_OS_BAL_AMT
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End Value_Type
   from EDRTLRP1D.BASEL_ANALYTCL_BL_INSTRMNT_FACT
   where MTH_TM_ID in ( &mth_tm_id, &mth_tm_id-40) and CONSM_PRD_TREATMNT_CD = ''A''  and SML_BUS_F = ''N'' 
                  and PIT_STAT_CD in (''CUR'',''DEF'') and TRNST_EXCLSN_F = ''N''
          and src_sys_cd = ''SPL''
   group by CCAR_BASEL_PRD_TP_NM,MTH_TM_ID
   order by 1,2
  /*-- 1*/
  ) A 
  group by Product 
  order by 1
 /*-- 2*/
) p'
WHERE AUDIT_CNFGRTN_ID=19150;





UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*19160*/
select 
  Product LOG_NM
  ,Curr  LOG_CRNT_VAL
 ,Base  LOG_BASLN_VAL
  ,ABS((Curr - Base)*100)/cast(Base as float) LOG_VARNC_VAL
 ,''''  LOG_CMB_RSLT
 from ( 
 /*-- 2*/
  select Product
   , sum(case when Value_Type=''Base_Value'' then AFTR_OS_BAL_AMT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then AFTR_OS_BAL_AMT else 0 end) Curr 
  from ( 
 /*-- 1*/
   SELECT  
     substr(CCAR_BASEL_PRD_TP_NM,1,Length(CCAR_BASEL_PRD_TP_NM)-9) Product
     , MTH_TM_ID
     , SUM(CASE WHEN (ADJUSTED_OS_BAL_AMT/1000000 ) <= 0 THEN 0 
                ELSE (ADJUSTED_OS_BAL_AMT/1000000 ) END) AS AFTR_OS_BAL_AMT
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End Value_Type
   from EDRTLRP1D.BASEL_ANALYTCL_BL_INSTRMNT_FACT
   where MTH_TM_ID in ( &mth_tm_id, &mth_tm_id-40) and CONSM_PRD_TREATMNT_CD = ''A''  and SML_BUS_F = ''N'' 
                  and PIT_STAT_CD in (''CUR'',''DEF'') and TRNST_EXCLSN_F = ''N''
          and src_sys_cd = ''KS''
   group by CCAR_BASEL_PRD_TP_NM,MTH_TM_ID
   order by 1,2
  /*-- 1*/
  ) A 
  group by Product 
  order by 1
 /*-- 2*/
) p'
WHERE AUDIT_CNFGRTN_ID=19160;





