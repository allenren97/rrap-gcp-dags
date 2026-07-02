/************ BASEL III CHANGES ****************************************/
/* SQL CHANGES FOR BASEL  III IIAS  */
/* Created by: N Grewal */
/* Last Modified: Jan 31, 2023 */
/*********************************************************************/


/***** RRMSS-1943 - Update truncated records in PSNL_LOAN_RPTG_PRD_LKP *****/
UPDATE EDRTLRP1D.PSNL_LOAN_RPTG_PRD_LKP SET BASEL_PRD_TP_CD = 'ITL LEISURE', UPDT_PROCESS_TMSTMP = CURRENT TIMESTAMP WHERE PRD_ID = 'S12'; 
UPDATE EDRTLRP1D.PSNL_LOAN_RPTG_PRD_LKP SET BASEL_PRD_TP_CD = 'ITL AUTO RS', UPDT_PROCESS_TMSTMP = CURRENT TIMESTAMP WHERE PRD_ID = 'S10'; 
UPDATE EDRTLRP1D.PSNL_LOAN_RPTG_PRD_LKP SET BASEL_PRD_TP_CD = 'ITL AUTO REG', UPDT_PROCESS_TMSTMP = CURRENT TIMESTAMP WHERE PRD_ID = 'S09'; 


/************* Create table backups in IIAS PROD ****************************/
1. EDRTLRP1D.BASEL_REVLVNG_CR_BASE_DRVD_VARS
2. EDRTLRP1D.BASEL_REVLVNG_CR_ACCT_DRVD_VARS
3. EDRTLRP1D.REVLVNG_CR_OBSVTN_PT_DRVD_VAR
4. EDRTLRP1D.PD_SEG_ACCT_XREF
5. EDRTLRP1D.EAD_SEG_ACCT_XREF
6. EDRTLRP1D.LGD_SEG_ACCT_XREF
7. EDRTLRP1D.BASEL_MODEL
8. EDRTLRP1D.BASEL_MODEL_REL
9. EDRTLRP1D.BASEL_MODEL_SCORECRD_HDR
10. EDRTLRP1D.BASEL_MODEL_SCORECRD_DTL 	
11. EDRTLRP1D.BASEL_SEG    				

/************* Data Patch Tables - copy from IIAS UAT to IIAS PROD **********/
1. Copy from EDRTLRPD1D.BASEL_REVLVNG_CR_BASE_DRVD_VARS to EDRTLRP1D.BASEL_REVLVNG_CR_BASE_DRVD_VARS (mth_tm_id >= and mth_tm_id <= )
2. Copy from EDRTLRPD1D.BASEL_REVLVNG_CR_ACCT_DRVD_VARS to EDRTLRP1D.BASEL_REVLVNG_CR_ACCT_DRVD_VARS (mth_tm_id >= and mth_tm_id <= )
3. Copy from EDRTLRPD1D.REVLVNG_CR_OBSVTN_PT_DRVD_VAR to EDRTLRP1D.REVLVNG_CR_OBSVTN_PT_DRVD_VAR (process_mth_tm_id >= and process_mth_tm_id <= )
4. Copy from EDRTLRPD1D.PD_SEG_ACCT_XREFto EDRTLRP1D.PD_SEG_ACCT_XREF (mth_tm_id >= and mth_tm_id <= )
5. Copy from EDRTLRPD1D.EAD_SEG_ACCT_XREF to EDRTLRP1D.EAD_SEG_ACCT_XREF (mth_tm_id >= and mth_tm_id <= )
6. Copy from EDRTLRPD1D.LGD_SEG_ACCT_XREF to EDRTLRP1D.LGD_SEG_ACCT_XREF (mth_tm_id >= and mth_tm_id <= )
7. Copy from EDRTLRPD1D.BASEL_MODEL to EDRTLRP1D.BASEL_MODEL
8. Copy from EDRTLRPD1D.BASEL_MODEL_REL to EDRTLRP1D.BASEL_MODEL_REL
9. Copy from EDRTLRPD1D.BASEL_MODEL_SCORECRD_HDR to EDRTLRP1D.BASEL_MODEL_SCORECRD_HDR
10.Copy from EDRTLRPD1D.BASEL_MODEL_SCORECRD_DTL to EDRTLRP1D.BASEL_MODEL_SCORECRD_DTL
11.Copy from EDRTLRPD1D.BASEL_SEG to EDRTLRP1D.BASEL_SEG


/************** Copy data from IIAS UAT to IIAS PROD (New Tables) ***********************/
1. Copy from EDRTLRPD1D.RPTG_CCF_LKP to EDRTLRP1D.RPTG_CCF_LKP
2. Copy from EDRTLRPD1D.PD_BAND_DIM to EDRTLRP1D.PD_BAND_DIM
3. Copy from EDRTLRPD1D.BASEL_CCAR_PD_CURVE to EDRTLRP1D.BASEL_CCAR_PD_CURVE (MTH_END_DT = 2022-01-31)
4. Copy from EDRTLRPD1D.BASEL_NCR_HIERARCHY_LKP to EDRTLRP1D.BASEL_NCR_HIERARCHY_LKP
5. Copy from EDRTLRPD1D.BASEL_ACCT_PRFM_FACT to EDRTLRP1D.BASEL_ACCT_PRFM_FACT (MTH_END_DT = 2022-01-31) /* Required for Exception Report */
6. Copy from EDRTLRPD1D.RMA_BASEL3_PRDCT_SCHD_BCAR50_LKP to EDRTLRP1D.RMA_BASEL3_PRDCT_SCHD_BCAR50_LKP /* RRMSS-1943 - for 2 6 report  */


/* DIMENSION TABLES INCLUDING SCORING & SEGMENTATION */
DELETE FROM EDRTLRPD1D.BASEL_MODEL_SCORECRD_DTL WHERE BASEL_MODEL_SCORECRD_HDR_ID in (8025,8026,8027); /*cc pd* - new */

UPDATE EDRTLRPD1D.BASEL_MODEL_SCORECRD_DTL SET BIN_CRTRIA_SQL_CD_STRG = null, updt_process_tmstmp = sysdate WHERE BASEL_MODEL_SCORECRD_HDR_ID=8004; /*cc pd*/
UPDATE EDRTLRPD1D.BASEL_MODEL_SCORECRD_DTL SET BIN_CRTRIA_SQL_CD_STRG = null, updt_process_tmstmp = sysdate WHERE BASEL_MODEL_SCORECRD_HDR_ID=8002; /*cc lgdd*/
UPDATE EDRTLRPD1D.BASEL_MODEL_SCORECRD_DTL SET BIN_CRTRIA_SQL_CD_STRG = null, updt_process_tmstmp = sysdate WHERE BASEL_MODEL_SCORECRD_HDR_ID=8003; /*cc lgdnd*/

UPDATE EDRTLRP1D.BASEL_MODEL_SCORECRD_DTL SET BIN_CRTRIA_SQL_CD_STRG = REPLACE(BIN_CRTRIA_SQL_CD_STRG, 'EDRTLRD1D', 'EDRTLRP1D');
UPDATE EDRTLRP1D.BASEL_MODEL_SCORECRD_DTL SET BIN_CRTRIA_SQL_CD_STRG = REPLACE(BIN_CRTRIA_SQL_CD_STRG, 'EDRTLRPD1D', 'EDRTLRP1D'); 

UPDATE EDRTLRPD1D.BASEL_SEG SET BIN_CRTRIA_SQL_CD_STRG = REPLACE(BIN_CRTRIA_SQL_CD_STRG, 'EDRTLRD1D', 'EDRTLRP1D');
UPDATE EDRTLRPD1D.BASEL_SEG SET BIN_CRTRIA_SQL_CD_STRG = REPLACE(BIN_CRTRIA_SQL_CD_STRG, 'EDRTLRPD1D', 'EDRTLRP1D'); 

/************* CHECK THIS BEFORE PROD **************************************/
INSERT INTO EDRTLRP1D.RPTG_PRD_LKP_THRSHLD (THRSHLD, CRNT_F, EFF_FROM_YR_MTH, EFF_TO_YR_MTH) VALUES(125000, 'N', TO_DATE('2000-10-31','YYYY-MM-DD'), TO_DATE('2022-06-30','YYYY-MM-DD'));
INSERT INTO EDRTLRP1D.RPTG_PRD_LKP_THRSHLD (THRSHLD, CRNT_F, EFF_FROM_YR_MTH, EFF_TO_YR_MTH) VALUES(150000, 'Y', TO_DATE('2022-07-31','YYYY-MM-DD'), TO_DATE('9999-12-01','YYYY-MM-DD'));


/* BASEL_SEG_RPTG_PARM - RUN AFTER THE NEW FILE IS LOADED */
/* Manually deactivate deleted segments */
UPDATE edrtlrp1d.basel_Seg_rptg_parm SET EFF_TO_DT = '2022-06-30' WHERE BASEL_SEG_ID IN (16049,16053,16054) AND EFF_TO_DT = '9999-12-31';

/*BASEL CCAR AUDIT JOB, EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP TABLE CHANGES*/

UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*10110*/
select p.Out_Name as LOG_NM, p.Curr AS LOG_CRNT_VAL, p.Base as LOG_BASLN_VAL
  ,(ABS((p.Curr - p.Base))*100/p.Base) AS LOG_VARNC_VAL
  , '''' AS LOG_CMB_RSLT
from ( 
select a.Out_Name
   , sum(case when a.Value_Type=''Base_Value'' then Val else 0 end) Base
   , sum(case when a.Value_Type=''Current_Value'' then Val else 0 end) Curr 
from (
  Select 
    CASE 
      WHEN R.BASEL_PRODUCT_NAME like ''%BNS%'' THEN ''BNS-MOR''
      WHEN R.BASEL_PRODUCT_NAME like ''%TNG%'' THEN ''TNG-MOR''
      WHEN R.BASEL_PRODUCT_NAME like ''_TL%''  THEN ''SPL''
      WHEN (R.BASEL_PRODUCT_NAME like ''%_CON_C_%'' OR R.BASEL_PRODUCT_NAME like ''%_CON_HL%'') THEN ''CVG/SL''
      WHEN R.BASEL_PRODUCT_NAME like ''%_CON_SL%''  THEN ''SSL''
   END as Out_Name,
    r.RPTG_DT_TEXT AS period 
   , case  
         When (rank() over(order by r.RPTG_DT_TEXT) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End AS Value_Type
   , sum(CAST(Drawn_Amount as Float))/1000000 as Val 
   from EDRTLRP1D.BASEL_CCAR_EXPSR_RPTG_EXTR_CACAP R 
   JOIN EDRTLRP1D.TM_DIM T on to_char(T.DAY_DT,''YYYYMMDD'') = R.RPTG_DT_TEXT 
   where r.RPTG_TP = ''DR-AIRB-EXPOSURES''  AND T.FNCL_MTH_KEY in (&mth_tm_id , &mth_tm_id - 40) 
   group by BASEL_PRODUCT_NAME,RPTG_DT_TEXT
   order by BASEL_PRODUCT_NAME,period

  ) A 
group by a.Out_Name
) p
order by p.Out_Name'
WHERE AUDIT_CNFGRTN_ID=10110;




UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*10120*/
select sub_Prod as LOG_NM, Curr_EA AS LOG_CRNT_VAL, Base_EA AS LOG_BASLN_VAL 
  ,NVL(ABS(Curr_EA - Base_EA)*100/Base_EA, 100) AS LOG_VARNC_VAL
  , '''' AS LOG_CMB_RSLT
from ( 
select a.sub_Prod
   , sum(case when a.CurrentTime is not null then EA else null end) AS Curr_EA 
   , sum(case when BaseTime is not null then 
     EA else null end) AS Base_EA
from ( 
   Select 
     substring(r.BASEL_PRODUCT_NAME,1,length(r.BASEL_PRODUCT_NAME)-9) AS sub_Prod
     , T1.DAY_DT AS CurrentTime
     , T2.DAY_DT AS BaseTime  
     , sum(CAST(Drawn_Amount as Float))/1000000 AS EA 
   from EDRTLRP1D.BASEL_CCAR_EXPSR_RPTG_EXTR_CACAP R 
   Left JOIN EDRTLRP1D.TM_DIM T1 on to_char(T1.DAY_DT,''YYYYMMDD'') = R.RPTG_DT_TEXT AND T1.FNCL_MTH_KEY in (&mth_tm_id)
   Left JOIN EDRTLRP1D.TM_DIM T2 on to_char(T2.DAY_DT,''YYYYMMDD'') = R.RPTG_DT_TEXT AND T2.FNCL_MTH_KEY in (&mth_tm_id-40)
   where R.RPTG_TP = ''DR-AIRB-EXPOSURES'' 
   and (T1.DAY_DT is not null or T2.DAY_DT is not null)
   group by r.BASEL_PRODUCT_NAME,T1.DAY_DT,T2.DAY_DT
   order by r.BASEL_PRODUCT_NAME,T1.DAY_DT
  ) A 
group by a.sub_Prod
) p
order by 1'
WHERE AUDIT_CNFGRTN_ID=10120;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*10210*/
select p.Out_Name as LOG_NM, p.Curr AS LOG_CRNT_VAL, p.Base as LOG_BASLN_VAL
  ,(ABS((p.Curr - p.Base))*100/p.Base) AS LOG_VARNC_VAL
  , '''' AS LOG_CMB_RSLT
from ( 
select a.Out_Name
   , sum(case when a.Value_Type=''Base_Value'' then Val else 0 end) Base
   , sum(case when a.Value_Type=''Current_Value'' then Val else 0 end) Curr 
from ( 
  Select 
    CASE 
      WHEN R.BASEL_PRODUCT_NAME like ''%BNS%'' THEN ''BNS-MOR''
      WHEN R.BASEL_PRODUCT_NAME like ''%TNG%'' THEN ''TNG-MOR''
      WHEN R.BASEL_PRODUCT_NAME like ''_TL%''  THEN ''SPL''
      WHEN (R.BASEL_PRODUCT_NAME like ''%_CON_C_%'' OR R.BASEL_PRODUCT_NAME like ''%_CON_HL%'') THEN ''CVG/SL''
      WHEN R.BASEL_PRODUCT_NAME like ''%_CON_SL%''  THEN ''SSL''
   END as Out_Name,
    r.RPTG_DT_TEXT AS period 
   , case  
         When (rank() over(order by r.RPTG_DT_TEXT) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End AS Value_Type
   , sum(CAST(Drawn_Amount as Float))/1000000 as Val 
   from EDRTLRP1D.BASEL_CCAR_EXPSR_RPTG_EXTR_CACAP R 
   JOIN EDRTLRP1D.TM_DIM T on to_char(T.DAY_DT,''YYYYMMDD'') = R.RPTG_DT_TEXT 
   where r.RPTG_TP = ''DR-AIRB-EXPOSURES-BEF_NEG_NET''  AND T.FNCL_MTH_KEY in (&mth_tm_id , &mth_tm_id - 40) 
   group by BASEL_PRODUCT_NAME,RPTG_DT_TEXT
   order by BASEL_PRODUCT_NAME,period
  ) A 
group by a.Out_Name
) p
order by p.Out_Name'
WHERE AUDIT_CNFGRTN_ID=10210;




UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*10220*/
select sub_Prod as LOG_NM, Curr_EA AS LOG_CRNT_VAL, Base_EA AS LOG_BASLN_VAL 
  ,NVL(ABS(Curr_EA - Base_EA)*100/Base_EA, 100) AS LOG_VARNC_VAL
  , '''' AS LOG_CMB_RSLT
from ( 
select a.sub_Prod
   , sum(case when a.CurrentTime is not null then EA else null end) AS Curr_EA 
   , sum(case when BaseTime is not null then 
     EA else null end) AS Base_EA
from ( 
   Select
     substring(r.BASEL_PRODUCT_NAME,1,length(r.BASEL_PRODUCT_NAME)-9) AS sub_Prod
     , T1.DAY_DT AS CurrentTime
     , T2.DAY_DT AS BaseTime  
     , sum(CAST(Drawn_Amount as Float))/1000000 AS EA 
   from EDRTLRP1D.BASEL_CCAR_EXPSR_RPTG_EXTR_CACAP R 
   Left JOIN EDRTLRP1D.TM_DIM T1 on to_char(T1.DAY_DT,''YYYYMMDD'') = R.RPTG_DT_TEXT AND T1.FNCL_MTH_KEY in (&mth_tm_id)
   Left JOIN EDRTLRP1D.TM_DIM T2 on to_char(T2.DAY_DT,''YYYYMMDD'') = R.RPTG_DT_TEXT AND T2.FNCL_MTH_KEY in (&mth_tm_id-40)
   where R.RPTG_TP = ''DR-AIRB-EXPOSURES-BEF_NEG_NET'' 
   and (T1.DAY_DT is not null or T2.DAY_DT is not null)
   group by r.BASEL_PRODUCT_NAME,T1.DAY_DT,T2.DAY_DT
   order by r.BASEL_PRODUCT_NAME,T1.DAY_DT
  ) A 
group by a.sub_Prod
) p
order by 1'
WHERE AUDIT_CNFGRTN_ID=10220;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*10310*/
select Source_sys LOG_NM, Curr  LOG_CRNT_VAL, Base LOG_BASLN_VAL
  ,ABS((Curr - Base)*100)/cast(Base as float) LOG_VARNC_VAL
  ,'''' LOG_CMB_RSLT
from ( 
select A.Source_sys
   , sum(case when Value_Type=''Base'' then A.EAD_AMT else 0 end) Base
   , sum(case when Value_Type=''Curr'' then A.EAD_AMT else 0 end) Curr
from ( 
   Select SRC_SYS_CD Source_sys
     , MTH_TM_ID period 
     , sum(EAD_TOT_AF_CRM_AMT) EAD_AMT
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base'' 
         Else ''Curr''
         End Value_Type
 FROM EDRRAPT.BASEL_RISK_WGHTD_AVG_FACT
where MTH_TM_ID in (&mth_tm_id, &mth_tm_id - 40)
group by MTH_TM_ID, SRC_SYS_CD
  ) A 
group by A.Source_sys
) p'
WHERE AUDIT_CNFGRTN_ID=10310;




UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*10320*/
select Source_sys LOG_NM, Curr  LOG_CRNT_VAL , Base  LOG_BASLN_VAL
  ,ABS((Curr - Base)*100)/cast(Base as float) LOG_VARNC_VAL
  ,'''' LOG_CMB_RSLT
from ( 
/*-- 2*/
select A.Source_sys
   , sum(case when Value_Type=''Base'' then A.EAD_AMT else 0 end) Base
   , sum(case when Value_Type=''Curr'' then A.EAD_AMT else 0 end) Curr
from ( 
/*-- 1*/
   Select SRC_SYS_CD Source_sys
     , MTH_TM_ID period 
     , sum(EL_TOT_AF_CRM_AMT) EAD_AMT
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base'' 
         Else ''Curr''
         End Value_Type
 FROM EDRRAPT.BASEL_RISK_WGHTD_AVG_FACT
where MTH_TM_ID in (&mth_tm_id, &mth_tm_id - 40)
group by MTH_TM_ID, SRC_SYS_CD
/*-- 1*/
  ) A 
group by A.Source_sys
/*-- 2*/
) p'
WHERE AUDIT_CNFGRTN_ID=10320;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*10330*/
select Source_sys LOG_NM, Curr LOG_CRNT_VAL, Base LOG_BASLN_VAL
  ,ABS((Curr - Base)*100)/cast(Base as float) LOG_VARNC_VAL
  ,'''' LOG_CMB_RSLT
from ( 
/*-- 2*/
select A.Source_sys
   , sum(case when Value_Type=''Base'' then A.EAD_AMT else 0 end) Base
   , sum(case when Value_Type=''Curr'' then A.EAD_AMT else 0 end) Curr
from ( 
/*-- 1*/
   Select SRC_SYS_CD Source_sys
     , MTH_TM_ID period 
     , sum(RWA_TOT_AF_CRM_FLOORED_AMT) EAD_AMT
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base'' 
         Else ''Curr''
         End Value_Type
 FROM EDRRAPT.BASEL_RISK_WGHTD_AVG_FACT
where MTH_TM_ID in (&mth_tm_id, &mth_tm_id- 40)
group by MTH_TM_ID, SRC_SYS_CD
/*-- 1*/
  ) A 
group by A.Source_sys
/*-- 2*/
) p'
WHERE AUDIT_CNFGRTN_ID=10330;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*10410*/
select  PRD_TP_NM LOG_NM
,PD_MIN_RTO LOG_CRNT_VAL
,PD_MAX_RTO LOG_BASLN_VAL
/*,cast(substring(R.PD_VAL_TEXT,1, length(R.PD_VAL_TEXT) -1) as float ) LOG_VARNC_VAL*/
,Case 
 WHEN cast(substring(R.PD_VAL_TEXT,1, length(R.PD_VAL_TEXT) -1) as float ) between 
  cast (PD_MIN_RTO as float) and cast (PD_MAX_RTO as float)
 THEN 0
 ELSE 100
 End  LOG_VARNC_VAL
,PRD_TP_NM||''~''|| PD_BAND||''~''|| PD_MIN_RTO ||''~''|| PD_MAX_RTO
||''~''|| substring(R.PD_VAL_TEXT,1, length(R.PD_VAL_TEXT) -1) LOG_CMB_RSLT
from EDRTLRP1D.BASEL_CCAR_PD_RPTG_EXTR_CACAP R
JOIN EDRTLRP1D.TM_DIM  T on to_char(T.DAY_DT,''YYYYMMDD'') = R.RPTG_DT_TEXT and T.FNCL_MTH_KEY = &mth_tm_id
and cast(substring(R.PD_VAL_TEXT,1, length(R.PD_VAL_TEXT) -1) as Decimal(12,8)) <> 0
order by 1'
WHERE AUDIT_CNFGRTN_ID=10410;






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






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*10520*/
select distinct BASEL_PRODUCT_NAME LOG_NM
,cast(substring(R.EAD_FACTOR,1, length(R.EAD_FACTOR) -1) as decimal (12,8)) / 100 LOG_CRNT_VAL
,cast(substring(R.Loss_Given_Default,1, length(R.Loss_Given_Default) -1) as decimal (12,8)) / 100 LOG_BASLN_VAL
, 0 LOG_VARNC_VAL
, '''' LOG_CMB_RSLT
from EDRTLRP1D.BASEL_CCAR_EXPSR_RPTG_EXTR_CACAP R
JOIN EDRTLRP1D.TM_DIM T on to_char(T.DAY_DT,''YYYYMMDD'') = R.RPTG_DT_TEXT 
where  T.FNCL_MTH_KEY in (&mth_tm_id ) 
order by BASEL_PRODUCT_NAME'
WHERE AUDIT_CNFGRTN_ID=10520;






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






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*18130*/
select  F.CCAR_BASEL_PRD_TP_NM AS LOG_NM, 0 AS LOG_CRNT_VAL, 0 AS LOG_BASLN_VAL, count(1) as LOG_VARNC_VAL, '' '' AS LOG_CMB_RSLT
   from EDRTLRP1D.BASEL_PSNL_LN_ANL_BL_INST_FACT F
  left outer join EDRTLRP1D.PSNL_LOAN_RPTG_PRD_LKP L 
     on rtrim(L.CCAR_BASEL_PRD_TP_NM) = substring(F.CCAR_BASEL_PRD_TP_NM,1,length(F.CCAR_BASEL_PRD_TP_NM)-9) AND
     L.PRD_ID=F.PRD_ID
  where F.MTH_TM_ID = &mth_tm_id
     and L.CCAR_BASEL_PRD_TP_NM is null
  group by F.CCAR_BASEL_PRD_TP_NM  
  UNION  
  SELECT 
   '' Checking RI for CCAR_BASEL_PRD_TP_NM in PSNL_LOAN_RPTG_PRD_LKP'', 0 AS LOG_CRNT_VAL, 0 AS LOG_BASLN_VAL, 0 AS LOG_VARNC_VAL, '' '' AS LOG_CMB_RSLT
   FROM EDRTLRP1D.TM_DIM'
WHERE AUDIT_CNFGRTN_ID=18130;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*18140*/
select Product LOG_NM
, 0 LOG_CRNT_VAL
, 0 LOG_BASLN_VAL
, cnt LOG_VARNC_VAL 
, cast(rnk  as varchar (50)) LOG_CMB_RSLT
from (
  select F.CCAR_BASEL_PRD_TP_NM Product, count(1) cnt, rank() over (order by count(1) DESC ) Rnk
  from EDRTLRP1D.BASEL_ANALYTCL_BL_INSTRMNT_FACT F
  left outer join EDRTLRP1D.BASEL_RPTG_PRD_LKP L 
     on rtrim(L.CCAR_BASEL_PRD_TP_NM) = substr(F.CCAR_BASEL_PRD_TP_NM,1,length(F.CCAR_BASEL_PRD_TP_NM)-9) AND
     L.PRD_ID=F.PRD_ID
  where F.MTH_TM_ID = &mth_tm_id
     and L.CCAR_BASEL_PRD_TP_NM is null
  group by F.CCAR_BASEL_PRD_TP_NM
) R'
WHERE AUDIT_CNFGRTN_ID=18140;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*19110*/
SELECT BASEL_PRODUCT_NAME AS LOG_NM, COUNT(BASEL_PRODUCT_NAME) AS LOG_CRNT_VAL, 0 AS LOG_BASLN_VAL, count(BASEL_PRODUCT_NAME) AS LOG_VARNC_VAL, '''' AS LOG_CMB_RSLT FROM
 (SELECT * FROM
     (SELECT *,ROWNUMBER() OVER (PARTITION BY RPTG_TP,RPTG_DT_TEXT,UNIQUE_IDENTIFIER,BASEL_PRODUCT_NAME,ProbabilityDefault_Band,LEGAL_ENTITY,DEFAULTED_EXPOSURE_FLAG,Unconditionally_Cancelable_Flag,Drawn_Amount,Exposures_at_Default,Currency,Expected_Loss,Undrawn_Amount,Accrued_Interest,Partial_Write_Off,Loss_Given_Default,EAD_FACTOR,Insurance_Flag,Downturn_LGD,LTV_BUCKET) AS RN
      FROM EDRTLRP1D.BASEL_CCAR_EXPSR_RPTG_EXTR_CACAP R LEFT JOIN EDRTLRP1D.TM_DIM T ON to_char(T.DAY_DT,''YYYYMMDD'') = R.RPTG_DT_TEXT WHERE FNCL_MTH_KEY=&mth_tm_id AND RPTG_TP = ''DR-AIRB-EXPOSURES'') AS A
 WHERE RN > 1)
GROUP BY BASEL_PRODUCT_NAME
UNION
 SELECT 
    '' Check DR-AIRB-EXPOSURES duplicates'' AS LOG_NM , 0 AS LOG_CRNT_VAL , 0 AS LOG_BASLN_VAL , 0 AS LOG_VARNC_VAL, '' '' AS LOG_CMB_RSLT
    FROM EDRTLRP1D.TM_DIM
 order by 1'
 WHERE AUDIT_CNFGRTN_ID=19110;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*19130*/
select 
  Product LOG_NM
  ,Curr  LOG_CRNT_VAL
 ,Base  LOG_BASLN_VAL
  ,ABS((Curr - Base)*100)/cast(Base as float) LOG_VARNC_VAL
 ,''''  LOG_CMB_RSLT
 from ( 
  select Product
   , sum(case when Value_Type=''Base_Value'' then AFTR_OS_BAL_AMT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then AFTR_OS_BAL_AMT else 0 end) Curr 
  from ( 
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
  ) A 
  group by Product 
  order by 1
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
  select Product
   , sum(case when Value_Type=''Base_Value'' then AFTR_OS_BAL_AMT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then AFTR_OS_BAL_AMT else 0 end) Curr 
  from ( 
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
  ) A 
  group by Product 
  order by 1
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
  select Product
   , sum(case when Value_Type=''Base_Value'' then AFTR_OS_BAL_AMT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then AFTR_OS_BAL_AMT else 0 end) Curr 
  from ( 
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
  ) A 
  group by Product 
  order by 1
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
  select Product
   , sum(case when Value_Type=''Base_Value'' then AFTR_OS_BAL_AMT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then AFTR_OS_BAL_AMT else 0 end) Curr 
  from ( 
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
  ) A 
  group by Product 
  order by 1
) p'
WHERE AUDIT_CNFGRTN_ID=19160;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*21010*/
select 
  Product LOG_NM
  ,Curr  LOG_CRNT_VAL
 ,Base  LOG_BASLN_VAL
 ,Case 
  When Base = 0 then (ABS(Curr - Base)*100)
  else   ABS((Curr - Base)*100)/cast(Base as float) 
  end  LOG_VARNC_VAL
 ,''NCR_BD_RECD_TP_ID~NCR_PD_BAND_ID~NCR_EXPSR_SIZE_ID~NCR_GEO_ID~NCR_LTV_ID~EXPSR_CL_ID~NCR_SECRTZTN_ID~NCR_RT_ID~DLQNT_ID''  LOG_CMB_RSLT
 from ( 
  select Product
   , sum(case when Value_Type=''Base_Value'' then EAD_REALZ_DEFLTD_OS_BAL_AMT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then EAD_REALZ_DEFLTD_OS_BAL_AMT else 0 end) Curr 
  from ( 
   SELECT  
    cast(NCR_RISK_RT_SYS_ID as varchar(10)) 
    ||''~''||cast(EXPSR_CL_ID as varchar(10)) 
    ||''~''||cast(NCR_PD_SEG_ID as varchar(10)) 
    ||''~''||cast(NCR_LGD_SEG_ID as varchar(10)) 
    ||''~''||cast(NCR_EAD_SEG_ID as varchar(10)) 
     As Product
     , MTH_TM_ID
     , SUM (NVL(EAD_REALZ_DEFLTD_OS_BAL_AMT,0)) As EAD_REALZ_DEFLTD_OS_BAL_AMT
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End Value_Type
   from EDRRAPT.BASEL_CANDN_RTL_NCR_BE_FACT
   where MTH_TM_ID in ( &mth_tm_id ,&mth_tm_id - 40) 
   and EAD_REALZ_DEFLTD_OS_BAL_AMT > 0
   group by     NCR_RISK_RT_SYS_ID,EXPSR_CL_ID,NCR_PD_SEG_ID,NCR_LGD_SEG_ID,NCR_EAD_SEG_ID
   , MTH_TM_ID
   order by 1,2
  ) A
  group by Product 
  order by 1
) p'
WHERE AUDIT_CNFGRTN_ID=21010;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*21020*/
select 
  Product LOG_NM
  ,Curr  LOG_CRNT_VAL
 ,Base  LOG_BASLN_VAL
 ,Case 
  When Base = 0 then (ABS(Curr - Base)*100)
  else     ABS((Curr - Base)*100)/cast(Base as float) 
  end  LOG_VARNC_VAL
 ,''SRC_SYS_CD~NCR_EXPSR_CL_KEY_VAL~NCR_RT_SYS_KEY_VAL~NCR_PD_SEG_KEY_VAL~NCR_LGD_SEG_KEY_VAL''  LOG_CMB_RSLT
 from ( 
  select Product
   , sum(case when Value_Type=''Base_Value'' then LGD_FINAL_RPTG_RTO else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then LGD_FINAL_RPTG_RTO else 0 end) Curr 
  from ( 
   SELECT  
    SRC_SYS_CD
 ||''~''||NCR_EXPSR_CL_KEY_VAL
 ||''~''||NCR_RT_SYS_KEY_VAL
 ||''~''||NCR_PD_SEG_KEY_VAL
 ||''~''||NCR_LGD_SEG_KEY_VAL 
  As Product
     , MTH_TM_ID
     , SUM(LGD_FINAL_RPTG_RTO) AS LGD_FINAL_RPTG_RTO
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End Value_Type
   from EDRRAPT.BASEL_RPTG_BL_AGGRTD_NCR_BE_FACT
   where MTH_TM_ID in (&mth_tm_id,&mth_tm_id- 40) and SRC_SYS_CD in ( ''TNG-MOR'' , ''MOR'')
   and LGD_FINAL_RPTG_RTO > 0
   group by  SRC_SYS_CD,NCR_EXPSR_CL_KEY_VAL,NCR_RT_SYS_KEY_VAL,NCR_PD_SEG_KEY_VAL,NCR_LGD_SEG_KEY_VAL,MTH_TM_ID
   order by 1,2
  ) A 
  group by Product 
  order by 1
) p'
WHERE AUDIT_CNFGRTN_ID=21020;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*21030*/
select 
  Product LOG_NM
  ,Curr LOG_CRNT_VAL
 ,Base  LOG_BASLN_VAL
 ,Case 
  When Base = 0 then (ABS(Curr - Base)*100)
  else  ABS((Curr - Base)*100)/cast(Base as float) 
  end  LOG_VARNC_VAL
 ,''SRC_SYS_CD~NCR_EXPSR_CL_KEY_VAL~NCR_RT_SYS_KEY_VAL~NCR_PD_SEG_KEY_VAL~NCR_LGD_SEG_KEY_VAL''  LOG_CMB_RSLT
 from ( 
  select Product
   , sum(case when Value_Type=''Base_Value'' then EAD_FINAL_RPTG_RTO else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then EAD_FINAL_RPTG_RTO else 0 end) Curr 
  from ( 
   SELECT  
    SRC_SYS_CD
 ||''~''||NCR_EXPSR_CL_KEY_VAL
 ||''~''||NCR_RT_SYS_KEY_VAL
 ||''~''||NCR_PD_SEG_KEY_VAL
 ||''~''||NCR_LGD_SEG_KEY_VAL 
  As Product
     , MTH_TM_ID
     , SUM(EAD_FINAL_RPTG_RTO) AS EAD_FINAL_RPTG_RTO
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End Value_Type
   from EDRRAPT.BASEL_RPTG_BL_AGGRTD_NCR_BE_FACT
   where MTH_TM_ID in ( &mth_tm_id,&mth_tm_id- 40) and SRC_SYS_CD in ( ''TNG-MOR'' , ''MOR'')
   and EAD_FINAL_RPTG_RTO > 0
     group by  SRC_SYS_CD,NCR_EXPSR_CL_KEY_VAL,NCR_RT_SYS_KEY_VAL,NCR_PD_SEG_KEY_VAL,NCR_LGD_SEG_KEY_VAL,MTH_TM_ID
   order by 1,2
  ) A 
  group by Product 
  order by 1
) p'
WHERE AUDIT_CNFGRTN_ID=21030;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*22110*/
select 
  Product LOG_NM
  ,Curr  LOG_CRNT_VAL
 ,Base  LOG_BASLN_VAL
 ,Case 
  When Base = 0 then (ABS(Curr - Base)*100)
  else   ABS((Curr - Base)*100)/cast(Base as float) 
  end  LOG_VARNC_VAL
 ,''NCR_BD_RECD_TP_ID~NCR_PD_BAND_ID~NCR_EXPSR_SIZE_ID~NCR_GEO_ID~NCR_LTV_ID~EXPSR_CL_ID~NCR_SECRTZTN_ID~NCR_RT_ID~DLQNT_ID''  LOG_CMB_RSLT
 from ( 
  select Product
   , sum(case when Value_Type=''Base_Value'' then ACCT_CNT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then ACCT_CNT else 0 end) Curr 
  from ( 
   SELECT  
     cast(NCR_BD_RECD_TP_ID as varchar(10)) 
    ||''~''||cast(NCR_PD_BAND_ID as varchar(10)) 
    ||''~''||cast(NCR_EXPSR_SIZE_ID as varchar(10)) 
    ||''~''||cast(NCR_GEO_ID as varchar(10)) 
    ||''~''||cast(NCR_LTV_ID as varchar(10)) 
    ||''~''||cast(EXPSR_CL_ID as varchar(10))
    ||''~''||cast(NCR_SECRTZTN_ID as varchar(10)) 
    ||''~''||cast(NCR_RT_ID as varchar(10)) 
    ||''~''||cast(DLQNT_ID as varchar(10)) 
      As Product
     , MTH_TM_ID
     , SUM(CASE WHEN (ACCT_CNT) <= 0 THEN 0 
                ELSE (ACCT_CNT) END) AS ACCT_CNT
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End Value_Type
   from edrrapt.Basel_candn_rtl_ncr_bd_fact
   where MTH_TM_ID in ( &mth_tm_id,&mth_tm_id - 40) 
   and ACCT_CNT > 0
   group by     NCR_BD_RECD_TP_ID, NCR_PD_BAND_ID,NCR_EXPSR_SIZE_ID,NCR_GEO_ID,NCR_LTV_ID,EXPSR_CL_ID,NCR_SECRTZTN_ID,NCR_RT_ID,DLQNT_ID
   , MTH_TM_ID
   order by 1,2
  ) A 
  group by Product 
  order by 1
) p'
WHERE AUDIT_CNFGRTN_ID=22110;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*22120*/
select 
  Product LOG_NM
  ,Curr  LOG_CRNT_VAL
 ,Base  LOG_BASLN_VAL
 ,Case 
  When Base = 0 then (ABS(Curr - Base)*100)
  else   ABS((Curr - Base)*100)/cast(Base as float) 
  end  LOG_VARNC_VAL
 ,''NCR_BD_RECD_TP_ID~NCR_PD_BAND_ID~NCR_EXPSR_SIZE_ID~NCR_GEO_ID~NCR_LTV_ID~EXPSR_CL_ID~NCR_SECRTZTN_ID~NCR_RT_ID~DLQNT_ID''  LOG_CMB_RSLT
 from ( 
  select Product
   , sum(case when Value_Type=''Base_Value'' then OS_BAL_AMT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then OS_BAL_AMT else 0 end) Curr 
  from ( 
   SELECT  
     cast(NCR_BD_RECD_TP_ID as varchar(10)) 
    ||''~''||cast(NCR_PD_BAND_ID as varchar(10)) 
    ||''~''||cast(NCR_EXPSR_SIZE_ID as varchar(10)) 
    ||''~''||cast(NCR_GEO_ID as varchar(10)) 
    ||''~''||cast(NCR_LTV_ID as varchar(10)) 
    ||''~''||cast(EXPSR_CL_ID as varchar(10))
    ||''~''||cast(NCR_SECRTZTN_ID as varchar(10)) 
    ||''~''||cast(NCR_RT_ID as varchar(10)) 
    ||''~''||cast(DLQNT_ID as varchar(10)) 
      As Product
     , MTH_TM_ID
     , SUM(OS_BAL_AMT) AS OS_BAL_AMT
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End Value_Type
   from EDRRAPT.Basel_candn_rtl_ncr_bd_fact
   where MTH_TM_ID in ( &mth_tm_id, &mth_tm_id - 40) 
   and OS_BAL_AMT > 0
   group by     NCR_BD_RECD_TP_ID, NCR_PD_BAND_ID,NCR_EXPSR_SIZE_ID,NCR_GEO_ID,NCR_LTV_ID,EXPSR_CL_ID,NCR_SECRTZTN_ID,NCR_RT_ID,DLQNT_ID
   , MTH_TM_ID
   order by 1,2
  ) A 
  group by Product 
  order by 1
) p'
WHERE AUDIT_CNFGRTN_ID=22120;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*22210*/
select 
  Product LOG_NM
  ,Curr  LOG_CRNT_VAL
 ,Base  LOG_BASLN_VAL
 ,Case 
  When Base = 0 then (ABS(Curr - Base)*100)
  else   ABS((Curr - Base)*100)/cast(Base as float)  
  end  LOG_VARNC_VAL
 ,''NCR_BD_RECD_TP_ID~NCR_PD_BAND_ID~NCR_EXPSR_SIZE_ID~NCR_GEO_ID~NCR_LTV_ID~EXPSR_CL_ID~NCR_SECRTZTN_ID~NCR_RT_ID~DLQNT_ID''  LOG_CMB_RSLT
 from ( 
  select Product
   , sum(case when Value_Type=''Base_Value'' then PD_REALZ_DEFLTD_OS_BAL_AMT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then PD_REALZ_DEFLTD_OS_BAL_AMT else 0 end) Curr 
  from ( 
   SELECT  
    cast(NCR_RISK_RT_SYS_ID as varchar(10)) 
    ||''~''||cast(EXPSR_CL_ID as varchar(10)) 
    ||''~''||cast(NCR_PD_SEG_ID as varchar(10)) 
    ||''~''||cast(NCR_LGD_SEG_ID as varchar(10)) 
    ||''~''||cast(NCR_EAD_SEG_ID as varchar(10)) 
     As Product
     , MTH_TM_ID
     , SUM (NVL(PD_REALZ_DEFLTD_OS_BAL_AMT,0)) As PD_REALZ_DEFLTD_OS_BAL_AMT
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End Value_Type
   from EDRRAPT.BASEL_CANDN_RTL_NCR_BE_FACT
   where MTH_TM_ID in ( &mth_tm_id, &mth_tm_id - 40) 
  and PD_REALZ_DEFLTD_OS_BAL_AMT > 0
   group by     NCR_RISK_RT_SYS_ID,EXPSR_CL_ID,NCR_PD_SEG_ID,NCR_LGD_SEG_ID,NCR_EAD_SEG_ID
   , MTH_TM_ID
   order by 1,2
  ) A 
  group by Product 
  order by 1
) p'
WHERE AUDIT_CNFGRTN_ID=22210;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*22220*/
select 
  Product LOG_NM
  ,Curr  LOG_CRNT_VAL
 ,Base  LOG_BASLN_VAL
 ,Case 
  When Base = 0 then (ABS(Curr - Base)*100)
  else   ABS((Curr - Base)*100)/cast(Base as float) 
  end  LOG_VARNC_VAL
 ,''NCR_BD_RECD_TP_ID~NCR_PD_BAND_ID~NCR_EXPSR_SIZE_ID~NCR_GEO_ID~NCR_LTV_ID~EXPSR_CL_ID~NCR_SECRTZTN_ID~NCR_RT_ID~DLQNT_ID''  LOG_CMB_RSLT
 from ( 
  select Product
   , sum(case when Value_Type=''Base_Value'' then LGD_REALZ_DEFLTD_OS_BAL_AMT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then LGD_REALZ_DEFLTD_OS_BAL_AMT else 0 end) Curr 
  from ( 
   SELECT  
    cast(NCR_RISK_RT_SYS_ID as varchar(10)) 
    ||''~''||cast(EXPSR_CL_ID as varchar(10)) 
    ||''~''||cast(NCR_PD_SEG_ID as varchar(10)) 
    ||''~''||cast(NCR_LGD_SEG_ID as varchar(10)) 
    ||''~''||cast(NCR_EAD_SEG_ID as varchar(10)) 
     As Product
     , MTH_TM_ID
     , SUM (NVL(LGD_REALZ_DEFLTD_OS_BAL_AMT,0)) As LGD_REALZ_DEFLTD_OS_BAL_AMT
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End Value_Type
   from EDRRAPT.BASEL_CANDN_RTL_NCR_BE_FACT
   where MTH_TM_ID in (&mth_tm_id, &mth_tm_id- 40) 
  and LGD_REALZ_DEFLTD_OS_BAL_AMT > 0
   group by     NCR_RISK_RT_SYS_ID,EXPSR_CL_ID,NCR_PD_SEG_ID,NCR_LGD_SEG_ID,NCR_EAD_SEG_ID
   , MTH_TM_ID
   order by 1,2
  ) A 
  group by Product 
  order by 1
) p'
WHERE AUDIT_CNFGRTN_ID=22220;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*22240*/
select 
  Product LOG_NM
  ,Curr  LOG_CRNT_VAL
 ,Base  LOG_BASLN_VAL
 ,Case 
  When Base = 0 then (ABS(Curr - Base)*100)
  else   ABS((Curr - Base)*100)/cast(Base as float) 
  end  LOG_VARNC_VAL
 ,''NCR_BD_RECD_TP_ID~NCR_PD_BAND_ID~NCR_EXPSR_SIZE_ID~NCR_GEO_ID~NCR_LTV_ID~EXPSR_CL_ID~NCR_SECRTZTN_ID~NCR_RT_ID~DLQNT_ID''  LOG_CMB_RSLT
 from ( 
  select Product
   , sum(case when Value_Type=''Base_Value'' then EAD_REALZ_DEFLTD_OS_BAL_AMT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then EAD_REALZ_DEFLTD_OS_BAL_AMT else 0 end) Curr 
  from ( 
   SELECT  
    cast(NCR_RISK_RT_SYS_ID as varchar(10)) 
    ||''~''||cast(EXPSR_CL_ID as varchar(10)) 
    ||''~''||cast(NCR_PD_SEG_ID as varchar(10)) 
    ||''~''||cast(NCR_LGD_SEG_ID as varchar(10)) 
    ||''~''||cast(NCR_EAD_SEG_ID as varchar(10)) 
     As Product
     , MTH_TM_ID
     , SUM (NVL(EAD_REALZ_DEFLTD_OS_BAL_AMT,0)) As EAD_REALZ_DEFLTD_OS_BAL_AMT
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End Value_Type
   from EDRRAPT.BASEL_CANDN_RTL_NCR_BE_FACT
   where MTH_TM_ID in ( &mth_tm_id, &mth_tm_id - 40) 
   and EAD_REALZ_DEFLTD_OS_BAL_AMT > 0
   group by     NCR_RISK_RT_SYS_ID,EXPSR_CL_ID,NCR_PD_SEG_ID,NCR_LGD_SEG_ID,NCR_EAD_SEG_ID
   , MTH_TM_ID
   order by 1,2
  ) A 
  group by Product 
  order by 1
) p'
WHERE AUDIT_CNFGRTN_ID=22240;






UPDATE EDRTLRP1D.AUDIT_CHECK_CNFGRTN_CCAR_ACAP SET SQL_TEXT='/*22250*/
select 
  Product LOG_NM
  ,Curr  LOG_CRNT_VAL
 ,Base  LOG_BASLN_VAL
 ,Case 
  When Base = 0 then (ABS(Curr - Base)*100)
  else   ABS((Curr - Base)*100)/cast(Base as float)  
  end  LOG_VARNC_VAL
 ,''NCR_BD_RECD_TP_ID~NCR_PD_BAND_ID~NCR_EXPSR_SIZE_ID~NCR_GEO_ID~NCR_LTV_ID~EXPSR_CL_ID~NCR_SECRTZTN_ID~NCR_RT_ID~DLQNT_ID''  LOG_CMB_RSLT
 from ( 
  select Product
   , sum(case when Value_Type=''Base_Value'' then ECONMC_CAPTL_AMT else 0 end) Base
   , sum(case when Value_Type=''Current_Value'' then ECONMC_CAPTL_AMT else 0 end) Curr 
  from ( 
   SELECT  
    cast(NCR_RISK_RT_SYS_ID as varchar(10)) 
    ||''~''||cast(EXPSR_CL_ID as varchar(10)) 
    ||''~''||cast(NCR_PD_SEG_ID as varchar(10)) 
    ||''~''||cast(NCR_LGD_SEG_ID as varchar(10)) 
    ||''~''||cast(NCR_EAD_SEG_ID as varchar(10)) 
     As Product
     , MTH_TM_ID
     , SUM (NVL(ECONMC_CAPTL_AMT,0)) As ECONMC_CAPTL_AMT
     , case  
         When (rank() over(order by MTH_TM_ID) = 1) Then ''Base_Value'' 
         Else ''Current_Value''
         End Value_Type
   from EDRRAPT.BASEL_CANDN_RTL_NCR_BE_FACT
   where MTH_TM_ID in ( &mth_tm_id, &mth_tm_id - 40) 
   and ECONMC_CAPTL_AMT > 0
   group by     NCR_RISK_RT_SYS_ID,EXPSR_CL_ID,NCR_PD_SEG_ID,NCR_LGD_SEG_ID,NCR_EAD_SEG_ID
   , MTH_TM_ID
   order by 1,2
  ) A 
  group by Product 
  order by 1
) p'
WHERE AUDIT_CNFGRTN_ID=22250;

/*AUDIT JOB TABLE CHANGES END*/
