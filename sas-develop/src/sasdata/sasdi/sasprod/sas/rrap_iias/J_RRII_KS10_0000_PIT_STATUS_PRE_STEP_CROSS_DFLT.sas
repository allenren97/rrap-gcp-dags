/*--------------------------------------------------------------------------------
 * NAME:      J_RRII_KS10_0000_PIT_STATUS_PRE_STEP_CROSS_DFLT.sas
 *
 * PURPOSE:   Derives pre-Step Pit status for KS/SPL/MOR accounts, 
 *            perform Step cross default calculation, mark written-out
 *            accounts for KS/SPL and set default flags
 *             
 * FREQUENCY: Monthly
 *             
 * SOURCES:   KS - EDRTLRP1D.BASEL_REVLVNG_CR_MTH_SNAPSHOT 
 *            SPL - EDRTLRP1D.BASEL_PSNL_LOAN_MTH_SNAPSHOT 
 *            MOR - FRG_USER_DATA.AIRB_MORT_MTH_SNAPSHOT, 
 *                  EDRTLRP1D.BASEL_MORT_MTH_SNAPSHOT
 *             
 * TARGET:    EDRTLRP1D.PIT_STATUS_PRE_STEP 
 *             
 * NOTES:     Pit V2 logic mimic following jobs; keep them aligned when changing,
 *              KS - J_RRII_KS10_2103_BASEL_REVLVNG_CR_BASE_DRVD_VARS.sas 
 *              SPL - J_RRAP_TL10_2104_BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2.sas 
 *              MOR - RRAP_MOR_MODEL_01_DEFINE_STATUS_G.sas
 *             
 * CHANGES:   Jul 2024  Roger - initial development (RRMSS-2843)  
 *
 *-------------------------------------------------------------------------------*/

options errorabend;

%rrap_dlgd_autoexec;
%let net_db = &RRAP_DB;
%let FRG_DB = &FRG_USR;
%let tm_id = &MTH_TM_ID;
%put NOTE: tm_id is &TM_ID;


/* KS Pit pre-Step */
proc sql;
connect using NZRRAP as nzcon;
execute (delete from &net_db..PIT_STATUS_PRE_STEP where SRC_SYS_CD='KS' and MTH_TM_ID=&mth_tm_id.) by nzcon;

execute	(insert into &net_db..PIT_STATUS_PRE_STEP 
            (MTH_TM_ID,	SRC_SYS_CD, BASEL_ACCT_ID, STEP_PLN_SNAPSHOT_ID, STEP_PLN_AGRMNT_NUM, PIT_STATUS_PRE_STEP, STEP_DFLT_F, INSRT_PROCESS_TMSTMP, UPDT_PROCESS_TMSTMP)
            WITH 
                   Snap  as
                   (
                   SELECT  MTH_TM_ID, 
                            BASEL_ACCT_ID, 
                            STEP_PLN_SNAPSHOT_ID, 
                            STEP_PLN_AGRMNT_NUM,
                            PRD_CD, 
                            SUB_PRD_CD, 
                            A.BLOCK_RECL_CD, 
                            TOT_NEW_BAL_AMT, 
                            CHRG_OFF_CD, 
                            BNS_DLQNT_DAY, 
                            TOT_UNPAID_FNCL_CHRG_AMT, 
                            PRIM_BASEL_CUST_ID AS BASEL_CUST_ID, 
                            (CASE 
                                WHEN SUB_PRD_CD='RS' OR STEP_PLN_SNAPSHOT_ID NOT IN (-1,-2) 
                                  THEN 'Y'
                                 ELSE 'N'
                               END)  AS HELOC_F,
                           (CASE 
                             WHEN  LK_RECL.BLOCK_RECL_CD IS NULL THEN '1'
                             ELSE '0'
                           END) as  v_PT_STAT_BLCK_RECL_CD_LKP_CUR 
                            FROM &net_db..BASEL_REVLVNG_CR_MTH_SNAPSHOT a
                                left join  (
                                            SELECT BLOCK_RECL_CD as BLOCK_RECL_CD 
                                            FROM &net_db..BLOCK_RECL_LKP a, 
                                            (SELECT substr(tm_lvl_end_dt,1,4)||substr(tm_lvl_end_dt,6,2) AS yrmt FROM &net_db..TM_DIM WHERE tm_id=&TM_ID.) AS ymt
                                            WHERE BNKRPY_F='Y' AND yrmt BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH) as LK_RECL on a.BLOCK_RECL_CD=LK_RECL.BLOCK_RECL_CD
                             WHERE MTH_TM_ID = &TM_ID. 
                   ), 
                 Prev_Snap 
                  as
                   (SELECT 
                    BASEL_ACCT_ID,  PRIM_BASEL_CUST_ID AS BASEL_CUST_ID, 
                    TOT_NEW_BAL_AMT AS PREV_TOT_NEW_BAL_AMT,
                    a.CHRG_OFF_CD     AS PREV_CHRG_OFF_CD,
                    (CASE  WHEN TOT_NEW_BAL_AMT > 0 AND LK_RECL.BLOCK_RECL_CD IS NULL THEN '1'
                              ELSE '0'
                   END ) as  v_PT_STAT_BLCK_RECL_CD_LKP_PRV, 
                   
                   (CASE WHEN LK_CHRG.CHRG_OFF_CD IS NULL THEN '1' ELSE '0' END) AS v_PT_STAT_CHRG_OFF_LKP_PREV2,
                   TOT_UNPAID_FNCL_CHRG_AMT AS PREV_TOT_UNPAID_FNCL_CHRG_AMT 
                   FROM &net_db..BASEL_REVLVNG_CR_MTH_SNAPSHOT  a
                   left join  (SELECT BLOCK_RECL_CD as BLOCK_RECL_CD 
                               FROM &net_db..BLOCK_RECL_LKP a, 
                                (SELECT substr(tm_lvl_end_dt,1,4)||substr(tm_lvl_end_dt,6,2) AS yrmt FROM &net_db..TM_DIM WHERE tm_id=&TM_ID.) AS ymt
                                WHERE BNKRPY_F='Y' AND yrmt BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH) as LK_RECL on a.BLOCK_RECL_CD=LK_RECL.BLOCK_RECL_CD
                    left join  (SELECT CHRG_OFF_CD as CHRG_OFF_CD 
                                FROM &net_db..CHRG_OFF_LKP a, 
                                     (SELECT substr(tm_lvl_end_dt,1,4)||substr(tm_lvl_end_dt,6,2) AS yrmt
                                     FROM &net_db..TM_DIM WHERE tm_id=&TM_ID.) ymt
                                WHERE CHRG_OFF_STAT_F = 'Y'
                                AND yrmt BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
                                ORDER BY CHRG_OFF_CD ) LK_CHRG ON A.CHRG_OFF_CD=LK_CHRG.CHRG_OFF_CD
                   WHERE MTH_TM_ID = (&TM_ID. - 40)   
                   )
            SELECT 
            s.MTH_TM_ID,
            'KS' as SRC_SYS_CD,
            s.BASEL_ACCT_ID,
            s.STEP_PLN_SNAPSHOT_ID,
            s.STEP_PLN_AGRMNT_NUM, 
            /* this mimics logic in J_RRII_KS10_2103_BASEL_REVLVNG_CR_BASE_DRVD_VARS for Pit status derivation */
            (case when SP.BASEL_PRD_CD='CC' AND HELOC_F = 'N' then 
                 ( CASE  
                   WHEN CHRG_OFF_CD ='1' THEN  'CHG' 
                   WHEN (  BNS_DLQNT_DAY<210 and NOT (TOT_NEW_BAL_AMT> 0 AND  CHRG_OFF_CD IN ('N','Q')) and  v_PT_STAT_BLCK_RECL_CD_LKP_CUR='1') THEN  'CUR'       
                   WHEN  ( 
                        TOT_NEW_BAL_AMT>0 and TOT_NEW_BAL_AMT=TOT_UNPAID_FNCL_CHRG_AMT and v_PT_STAT_CHRG_OFF_LKP_PREV2='1' 
                         and  NOT ( PREV_TOT_NEW_BAL_AMT>0 and PREV_CHRG_OFF_CD IN ('N','Q'))
                         and (PREV_TOT_NEW_BAL_AMT>0 and v_PT_STAT_BLCK_RECL_CD_LKP_PRV='1') ) THEN 'CUR'
                   WHEN (  TOT_NEW_BAL_AMT=0 and PREV_TOT_NEW_BAL_AMT=PREV_TOT_UNPAID_FNCL_CHRG_AMT and v_PT_STAT_CHRG_OFF_LKP_PREV2='1'
                         and  NOT ( PREV_TOT_NEW_BAL_AMT>0 and PREV_CHRG_OFF_CD IN ('N','Q' ))
                         and (PREV_TOT_NEW_BAL_AMT>0 and v_PT_STAT_BLCK_RECL_CD_LKP_PRV='1')) THEN 'CUR'
                    ELSE 'DEF'  
                   END )   
            else 
                  (CASE 
                     WHEN  s.HELOC_F='N' THEN
                          CASE  
                           WHEN CHRG_OFF_CD ='1' THEN  'CHG' 
                           WHEN (  BNS_DLQNT_DAY<120 and NOT (TOT_NEW_BAL_AMT> 0 AND  CHRG_OFF_CD IN ('N','Q')) and  v_PT_STAT_BLCK_RECL_CD_LKP_CUR='1') THEN  'CUR'       
                           WHEN  ( 
                                TOT_NEW_BAL_AMT>0 and TOT_NEW_BAL_AMT=TOT_UNPAID_FNCL_CHRG_AMT and v_PT_STAT_CHRG_OFF_LKP_PREV2='1' 
                                 and  NOT ( PREV_TOT_NEW_BAL_AMT>0 and PREV_CHRG_OFF_CD IN ('N','Q'))
                                 and (PREV_TOT_NEW_BAL_AMT>0 and v_PT_STAT_BLCK_RECL_CD_LKP_PRV='1') ) THEN 'CUR'
                           WHEN (  TOT_NEW_BAL_AMT=0 and PREV_TOT_NEW_BAL_AMT=PREV_TOT_UNPAID_FNCL_CHRG_AMT and v_PT_STAT_CHRG_OFF_LKP_PREV2='1'
                                 and  NOT ( PREV_TOT_NEW_BAL_AMT>0 and PREV_CHRG_OFF_CD IN ('N','Q' ))
                                 and (PREV_TOT_NEW_BAL_AMT>0 and v_PT_STAT_BLCK_RECL_CD_LKP_PRV='1')) THEN 'CUR'
                            ELSE 'DEF'  
                           END   
                     ELSE 
                        CASE  
                           WHEN CHRG_OFF_CD ='1' THEN  'CHG' 
                           WHEN ( BNS_DLQNT_DAY<120 and NOT (TOT_NEW_BAL_AMT> 0 AND  CHRG_OFF_CD IN ('N','Q')) ) THEN  'CUR'       
                            WHEN (   TOT_NEW_BAL_AMT>0 and TOT_NEW_BAL_AMT=TOT_UNPAID_FNCL_CHRG_AMT and CHRG_OFF_CD<>'1'
                                  and  NOT( PREV_TOT_NEW_BAL_AMT>0 and  PREV_CHRG_OFF_CD IN ('N','Q' ))
                                  and PREV_TOT_NEW_BAL_AMT>0 ) THEN 'CUR'

                            WHEN ( TOT_NEW_BAL_AMT=0 and PREV_TOT_NEW_BAL_AMT=PREV_TOT_UNPAID_FNCL_CHRG_AMT and PREV_CHRG_OFF_CD<>'1'
                                  and  NOT( PREV_TOT_NEW_BAL_AMT>0 and PREV_CHRG_OFF_CD IN ('N','Q' )) and PREV_TOT_NEW_BAL_AMT>0
                                ) THEN 'CUR'  
                            ELSE 'DEF'  
                           END   
                    END) 
            end) AS PIT_STAT_VER_2_CD,
            /* mark written-out accounts as W in STEP_DFLT_F */
            case when s.CHRG_OFF_CD='1' and s.BLOCK_RECL_CD like 'D%' then 'W' 
                 when s.STEP_PLN_SNAPSHOT_ID>0 then 'N' 
            end as STEP_F,
            CURRENT_TIMESTAMP(0),
            CURRENT_TIMESTAMP(0)
            FROM SNAP s left JOIN PREV_SNAP ps ON s.BASEL_ACCT_ID = ps.BASEL_ACCT_ID AND s.BASEL_CUST_ID  = ps.BASEL_CUST_ID
                        /* this join is used to check for CC product in order to determine 90d or 180d Pit above */
                        left join (SELECT DISTINCT STRIP(BASEL_PRD_CD) as BASEL_PRD_CD,
                                                   STRIP(SRC_PRD_CD) as SRC_PRD_CD, 
                                                   STRIP(SRC_SUB_PRD_CD) as SRC_SUB_PRD_CD 
                                    FROM &net_db..SRC_PRD_LKP 
                                    WHERE STRIP(PRD_SYS_CD)='KS' 
                                    AND &yrmth. BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH) SP ON s.PRD_CD=SP.SRC_PRD_CD AND s.SUB_PRD_CD=SP.SRC_SUB_PRD_CD
        ) by nzcon;
disconnect from NZCON; 
quit;



/* SPL Pit pre-Step */
%put Start and End Dates for SPL Models:;
%put start_period_dt: &start_period_dt ;
%put end_period_dt: &end_period_dt;

proc sql;
connect using NZRRAP as nzcon;
execute (delete from &RRAP_DB..PIT_STATUS_PRE_STEP where SRC_SYS_CD='SPL' and MTH_TM_ID=&mth_tm_id.) by nzcon;

execute (insert into &RRAP_DB..PIT_STATUS_PRE_STEP 
            (MTH_TM_ID, SRC_SYS_CD, BASEL_ACCT_ID, STEP_PLN_SNAPSHOT_ID, STEP_PLN_AGRMNT_NUM, PIT_STATUS_PRE_STEP, STEP_DFLT_F, INSRT_PROCESS_TMSTMP, UPDT_PROCESS_TMSTMP)
         select MTH_TM_ID, 
                'SPL' as SRC_SYS_CD,
                BASEL_ACCT_ID, 
                STEP_PLN_SNAPSHOT_ID,
                STEP_PLN_AGRMNT_NUM,
                case
                  when (RECD_STAT_CD = 9 or RECD_STAT_CD = 0 or RECD_STAT_CD is null) then 'CLO'
                  when (RECD_STAT_CD = 6 or RECD_STAT_CD = 7 or RECD_STAT_CD = 8) then 'CHG'
                  when CHRG_OFF_DT is not null then 'CHG'   
                  when DAY_ODUE >= 90 or RECD_STAT_CD = 5 then 'DEF'
                  when DAY_ODUE < 90 and RECD_STAT_CD = 4 then 'CUR'
                  else null
                end as PIT_STATUS,
                case when RECD_STAT_CD = 8 then 'W' 
                     when STEP_PLN_SNAPSHOT_ID>0 then 'N' 
                end as STEP_F,
                CURRENT_TIMESTAMP(0), 
                CURRENT_TIMESTAMP(0)
         from &RRAP_DB..BASEL_PSNL_LOAN_MTH_SNAPSHOT  
         where MTH_TM_ID = &mth_tm_id. and /* between &start_period_tm_key and &end_period_tm_key and */
               RECD_STAT_CD in (4,5,6,7,8) 
    ) by nzcon;
disconnect from nzcon;
quit;



/* MOR Pit pre-Step */
proc sql;
connect using NZRRAP as nzcon;
execute (delete from &RRAP_DB..PIT_STATUS_PRE_STEP where SRC_SYS_CD='MOR' and MTH_TM_ID=&mth_tm_id.) by nzcon;

execute (insert into &RRAP_DB..PIT_STATUS_PRE_STEP 
            (MTH_TM_ID, SRC_SYS_CD, BASEL_ACCT_ID, MORT_NUM, MORT_PROCESS_DATE, STEP_PLN_SNAPSHOT_ID, STEP_PLN_AGRMNT_NUM, PIT_STATUS_PRE_STEP, STEP_DFLT_F, INSRT_PROCESS_TMSTMP, UPDT_PROCESS_TMSTMP)
         select a.TM_ID, 
                'MOR' as SRC_SYS_CD,
                b.BASEL_ACCT_ID, 
                a.MORT_NUM,
                a.MTH_END_DT,
                b.STEP_PLN_SNAPSHOT_ID,
                b.STEP_PLN_AGRMNT_NUM,
                case 
                  when upper(a.COMM_TP)='RESIDENTIAL' and a.PD_OFF_DT is null and (( DLQNT_DAY<90 and DLQNT_MTH<4) and upper(nvl(a.FRCLSR_F, ''))<>'Y' and
                       CRNT_BAL<>0 and upper(nvl(a.LRA_STAT, ''))<>'Y' ) or CRNT_BAL<0 then 'CUR'
                  when (upper(a.COMM_TP)='RESIDENTIAL' and a.PD_OFF_DT is null and ((DLQNT_DAY>=90 or DLQNT_MTH>=4) or upper(a.FRCLSR_F)='Y' or upper(a.LRA_STAT)='Y') and CRNT_BAL>0) 
                       or (upper(a.COMM_TP)='RESIDENTIAL' and upper(a.FRCLSR_F)='Y' and upper(a.PD_OFF_F)='Y' and (max(CRNT_BAL, nvl(-TOT_SUSP_BAL, 0))>0)) then 'DEF'
                end as NEW_STATUS, 
                case when b.STEP_PLN_SNAPSHOT_ID>0 then 'N' end as STEP_F,
                CURRENT_TIMESTAMP(0), 
                CURRENT_TIMESTAMP(0)
         from &FRG_DB..AIRB_MORT_MTH_SNAPSHOT a 
         left join &RRAP_DB..BASEL_MORT_MTH_SNAPSHOT b on a.MORT_NUM = b.MORT_NUM and a.TM_ID = b.MTH_TM_ID 
         where tm_id = &mth_tm_id.
    ) by nzcon;
disconnect from nzcon;
quit;



/* lag UPDT_PROCESS_TMSTMP */
data _null_;
  x = sleep(20,1);
run;


/* Step plan cross default */
proc sql noprint;
connect using NZRRAP as nzcon;
select cnt into :loaded from connection to nzcon 
    (select count(*) as cnt from &RRAP_DB..PIT_STATUS_PRE_STEP where MTH_TM_ID = &mth_tm_id.);
%PUT NOTE: %trim(&loaded) rows were inserted into EDRTLRP1D.PIT_STATUS_PRE_STEP;

execute (update &RRAP_DB..PIT_STATUS_PRE_STEP 
            set CROSS_DFLT_PIT_STATUS = PIT_STATUS_PRE_STEP 
            where MTH_TM_ID = &mth_tm_id. and PIT_STATUS_PRE_STEP is not null
    ) by nzcon;

execute (update &RRAP_DB..PIT_STATUS_PRE_STEP 
            set STEP_DFLT_F = 'Y'
                ,CROSS_DFLT_PIT_OVERRIDE_F = case when PIT_STATUS_PRE_STEP = 'CUR' then 'Y' else 'N' end
                ,CROSS_DFLT_PIT_STATUS = case when PIT_STATUS_PRE_STEP = 'CUR' then 'DEF' else PIT_STATUS_PRE_STEP end
                ,UPDT_PROCESS_TMSTMP = CURRENT_TIMESTAMP(0) 
            where MTH_TM_ID = &mth_tm_id. 
              and STEP_DFLT_F <> 'W' 
              and STEP_PLN_SNAPSHOT_ID in
                    (select distinct STEP_PLN_SNAPSHOT_ID 
                     from &RRAP_DB..PIT_STATUS_PRE_STEP  
                     where MTH_TM_ID = &mth_tm_id. 
                       and PIT_STATUS_PRE_STEP in ('CHG','DEF') 
                       and STEP_PLN_SNAPSHOT_ID > 0 
                       and STEP_DFLT_F <> 'W')
    ) by nzcon;
disconnect from nzcon;
quit;
