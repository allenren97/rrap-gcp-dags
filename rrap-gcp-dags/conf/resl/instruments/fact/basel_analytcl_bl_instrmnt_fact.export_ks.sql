WITH spine AS (
    SELECT
        OBSN_DT,
        STREAM,
        BASEL_ACCT_ID,
        SRC_SYS_CD,
        PIT_STAT_CD
    FROM instruments.PIT_STAT_CD
    WHERE OBSN_DT = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
      AND STREAM = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
      AND SRC_SYS_CD = 'KS'
)
SELECT
    sp.OBSN_DT,
    sp.STREAM,
    {{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }} AS MTH_TM_ID,
    sp.SRC_SYS_CD,
    sp.BASEL_ACCT_ID,
    sp.PIT_STAT_CD,
    j0.AMORT AS AMORT,
    j1.CCAR_BASEL_PRD_TP_NM AS CCAR_BASEL_PRD_TP_NM,
    j2.CCAR_F AS CCAR_F,
    fe_md24.MONTH_DEF_24M AS CONS_DFT_MTH_CNT,
    j4.CRNT_LTV_RTO AS CRNT_LTV_RTO,
    j5.CRNT_PRPTY_VAL_AMT AS CRNT_PRPTY_VAL_AMT,
    j6.DLGD_F AS DLGD_F,
    j7.DLGD_FLR AS DLGD_FLR,
    j8.DLGD_RPTG_RTO AS DLGD_RPTG_RTO,
    j9.EAD_ACCT_SCORE AS EAD_ACCT_SCORE,
    j10.EAD_BASEL_SEG_NUM AS EAD_BASEL_SEG_NUM,
    j11.EAD_FINAL_RPTG_RTO AS EAD_FINAL_RPTG_RTO,
    j12.EAD_FLR AS EAD_FLR,
    j13.EAD_FLRD_RPTG_RTO AS EAD_FLRD_RPTG_RTO,
    j14.EAD_LD_PV_AD_SV_DT_RPTG_RTO AS EAD_LD_PV_AD_SV_DT_RPTG_RTO,
    j15.EAD_LD_PV_AD_SV_RPTG_RTO AS EAD_LD_PV_AD_SV_RPTG_RTO,
    j16.EAD_LR_PV_AD_RPTG_RTO AS EAD_LR_PV_AD_RPTG_RTO,
    j17.EAD_LR_PV_AD_SV_DT_AA_RTO AS EAD_LR_PV_AD_SV_DT_AA_RTO,
    j18.EAD_LR_PV_RPTG_RTO AS EAD_LR_PV_RPTG_RTO,
    j19.EAD_LR_RPTG_RTO AS EAD_LR_RPTG_RTO,
    j20.EAD_MODEL_NM AS EAD_MODEL_NM,
    j21.EAD_MODEL_VER AS EAD_MODEL_VER,
    j22.EAD_SEG_VER AS EAD_SEG_VER,
    j23.EAD_UNADJUSTED_RPTG_RTO AS EAD_UNADJUSTED_RPTG_RTO,
    j24.EXPOSURE AS EXPOSURE,
    j25.EXPOSURE_SECURED AS EXPOSURE_SECURED,
    j26.EXPOSURE_SECURED_MAXIMUM AS EXPOSURE_SECURED_MAXIMUM,
    j27.EXPOSURE_UNSECURED AS EXPOSURE_UNSECURED,
    j28.EXPSR_AT_DFT_RTO AS EXPSR_AT_DFT_RTO,
    j29.FULLY_SECURED_F AS FULLY_SECURED_F,
    j30.INDEXED_CCLTV_RTO AS INDEXED_LOAN_TO_VAL_RTO,
    j31.INDEXED_PRPTY_VAL_AMT AS INDEXED_PRPTY_VAL_AMT,
    fe_mn.MORT_NUM AS MORT_NUM,
    j32.INDEX_TERANETV AS INDEX_TERANETV,
    j33.INSUR_F AS INSUR_F,
    j34.INTR_ACCR_AMT AS INTR_ACCR_AMT,
    j35.LGD_ACCT_SCORE AS LGD_ACCT_SCORE,
    j36.LGD_BASEL_SEG_NUM AS LGD_BASEL_SEG_NUM,
    j37.LGD_FINAL_RPTG_RTO AS LGD_FINAL_RPTG_RTO,
    j38.LGD_FLR AS LGD_FLR,
    j39.LGD_FLRD_RPTG_RTO AS LGD_FLRD_RPTG_RTO,
    j40.LGD_LD_PV_AD_SV_DT_RPTG_RTO AS LGD_LD_PV_AD_SV_DT_RPTG_RTO,
    j41.LGD_LD_PV_AD_SV_RPTG_RTO AS LGD_LD_PV_AD_SV_RPTG_RTO,
    j42.LGD_LR_PV_AD_RPTG_RTO AS LGD_LR_PV_AD_RPTG_RTO,
    j43.LGD_LR_PV_AD_SV_DT_AA_RTO AS LGD_LR_PV_AD_SV_DT_AA_RTO,
    j44.LGD_LR_PV_RPTG_RTO AS LGD_LR_PV_RPTG_RTO,
    j45.LGD_LR_RPTG_RTO AS LGD_LR_RPTG_RTO,
    j46.LGD_MODEL_NM AS LGD_MODEL_NM,
    j47.LGD_MODEL_VER AS LGD_MODEL_VER,
    j48.LGD_SEG_VER AS LGD_SEG_VER,
    j49.LGD_UNADJUSTED_RPTG_RTO AS LGD_UNADJUSTED_RPTG_RTO,
    j50.LNG_RUN_LGD_ADD_ON_RTO AS LNG_RUN_LGD_ADD_ON_RTO,
    j51.METRPL_AREA_NM AS METRPL_AREA_NM,
    j52.NCR_PD_BAND_KEY_VAL AS NCR_PD_BAND_KEY_VAL,
    j53.PD_90_DAY_F AS PD_90_DAY_F,
    j54.PD_ACCT_SCORE AS PD_ACCT_SCORE,
    j55.PD_BAND AS PD_BAND,
    j56.PD_BASEL_SEG_NUM AS PD_BASEL_SEG_NUM,
    j57.PD_FINAL_RPTG_RTO AS PD_FINAL_RPTG_RTO,
    j58.PD_FLR AS PD_FLR,
    j59.PD_FLRD_RPTG_RTO AS PD_FLRD_RPTG_RTO,
    j60.PD_LD_PV_AD_SV_RPTG_RTO AS PD_LD_PV_AD_SV_RPTG_RTO,
    j61.PD_LR_PV_AD_RPTG_RTO AS PD_LR_PV_AD_RPTG_RTO,
    j62.PD_LR_PV_AD_SV_DT_AA_RTO AS PD_LR_PV_AD_SV_DT_AA_RTO,
    j63.PD_LR_PV_RPTG_RTO AS PD_LR_PV_RPTG_RTO,
    j64.PD_LR_RPTG_RTO AS PD_LR_RPTG_RTO,
    j65.PD_MODEL_NM AS PD_MODEL_NM,
    j66.PD_MODEL_VER AS PD_MODEL_VER,
    j67.PD_SEG_VER AS PD_SEG_VER,
    j68.PD_UNADJUSTED_RPTG_RTO AS PD_UNADJUSTED_RPTG_RTO,
    j69.PMI_LGD_INSURED_RPTG_RTO AS PMI_LGD_INSURED_RPTG_RTO,
    j70.PMI_LGD_UNADJUSTED_RPTG_RTO AS PMI_LGD_UNADJUSTED_RPTG_RTO,
    j71.PREV_12_QTR_PRPTY_VAL_AMT AS PREV_12_QTR_PRPTY_VAL_AMT,
    j72.PRE_INSURANCE_LGD AS PRE_INSURANCE_LGD,
    j73.PRPTY_VAL_CORR_PCTG AS PRPTY_VAL_CORR_PCTG,
    j74.RESIDUAL_MAT AS RESIDUAL_MAT,
    j75.RNTL_PRPTY_F AS RNTL_PRPTY_F,
    j76.UNDRAWN AS UNDRAWN,
    j77.UNINSURED_DLGD_RTO AS UNINSURED_DLGD_RTO,
    j78.UNINSURED_FLRD_LGD_RTO AS UNINSURED_FLRD_LGD_RTO,
    j79.UNINSURED_LGD_RTO AS UNINSURED_LGD_RTO,
    j80.UNINSURED_LGD_SEG_NUM AS UNINSURED_LGD_SEG_NUM,
    j81.WEIGHT_SECURED AS WEIGHT_SECURED,
    j82.WEIGHT_UNSECURED AS WEIGHT_UNSECURED,
    fe_tf.TRANSACTOR_FLAG_QRR AS TRANSACTOR_FLAG_QRR,
    fe_cm.CMHC_F AS CMHC_F,
    fe_dr.DRAWN AS DRAWN,
    CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
    CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
FROM spine sp
        LEFT JOIN instruments.AMORT j0
            ON sp.BASEL_ACCT_ID = j0.BASEL_ACCT_ID
           AND sp.OBSN_DT = j0.OBSN_DT
           AND sp.STREAM = j0.STREAM
        LEFT JOIN instruments.CCAR_BASEL_PRD_TP_NM j1
            ON sp.BASEL_ACCT_ID = j1.BASEL_ACCT_ID
           AND sp.OBSN_DT = j1.OBSN_DT
           AND sp.STREAM = j1.STREAM
        LEFT JOIN instruments.CCAR_F j2
            ON sp.BASEL_ACCT_ID = j2.BASEL_ACCT_ID
           AND sp.OBSN_DT = j2.OBSN_DT
           AND sp.STREAM = j2.STREAM
        LEFT JOIN features.MONTH_DEF_24M fe_md24
            ON sp.BASEL_ACCT_ID = fe_md24.BASEL_ACCT_ID
           AND sp.OBSN_DT = fe_md24.OBSN_DT
           AND sp.SRC_SYS_CD = fe_md24.SRC_SYS_CD
        LEFT JOIN instruments.CRNT_LTV_RTO j4
            ON sp.BASEL_ACCT_ID = j4.BASEL_ACCT_ID
           AND sp.OBSN_DT = j4.OBSN_DT
           AND sp.STREAM = j4.STREAM
        LEFT JOIN instruments.CRNT_PRPTY_VAL_AMT j5
            ON sp.BASEL_ACCT_ID = j5.BASEL_ACCT_ID
           AND sp.OBSN_DT = j5.OBSN_DT
           AND sp.STREAM = j5.STREAM
        LEFT JOIN instruments.DLGD_F j6
            ON sp.BASEL_ACCT_ID = j6.BASEL_ACCT_ID
           AND sp.OBSN_DT = j6.OBSN_DT
           AND sp.STREAM = j6.STREAM
        LEFT JOIN instruments.DLGD_FLR j7
            ON sp.BASEL_ACCT_ID = j7.BASEL_ACCT_ID
           AND sp.OBSN_DT = j7.OBSN_DT
           AND sp.STREAM = j7.STREAM
        LEFT JOIN instruments.DLGD_RPTG_RTO j8
            ON sp.BASEL_ACCT_ID = j8.BASEL_ACCT_ID
           AND sp.OBSN_DT = j8.OBSN_DT
           AND sp.STREAM = j8.STREAM
        LEFT JOIN instruments.EAD_ACCT_SCORE j9
            ON sp.BASEL_ACCT_ID = j9.BASEL_ACCT_ID
           AND sp.OBSN_DT = j9.OBSN_DT
           AND sp.STREAM = j9.STREAM
        LEFT JOIN instruments.EAD_BASEL_SEG_NUM j10
            ON sp.BASEL_ACCT_ID = j10.BASEL_ACCT_ID
           AND sp.OBSN_DT = j10.OBSN_DT
           AND sp.STREAM = j10.STREAM
        LEFT JOIN instruments.EAD_FINAL_RPTG_RTO j11
            ON sp.BASEL_ACCT_ID = j11.BASEL_ACCT_ID
           AND sp.OBSN_DT = j11.OBSN_DT
           AND sp.STREAM = j11.STREAM
        LEFT JOIN instruments.EAD_FLR j12
            ON sp.BASEL_ACCT_ID = j12.BASEL_ACCT_ID
           AND sp.OBSN_DT = j12.OBSN_DT
           AND sp.STREAM = j12.STREAM
        LEFT JOIN instruments.EAD_FLRD_RPTG_RTO j13
            ON sp.BASEL_ACCT_ID = j13.BASEL_ACCT_ID
           AND sp.OBSN_DT = j13.OBSN_DT
           AND sp.STREAM = j13.STREAM
        LEFT JOIN instruments.EAD_LD_PV_AD_SV_DT_RPTG_RTO j14
            ON sp.BASEL_ACCT_ID = j14.BASEL_ACCT_ID
           AND sp.OBSN_DT = j14.OBSN_DT
           AND sp.STREAM = j14.STREAM
        LEFT JOIN instruments.EAD_LD_PV_AD_SV_RPTG_RTO j15
            ON sp.BASEL_ACCT_ID = j15.BASEL_ACCT_ID
           AND sp.OBSN_DT = j15.OBSN_DT
           AND sp.STREAM = j15.STREAM
        LEFT JOIN instruments.EAD_LR_PV_AD_RPTG_RTO j16
            ON sp.BASEL_ACCT_ID = j16.BASEL_ACCT_ID
           AND sp.OBSN_DT = j16.OBSN_DT
           AND sp.STREAM = j16.STREAM
        LEFT JOIN instruments.EAD_LR_PV_AD_SV_DT_AA_RTO j17
            ON sp.BASEL_ACCT_ID = j17.BASEL_ACCT_ID
           AND sp.OBSN_DT = j17.OBSN_DT
           AND sp.STREAM = j17.STREAM
        LEFT JOIN instruments.EAD_LR_PV_RPTG_RTO j18
            ON sp.BASEL_ACCT_ID = j18.BASEL_ACCT_ID
           AND sp.OBSN_DT = j18.OBSN_DT
           AND sp.STREAM = j18.STREAM
        LEFT JOIN instruments.EAD_LR_RPTG_RTO j19
            ON sp.BASEL_ACCT_ID = j19.BASEL_ACCT_ID
           AND sp.OBSN_DT = j19.OBSN_DT
           AND sp.STREAM = j19.STREAM
        LEFT JOIN instruments.EAD_MODEL_NM j20
            ON sp.BASEL_ACCT_ID = j20.BASEL_ACCT_ID
           AND sp.OBSN_DT = j20.OBSN_DT
           AND sp.STREAM = j20.STREAM
        LEFT JOIN instruments.EAD_MODEL_VER j21
            ON sp.BASEL_ACCT_ID = j21.BASEL_ACCT_ID
           AND sp.OBSN_DT = j21.OBSN_DT
           AND sp.STREAM = j21.STREAM
        LEFT JOIN instruments.EAD_SEG_VER j22
            ON sp.BASEL_ACCT_ID = j22.BASEL_ACCT_ID
           AND sp.OBSN_DT = j22.OBSN_DT
           AND sp.STREAM = j22.STREAM
        LEFT JOIN instruments.EAD_UNADJUSTED_RPTG_RTO j23
            ON sp.BASEL_ACCT_ID = j23.BASEL_ACCT_ID
           AND sp.OBSN_DT = j23.OBSN_DT
           AND sp.STREAM = j23.STREAM
        LEFT JOIN instruments.EXPOSURE j24
            ON sp.BASEL_ACCT_ID = j24.BASEL_ACCT_ID
           AND sp.OBSN_DT = j24.OBSN_DT
           AND sp.STREAM = j24.STREAM
        LEFT JOIN instruments.EXPOSURE_SECURED j25
            ON sp.BASEL_ACCT_ID = j25.BASEL_ACCT_ID
           AND sp.OBSN_DT = j25.OBSN_DT
           AND sp.STREAM = j25.STREAM
        LEFT JOIN instruments.EXPOSURE_SECURED_MAXIMUM j26
            ON sp.BASEL_ACCT_ID = j26.BASEL_ACCT_ID
           AND sp.OBSN_DT = j26.OBSN_DT
           AND sp.STREAM = j26.STREAM
        LEFT JOIN instruments.EXPOSURE_UNSECURED j27
            ON sp.BASEL_ACCT_ID = j27.BASEL_ACCT_ID
           AND sp.OBSN_DT = j27.OBSN_DT
           AND sp.STREAM = j27.STREAM
        LEFT JOIN instruments.EXPSR_AT_DFT_RTO j28
            ON sp.BASEL_ACCT_ID = j28.BASEL_ACCT_ID
           AND sp.OBSN_DT = j28.OBSN_DT
           AND sp.STREAM = j28.STREAM
        LEFT JOIN instruments.FULLY_SECURED_F j29
            ON sp.BASEL_ACCT_ID = j29.BASEL_ACCT_ID
           AND sp.OBSN_DT = j29.OBSN_DT
           AND sp.STREAM = j29.STREAM
        LEFT JOIN instruments.INDEXED_CCLTV_RTO j30
            ON sp.BASEL_ACCT_ID = j30.BASEL_ACCT_ID
           AND sp.OBSN_DT = j30.OBSN_DT
           AND sp.STREAM = j30.STREAM
        LEFT JOIN instruments.INDEXED_PRPTY_VAL_AMT j31
            ON sp.BASEL_ACCT_ID = j31.BASEL_ACCT_ID
           AND sp.OBSN_DT = j31.OBSN_DT
           AND sp.STREAM = j31.STREAM
        LEFT JOIN instruments.INDEX_TERANETV_CMA j32
            ON sp.BASEL_ACCT_ID = j32.BASEL_ACCT_ID
           AND sp.OBSN_DT = j32.OBSN_DT
           AND sp.STREAM = j32.STREAM
        LEFT JOIN instruments.INSUR_F j33
            ON sp.BASEL_ACCT_ID = j33.BASEL_ACCT_ID
           AND sp.OBSN_DT = j33.OBSN_DT
           AND sp.STREAM = j33.STREAM
        LEFT JOIN instruments.INTR_ACCR_AMT j34
            ON sp.BASEL_ACCT_ID = j34.BASEL_ACCT_ID
           AND sp.OBSN_DT = j34.OBSN_DT
           AND sp.STREAM = j34.STREAM
        LEFT JOIN instruments.LGD_ACCT_SCORE j35
            ON sp.BASEL_ACCT_ID = j35.BASEL_ACCT_ID
           AND sp.OBSN_DT = j35.OBSN_DT
           AND sp.STREAM = j35.STREAM
        LEFT JOIN instruments.LGD_BASEL_SEG_NUM j36
            ON sp.BASEL_ACCT_ID = j36.BASEL_ACCT_ID
           AND sp.OBSN_DT = j36.OBSN_DT
           AND sp.STREAM = j36.STREAM
        LEFT JOIN instruments.LGD_FINAL_RPTG_RTO j37
            ON sp.BASEL_ACCT_ID = j37.BASEL_ACCT_ID
           AND sp.OBSN_DT = j37.OBSN_DT
           AND sp.STREAM = j37.STREAM
        LEFT JOIN instruments.LGD_FLR j38
            ON sp.BASEL_ACCT_ID = j38.BASEL_ACCT_ID
           AND sp.OBSN_DT = j38.OBSN_DT
           AND sp.STREAM = j38.STREAM
        LEFT JOIN instruments.LGD_FLRD_RPTG_RTO j39
            ON sp.BASEL_ACCT_ID = j39.BASEL_ACCT_ID
           AND sp.OBSN_DT = j39.OBSN_DT
           AND sp.STREAM = j39.STREAM
        LEFT JOIN instruments.LGD_LD_PV_AD_SV_DT_RPTG_RTO j40
            ON sp.BASEL_ACCT_ID = j40.BASEL_ACCT_ID
           AND sp.OBSN_DT = j40.OBSN_DT
           AND sp.STREAM = j40.STREAM
        LEFT JOIN instruments.LGD_LD_PV_AD_SV_RPTG_RTO j41
            ON sp.BASEL_ACCT_ID = j41.BASEL_ACCT_ID
           AND sp.OBSN_DT = j41.OBSN_DT
           AND sp.STREAM = j41.STREAM
        LEFT JOIN instruments.LGD_LR_PV_AD_RPTG_RTO j42
            ON sp.BASEL_ACCT_ID = j42.BASEL_ACCT_ID
           AND sp.OBSN_DT = j42.OBSN_DT
           AND sp.STREAM = j42.STREAM
        LEFT JOIN instruments.LGD_LR_PV_AD_SV_DT_AA_RTO j43
            ON sp.BASEL_ACCT_ID = j43.BASEL_ACCT_ID
           AND sp.OBSN_DT = j43.OBSN_DT
           AND sp.STREAM = j43.STREAM
        LEFT JOIN instruments.LGD_LR_PV_RPTG_RTO j44
            ON sp.BASEL_ACCT_ID = j44.BASEL_ACCT_ID
           AND sp.OBSN_DT = j44.OBSN_DT
           AND sp.STREAM = j44.STREAM
        LEFT JOIN instruments.LGD_LR_RPTG_RTO j45
            ON sp.BASEL_ACCT_ID = j45.BASEL_ACCT_ID
           AND sp.OBSN_DT = j45.OBSN_DT
           AND sp.STREAM = j45.STREAM
        LEFT JOIN instruments.LGD_MODEL_NM j46
            ON sp.BASEL_ACCT_ID = j46.BASEL_ACCT_ID
           AND sp.OBSN_DT = j46.OBSN_DT
           AND sp.STREAM = j46.STREAM
        LEFT JOIN instruments.LGD_MODEL_VER j47
            ON sp.BASEL_ACCT_ID = j47.BASEL_ACCT_ID
           AND sp.OBSN_DT = j47.OBSN_DT
           AND sp.STREAM = j47.STREAM
        LEFT JOIN instruments.LGD_SEG_VER j48
            ON sp.BASEL_ACCT_ID = j48.BASEL_ACCT_ID
           AND sp.OBSN_DT = j48.OBSN_DT
           AND sp.STREAM = j48.STREAM
        LEFT JOIN instruments.LGD_UNADJUSTED_RPTG_RTO j49
            ON sp.BASEL_ACCT_ID = j49.BASEL_ACCT_ID
           AND sp.OBSN_DT = j49.OBSN_DT
           AND sp.STREAM = j49.STREAM
        LEFT JOIN instruments.LNG_RUN_LGD_ADD_ON_RTO j50
            ON sp.BASEL_ACCT_ID = j50.BASEL_ACCT_ID
           AND sp.OBSN_DT = j50.OBSN_DT
           AND sp.STREAM = j50.STREAM
        LEFT JOIN instruments.METRPL_AREA_NM j51
            ON sp.BASEL_ACCT_ID = j51.BASEL_ACCT_ID
           AND sp.OBSN_DT = j51.OBSN_DT
           AND sp.STREAM = j51.STREAM
        LEFT JOIN instruments.NCR_PD_BAND_KEY_VAL j52
            ON sp.BASEL_ACCT_ID = j52.BASEL_ACCT_ID
           AND sp.OBSN_DT = j52.OBSN_DT
           AND sp.STREAM = j52.STREAM
        LEFT JOIN instruments.PD_90_DAY_F j53
            ON sp.BASEL_ACCT_ID = j53.BASEL_ACCT_ID
           AND sp.OBSN_DT = j53.OBSN_DT
           AND sp.STREAM = j53.STREAM
        LEFT JOIN instruments.PD_ACCT_SCORE j54
            ON sp.BASEL_ACCT_ID = j54.BASEL_ACCT_ID
           AND sp.OBSN_DT = j54.OBSN_DT
           AND sp.STREAM = j54.STREAM
        LEFT JOIN instruments.PD_BAND j55
            ON sp.BASEL_ACCT_ID = j55.BASEL_ACCT_ID
           AND sp.OBSN_DT = j55.OBSN_DT
           AND sp.STREAM = j55.STREAM
        LEFT JOIN instruments.PD_BASEL_SEG_NUM j56
            ON sp.BASEL_ACCT_ID = j56.BASEL_ACCT_ID
           AND sp.OBSN_DT = j56.OBSN_DT
           AND sp.STREAM = j56.STREAM
        LEFT JOIN instruments.PD_FINAL_RPTG_RTO j57
            ON sp.BASEL_ACCT_ID = j57.BASEL_ACCT_ID
           AND sp.OBSN_DT = j57.OBSN_DT
           AND sp.STREAM = j57.STREAM
        LEFT JOIN instruments.PD_FLR j58
            ON sp.BASEL_ACCT_ID = j58.BASEL_ACCT_ID
           AND sp.OBSN_DT = j58.OBSN_DT
           AND sp.STREAM = j58.STREAM
        LEFT JOIN instruments.PD_FLRD_RPTG_RTO j59
            ON sp.BASEL_ACCT_ID = j59.BASEL_ACCT_ID
           AND sp.OBSN_DT = j59.OBSN_DT
           AND sp.STREAM = j59.STREAM
        LEFT JOIN instruments.PD_LD_PV_AD_SV_RPTG_RTO j60
            ON sp.BASEL_ACCT_ID = j60.BASEL_ACCT_ID
           AND sp.OBSN_DT = j60.OBSN_DT
           AND sp.STREAM = j60.STREAM
        LEFT JOIN instruments.PD_LR_PV_AD_RPTG_RTO j61
            ON sp.BASEL_ACCT_ID = j61.BASEL_ACCT_ID
           AND sp.OBSN_DT = j61.OBSN_DT
           AND sp.STREAM = j61.STREAM
        LEFT JOIN instruments.PD_LR_PV_AD_SV_DT_AA_RTO j62
            ON sp.BASEL_ACCT_ID = j62.BASEL_ACCT_ID
           AND sp.OBSN_DT = j62.OBSN_DT
           AND sp.STREAM = j62.STREAM
        LEFT JOIN instruments.PD_LR_PV_RPTG_RTO j63
            ON sp.BASEL_ACCT_ID = j63.BASEL_ACCT_ID
           AND sp.OBSN_DT = j63.OBSN_DT
           AND sp.STREAM = j63.STREAM
        LEFT JOIN instruments.PD_LR_RPTG_RTO j64
            ON sp.BASEL_ACCT_ID = j64.BASEL_ACCT_ID
           AND sp.OBSN_DT = j64.OBSN_DT
           AND sp.STREAM = j64.STREAM
        LEFT JOIN instruments.PD_MODEL_NM j65
            ON sp.BASEL_ACCT_ID = j65.BASEL_ACCT_ID
           AND sp.OBSN_DT = j65.OBSN_DT
           AND sp.STREAM = j65.STREAM
        LEFT JOIN instruments.PD_MODEL_VER j66
            ON sp.BASEL_ACCT_ID = j66.BASEL_ACCT_ID
           AND sp.OBSN_DT = j66.OBSN_DT
           AND sp.STREAM = j66.STREAM
        LEFT JOIN instruments.PD_SEG_VER j67
            ON sp.BASEL_ACCT_ID = j67.BASEL_ACCT_ID
           AND sp.OBSN_DT = j67.OBSN_DT
           AND sp.STREAM = j67.STREAM
        LEFT JOIN instruments.PD_UNADJUSTED_RPTG_RTO j68
            ON sp.BASEL_ACCT_ID = j68.BASEL_ACCT_ID
           AND sp.OBSN_DT = j68.OBSN_DT
           AND sp.STREAM = j68.STREAM
        LEFT JOIN instruments.PMI_LGD_INSURED_RPTG_RTO j69
            ON sp.BASEL_ACCT_ID = j69.BASEL_ACCT_ID
           AND sp.OBSN_DT = j69.OBSN_DT
           AND sp.STREAM = j69.STREAM
        LEFT JOIN instruments.PMI_LGD_UNADJUSTED_RPTG_RTO j70
            ON sp.BASEL_ACCT_ID = j70.BASEL_ACCT_ID
           AND sp.OBSN_DT = j70.OBSN_DT
           AND sp.STREAM = j70.STREAM
        LEFT JOIN instruments.PREV_12_QTR_PRPTY_VAL_AMT j71
            ON sp.BASEL_ACCT_ID = j71.BASEL_ACCT_ID
           AND sp.OBSN_DT = j71.OBSN_DT
           AND sp.STREAM = j71.STREAM
        LEFT JOIN instruments.PRE_INSURANCE_LGD j72
            ON sp.BASEL_ACCT_ID = j72.BASEL_ACCT_ID
           AND sp.OBSN_DT = j72.OBSN_DT
           AND sp.STREAM = j72.STREAM
        LEFT JOIN instruments.PRPTY_VAL_CORR_PCTG j73
            ON sp.BASEL_ACCT_ID = j73.BASEL_ACCT_ID
           AND sp.OBSN_DT = j73.OBSN_DT
           AND sp.STREAM = j73.STREAM
        LEFT JOIN instruments.RESIDUAL_MAT j74
            ON sp.BASEL_ACCT_ID = j74.BASEL_ACCT_ID
           AND sp.OBSN_DT = j74.OBSN_DT
           AND sp.STREAM = j74.STREAM
        LEFT JOIN instruments.RNTL_PRPTY_F j75
            ON sp.BASEL_ACCT_ID = j75.BASEL_ACCT_ID
           AND sp.OBSN_DT = j75.OBSN_DT
           AND sp.STREAM = j75.STREAM
        LEFT JOIN instruments.UNDRAWN j76
            ON sp.BASEL_ACCT_ID = j76.BASEL_ACCT_ID
           AND sp.OBSN_DT = j76.OBSN_DT
           AND sp.STREAM = j76.STREAM
        LEFT JOIN instruments.UNINSURED_DLGD_RTO j77
            ON sp.BASEL_ACCT_ID = j77.BASEL_ACCT_ID
           AND sp.OBSN_DT = j77.OBSN_DT
           AND sp.STREAM = j77.STREAM
        LEFT JOIN instruments.UNINSURED_FLRD_LGD_RTO j78
            ON sp.BASEL_ACCT_ID = j78.BASEL_ACCT_ID
           AND sp.OBSN_DT = j78.OBSN_DT
           AND sp.STREAM = j78.STREAM
        LEFT JOIN instruments.UNINSURED_LGD_RTO j79
            ON sp.BASEL_ACCT_ID = j79.BASEL_ACCT_ID
           AND sp.OBSN_DT = j79.OBSN_DT
           AND sp.STREAM = j79.STREAM
        LEFT JOIN instruments.UNINSURED_LGD_SEG_NUM j80
            ON sp.BASEL_ACCT_ID = j80.BASEL_ACCT_ID
           AND sp.OBSN_DT = j80.OBSN_DT
           AND sp.STREAM = j80.STREAM
        LEFT JOIN instruments.WEIGHT_SECURED j81
            ON sp.BASEL_ACCT_ID = j81.BASEL_ACCT_ID
           AND sp.OBSN_DT = j81.OBSN_DT
           AND sp.STREAM = j81.STREAM
        LEFT JOIN instruments.WEIGHT_UNSECURED j82
            ON sp.BASEL_ACCT_ID = j82.BASEL_ACCT_ID
           AND sp.OBSN_DT = j82.OBSN_DT
           AND sp.STREAM = j82.STREAM
        LEFT JOIN features.MORT_NUM fe_mn
            ON sp.BASEL_ACCT_ID = fe_mn.BASEL_ACCT_ID
           AND sp.OBSN_DT = fe_mn.OBSN_DT
           AND sp.SRC_SYS_CD = fe_mn.SRC_SYS_CD
        LEFT JOIN features.TRANSACTOR_FLAG_QRR fe_tf
            ON sp.BASEL_ACCT_ID = fe_tf.BASEL_ACCT_ID
           AND sp.OBSN_DT = fe_tf.OBSN_DT
           
        LEFT JOIN features.CMHC_F fe_cm
            ON sp.BASEL_ACCT_ID = fe_cm.BASEL_ACCT_ID
           AND sp.OBSN_DT = fe_cm.OBSN_DT
           
        LEFT JOIN features.DRAWN fe_dr
            ON sp.BASEL_ACCT_ID = fe_dr.BASEL_ACCT_ID
           AND sp.OBSN_DT = fe_dr.OBSN_DT
           