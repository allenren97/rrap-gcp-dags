"""
Gather instruments.BASEL_ANALYTCL_BL_INSTRUMNT_FACT from instruments.* column tables.

Mirrors J_RRAP_2700 / J_PLL_BASEL_ANALYTCL_BL_INSTRMNT_FACT output shape by left-joining
all account-level instruments.fact loaders on (OBSN_DT, BASEL_ACCT_ID, STREAM).

Spine: instruments.PIT_STAT_CD (KS, MOR, SPL, TNG-MOR).
SQL: conf/{non_resl,resl,ifrs9}/instruments/fact/basel_analytcl_bl_instrmnt_fact.export_{ks,mor,spl,tng}.sql

Prerequisite: run instrument fact column DAGs for the process month before this job.
instruments.FINAL_RTO is excluded (model/segment grain, not account grain).
"""

_INSTRUMENT_TABLES = [
    "AMORT",
    "CCAR_BASEL_PRD_TP_NM",
    "CCAR_F",
    "CONS_DFT_MTH_CNT",
    "CRNT_LTV_RTO",
    "CRNT_PRPTY_VAL_AMT",
    "DLGD_F",
    "DLGD_FLR",
    "DLGD_RPTG_RTO",
    "EAD_ACCT_SCORE",
    "EAD_BASEL_SEG_NUM",
    "EAD_FINAL_RPTG_RTO",
    "EAD_FLR",
    "EAD_FLRD_RPTG_RTO",
    "EAD_LD_PV_AD_SV_DT_RPTG_RTO",
    "EAD_LD_PV_AD_SV_RPTG_RTO",
    "EAD_LR_PV_AD_RPTG_RTO",
    "EAD_LR_PV_AD_SV_DT_AA_RTO",
    "EAD_LR_PV_RPTG_RTO",
    "EAD_LR_RPTG_RTO",
    "EAD_MODEL_NM",
    "EAD_MODEL_VER",
    "EAD_SEG_VER",
    "EAD_UNADJUSTED_RPTG_RTO",
    "EXPOSURE",
    "EXPOSURE_SECURED",
    "EXPOSURE_SECURED_MAXIMUM",
    "EXPOSURE_UNSECURED",
    "EXPSR_AT_DFT_RTO",
    "FULLY_SECURED_F",
    "INDEXED_CCLTV_RTO",
    "INDEXED_PRPTY_VAL_AMT",
    "INDEX_TERANETV_CMA",
    "INSUR_F",
    "INTR_ACCR_AMT",
    "LGD_ACCT_SCORE",
    "LGD_BASEL_SEG_NUM",
    "LGD_FINAL_RPTG_RTO",
    "LGD_FLR",
    "LGD_FLRD_RPTG_RTO",
    "LGD_LD_PV_AD_SV_DT_RPTG_RTO",
    "LGD_LD_PV_AD_SV_RPTG_RTO",
    "LGD_LR_PV_AD_RPTG_RTO",
    "LGD_LR_PV_AD_SV_DT_AA_RTO",
    "LGD_LR_PV_RPTG_RTO",
    "LGD_LR_RPTG_RTO",
    "LGD_MODEL_NM",
    "LGD_MODEL_VER",
    "LGD_SEG_VER",
    "LGD_UNADJUSTED_RPTG_RTO",
    "LNG_RUN_LGD_ADD_ON_RTO",
    "METRPL_AREA_NM",
    "NCR_PD_BAND_KEY_VAL",
    "PD_90_DAY_F",
    "PD_ACCT_SCORE",
    "PD_BAND",
    "PD_BASEL_SEG_NUM",
    "PD_FINAL_RPTG_RTO",
    "PD_FLR",
    "PD_FLRD_RPTG_RTO",
    "PD_LD_PV_AD_SV_RPTG_RTO",
    "PD_LR_PV_AD_RPTG_RTO",
    "PD_LR_PV_AD_SV_DT_AA_RTO",
    "PD_LR_PV_RPTG_RTO",
    "PD_LR_RPTG_RTO",
    "PD_MODEL_NM",
    "PD_MODEL_VER",
    "PD_SEG_VER",
    "PD_UNADJUSTED_RPTG_RTO",
    "PMI_LGD_INSURED_RPTG_RTO",
    "PMI_LGD_UNADJUSTED_RPTG_RTO",
    "PREV_12_QTR_PRPTY_VAL_AMT",
    "PRE_INSURANCE_LGD",
    "PRPTY_VAL_CORR_PCTG",
    "RESIDUAL_MAT",
    "RNTL_PRPTY_F",
    "UNDRAWN",
    "UNINSURED_DLGD_RTO",
    "UNINSURED_FLRD_LGD_RTO",
    "UNINSURED_LGD_RTO",
    "UNINSURED_LGD_SEG_NUM",
    "WEIGHT_SECURED",
    "WEIGHT_UNSECURED",
]

_FEATURE_TABLES = [
    "MORT_NUM",
    "TRANSACTOR_FLAG_QRR",
    "CMHC_F",
    "DRAWN",
]

UPSTREAM_ASSET = (
    ["instruments.PIT_STAT_CD"]
    + [f"instruments.{table}" for table in _INSTRUMENT_TABLES]
    + [f"features.{table}" for table in _FEATURE_TABLES]
)

DOWNSTREAM_ASSET = "instruments.BASEL_ANALYTCL_BL_INSTRUMNT_FACT"

DEPENDENCIES = {
    "export_ks": ["duckdb_delete"],
    "export_spl": ["duckdb_delete"],
    "export_mor": ["duckdb_delete"],
    "export_tng": ["duckdb_delete"],
    "duckdb_delete": ["duckdb_load"],
}


def export_ks(
    duckdb_conn_id="duckdb-conn",
    config_file="basel_analytcl_bl_instrmnt_fact.export_ks.sql",
    config_type="instrument",
):
    pass


def export_spl(
    duckdb_conn_id="duckdb-conn",
    config_file="basel_analytcl_bl_instrmnt_fact.export_spl.sql",
    config_type="instrument",
):
    pass


def export_mor(
    duckdb_conn_id="duckdb-conn",
    config_file="basel_analytcl_bl_instrmnt_fact.export_mor.sql",
    config_type="instrument",
):
    pass


def export_tng(
    duckdb_conn_id="duckdb-conn",
    config_file="basel_analytcl_bl_instrmnt_fact.export_tng.sql",
    config_type="instrument",
):
    pass


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT *
        FROM read_parquet([
            '{{{{ task_instance.xcom_pull(task_ids="fact__basel_analytcl_bl_instrmnt_fact.export_ks", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__basel_analytcl_bl_instrmnt_fact.export_spl", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__basel_analytcl_bl_instrmnt_fact.export_mor", key="parquet") }}}}',
            '{{{{ task_instance.xcom_pull(task_ids="fact__basel_analytcl_bl_instrmnt_fact.export_tng", key="parquet") }}}}'
        ], union_by_name=true)
    )
    """,
):
    pass
