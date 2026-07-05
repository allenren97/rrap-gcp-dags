"""
Gather emulated.BASEL_ANALYTCL_BL_INSTRUMNT_FACT from instruments.* column tables.

Mirrors J_RRAP_2700 / J_PLL_BASEL_ANALYTCL_BL_INSTRMNT_FACT output shape by left-joining
all account-level instruments.fact loaders on (OBSN_DT, BASEL_ACCT_ID, STREAM).

Spine: instruments.PIT_STAT_CD (all portfolios: KS, MOR, SPL, TNG-MOR).
Supplementary features: MORT_NUM, TRANSACTOR_FLAG_QRR, CMHC_F, DRAWN.

Prerequisite: run instrument fact column DAGs for the process month before this job.
instruments.FINAL_RTO is excluded (model/segment grain, not account grain).
"""

# (instruments table, columns to pull) — excludes PIT_STAT_CD (spine) and FINAL_RTO.
_INSTRUMENT_SOURCES = [
    ("AMORT", ["AMORT"]),
    ("CCAR_BASEL_PRD_TP_NM", ["CCAR_BASEL_PRD_TP_NM"]),
    ("CCAR_F", ["CCAR_F"]),
    ("CONS_DFT_MTH_CNT", ["CONS_DFT_MTH_CNT"]),
    ("CRNT_LTV_RTO", ["CRNT_LTV_RTO"]),
    ("CRNT_PRPTY_VAL_AMT", ["CRNT_PRPTY_VAL_AMT"]),
    ("DLGD_F", ["DLGD_F"]),
    ("DLGD_FLR", ["DLGD_FLR"]),
    ("DLGD_RPTG_RTO", ["DLGD_RPTG_RTO"]),
    ("EAD_ACCT_SCORE", ["EAD_ACCT_SCORE"]),
    ("EAD_BASEL_SEG_NUM", ["EAD_BASEL_SEG_NUM"]),
    ("EAD_FINAL_RPTG_RTO", ["EAD_FINAL_RPTG_RTO"]),
    ("EAD_FLR", ["EAD_FLR"]),
    ("EAD_FLRD_RPTG_RTO", ["EAD_FLRD_RPTG_RTO"]),
    ("EAD_LD_PV_AD_SV_DT_RPTG_RTO", ["EAD_LD_PV_AD_SV_DT_RPTG_RTO"]),
    ("EAD_LD_PV_AD_SV_RPTG_RTO", ["EAD_LD_PV_AD_SV_RPTG_RTO"]),
    ("EAD_LR_PV_AD_RPTG_RTO", ["EAD_LR_PV_AD_RPTG_RTO"]),
    ("EAD_LR_PV_AD_SV_DT_AA_RTO", ["EAD_LR_PV_AD_SV_DT_AA_RTO"]),
    ("EAD_LR_PV_RPTG_RTO", ["EAD_LR_PV_RPTG_RTO"]),
    ("EAD_LR_RPTG_RTO", ["EAD_LR_RPTG_RTO"]),
    ("EAD_MODEL_NM", ["EAD_MODEL_NM"]),
    ("EAD_MODEL_VER", ["EAD_MODEL_VER"]),
    ("EAD_SEG_VER", ["EAD_SEG_VER"]),
    ("EAD_UNADJUSTED_RPTG_RTO", ["EAD_UNADJUSTED_RPTG_RTO"]),
    ("EXPOSURE", ["EXPOSURE"]),
    ("EXPOSURE_SECURED", ["EXPOSURE_SECURED"]),
    ("EXPOSURE_SECURED_MAXIMUM", ["EXPOSURE_SECURED_MAXIMUM"]),
    ("EXPOSURE_UNSECURED", ["EXPOSURE_UNSECURED"]),
    ("EXPSR_AT_DFT_RTO", ["EXPSR_AT_DFT_RTO"]),
    ("FULLY_SECURED_F", ["FULLY_SECURED_F"]),
    ("INDEXED_CCLTV_RTO", ["INDEXED_CCLTV_RTO"]),
    ("INDEXED_PRPTY_VAL_AMT", ["INDEXED_PRPTY_VAL_AMT"]),
    ("INDEX_TERANETV_CMA", ["INDEX_TERANETV"]),
    ("INSUR_F", ["INSUR_F"]),
    ("INTR_ACCR_AMT", ["INTR_ACCR_AMT"]),
    ("LGD_ACCT_SCORE", ["LGD_ACCT_SCORE"]),
    ("LGD_BASEL_SEG_NUM", ["LGD_BASEL_SEG_NUM"]),
    ("LGD_FINAL_RPTG_RTO", ["LGD_FINAL_RPTG_RTO"]),
    ("LGD_FLR", ["LGD_FLR"]),
    ("LGD_FLRD_RPTG_RTO", ["LGD_FLRD_RPTG_RTO"]),
    ("LGD_LD_PV_AD_SV_DT_RPTG_RTO", ["LGD_LD_PV_AD_SV_DT_RPTG_RTO"]),
    ("LGD_LD_PV_AD_SV_RPTG_RTO", ["LGD_LD_PV_AD_SV_RPTG_RTO"]),
    ("LGD_LR_PV_AD_RPTG_RTO", ["LGD_LR_PV_AD_RPTG_RTO"]),
    ("LGD_LR_PV_AD_SV_DT_AA_RTO", ["LGD_LR_PV_AD_SV_DT_AA_RTO"]),
    ("LGD_LR_PV_RPTG_RTO", ["LGD_LR_PV_RPTG_RTO"]),
    ("LGD_LR_RPTG_RTO", ["LGD_LR_RPTG_RTO"]),
    ("LGD_MODEL_NM", ["LGD_MODEL_NM"]),
    ("LGD_MODEL_VER", ["LGD_MODEL_VER"]),
    ("LGD_SEG_VER", ["LGD_SEG_VER"]),
    ("LGD_UNADJUSTED_RPTG_RTO", ["LGD_UNADJUSTED_RPTG_RTO"]),
    ("LNG_RUN_LGD_ADD_ON_RTO", ["LNG_RUN_LGD_ADD_ON_RTO"]),
    ("METRPL_AREA_NM", ["METRPL_AREA_NM"]),
    ("NCR_PD_BAND_KEY_VAL", ["NCR_PD_BAND_KEY_VAL"]),
    ("PD_90_DAY_F", ["PD_90_DAY_F"]),
    ("PD_ACCT_SCORE", ["PD_ACCT_SCORE"]),
    ("PD_BAND", ["PD_BAND"]),
    ("PD_BASEL_SEG_NUM", ["PD_BASEL_SEG_NUM"]),
    ("PD_FINAL_RPTG_RTO", ["PD_FINAL_RPTG_RTO"]),
    ("PD_FLR", ["PD_FLR"]),
    ("PD_FLRD_RPTG_RTO", ["PD_FLRD_RPTG_RTO"]),
    ("PD_LD_PV_AD_SV_RPTG_RTO", ["PD_LD_PV_AD_SV_RPTG_RTO"]),
    ("PD_LR_PV_AD_RPTG_RTO", ["PD_LR_PV_AD_RPTG_RTO"]),
    ("PD_LR_PV_AD_SV_DT_AA_RTO", ["PD_LR_PV_AD_SV_DT_AA_RTO"]),
    ("PD_LR_PV_RPTG_RTO", ["PD_LR_PV_RPTG_RTO"]),
    ("PD_LR_RPTG_RTO", ["PD_LR_RPTG_RTO"]),
    ("PD_MODEL_NM", ["PD_MODEL_NM"]),
    ("PD_MODEL_VER", ["PD_MODEL_VER"]),
    ("PD_SEG_VER", ["PD_SEG_VER"]),
    ("PD_UNADJUSTED_RPTG_RTO", ["PD_UNADJUSTED_RPTG_RTO"]),
    ("PMI_LGD_INSURED_RPTG_RTO", ["PMI_LGD_INSURED_RPTG_RTO"]),
    ("PMI_LGD_UNADJUSTED_RPTG_RTO", ["PMI_LGD_UNADJUSTED_RPTG_RTO"]),
    ("PREV_12_QTR_PRPTY_VAL_AMT", ["PREV_12_QTR_PRPTY_VAL_AMT"]),
    ("PRE_INSURANCE_LGD", ["PRE_INSURANCE_LGD"]),
    ("PRPTY_VAL_CORR_PCTG", ["PRPTY_VAL_CORR_PCTG"]),
    ("RESIDUAL_MAT", ["RESIDUAL_MAT"]),
    ("RNTL_PRPTY_F", ["RNTL_PRPTY_F"]),
    ("UNDRAWN", ["UNDRAWN"]),
    ("UNINSURED_DLGD_RTO", ["UNINSURED_DLGD_RTO"]),
    ("UNINSURED_FLRD_LGD_RTO", ["UNINSURED_FLRD_LGD_RTO"]),
    ("UNINSURED_LGD_RTO", ["UNINSURED_LGD_RTO"]),
    ("UNINSURED_LGD_SEG_NUM", ["UNINSURED_LGD_SEG_NUM"]),
    ("WEIGHT_SECURED", ["WEIGHT_SECURED"]),
    ("WEIGHT_UNSECURED", ["WEIGHT_UNSECURED"]),
]

# instruments column name -> emulated column name (IIAS target)
_OUTPUT_ALIASES = {
    "INDEXED_CCLTV_RTO": "INDEXED_LOAN_TO_VAL_RTO",
}

# (feature table, join alias, select expression, extra join predicate)
_FEATURE_SUPPLEMENTS = [
    ("MORT_NUM", "fe_mn", "CAST(fe_mn.MORT_NUM AS VARCHAR)", "AND sp.SRC_SYS_CD = fe_mn.SRC_SYS_CD"),
    ("TRANSACTOR_FLAG_QRR", "fe_tf", "fe_tf.TRANSACTOR_FLAG_QRR", ""),
    ("CMHC_F", "fe_cm", "fe_cm.CMHC_F", ""),
    ("DRAWN", "fe_dr", "fe_dr.DRAWN", ""),
]

UPSTREAM_ASSET = (
    ["instruments.PIT_STAT_CD"]
    + [f"instruments.{table}" for table, _ in _INSTRUMENT_SOURCES]
    + [f"features.{table}" for table, *_ in _FEATURE_SUPPLEMENTS]
)

DOWNSTREAM_ASSET = "emulated.BASEL_ANALYTCL_BL_INSTRUMNT_FACT"

_TASK_GROUP = "filteredtable__BASEL_ANALYTCL_BL_INSTRUMNT_FACT"

DEPENDENCIES = {
    "duckdb_delete": ["export_result"],
    "export_result": ["duckdb_load"],
}


def _output_name(column: str) -> str:
    return _OUTPUT_ALIASES.get(column, column)


def _build_gather_sql() -> str:
    rundate = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}'
    stream = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
    mth_tm_id = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}'

    select_parts = [
        "sp.OBSN_DT",
        "sp.STREAM",
        f"{mth_tm_id} AS MTH_TM_ID",
        "sp.SRC_SYS_CD",
        "sp.BASEL_ACCT_ID",
        "sp.PIT_STAT_CD",
    ]
    seen_outputs = {"OBSN_DT", "STREAM", "MTH_TM_ID", "SRC_SYS_CD", "BASEL_ACCT_ID", "PIT_STAT_CD"}
    join_parts = []

    for idx, (table, columns) in enumerate(_INSTRUMENT_SOURCES):
        alias = f"j{idx}"
        join_parts.append(
            f"""
        LEFT JOIN instruments.{table} {alias}
            ON sp.BASEL_ACCT_ID = {alias}.BASEL_ACCT_ID
           AND sp.OBSN_DT = {alias}.OBSN_DT
           AND sp.STREAM = {alias}.STREAM"""
        )
        for column in columns:
            out = _output_name(column)
            if out in seen_outputs:
                continue
            seen_outputs.add(out)
            select_parts.append(f"{alias}.{column} AS {out}")

    for feature_col, alias, expr, extra_predicate in _FEATURE_SUPPLEMENTS:
        join_parts.append(
            f"""
        LEFT JOIN features.{feature_col} {alias}
            ON sp.BASEL_ACCT_ID = {alias}.BASEL_ACCT_ID
           AND sp.OBSN_DT = {alias}.OBSN_DT
           {extra_predicate}"""
        )
        if feature_col not in seen_outputs:
            seen_outputs.add(feature_col)
            select_parts.append(f"{expr} AS {feature_col}")

    select_parts.extend(
        [
            "CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP",
            "CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP",
        ]
    )

    joins_sql = "".join(join_parts)
    select_sql = ",\n        ".join(select_parts)

    return f"""
    WITH spine AS (
        SELECT
            OBSN_DT,
            STREAM,
            BASEL_ACCT_ID,
            SRC_SYS_CD,
            PIT_STAT_CD
        FROM instruments.PIT_STAT_CD
        WHERE OBSN_DT = DATE '{rundate}'
          AND STREAM = '{stream}'
    )
    SELECT
        {select_sql}
    FROM spine sp{joins_sql}
    """


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE OBSN_DT = DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
      AND STREAM = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}}}'
    """,
):
    pass


def export_result(
    duckdb_conn_id="duckdb-conn",
    sql=_build_gather_sql(),
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    SELECT *
    FROM read_parquet(
        '{{{{ task_instance.xcom_pull(task_ids="{_TASK_GROUP}.export_result", key="parquet") }}}}'
    )
    """,
):
    pass
