import os
import re
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb as ddb

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [
    "ingestion.TM_DIM",
    "ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT",
    "ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT",
    "ingestion.MORT_MTH_SNAPSHOT",
    "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT",
    "ingestion.TERANET_ADDR_LKP",
    "ingestion.TERANET_HOUSE_PRC_INDEX",
    "ingestion.PROVNCL_HOUSE_INDEX_SUM",
    "reference.SRC_PRD_LKP", ]

DOWNSTREAM_ASSET = "features.LTV_METHOD2_FIX"

DEPENDENCIES = {
    "duckdb_delete": ["export_clean_step_pln_mth_snap"],
    "export_clean_step_pln_mth_snap": ["export_clean_teranet_addr_lkp"],
    "export_clean_teranet_addr_lkp": ["get_loc"],
    "get_loc": ["export_ltv_inputs"],
    "export_ltv_inputs": ["duckdb_load"],
}


def duckdb_delete(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        DELETE FROM {DOWNSTREAM_ASSET}
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """,
):
    pass

def export_clean_step_pln_mth_snap(
        duckdb_conn_id="duckdb-conn",
        sql=f"""
            SELECT STEP_PLN_SNAPSHOT_ID,
                TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                td.TM_ID AS APRSD_TM_ID,
                APRSD_VAL,
                CASE WHEN trim(UPPER(PRPTY_PROV_CD)) = 'MN' THEN 'MB'
                    WHEN trim(UPPER(PRPTY_PROV_CD)) = 'NF' THEN 'NL'
                    WHEN trim(UPPER(PRPTY_PROV_CD)) = 'PQ' THEN 'QC'
                    ELSE PRPTY_PROV_CD
                END AS PRPTY_PROV_CD,
                REGEXP_REPLACE(TRANSLATE(LOWER(NFC_NORMALIZE(trim(PRPTY_DESC_3))), 'àâäçéèêëîïôöùûüÿñ', 'aaaceeeeii oouuuyn'),
                    '[^a-z0-9]', '', 'g') AS CITY,
                REGEXP_REPLACE(TRANSLATE(LOWER(NFC_NORMALIZE(trim(PRPTY_DESC_2))), 'àâäçéèêëîïôöùûüÿñ', 'aaaceeeeii oouuuyn'),
                    '[^a-z0-9]', '', 'g') AS STREET,
                REGEXP_REPLACE(TRANSLATE(LOWER(NFC_NORMALIZE(trim(PRPTY_DESC_1))), 'àâäçéèêëîïôöùûüÿñ', 'aaaceeeeii oouuuyn'),
                    '[^a-z0-9]', '', 'g') AS LOT,
                PRPTY_DESC_3,
                PRPTY_DESC_2,
                PRPTY_DESC_1
            FROM ingestion.BASEL_STEP_PLN_MTH_SNAPSHOT asp
            LEFT JOIN ingestion.TM_DIM td 
                ON (CASE WHEN asp.APRSD_DT IS NULL THEN asp.CR_LMT_DT ELSE asp.APRSD_DT END)
                    BETWEEN TD.TM_LVL_ST_DT AND td.TM_LVL_END_DT AND TRIM(TM_LVL) = 'Month'
            WHERE asp.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
        """
):
    pass

def export_clean_teranet_addr_lkp(
        duckdb_conn_id="duckdb-conn",
        sql=f"""
            WITH t AS (
                SELECT REGEXP_REPLACE(TRANSLATE(LOWER(NFC_NORMALIZE(PRPTY_LOCTN_NM)), 'àâäçéèêëîïôöùûüÿñ', 'aaaceeeeii oouuuyn'),
                        '[^a-z0-9]', '', 'g') AS PRPTY,
                    TRIM(LOCTN_LABEL_1) AS LOCTN_LABEL_1,
                    TRIM(LOCTN_LABEL_2) AS LOCTN_LABEL_2
                FROM ingestion.TERANET_ADDR_LKP
                WHERE TRIM(LOCTN_LABEL_2) != 'N/A' 
                    AND EFF_FROM_YR_MTH <= strftime('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}')
                    AND strftime('%Y%m', DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")}}}}') <= EFF_TO_YR_MTH
            ) SELECT PRPTY, LOCTN_LABEL_1, LOCTN_LABEL_2 FROM t 
            GROUP BY PRPTY, LOCTN_LABEL_1, LOCTN_LABEL_2
        """
):
    pass

def get_loc():
    # TODO dynamic dag generator wraps every python function as an airflow task, thus could only nest helper function inside the task
    def _find_earliest_match(address, lkp):
        """
        Finds the earliest-occuring substring in the input string <address> that matches any key in dict <lkp>
        and return the corresponding substring
        
        Parameters:
        - address : str 
                    The input string to search within
        - lkp: dict 
                A lookup dict whose keys are property location names and substrings to search for in <address> 
        """
        rslt = None

        if not lkp or not address: 
            return rslt

        target = len(address)

        for k in lkp:
            idx = address.find(k)
            if idx != -1 and idx < target:
                target = idx
                rslt = k
        
        return rslt
    
    def _is_postal_code(prpty: str="") -> str:
        """
        Classify a cleaned string as a Canadian postal code type.

        This function checks whether the input string is a valid full or partial
        Canadian postal code. The input must be preprocessed to be lowercase and 
        contain no spaces or punctuation.

        Parameters:
        - prpty : str
            A cleaned string representing a potential postal code.

        Returns:
            str: One of the following classifications:
                - 'full_postal_code' for a full 6-character postal code (e.g., 'm1p2w3')
                - 'partial_postal_code' for a 3-character FSA code (e.g., 'm1p')
                - 'non_postal_code' if the string does not match either pattern

        """
        full_pattern = r'^[abceghj-nprstvxy]\d[abceghj-nprstv-z]\d[abceghj-nprstv-z]\d$'
        partial_pattern = r'^[abceghj-nprstvxy]\d[abceghj-nprstv-z]$'

        if re.match(full_pattern, prpty):
            return 'full_postal_code'
        elif re.match(partial_pattern, prpty):
            return 'partial_postal_code'
        else:
            return 'non_postal_code'

    context = get_current_context()
    BASEL_STEP_PLN_MTH_SNAPSHOT = context['ti'].xcom_pull(task_ids=f"derived__ltv_method2_fix.export_clean_step_pln_mth_snap", key="parquet")
    TERANET_ADDR_LKP = context['ti'].xcom_pull(task_ids=f"derived__ltv_method2_fix.export_clean_teranet_addr_lkp", key="parquet")
    
    query = f"SELECT * FROM '{BASEL_STEP_PLN_MTH_SNAPSHOT}'"
    snapshot = ddb.sql(query).fetchall()
    logging.warning(f"Successfully executed - {query}")
    
    query = f"SELECT * FROM '{TERANET_ADDR_LKP}'"
    lkp_rows = ddb.sql(query).fetchall()
    logging.warning(f"Successfully executed - {query}")

    lkp = {}
    full_postal_code = {}
    partial_postal_code = {}
    non_postal_code = {}

    for prpty, label1, label2 in lkp_rows:
        if label1 not in lkp:
            lkp[label1] = {}
        lkp[label1][prpty] = label2

        if _is_postal_code(prpty) == 'full_postal_code':
            if label1 not in full_postal_code:
                full_postal_code[label1] = {}
            full_postal_code[label1][prpty] = label2
        
        if _is_postal_code(prpty) == 'partial_postal_code':
            if label1 not in partial_postal_code:
                partial_postal_code[label1] = {}
            partial_postal_code[label1][prpty] = label2

        if _is_postal_code(prpty) == 'non_postal_code':
            if label1 not in non_postal_code:
                non_postal_code[label1] = {}
            non_postal_code[label1][prpty] = label2


    PROV = 4
    CITY = 5
    STREET = 6
    LOT = 7
    rslt = []
    for row in snapshot:
        prov = row[PROV]
        if prov in lkp:
            lkp_subset = lkp[prov]

            if row[CITY] in lkp_subset:
                CMA = lkp_subset[row[CITY]]
            elif row[STREET] in lkp_subset:
                CMA = lkp_subset[row[STREET]]
            elif row[LOT] in lkp_subset:
                CMA = lkp_subset[row[LOT]]
            else: 
                CMA = None
                substring1 = _find_earliest_match(row[CITY], full_postal_code.get(prov, {}))
                substring2 = _find_earliest_match(row[CITY], partial_postal_code.get(prov, {}))
                substring3 = _find_earliest_match(row[CITY], non_postal_code.get(prov, {}))
                if substring1:
                    CMA = lkp_subset[substring1]
                elif substring2:
                    CMA = lkp_subset[substring2]
                elif substring3:
                    CMA = lkp_subset[substring3]
        else:
            CMA = None
        new_row = row[0:5] + (CMA, )
        rslt.append(new_row)

    dag_id = context["dag"].dag_id
    run_id = context["dag_run"].run_id
    target = os.path.join('/bns/rrap/data/intermediates', dag_id, run_id, 'step_pln_mth_snap_with_cma.parquet') #TODO
    context["ti"].xcom_push(key="parquet", value=target)
    column_names = [
        'STEP_PLN_SNAPSHOT_ID',
        'STEP_PLN_AGRMNT_NUM',
        'APRSD_TM_ID',
        'APRSD_VAL',
        'LOCTN_LABEL_1',
        'LOCTN_LABEL_2'
    ]
    records = [dict(zip(column_names, row)) for row in rslt]
    table = pa.Table.from_pylist(records)
    pq.write_table(table, target)
    parquet_size = os.path.getsize(target)
    query = f"SELECT count(*) FROM '{target}'"
    n_rows = ddb.sql(query).fetchone()[0]

    logging.warning(f"{target} created, filesize: {parquet_size} with {n_rows} rows")

def export_ltv_inputs(
        duckdb_conn_id="duckdb-conn",
        sql=f"""
            WITH basel_rev_snapshot_wrk AS (
                SELECT 
                    BASEL_ACCT_ID,
                    STEP_PLN_SNAPSHOT_ID,
                    TOT_NEW_BAL_AMT,
                    TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM,
                    TRIM(LTV_TP_CD) AS LTV_TP_CD,
                    MTH_TM_ID
                FROM 
                    ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT RSN,
                    reference.SRC_PRD_LKP LKP
                WHERE
                    RSN.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    AND TRIM(RSN.PRD_CD) = TRIM(LKP.SRC_PRD_CD)
                    AND TRIM(RSN.SUB_PRD_CD) = TRIM(LKP.SRC_SUB_PRD_CD)
                    AND TRIM(LKP.PRD_SYS_CD) = 'KS'
                    AND TRIM(LKP.SML_BUS_F) = 'N'
                    AND TRIM(LKP.CRNT_F) = 'Y'
            ),
            -- aggregate to step_pln_agrmnt_num level
            osmt AS (
                SELECT 
                    SUM(SUM_OS_BAL_AMT) AS SUM_OS_BAL_AMT,
                    STEP_PLN_AGRMNT_NUM
                FROM (
                    -- Mortgage
                    SELECT  
                        SUM(COALESCE(CRNT_BAL_AMT, 0) + COALESCE(INTR_ACCR_AMT, 0)) AS SUM_OS_BAL_AMT,
                        TRIM(MS.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM
                    FROM ingestion.MORT_MTH_SNAPSHOT MS
                    JOIN (
                        SELECT DISTINCT STEP_PLN_SNAPSHOT_ID, STEP_PLN_AGRMNT_NUM
                        FROM basel_rev_snapshot_wrk
                        WHERE STEP_PLN_SNAPSHOT_ID NOT IN (-1, -2)) WRK
                    ON TRIM(MS.STEP_PLN_AGRMNT_NUM) = TRIM(WRK.STEP_PLN_AGRMNT_NUM)
                    WHERE MS.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    GROUP BY TRIM(MS.STEP_PLN_AGRMNT_NUM)
                    UNION ALL
                    -- Personal Loan
                    SELECT
                        SUM(COALESCE(TOT_CRNT_BAL_AMT, 0) + COALESCE(ADD_ON_BAL_AMT, 0) + COALESCE(ACCR_INTR, 0)) AS SUM_OS_BAL_AMT,
                        TRIM(MS.STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM
                    FROM ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT MS
                    JOIN (
                        SELECT DISTINCT STEP_PLN_SNAPSHOT_ID, STEP_PLN_AGRMNT_NUM
                        FROM basel_rev_snapshot_wrk
                        WHERE STEP_PLN_SNAPSHOT_ID NOT IN (-1, -2)) WRK
                    ON TRIM(MS.STEP_PLN_AGRMNT_NUM) = TRIM(WRK.STEP_PLN_AGRMNT_NUM)
                    WHERE MS.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                    GROUP BY TRIM(MS.STEP_PLN_AGRMNT_NUM)
                ) combined
                GROUP BY STEP_PLN_AGRMNT_NUM
            ), -- join wrk on STEP_PLN_AGRMNT_NUM
            -- aggregate to step_pln_agrmnt_num level
            rev_osmt AS (
                SELECT  
                    SUM(COALESCE(TOT_NEW_BAL_AMT, 0)) AS SUM_OS_BAL_AMT,
                    TRIM(STEP_PLN_AGRMNT_NUM) AS STEP_PLN_AGRMNT_NUM
                FROM ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT
                WHERE 
                    STEP_PLN_SNAPSHOT_ID NOT IN (-1, -2)
                    AND TRIM(PRD_CD) IN (
                        SELECT TRIM(SRC_PRD_CD)
                        FROM reference.SRC_PRD_LKP
                        WHERE TRIM(PRD_SYS_CD) = 'KS' AND TRIM(CRNT_F) = 'Y' AND TRIM(LTV_TP_CD) = 'LOC'
                    )
                    AND MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
                GROUP BY TRIM(STEP_PLN_AGRMNT_NUM)
            ), -- join wrk on STEP_PLN_AGRMNT_NUM         
            -- TERANET House Price Index for the aprsd date
            thpi_aprsd_dt AS (
                SELECT STEP_PLN_SNAPSHOT_ID, th."INDEX" AS THPIBYDTAPRSD_IDX
                FROM '{{{{ task_instance.xcom_pull(task_ids="derived__ltv_method2_fix.get_loc", key="parquet") }}}}' step_pln
                LEFT JOIN ingestion.TERANET_HOUSE_PRC_INDEX th
                    ON UPPER(TRIM(step_pln.LOCTN_LABEL_1)) = TRIM(LABEL_1) AND UPPER(TRIM(step_pln.LOCTN_LABEL_2)) = TRIM(LABEL_2) AND
                    th.MTH_TM_ID = (CASE WHEN step_pln.APRSD_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} THEN step_pln.APRSD_TM_ID - 40
                    ELSE step_pln.APRSD_TM_ID END)
            ), -- join wrk on STEP_PLN_SNAPSHOT_ID
            -- teranet house price index of latest available dt before/on aprsd date
            thpi_bf_apsdt AS (
                SELECT STEP_PLN_SNAPSHOT_ID, th."INDEX" AS THPI_CURDT_APSDT_IDX -- column name is misleading
                FROM (SELECT step_pln.STEP_PLN_SNAPSHOT_ID, MAX(th.MTH_TM_ID) AS MTH_TM_ID, TRIM(LABEL_1) AS LABEL_1, TRIM(LABEL_2) AS LABEL_2
                    FROM '{{{{ task_instance.xcom_pull(task_ids="derived__ltv_method2_fix.get_loc", key="parquet") }}}}' step_pln
                    LEFT JOIN ingestion.TERANET_HOUSE_PRC_INDEX th
                        ON UPPER(TRIM(step_pln.LOCTN_LABEL_1)) = TRIM(LABEL_1) AND UPPER(TRIM(step_pln.LOCTN_LABEL_2)) = TRIM(LABEL_2) AND th.MTH_TM_ID <= step_pln.APRSD_TM_ID
                    GROUP BY STEP_PLN_SNAPSHOT_ID, TRIM(LABEL_1), TRIM(LABEL_2)) tmp 
                LEFT JOIN ingestion.TERANET_HOUSE_PRC_INDEX th 
                    ON th.MTH_TM_ID = tmp.MTH_TM_ID AND TRIM(th.LABEL_1) = tmp.LABEL_1 AND TRIM(th.LABEL_2) = tmp.LABEL_2
            ), -- join wrk on STEP_PLN_SNAPSHOT_ID
            -- provincial index for the aprst dt
            phis_apsdt AS (
                SELECT STEP_PLN_SNAPSHOT_ID, ph.HOUSE_INDEX_RTO AS HIR
                FROM '{{{{ task_instance.xcom_pull(task_ids="derived__ltv_method2_fix.get_loc", key="parquet") }}}}' step_pln 
                LEFT JOIN ingestion.PROVNCL_HOUSE_INDEX_SUM ph
                    ON step_pln.LOCTN_LABEL_1 = ph.PROV_CD  AND step_pln.APRSD_TM_ID = ph.mth_tm_id
            ), -- join wrk on STEP_PLN_SNAPSHOT_ID
            -- Teranet index for the default location (COMPOSITE, 11) of latest available dt before/on aprsd date
            thpi_dft_apsdt AS (
                SELECT STEP_PLN_SNAPSHOT_ID, th."INDEX" AS THPI_BYLBL_DFT_APSDT
                FROM (SELECT STEP_PLN_SNAPSHOT_ID, max(th.MTH_TM_ID) AS MTH_TM_ID -- latest available dt before/on aprsd date
                    FROM '{{{{ task_instance.xcom_pull(task_ids="derived__ltv_method2_fix.get_loc", key="parquet") }}}}' step_pln
                    LEFT JOIN ingestion.TERANET_HOUSE_PRC_INDEX th
                    ON UPPER(TRIM(th.LABEL_1)) = 'COMPOSITE' AND UPPER(TRIM(th.LABEL_2)) = '11' AND th.MTH_TM_ID <= step_pln.APRSD_TM_ID
                    GROUP BY STEP_PLN_SNAPSHOT_ID) tmp
                LEFT JOIN ingestion.TERANET_HOUSE_PRC_INDEX th 
                    ON th.MTH_TM_ID = tmp.MTH_TM_ID
                WHERE UPPER(TRIM(th.LABEL_1)) = 'COMPOSITE' AND UPPER(TRIM(th.LABEL_2)) = '11'
            ), -- join wrk on STEP_PLN_SNAPSHOT_ID
            -- TERANET House Price Index for the current date
            thpi_curdt AS (
                SELECT STEP_PLN_SNAPSHOT_ID, th."INDEX" AS THPI_BYDT_CURDT
                FROM '{{{{ task_instance.xcom_pull(task_ids="derived__ltv_method2_fix.get_loc", key="parquet") }}}}' step_pln
                LEFT JOIN ingestion.TERANET_HOUSE_PRC_INDEX th
                    ON UPPER(TRIM(step_pln.LOCTN_LABEL_1)) = TRIM(th.LABEL_1) AND UPPER(TRIM(step_pln.LOCTN_LABEL_2)) = TRIM(th.LABEL_2)
                WHERE th.MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40
            ), -- join wrk on STEP_PLN_SNAPSHOT_ID
            -- teranet house price index of latest available dt before/on current date
            -- thpi_curdt_apsdt AS (
            -- ), -- join wrk on STEP_PLN_SNAPSHOT_ID
            -- provincial index for the current dt
            phis_curdt AS (
                SELECT STEP_PLN_SNAPSHOT_ID, ph.HOUSE_INDEX_RTO AS HIR_PROV
                FROM '{{{{ task_instance.xcom_pull(task_ids="derived__ltv_method2_fix.get_loc", key="parquet") }}}}' step_pln 
                LEFT JOIN ingestion.PROVNCL_HOUSE_INDEX_SUM ph
                    ON step_pln.LOCTN_LABEL_1 = ph.PROV_CD
                WHERE ph.mth_tm_id = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            ), -- join wrk on STEP_PLN_SNAPSHOT_ID
            -- Composite 11 Teranet Index of latest available dt before/on current date
            thpi_dft_curdt AS (
                SELECT
                    "INDEX" AS THPI_DFT
                FROM ingestion.TERANET_HOUSE_PRC_INDEX
                WHERE UPPER(TRIM(LABEL_1)) = 'COMPOSITE' AND UPPER(TRIM(LABEL_2)) = '11' 
                    AND MTH_TM_ID <= {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}} - 40
                ORDER BY MTH_TM_ID DESC LIMIT 1
            ) -- join wrk on STEP_PLN_SNAPSHOT_ID
            SELECT
                wrk.BASEL_ACCT_ID,
                wrk.STEP_PLN_SNAPSHOT_ID,
                wrk.STEP_PLN_AGRMNT_NUM,
                wrk.LTV_TP_CD,
                wrk.TOT_NEW_BAL_AMT,
                COALESCE(osmt.SUM_OS_BAL_AMT, 0) AS MORT_PSNL_OS_BAL_AMT,
                COALESCE(rev_osmt.SUM_OS_BAL_AMT, 0) AS REV_OS_BAL_AMT,
                CASE WHEN LTV_TP_CD = 'LOC' THEN COALESCE(osmt.SUM_OS_BAL_AMT, 0) + COALESCE(wrk.TOT_NEW_BAL_AMT, 0)
                    WHEN LTV_TP_CD = 'VISA' THEN COALESCE(osmt.SUM_OS_BAL_AMT, 0) + COALESCE(rev_osmt.SUM_OS_BAL_AMT, 0) + COALESCE(wrk.TOT_NEW_BAL_AMT, 0)
                END AS TOTAL_OS_BAL_AMT,
                step_pln.APRSD_VAL,
                CAST(
                    CASE 
                        WHEN thpi_aprsd_dt.THPIBYDTAPRSD_IDX IS NOT NULL THEN CAST(thpi_aprsd_dt.THPIBYDTAPRSD_IDX AS DOUBLE)
                        ELSE CASE 
                            WHEN thpi_bf_apsdt.THPI_CURDT_APSDT_IDX IS NOT NULL THEN CAST(thpi_bf_apsdt.THPI_CURDT_APSDT_IDX AS DOUBLE)
                            ELSE CASE 
                                WHEN phis_apsdt.HIR IS NOT NULL THEN CAST(phis_apsdt.HIR AS DOUBLE)
                                ELSE CAST(thpi_dft_apsdt.THPI_BYLBL_DFT_APSDT AS DOUBLE)
                            END
                        END
                    END
                AS DOUBLE) AS TERANET_INDEX_BY_APRSD_DATE,
                CAST(
                    CASE 
                        WHEN thpi_curdt.THPI_BYDT_CURDT IS NOT NULL THEN CAST(thpi_curdt.THPI_BYDT_CURDT AS DOUBLE)
                        ELSE CASE 
                            WHEN thpi_bf_apsdt.THPI_CURDT_APSDT_IDX IS NOT NULL THEN CAST(thpi_bf_apsdt.THPI_CURDT_APSDT_IDX AS DOUBLE)
                            ELSE CASE 
                                WHEN phis_curdt.HIR_PROV IS NOT NULL THEN CAST(phis_curdt.HIR_PROV AS DOUBLE)
                                ELSE CAST(thpi_dft_curdt.THPI_DFT AS DOUBLE)
                            END
                        END
                    END
                AS DOUBLE) AS TERANET_INDEX_CURRENT_DATE,
                CAST(
                    CASE 
                        WHEN TERANET_INDEX_BY_APRSD_DATE = 0 
                        OR APRSD_VAL IS NULL 
                        OR TERANET_INDEX_CURRENT_DATE IS NULL 
                        OR TERANET_INDEX_BY_APRSD_DATE IS NULL 
                        THEN NULL
                        ELSE APRSD_VAL * (TERANET_INDEX_CURRENT_DATE / TERANET_INDEX_BY_APRSD_DATE)
                    END
                AS DECIMAL(17, 3)) AS APRSD_VAL_TERANET_INDEXED_AMT
            FROM basel_rev_snapshot_wrk wrk
            LEFT JOIN osmt ON wrk.STEP_PLN_AGRMNT_NUM = osmt.STEP_PLN_AGRMNT_NUM
            LEFT JOIN rev_osmt ON wrk.STEP_PLN_AGRMNT_NUM = rev_osmt.STEP_PLN_AGRMNT_NUM
            LEFT JOIN '{{{{ task_instance.xcom_pull(task_ids="derived__ltv_method2_fix.get_loc", key="parquet") }}}}' step_pln ON step_pln.STEP_PLN_SNAPSHOT_ID = wrk.STEP_PLN_SNAPSHOT_ID
            LEFT JOIN thpi_aprsd_dt ON thpi_aprsd_dt.STEP_PLN_SNAPSHOT_ID = wrk.STEP_PLN_SNAPSHOT_ID
            LEFT JOIN thpi_bf_apsdt ON thpi_bf_apsdt.STEP_PLN_SNAPSHOT_ID = wrk.STEP_PLN_SNAPSHOT_ID
            LEFT JOIN phis_apsdt ON phis_apsdt.STEP_PLN_SNAPSHOT_ID = wrk.STEP_PLN_SNAPSHOT_ID
            LEFT JOIN thpi_dft_apsdt ON thpi_dft_apsdt.STEP_PLN_SNAPSHOT_ID = wrk.STEP_PLN_SNAPSHOT_ID
            LEFT JOIN thpi_curdt ON thpi_curdt.STEP_PLN_SNAPSHOT_ID = wrk.STEP_PLN_SNAPSHOT_ID
            LEFT JOIN phis_curdt ON phis_curdt.STEP_PLN_SNAPSHOT_ID = wrk.STEP_PLN_SNAPSHOT_ID
            CROSS JOIN thpi_dft_curdt
        """
):
    pass


def duckdb_load(
    duckdb_conn_id="duckdb-conn",
    sql=f"""
        INSERT INTO {DOWNSTREAM_ASSET} BY NAME
        FROM (
            SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                BASEL_ACCT_ID,
                CASE
                    WHEN APRSD_VAL_TERANET_INDEXED_AMT = 0 THEN 0
                    ELSE CAST(TOTAL_OS_BAL_AMT / APRSD_VAL_TERANET_INDEXED_AMT AS DECIMAL(11, 4))
                END AS LTV_METHOD2_FIX, 
            FROM
                '{{{{ task_instance.xcom_pull(task_ids="derived__ltv_method2_fix.export_ltv_inputs", key="parquet")}}}}'
        )
    """,
):
    pass


