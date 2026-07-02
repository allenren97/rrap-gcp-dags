import os
import pyarrow as pa
from airflow.sdk import get_current_context, task


@task
def create_sq035_rundir():
    """Create sq035 run directory."""
    context = get_current_context()
    rundir = context["ti"].xcom_pull(task_ids="handle_month_context", key="RUNDIR")
    sq035_rundir = f"{rundir}/sq035"
    os.makedirs(sq035_rundir, exist_ok=True)


@task.beeline(
    task_id="get_airb_asst_src",
    beeline_conn_id="edlr-conn",
    sql="""
        select
            BASEL_ID,
            ASST_NUM AS ASSETNUMBER,
            REPLACE(ACCT_NUM, '\t', ' ') AS ACCOUNTNUMBER,
            TRNST_NUM AS TRANSITNUMBER,
            AGRMNT_TP AS AGREEMENTTYPE,
            PGM_TP AS PROGRAMTYPE,
            CAST(BAL_OWNG AS DOUBLE) AS BALANCEOWING,
            CAST(PRNCPL_OWNG_AT_ASGNMNT AS DOUBLE) AS PRINCIPALOWINGATASSIGNMENT,
            CAST(ACCR_INTR_AT_ASGNMNT AS DOUBLE) AS ACCRUEDINTERESTATASSIGNMENT,
            CAST(ADD_ON_COSTS_AT_ASGNMNT AS DOUBLE) AS ADDONCOSTSATASSIGNMENT,
            (CASE WHEN UPPER(ASGNMNT_DT) = 'NULL' THEN NULL ELSE ASGNMNT_DT END) AS ASSIGNMENTDATE,
            INSTRCTN AS INSTRUCTION,
            regexp_replace(stat, '\\u2013', '\\u001A')  AS STATUS,
            ACCT_STAT AS ACCOUNTSTATUS,
            (CASE WHEN UPPER(MTH_END_DT) = 'NULL' THEN NULL ELSE MTH_END_DT END) AS MONTHENDDATE,
            JDGMNT_IND AS JUDGEMENTINDICATOR,
            CAST(LEGAL_COSTS AS DOUBLE) AS LEGALCOSTS,
            CAST(PRPTY_MGT_COSTS AS DOUBLE) AS PROPERTYMANAGEMENTCOSTS,
            CAST(INSPCTN_FEES AS DOUBLE) AS INSPECTIONFEES,
            CAST(ENVMNTL_FEES AS DOUBLE) AS ENVIRONMENTALFEES,
            CAST(GST_ON_INCM AS DOUBLE) AS GSTONINCOME,
            CAST(UTLTS AS DOUBLE) AS UTILITIES,
            CAST(REPAIRS AS DOUBLE) AS REPAIRS,
            CAST(CR_RPTG_COSTS AS DOUBLE) AS CREDITREPORTINGCOSTS,
            CAST(CORP_RISK_INSUR AS DOUBLE) AS CORPORATERISKINSURANCE,
            CAST(TAXES AS DOUBLE) AS TAXES,
            CAST(APPRSL_FEES AS DOUBLE) AS APPRAISALFEES,
            CAST(CNDMNM_FEES AS DOUBLE) AS CONDOMINIUMFEES,
            CAST(MISCLNS_FEES AS DOUBLE) AS MISCELLANEOUSFEES,
            CAST(LOAD_FEE AS DOUBLE) AS LOADFEE,
            CAST(CMMSNS AS DOUBLE) AS COMMISIONS,
            CAST(TOT_COSTS_OR_EXPNSS AS DOUBLE) AS TOTALCOSTSOREXPENSES,
            CAST(DLLRS_RCVRD_RECVD AS DOUBLE) AS DOLLARSRECOVEREDRECEIVED,
            CAST(PRCD_TO_PAY_FOR_EXPNSS AS DOUBLE) AS PROCEEDSTOPAYFOREXP,
            CAST(TOT_RCVRS AS DOUBLE) AS TOTALRECOVERIES,
            CAST(HLDBCKS AS DOUBLE) AS HOLDBACKS,
            CAST(APPRSL AS DOUBLE) AS APPRAISAL,
            CAST(MTH_END_PRNCPL_BAL_OWNG AS DOUBLE) AS MONTHENDPRINCIPALBALOWING,
            CAST(MTH_END_ACCR_INTR_OWNG AS DOUBLE) AS MONTHENDACCRUEDINTERESTOWING,
            CAST(MTH_END_ADD_ON_COST AS DOUBLE) AS MONTHENDADDONCOST,
            HGHWY AS HIGHWAY,
            STATE,
            (CASE WHEN UPPER(CLSD_DT) = 'NULL' THEN NULL ELSE CLSD_DT END) AS CLOSEDDATE,
            HOST_MNEMONIC,
            date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss.SSSSSS') as INSRT_PROCESS_TMSTMP,
            NULL as UPDT_PROCESS_TMSTMP
        FROM {{ var.value.TSZ_B9XF_RRAP_SCHEMA }}.airb_asst_src
        WHERE businesseffectivedate = '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="MTH_END_DT") }}';
    """,
    rundir="{{ task_instance.xcom_pull(task_ids='handle_month_context', key='RUNDIR') }}/sq035",
    to_parquet=True,
    tmpfileloc="/bns/rrap/data/tmp",
    target="get_airb_asst_src.parquet",
    schema=pa.schema([
        ('BASID', pa.int64()),
        ('ASSETNUMBER', pa.int64()),
        ('ACCOUNTNUMBER', pa.string()),
        ('TRANSITNUMBER', pa.string()),
        ('AGREEMENTTYPE', pa.string()),
        ('PROGRAMTYPE', pa.string()),
        ('BALANCEOWING', pa.float64()),
        ('PRINCIPALOWINGATASSIGNMENT', pa.float64()),
        ('ACCRUEDINTERESTATASSIGNMENT', pa.float64()),
        ('ADDONCOSTSATASSIGNMENT', pa.float64()),
        ('ASSIGNMENTDATE', pa.timestamp('us')),
        ('INSTRUCTION', pa.string()),
        ('STATUS', pa.string()),
        ('ACCOUNTSTATUS', pa.string()),
        ('MONTHENDDATE', pa.timestamp('us')),
        ('JUDGEMENTINDICATOR', pa.string()),
        ('LEGALCOSTS', pa.float64()),
        ('PROPERTYMANAGEMENTCOSTS', pa.float64()),
        ('INSPECTIONFEES', pa.float64()),
        ('ENVIRONMENTALFEES', pa.float64()),
        ('GSTONINCOME', pa.float64()),
        ('UTILITIES', pa.float64()),
        ('REPAIRS', pa.float64()),
        ('CREDITREPORTINGCOSTS', pa.float64()),
        ('CORPORATERISKINSURANCE', pa.float64()),
        ('TAXES', pa.float64()),
        ('APPRAISALFEES', pa.float64()),
        ('CONDOMINIUMFEES', pa.float64()),
        ('MISCELLANEOUSFEES', pa.float64()),
        ('LOADFEE', pa.float64()),
        ('COMMISIONS', pa.float64()),
        ('TOTALCOSTSOREXPENSES', pa.float64()),
        ('DOLLARSRECOVEREDRECEIVED', pa.float64()),
        ('PROCEEDSTOPAYFOREXP', pa.float64()),
        ('TOTALRECOVERIES', pa.float64()),
        ('HOLDBACKS', pa.float64()),
        ('APPRAISAL', pa.float64()),
        ('MONTHENDPRINCIPALBALOWING', pa.float64()),
        ('MONTHENDACCRUEDINTERESTOWING', pa.float64()),
        ('MONTHENDADDONCOST', pa.float64()),
        ('HIGHWAY', pa.string()),
        ('STATE', pa.string()),
        ('CLOSEDDATE', pa.timestamp('us')),
        ('HOST_MNEMONIC', pa.string()),
        ('INSRT_PROCESS_TMSTMP', pa.timestamp('us')),
        ('UPDT_PROCESS_TMSTMP', pa.timestamp('us')),
    ]),
)
def get_airb_asst_src():
    """
    Extract asset source data.
    
    Extracts asset information including asset/account/transit numbers, balance
    and cost details, recovery metrics, property details, and administrative
    timestamps from AIRB_ASST_SRC for the current business effective date.
    Applies transformations: replaces tab characters in account numbers, handles
    null date strings, and casts numeric fields.
    """
    pass


rundir_task = create_sq035_rundir()
extract_task = get_airb_asst_src()

rundir_task >> extract_task
