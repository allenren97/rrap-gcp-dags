# Generic imports
from __future__ import annotations
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.models import Param
# from airflow.operators.python import PythonOperator
from bns.rrap.operators.empty import PythonOperator

from task_groups.sas.PRRII_H11_DM_M import H11_DM_M
from task_groups.sas.PRRII_H21_DM_M import H21_DM_M
from task_groups.sas.PRRII_H22_DM_M import H22_DM_M
from task_groups.sas.PRRII_H23_2 import H23_2
from task_groups.sas.PRRII_H23_DM_M import H23_DM_M
from task_groups.sas.PRRII_H24_DM_M import H24_DM_M
from task_groups.sas.PRRII_T21_DM_M import T21_DM_M
from task_groups.sas.PRRII_T22_DM_M import T22_DM_M
from task_groups.sas.PRRII_T23_DM_M import T23_DM_M
from task_groups.sas.PRRII_T24_1_DM_M import T24_1_DM_M
from task_groups.sas.PRRII_T24_2_DM_M import T24_2_DM_M

from task_groups.sas.PRRII_DB2_LOAD import DB2_LOAD

from task_groups.sas.PRRII_MO_BNSTG_1_M import MO_BNSTG_1_M
from task_groups.sas.PRRII_MO_BNSTG_2_M import MO_BNSTG_2_M
from task_groups.sas.PRRII_MO_BNSTG_3_M import MO_BNSTG_3_M
from task_groups.sas.PRRII_MO_BNSTG_3_2 import MO_BNSTG_3_2
from task_groups.sas.PRRII_MO_BNSTG_4_M import MO_BNSTG_4_M
from task_groups.sas.PRRII_INGRESS_2 import INGRESS_2

from task_groups.sas.PRRII_RECON_CHK import RECON_CHK
from task_groups.sas.PRRII_FRG_MOR_M import FRG_MOR_M
from task_groups.sas.PRRII_FRG_MOR_M2 import FRG_MOR_M2
from task_groups.sas.PRRII_REXCP_KS_M import REXCP_KS_M
from task_groups.sas.PRRII_REXCP_MO_M import REXCP_MO_M
from task_groups.sas.PRRII_REXCP_RW_M import REXCP_RW_M

from task_groups.sas.PRRII_RCCAR_DM_M import RCCAR_DM_M
from task_groups.sas.PRRII_TB_DM_M import TB_DM_M
from task_groups.sas.PRRII_TB_DM_M2 import TB_DM_M2

from task_groups.sas.PRRII_TR4_T_DM_M import TR4_T_DM_M
from task_groups.sas.PRRII_RB2_1_D_M2 import RB2_1_D_M2
from task_groups.sas.PRRII_RB2_1_DM_M import RB2_1_DM_M
from task_groups.sas.PRRII_RB2_2_DM_M import RB2_2_DM_M
from task_groups.sas.PRRII_RR4_K_DM_M import RR4_K_DM_M
from task_groups.sas.PRRII_DLGD_2_DM_M import DLGD_2_DM_M

from task_groups.sas.PRRII_DLGD_1_DM_M import DLGD_1_DM_M
from task_groups.sas.PRRII_SNP_DQ_M import SNP_DQ_M
from task_groups.sas.PRRII_REXCP_TL_M import REXCP_TL_M
from task_groups.sas.PRRII_H24_DM_Q import H24_DM_Q

from task_groups.sas.PRRII_TE3_DM_M import TE3_DM_M
from task_groups.sas.PRRII_RE3_DM_M import RE3_DM_M
from task_groups.sas.PRRII_FC_DT4_M import FC_DT4_M
from task_groups.sas.PRRII_DT4_M_JOBS import DT4_M_JOBS
from task_groups.sas.PRRII_RECAP_DM_M import RECAP_DM_M
from task_groups.sas.PRRII_DT4_DM_M import DT4_DM_M

from task_groups.sas.PRRII_RB2_3_DM_M import RB2_3_DM_M
from task_groups.sas.PRRII_RNCR_DM_M import RNCR_DM_M

from task_groups.sas.PRRII_CMON_TSKS import CMON_TSKS

from task_groups.sas.PRRII_RSK_REP_M import RSK_REP_M
from task_groups.sas.KS_PLL import KS_PLL

from callbacks.pre_execute import skip_task_check
from callbacks.utils import require_approval


with DAG(
    "1.RRAP-Batch",
    default_args={
        "depends_on_past": False,
        "email": ["{EMAIL_DISTRIBUTION}"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "pre_execute": skip_task_check,
        "trigger_rule": "none_failed",
    },
    # [END default_args]
    description="All tested DAGs and dependencies for RRAP full batch",
    params={
        "EDW_schema_EDRTLRP1D": Param("EDRTLRP1D", type="string"),
        "EDW_schema_EDRRAPT": Param("EDRRAPT", type="string"),
        "skip": Param([
            "#UNCOMMENT THE LINES YOU NEED SKIPPED",
            "#H23_DM_M.OW_DM_RRIIH22_INFA_SANITY_CHECKS_H22",
            "#H23_DM_M.OW_DM_RRIIH22_INFA_SANITY_CHK_WRNG_LOAD",
            "#H23_DM_M.OW_DM_RRIIH22_INFA_AUDIT_H22",
            "#H24_DM_M.OW_DM_RRIIH24_INFA_SANITY_CNT_CHECK_H24",
            "#H24_DM_M.OW_DM_RRIIH24_INFA_SANITY_CHECKS_H24",
            "#H24_DM_M.OW_DM_RRIIH24_INFA_POST_LOAD",
            "#MO_BNSTG_1_M.OW_SAS_DM_RRII_MORSCRD08_LOAD_MOR_TO_NZ",
            "#MO_BNSTG_1_M.OW_SAS_TNG_II_00_MORT_SOURCE_CHECK",
            "#MO_BNSTG_1_M.OW_SAS_TNG_II_SRC_COMPT_INS_TRG_BRDM",
        ], type="array",
        description="Provide the full task ids for the tasks you wish to skip. "+\
                    "You can check the task id by clicking on the task and going "+\
                    "to details in the airflow UI. Also supports regex matching "+\
                    "using python's re.fullmatch (see https://docs.python.org/3/library/re.html#regular-expression-syntax).")
    },
    start_date=datetime(2024, 2, 1),
    catchup=False,
    schedule=None,
) as dag:

    # Manual approval required to start these jobs
    # RECON_CHK, H21_DM_M, RB2_3_DM_M
    RECON_CHK_APPROVED = PythonOperator(
        task_id="RECON_CHK_APPROVED",
        python_callable=require_approval
    )

    RECON_CHK_APPROVED.doc_md= dedent(
        """
        # Before triggering this task, please ensure the following conditions are met:\n\n\n
        This task **requires RMAS' approval of reconciliation results in the GL Reconciliation portal.**
        """
    )

    H21_APPROVED = PythonOperator(
        task_id="H21_DM_M_APPROVED",
        python_callable=require_approval
    )

    H21_APPROVED.doc_md= dedent(
        """
        # Triggering of H21 requires manual approval by RRAP team.\n\n\n
        This is due to **required manual checking of the EDRTLRP1D.BASEL_ACCT_PRFM_FACT table, which is newly written to by the task group BSL_ACT_PRFM_FACT in H11_DM_M.**
        """
    )

    MO_3_2_APPROVED = PythonOperator(
        task_id="MO_3_2_APPROVED",
        python_callable=require_approval
    )

    RB2_3_APPROVED = PythonOperator(
        task_id="RB2_3_DM_M_APPROVED",
        python_callable=require_approval
    )

    RB2_3_APPROVED.doc_md = dedent(
        """
        # RB2_3_DM_M task group should ONLY be manually triggered during QUARTER END runs.\n
        **Before triggering this task, please ensure the following conditions are met**: \n
        RMA inputs NCR BD to the relevant portal and provides their confirmation.
        """
    )

    RCCAR_APPROVED = PythonOperator(
        task_id="RCCAR_APPROVED",
        python_callable=require_approval
    )

    RCCAR_APPROVED.doc_md=dedent(
        """
        # This task should ONLY be triggered once RMA has reviewed the CCAR reports and provides their approval.
        """
    )

    # Quarter-end only requires manual mark success prior to triggering
    RUN_QUARTER_END_H24 = PythonOperator(
        task_id="RUN_QUARTER_END_H24",
        python_callable=require_approval
    )

    RUN_QUARTER_END_H24.doc_md=dedent(
        """
        # H24_DM_Q task group should ONLY be manually triggered during QUARTER END runs.
        """
    )

    RUN_QUARTER_END_DT4 = PythonOperator(
        task_id="RUN_QUARTER_END_DT4",
        python_callable=require_approval
    )

    RUN_QUARTER_END_DT4.doc_md=dedent(
        """
        # DT4 Report task group should ONLY be manually triggered during QUARTER END runs.
        """
    )



    # TaskGroup dependencies
    H11, H21, H22, H23, H24 = H11_DM_M(), H21_DM_M(), H22_DM_M(), H23_DM_M(), H24_DM_M()
    T21, T22, T23, T24_1, T24_2 = T21_DM_M(), T22_DM_M(), T23_DM_M(), T24_1_DM_M(), T24_2_DM_M()
    MO_BNSTG_1, MO_BNSTG_2, MO_BNSTG_3, MO_BNSTG_4 = MO_BNSTG_1_M(), MO_BNSTG_2_M(), MO_BNSTG_3_M(), MO_BNSTG_4_M()
    TB, TB2 = TB_DM_M(), TB_DM_M2()
    FRG_MOR, FRG_MOR2 = FRG_MOR_M(), FRG_MOR_M2()
    RB2_1, RB2_1_2, RB2_2 = RB2_1_DM_M(), RB2_1_D_M2(), RB2_2_DM_M()

    RECON_CHK_DAG = RECON_CHK()
    H23_2_DAG = H23_2()
    MO_3_2_DAG = MO_BNSTG_3_2()
    DB2_LOAD_DAG = DB2_LOAD()

    REXCP_RW = REXCP_RW_M()
    REXCP_KS = REXCP_KS_M()
    REXCP_MO = REXCP_MO_M()
    RCCAR = RCCAR_DM_M()
    TR4_T = TR4_T_DM_M()
    RSK_REP = RSK_REP_M()

    RR4_K = RR4_K_DM_M()
    DLGD_2 = DLGD_2_DM_M()
    I2 = INGRESS_2()

    DLGD_1 = DLGD_1_DM_M()
    SNP = SNP_DQ_M()
    REXCP_TL = REXCP_TL_M()
    H24_Q = H24_DM_Q()

    TE3 = TE3_DM_M()
    RE3 = RE3_DM_M()
    FC_DT4 = FC_DT4_M()
    DT4_JOBS = DT4_M_JOBS()
    RECAP = RECAP_DM_M()
    DT4 = DT4_DM_M()

    RB2_3 = RB2_3_DM_M()
    RNCR = RNCR_DM_M()

    CMON_TSKS = CMON_TSKS()

    KS_PLL = KS_PLL()



    CMON_TSKS >> DLGD_1 >> H11
    [ H11, H21_APPROVED ] >> H21
    H21 >> [ T21, H22, MO_BNSTG_1 ]
    H22 >> [ T22, H23 ]
    [ H22, T24_1 ] >> H23

    H23 >> [ H24, T24_2 ]

    H23_2_DAG >> [ H24, T24_2 ]

    H24 >> REXCP_KS
    H24 >> H24_Q

    T21 >> [ T23, T22 ]
    [ T22, T23 ] >> T24_1
    T24_2 >> REXCP_TL
    RECON_CHK_APPROVED >> RECON_CHK_DAG
    [ RECON_CHK_DAG, T21 ] >> TR4_T

    MO_BNSTG_1 >> [SNP, MO_BNSTG_2]
    [ MO_BNSTG_2, H23] >> MO_BNSTG_3
    MO_BNSTG_3 >> DLGD_2

    [MO_BNSTG_3, MO_3_2_APPROVED] >> MO_3_2_DAG
    [MO_BNSTG_3, MO_3_2_APPROVED] >> H23_2_DAG
    MO_3_2_DAG >> DLGD_2

    [DLGD_2, RECON_CHK_DAG] >> MO_BNSTG_4
    [T24_2, MO_BNSTG_4] >> I2

    [ H21, RECON_CHK_DAG ] >> RR4_K
    [ TB, RR4_K, H24 ] >> RB2_1
    [ MO_BNSTG_4, RB2_1 ] >> FRG_MOR
    [ DLGD_2, T24_2, TR4_T ] >> TB
    FRG_MOR >> DB2_LOAD_DAG
    DB2_LOAD_DAG >> KS_PLL
    DB2_LOAD_DAG >> TB2
    TB2 >> RB2_1_2
    RB2_1_2 >> FRG_MOR2
    DB2_LOAD_DAG >> REXCP_MO
    TB2 >> TE3
    RB2_1_2 >> RE3
    FRG_MOR2 >> RB2_2
    RB2_2  >> RCCAR

    RCCAR_APPROVED >> RECAP
    RCCAR >> [ FC_DT4, DT4_JOBS, RECAP ]
    [ FC_DT4, DT4_JOBS ] >> DT4
    DT4_JOBS >> [ DT4, REXCP_RW ]
    REXCP_RW >> RSK_REP

    RB2_3_APPROVED >> RB2_3
    [ RB2_3, RB2_2 ] >> RNCR

    # For quarter end specifically:
    RUN_QUARTER_END_H24 >> H24_Q
    RUN_QUARTER_END_DT4 >> FC_DT4
