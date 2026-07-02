/***************************************************************************************************************************/
/***************************************************************************************************************************/
/***************************************************************************************************************************/
/*                                                                                                                         */
/*  Source Database: IIAS EDRTLRP1D                                                                                        */
/*  Source Table1:  REVLVNG_CR_EAD_OBSVTN_PT_REALZ_VAL                                                                     */
/*  Source Table2:  BASEL_REVLVNG_CR_BASE_DRVD_VARS                                                                        */
/*  Target Database: IIAS EDRTLRP1D                                                                                        */
/*  Target Table:  EAD_SEG_QTR_REALZ_VAL                                                                                   */
/*                                                                                                                         */
/*  Purpose: Load EAD_SEG_QTR_REALZ_VAL                                                                                    */
/*                                                                                                                         */
/*  Frequency: QTR End runs                                                                                                */
/*                                                                                                                         */
/*  Notes:                                                                                                                 */
/*  		                                                                                                               */
/*	Change Log:                                                                                                          */
/*	2025-08-18: Dhaivat Patel - Initial Development                                                                      */
/*                                                                                                                         */
/*                                                                                                                         */
/*                                                                                                                         */
/***************************************************************************************************************************/


%RRAP_AUTOEXEC


%PUT &=NET_DB;
%PUT &=MTH_TM_ID;

%MACRO CALC_QTR_REALZ_VALS();
PROC SQL;
      CONNECT USING NZRRAP AS IIASCON;
EXECUTE(
      DELETE FROM
            &NET_DB..EAD_SEG_QTR_REALZ_VAL
      WHERE
            QTR_MTH_TM_ID >= &MTH_TM_ID
) BY IIASCON;
EXECUTE(
      INSERT INTO &NET_DB..EAD_SEG_QTR_REALZ_VAL
      (SELECT * FROM
      (WITH MAIN_POPULATION AS (
SELECT
	A.OBSVTN_MTH_TM_ID,
	A.BASEL_ACCT_ID,
	A.EAD_RTO,
	A.EAD_FLR_CAP_200_RTO
FROM
	&NET_DB..REVLVNG_CR_EAD_OBSVTN_PT_REALZ_VAL A
LEFT JOIN (SELECT DISTINCT BASEL_ACCT_ID, MTH_TM_ID, CONSM_PRD_TREATMNT_CD, TRNST_EXCLSN_F, SML_BUS_F FROM &NET_DB..BASEL_REVLVNG_CR_BASE_DRVD_VARS) B ON
	A.BASEL_ACCT_ID = B.BASEL_ACCT_ID
	AND A.OBSVTN_MTH_TM_ID = B.MTH_TM_ID
WHERE
	A.PROCESS_MTH_TM_ID <= &MTH_TM_ID.
	AND A.PROCESS_MTH_TM_ID >= &MTH_TM_ID. - 80
	AND B.CONSM_PRD_TREATMNT_CD = 'A'
	AND B.TRNST_EXCLSN_F <> 'Y'
	AND B.SML_BUS_F <> 'Y')
SELECT
	&MTH_TM_ID. AS QTR_MTH_TM_ID,
	&MTH_TM_ID.-480 AS OBSVTN_QTR_TM_ID,
	SEG.BASEL_MODEL_ID,
	SEG.BASEL_SEG_ID,
	SEG.BASEL_MODEL_REL_ID,
	AVG(MAIN.EAD_RTO) AS AVG_EAD_SEG_RTO,
	AVG(MAIN.EAD_FLR_CAP_200_RTO) AS AVG_EAD_FLR_CAP_200_RTO,
	COUNT(MAIN.BASEL_ACCT_ID) AS MODEL_DEF_F_TOT_CNT,
	STDDEV(MAIN.EAD_FLR_CAP_200_RTO) AS CALC_STD,
	CURRENT_TIMESTAMP AS INST_PROCESS_TMSTMP,
	CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP
FROM
	MAIN_POPULATION MAIN
LEFT JOIN &NET_DB..EAD_SEG_ACCT_XREF SEG ON
	MAIN.BASEL_ACCT_ID = SEG.BASEL_ACCT_ID
	AND MAIN.OBSVTN_MTH_TM_ID = SEG.MTH_TM_ID
GROUP BY
	SEG.BASEL_SEG_ID,
	SEG.BASEL_MODEL_ID,
	SEG.BASEL_MODEL_REL_ID
ORDER BY SEG.BASEL_SEG_ID)))BY IIASCON;
DISCONNECT FROM IIASCON;
QUIT;

%MEND CALC_QTR_REALZ_VALS;


PROC SQL NOPRINT;
      SELECT CLNDR_QTR_CD INTO :CLNDR_QTR_CD FROM &NET_DB.TM_DIM WHERE TM_ID = &MTH_TM_ID AND TM_LVL = 'Month';
      SELECT FNCL_QTR_CD INTO :FNCL_QTR_CD FROM &NET_DB.TM_DIM WHERE TM_ID = &MTH_TM_ID AND TM_LVL = 'Month';
QUIT;

%PUT &=CLNDR_QTR_CD;
%PUT &=FNCL_QTR_CD;

DATA _NULL_;
      %IF &CLNDR_QTR_CD = &FNCL_QTR_CD %THEN %DO;
            %CALC_QTR_REALZ_VALS
      %END;
      %ELSE %DO;
            %PUT "NOT A QTR END MTH";
      %END;
RUN;