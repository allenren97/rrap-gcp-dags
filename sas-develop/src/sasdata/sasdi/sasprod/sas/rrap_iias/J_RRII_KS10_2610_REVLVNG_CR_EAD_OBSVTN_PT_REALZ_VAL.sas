/***************************************************************************************************************************/
/***************************************************************************************************************************/
/***************************************************************************************************************************/
/*                                                                                                                         */
/*  Source Database: IIAS EDRTLRP1D, EDRLGD                                                                                */
/*  Source Table1:  BASEL_REVLVNG_CR_BASE_DRVD_VARS                                                                        */
/*  Source Table2:  NEW_DFLT_EVENTS                                                                                        */
/*  Source Table3:  BASEL_REVLVNG_CR_MTH_SNAPSHOT												   */
/*  Target Database: IIAS EDRTLRP1D                                                                                        */
/*  Target Table:  REVLVNG_CR_EAD_OBSVTN_PT_REALZ_VAL                                                                      */
/*                                                                                                                         */
/*  Purpose: Load REVLVNG_CR_EAD_OBSVTN_PT_REALZ_VAL                                                                       */
/*                                                                                                                         */
/*  Frequency: Month End runs                                                                                              */
/*                                                                                                                         */
/*  Notes:                                                                                                                 */
/*  		                                                                                                               */
/*	Change Log:                                                                                                          */
/*	2025-07-24: Dhaivat Patel - Initial Development                                                                      */
/*                                                                                                                         */
/*                                                                                                                         */
/*                                                                                                                         */
/***************************************************************************************************************************/


%RRAP_AUTOEXEC

%PUT &=NET_DB;
%PUT &=NET_LGD_DB;
%PUT &=MTH_TM_ID;

PROC SQL NOPRINT;
      CONNECT USING NZRRAP AS IIASCON;
SELECT RECORDS INTO :PREVIOUS_COUNT FROM 
CONNECTION TO IIASCON
(SELECT COUNT(*) AS RECORDS FROM &NET_DB..REVLVNG_CR_EAD_OBSVTN_PT_REALZ_VAL WHERE PROCESS_MTH_TM_ID >= &MTH_TM_ID);
EXECUTE(DELETE FROM &NET_DB..REVLVNG_CR_EAD_OBSVTN_PT_REALZ_VAL WHERE PROCESS_MTH_TM_ID >= &MTH_TM_ID)BY IIASCON;
SELECT RECORDS INTO :AFTER_CLEANUP_COUNT FROM
CONNECTION TO IIASCON
(SELECT COUNT(*) AS RECORDS FROM &NET_DB..REVLVNG_CR_EAD_OBSVTN_PT_REALZ_VAL WHERE PROCESS_MTH_TM_ID >= &MTH_TM_ID);
EXECUTE(INSERT INTO &NET_DB..REVLVNG_CR_EAD_OBSVTN_PT_REALZ_VAL
(SELECT * FROM
(WITH PIT_STATUS AS (
	SELECT
		BASEL_ACCT_ID,
		PIT_STAT_VER_2_CD AS PIT_STATUS,
		&MTH_TM_ID. AS PROCESS_MTH,
		&MTH_TM_ID. - 480 AS OBS_MTH_TM_ID,
		(
			SELECT
				TM_LVL_END_DT
			FROM
				&NET_DB..TM_DIM
			WHERE
				TM_LVL = 'Month'
				AND TM_ID = &MTH_TM_ID.
		) AS PROCESS_LVL_END_DT,
		(
			SELECT
				TM_LVL_END_DT
			FROM
				&NET_DB..TM_DIM
			WHERE
				TM_LVL = 'Month'
				AND TM_ID = &MTH_TM_ID. - 480
		) AS OBS_LVL_END_DT
	FROM
		&NET_DB..BASEL_REVLVNG_CR_BASE_DRVD_VARS
	WHERE
		MTH_TM_ID = &MTH_TM_ID. - 480
		AND PIT_STAT_VER_2_CD = 'CUR'
),
NEW_DEFAULT_DATA AS (
	SELECT
		BASEL_ACCT_ID,
		DEFAULT_MTH,
		BDEF
	FROM
		(
			SELECT
				*,
				ROW_NUMBER() OVER (
					PARTITION BY BASEL_ACCT_ID
					ORDER BY
						PROCESS_MTH DESC
				) AS RN,
				B.TM_ID AS MTH_TM_ID,
				A.DEFAULT_BAL AS BDEF
			FROM
				&NET_LGD_DB..NEW_DFLT_EVENTS A
				INNER JOIN &NET_DB..TM_DIM B ON LAST_DAY(A.PROCESS_MTH) = B.TM_LVL_END_DT
				AND SRC_SYS_CD = 'KS'
				AND B.TM_LVL = 'Month'
				AND TM_ID <= &MTH_TM_ID.
		)
	WHERE
		RN = 1
		AND MTH_TM_ID >= &MTH_TM_ID. - 480
		AND MTH_TM_ID <= &MTH_TM_ID.
),
SNAPSHOT_BAL_DATA AS (
	SELECT
		BASEL_ACCT_ID,
		TOT_NEW_BAL_AMT AS BOBS,
		CR_LMT_AMT AS CLOBS
	FROM
		&NET_DB..BASEL_REVLVNG_CR_MTH_SNAPSHOT
	WHERE
		MTH_TM_ID = &MTH_TM_ID. - 480
),
SNAPSHOT_ACCT_OPEN_DATA AS (
	SELECT
		BASEL_ACCT_ID,
		ACCT_OPND_DT
	FROM
		(
			SELECT
				*,
				ROW_NUMBER() OVER (
					PARTITION BY BASEL_ACCT_ID
					ORDER BY
						ACCT_OPND_DT DESC
				) AS RN
			FROM
				(
					SELECT
						DISTINCT BASEL_ACCT_ID,
						ACCT_OPND_DT
					FROM
						&NET_DB..BASEL_REVLVNG_CR_MTH_SNAPSHOT
				)
		)
	WHERE
		RN = 1
)
SELECT
	PS.OBS_MTH_TM_ID AS OBSVTN_MTH_TM_ID,
	PS.BASEL_ACCT_ID,
	NDD.BDEF / MAX(SBD.BOBS, SBD.CLOBS) AS EAD_RATIO,
	CASE 
		WHEN NDD.BDEF / MAX(SBD.BOBS, SBD.CLOBS) <0 THEN 0
		WHEN NDD.BDEF / MAX(SBD.BOBS, SBD.CLOBS) > 1 THEN 1 
		ELSE NDD.BDEF / MAX(SBD.BOBS, SBD.CLOBS) 
		END AS EAD_FLR_CAP_100_RTO,
	CASE 
		WHEN NDD.BDEF / MAX(SBD.BOBS, SBD.CLOBS) <0 THEN 0
		WHEN NDD.BDEF / MAX(SBD.BOBS, SBD.CLOBS) > 1.25 THEN 1.25 
		ELSE NDD.BDEF / MAX(SBD.BOBS, SBD.CLOBS) 
		END AS EAD_FLR_CAP_200_RTO,
	CURRENT_TIMESTAMP AS INSRT_PROCESS_TMSTMP,
	CURRENT_TIMESTAMP AS UPDT_PROCESS_TMSTMP,
	PS.PROCESS_MTH
FROM
	PIT_STATUS AS PS
	INNER JOIN NEW_DEFAULT_DATA AS NDD ON PS.BASEL_ACCT_ID = NDD.BASEL_ACCT_ID
	LEFT JOIN SNAPSHOT_BAL_DATA AS SBD ON SBD.BASEL_ACCT_ID = PS.BASEL_ACCT_ID
	LEFT JOIN SNAPSHOT_ACCT_OPEN_DATA SAOD ON SAOD.BASEL_ACCT_ID = PS.BASEL_ACCT_ID
WHERE
	(
		LAST_DAY(NDD.DEFAULT_MTH) = PS.PROCESS_LVL_END_DT
		OR LAST_DAY(SAOD.ACCT_OPND_DT) = PS.OBS_LVL_END_DT
	)
	AND (
		SBD.BOBS > 0
		OR SBD.CLOBS > 0
	))))BY IIASCON;
SELECT RECORDS INTO :AFTER_INSERT FROM
CONNECTION TO IIASCON
(SELECT COUNT(*) AS RECORDS FROM &NET_DB..REVLVNG_CR_EAD_OBSVTN_PT_REALZ_VAL WHERE PROCESS_MTH_TM_ID >= &MTH_TM_ID);
DISCONNECT FROM IIASCON;
QUIT;

DATA _NULL_;
      IF &AFTER_INSERT = 0 THEN DO;
            PUT "NO DATA INSERTED";
            ABORT ABEND 255;
      END;
      ELSE DO;
            PUT "RECORDS INSERTED = &AFTER_INSERT.";
      END;
RUN;