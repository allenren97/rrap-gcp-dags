/*BB*/
/**************************************************************************************** 
* INFA Job Name			: WF_DM_RRAP_Load_BASEL_CC_LOC_CUST_SCORECRD_DRVD_VARS			* 
* SAS Job Name			: RRAP_IIAS_Load_BASEL_CC_LOC_CUST_SCORECRD_DRVD_VARS			* 
* Description			: This code is as a part of INFA to SAS Migration. 				*
* Source Server			: cs2iwiiasp01.bns	and cs2iw501.bns							*	 
* Source Database/Schema: IIASDB and OWSTAR / EDRTLRP1D and RRAP/OWDSS/OWTACT   		* 
* Source Table Names	: BASEL_CUST_ACCT_RLTNP_SNAPSHOT, BASEL_CUST_DIM,				*
*						  BASEL_CUST_MTH_DEP_TXN_SUM,BASEL_CUST_MTH_POSTN_SUM_FACT,		*
*						  BASEL_MORT_ACCT_DRVD_VARS,BASEL_MORT_MTH_SNAPSHOT,			*
*						  BASEL_PSNL_LOAN_ACCT_DRVD_VARS,BASEL_REVLVNG_CR_MTH_SNAPSHOT,	*
*						  BASEL_PSNL_LOAN_MTH_SNAPSHOT,BASEL_REVLVNG_CR_BASE_DRVD_VARS,	*
*						  BASEL_REVLVNG_CR_ACCT_DRVD_VARS,,CR_BUREAU_DELI_MTH_SNAPSHOT,	*
*						  OWDSS.CUST_MODEL,OWTACT.CUST_XREF,RRAP.IWD_CUST				*
* Target Server			: cs2iwiiasp01.bns												*
* Target Database/Schema: IIASDB / EDRTLRP1D											*
* Target Table Name 	: BASEL_CC_LOC_CUST_SCORECRD_DRVD_VARS							*
* SAS Code Location		: /sasdata/sasdi/sasprod/sas/rrap_iias							* 
* Server				: SASApp                                 						* 
* Created on			: Monday, Dec 20, 2021 					                		* 
* Created by			: Vijay Kadiyala                                        		* 
* Updated on			: Wednesday, Apr 20. 2021										*
* Updated by			: Vijay Kadiyala												*
* Updated Reason		: TO address NULL vs Zero values issue for the metrics			*
*							CUST_ACCT_CNT, DFT_TOT_BAL_AMT								*
* Version				: SAS Enterprise Guide 7.1 		                  				* 
****************************************************************************************/ 

%put WORK LOCATION: %sysfunc(getoption(work));
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);


/* Create metadata macro variables */
%let IOMServer      = %nrquote(SASApp);
%let metaPort       = %nrquote(8563);
%let metaServer     = %nrquote(iw503);

/* Set metadata options */
options metaport       = &metaPort 
        metaserver     = "&metaServer"; 

/* Setup for capturing job status  */ 
%let etls_startTime = %sysfunc(datetime(),datetime.);
%let etls_recordsBefore = 0;
%let etls_recordsAfter = 0;
%let etls_lib = 0;
%let etls_table = 0;


/* creating source qualifer table */
proc sql ;
      CONNECT USING NZRRAP AS NZCON;
       create table TEMP_SOURCE_Q as 
          select *              
          from connection to NZCON
      (SELECT DISTINCT  PRIM_BASEL_CUST_ID,
                              MTH_TM_ID,
                              ssb_PIT_STAT_VER_2_CD, 
                              SRC_SYS_CD, 
                              KS_PRIM_BASEL_CUST_ID,
                              v_KS_SUM_OS_BAL_AMT_lkp,
                              v_MO_SUM_OS_BAL_AMT_lkp,
                              v_SPL_SUM_OS_BAL_AMT_lkp

/* SQ_BASEL_REVLVNG_CR_MTH_SNAPSHOT */                      
       FROM (
                  SELECT  ss.MTH_TM_ID AS MTH_TM_ID, 
                              ss.PRIM_BASEL_CUST_ID AS PRIM_BASEL_CUST_ID,
                              ssb.PIT_STAT_VER_2_CD as ssb_PIT_STAT_VER_2_CD, 
                              'KS' as SRC_SYS_CD FROM
                  
      &net_db..BASEL_REVLVNG_CR_MTH_SNAPSHOT ss
      ,&net_db..BASEL_REVLVNG_CR_BASE_DRVD_VARS ssb
      where 
      ss.PRIM_BASEL_CUST_ID=ssb.BASEL_CUST_ID AND
      ss.PRIM_BASEL_CUST_ID IS NOT NULL  AND
      ss.PRIM_BASEL_CUST_ID <>-1 AND
      ssb.SML_BUS_F='N' AND
      TRIM(ssb.CONSM_PRD_TREATMNT_CD)='A' AND
      ss.MTH_TM_ID=&MTH_TM_ID AND 
      ssb.MTH_TM_ID =&MTH_TM_ID
          
        UNION ALL

      SELECT  ss.MTH_TM_ID AS MTH_TM_ID, 
                  ss.PRIM_BASEL_CUST_ID AS PRIM_BASEL_CUST_ID, 
                  ssb.PIT_STAT_VER_1_CD as ssb_PIT_STAT_VER_2_CD, 
                  'MO' as SRC_SYS_CD 
      FROM
      &net_db..BASEL_MORT_MTH_SNAPSHOT ss
      ,&net_db..BASEL_MORT_ACCT_DRVD_VARS ssb
      where 
      ss.PRIM_BASEL_CUST_ID=ssb.BASEL_CUST_ID AND
      ss.PRIM_BASEL_CUST_ID IS NOT NULL  AND
      ss.PRIM_BASEL_CUST_ID <>-1 AND
      TRIM(ssb.CONSM_PRD_TREATMNT_CD)='A' AND
      ss.MTH_TM_ID =&MTH_TM_ID. AND 
      ssb.MTH_TM_ID =&MTH_TM_ID.
        
        UNION ALL

      SELECT  ss.MTH_TM_ID AS MTH_TM_ID, 
                  ss.PRIM_BASEL_CUST_ID AS PRIM_BASEL_CUST_ID, 
                  ssb.PIT_STAT_VER_1_CD as ssb_PIT_STAT_VER_2_CD, 
                  'SPL' as SRC_SYS_CD
      FROM
      &net_db..BASEL_PSNL_LOAN_MTH_SNAPSHOT ss
      ,&net_db..BASEL_PSNL_LOAN_ACCT_DRVD_VARS ssb
      where 
      ss.PRIM_BASEL_CUST_ID=ssb.BASEL_CUST_ID AND
      ss.PRIM_BASEL_CUST_ID IS NOT NULL  AND
      ss.PRIM_BASEL_CUST_ID <>-1 AND
      TRIM(ssb.CONSM_PRD_TREATMNT_CD)='A' AND
      ss.MTH_TM_ID=&MTH_TM_ID. AND 
      ssb.MTH_TM_ID= &MTH_TM_ID.
      ) as X

/* exp_DataGather_KsConfirm */
LEFT JOIN 
      (
      SELECT  &net_db..BASEL_REVLVNG_CR_MTH_SNAPSHOT.PRIM_BASEL_CUST_ID AS KS_PRIM_BASEL_CUST_ID 
      FROM &net_db..BASEL_REVLVNG_CR_MTH_SNAPSHOT
      WHERE &net_db..BASEL_REVLVNG_CR_MTH_SNAPSHOT.MTH_TM_ID=&MTH_TM_ID.
      ORDER BY  PRIM_BASEL_CUST_ID ) LK1
      ON X.PRIM_BASEL_CUST_ID = LK1.KS_PRIM_BASEL_CUST_ID

/* exp_OsBalAmt_CALC */
LEFT OUTER JOIN
(
SELECT  SUM(DV.OS_BAL_AMT) AS v_KS_SUM_OS_BAL_AMT_lkp,  
            DV.BASEL_PRIM_CUST_ID AS BASEL_PRIM_CUST_ID,  
            BDV.PIT_STAT_VER_2_CD AS PIT_STAT_VER_2_CD 
FROM &net_db..BASEL_REVLVNG_CR_ACCT_DRVD_VARS DV
,&net_db..BASEL_REVLVNG_CR_BASE_DRVD_VARS BDV
WHERE
DV.MTH_TM_ID=&MTH_TM_ID. AND
DV.BASEL_PRIM_CUST_ID>0
AND BDV.BASEL_ACCT_ID = DV.BASEL_ACCT_ID
AND BDV.MTH_TM_ID = DV.MTH_TM_ID
AND BDV.PIT_STAT_VER_2_CD IN  ('DEF', 'CHG')

AND BDV.SML_BUS_F='N' 
AND TRIM(BDV.CONSM_PRD_TREATMNT_CD)='A'

GROUP BY DV.BASEL_PRIM_CUST_ID, BDV.PIT_STAT_VER_2_CD

ORDER BY  DV.BASEL_PRIM_CUST_ID, BDV.PIT_STAT_VER_2_CD )LK2
      ON X.PRIM_BASEL_CUST_ID = LK2.BASEL_PRIM_CUST_ID AND
            X.ssb_PIT_STAT_VER_2_CD = LK2.PIT_STAT_VER_2_CD 

LEFT JOIN
            (
      SELECT  SUM(OS_BAL_AMT) AS v_MO_SUM_OS_BAL_AMT_lkp,  
                  BASEL_CUST_ID AS BASEL_CUST_ID,  
                  PIT_STAT_VER_1_CD AS PIT_STAT_VER_1_CD 
      FROM &net_db..BASEL_MORT_ACCT_DRVD_VARS
      WHERE MTH_TM_ID=&MTH_TM_ID. AND TRIM(CONSM_PRD_TREATMNT_CD)='A'
      GROUP BY BASEL_CUST_ID, PIT_STAT_VER_1_CD
      ORDER BY BASEL_CUST_ID, PIT_STAT_VER_1_CD )LK3
      ON X.PRIM_BASEL_CUST_ID = LK3.BASEL_CUST_ID AND
            X.ssb_PIT_STAT_VER_2_CD = LK3.PIT_STAT_VER_1_CD

LEFT JOIN
      (
      SELECT 
      SUM(OS_BAL_AMT) as v_SPL_SUM_OS_BAL_AMT_lkp, 
      BASEL_CUST_ID as BASEL_CUST_ID,
      PIT_STAT_VER_1_CD as PIT_STAT_VER_1_CD
      FROM 
      &net_db..BASEL_PSNL_LOAN_ACCT_DRVD_VARS
	WHERE MTH_TM_ID=&MTH_TM_ID.	AND TRIM(CONSM_PRD_TREATMNT_CD)='A'
      GROUP BY BASEL_CUST_ID, PIT_STAT_VER_1_CD
      ORDER BY BASEL_CUST_ID, PIT_STAT_VER_1_CD)LK4
      ON X.PRIM_BASEL_CUST_ID = LK4.BASEL_CUST_ID AND
            X.ssb_PIT_STAT_VER_2_CD = LK4.PIT_STAT_VER_1_CD

);
      disconnect from NZCON; 
QUIT;


/* calculating expression */
data exp_OsBalAmt_CALC;
      set TEMP_SOURCE_Q;

      IF KS_PRIM_BASEL_CUST_ID ne . AND STRIP(SRC_SYS_CD) = 'KS' AND STRIP(ssb_PIT_STAT_VER_2_CD) = 'DEF' 
            then v_KS_SUM_OS_BAL_AMT = round(coalesce(v_KS_SUM_OS_BAL_AMT_lkp,0),0.001);

      IF KS_PRIM_BASEL_CUST_ID ne . AND STRIP(SRC_SYS_CD) = 'MO' AND STRIP(ssb_PIT_STAT_VER_2_CD) = 'DEF' 
            then v_MO_SUM_OS_BAL_AMT = round(coalesce(v_MO_SUM_OS_BAL_AMT_lkp,0),0.001);

      IF KS_PRIM_BASEL_CUST_ID ne . AND STRIP(SRC_SYS_CD) = 'SPL' AND STRIP(ssb_PIT_STAT_VER_2_CD) = 'DEF' 
      then v_SPL_SUM_OS_BAL_AMT = round(coalesce(v_SPL_SUM_OS_BAL_AMT_lkp,0),0.001);

      DFT_TOT_BAL_AMT = sum(v_KS_SUM_OS_BAL_AMT,v_MO_SUM_OS_BAL_AMT,v_SPL_SUM_OS_BAL_AMT);
run;

/* Temp of testing to be deleted*/
proc sql;
      create table TEMP_SOURCE_Q as
      select MTH_TM_ID, PRIM_BASEL_CUST_ID, KS_PRIM_BASEL_CUST_ID,coalesce(sum(DFT_TOT_BAL_AMT),0) as DFT_TOT_BAL_AMT
      from exp_OsBalAmt_CALC 
      group by MTH_TM_ID, PRIM_BASEL_CUST_ID, KS_PRIM_BASEL_CUST_ID;
quit;

/* Delete temp dataset if exists */
PROC SQL NOPRINT;
       CONNECT USING NZRRAP AS NZCON;
        EXECUTE (drop table &net_db..TEMP_SOURCE_Q if exists) BY NZCON;
QUIT;

/* creating source input table in database for further processing */
/* proc sql; */
/*       create table NZRRAP.TEMP_SOURCE_Q as */
/*       select MTH_TM_ID, PRIM_BASEL_CUST_ID, KS_PRIM_BASEL_CUST_ID,coalesce(sum(DFT_TOT_BAL_AMT),0) as DFT_TOT_BAL_AMT */
/*       from exp_OsBalAmt_CALC  */
/*       group by MTH_TM_ID, PRIM_BASEL_CUST_ID, KS_PRIM_BASEL_CUST_ID; */
/* quit; */

PROC APPEND BASE=NZRRAP.TEMP_SOURCE_Q (BULKLOAD=YES BL_METHOD=CLILOAD)
            DATA=TEMP_SOURCE_Q;
RUN;

/********************************************************************/
/* 		LOOKTABLES DATA EXTRACT and JOIN to SOURCE INPUT Table 		*/
/********************************************************************/

/* Creating final Input dataset */

proc sql ;
      CONNECT USING NZRRAP AS NZCON;
       create table SOURCE_INPUT as 
          select *              
          from connection to NZCON
      (
            select SQ1.MTH_TM_ID, SQ1.PRIM_BASEL_CUST_ID,SQ1.KS_PRIM_BASEL_CUST_ID,
      SQ1.DFT_TOT_BAL_AMT,

      L5.CUST_CID as v_CUST_CID,
      L9.MAX_BNS_DLQNT_DAY as LST12MTHRL_ACT_WST_DLQNT_DAY_CNT,
      L6.MAX_DLQNT_DAY_CNT as v_MAX_DLQNT_DAY_CNT_CurrMth,
      L7.MAX_DLQNT_DAY_CNT as v_MAX_DLQNT_DAY_CNT_5Mth,
      L8.SUM_DLQNT_DAY_CNT as v_MAX_DLQNT_DAY_CNT_between_mont,
      L18.SUM_TOT_NSF_AMT,
      L23.CNT_BASEL_CUST_ID_23, 
      L12.MAX_BNKRPY_CNT,
      L12.SUM_TRADE_NEVER_DLQNT_PC_12 ,
      L12.MAX_INQRY_CNT,
      L24.CNT_BASEL_CUST_ID_24 ,
      L11.MAX_TRADE_NEVER_DLQNT_PC AS LAST_12_MTH_MAX_TRD_NVR_DLQNT_PC,
      L11.SUM_INQRY_PAST_6_MTH_CNT,
      L11.MAX_INQRY_PAST_6_MTH_CNT AS LAST_12_MTH_MAX_CR_INQ_6_MTH_CNT,
      /* UPDATED: Now fetches Current Month Value */
      L11.BY34, 
      L13.MAX_OVDR_CNT,
      L10.CUST_CID,
/*	L10XRF.CUST_BASE_KEY,*/
      L17.SUM_NON_REGISTERED_INVSTMNT_BAL_AMT, /*SUM_NON_REGISTERED_INVST_BAL_AMT,*/
      L26.CNT_BASEL_CUST_ID_26,
      L19.SUM_TRADE_NEVER_DLQNT_PC_19,
      L19.MAX_MAX_REVLVNG_CR_CRNT_UTLTN_AMT,
      /* UPDATED: Now fetches Current Month Value */
      L19.AT147, 
      L25.CNT_BASEL_CUST_ID_25,
      L20.SUM_D2D_BAL_AMT,
      L20.MIN_D2D_BAL_AMT,
      L15.SUM_MTH_SINCE_LAST_60_DAY_DLQNT_CNT, /*SUM_MTH_SNE_LST_60_DAY_DLQNT_CNT,*/
      L15.MAX_TOT_BAL_TP_BANKCARD_AMT, 
      L15.MAX_TRADE_NEVER_DLQNT_PC,
      L15.SUM_TOT_UTLTN_AMT_15,
      L15.SUM_HIGHST_ACTV_UTLTN,
      L15.SUM_TOT_AVL_CR_NOT_UTILIZED_AMT,
      L15.SUM_INQRY_CNT,
      L15.MAX_INQRY_PAST_6_MTH_CNT ,
      L15.SUM_ACTV_INSTLMNT_TRDS_BAL_GT_ZERO_CNT, /*SUM_ACTV_INSTLMNT_TRDS_BAL_GT_ZERO_CNT,*/    
      L16.CURR_TOT_UTLTN_AMT,
      L16.PREV_TOT_UTLTN_AMT,
      L16.SUM_TOT_UTLTN_AMT_16,
      L16.CURR_TRADE_NEVER_DLQNT_PC,
      L16.PREV_TRADE_NEVER_DLQNT_PC,
      L16.SUM_TRADE_NEVER_DLQNT_PC_16,
      L21.SUM_TOT_NSF_AMT_21,
      L14.MAX_D2D_BAL_AMT,
      L14.SUM_RGSTRD_INVSTMNT_BAL_ACCT_CNT, 
      L14.MIN_NON_REGISTERED_INVSTMNT_ACCT_CNT,
      /* ADDED: New 3 Month Sum Var */
      L_NEW1.CSH_AD_CRNT_C_BAL_KSCSUM3M

      FROM &net_db..TEMP_SOURCE_Q SQ1

      LEFT JOIN 
      (
      SELECT CUST_CID,BASEL_CUST_ID 
      FROM &net_db..BASEL_CUST_DIM 
      ORDER BY BASEL_CUST_ID
      ) L5
      on SQ1.PRIM_BASEL_CUST_ID = L5.BASEL_CUST_ID

      LEFT JOIN
      (
      SELECT  MAX(CASE WHEN (BNS_DLQNT_DAY-30)<0 THEN 0 
                              ELSE (BNS_DLQNT_DAY-30) END) AS MAX_BNS_DLQNT_DAY,  
                  PRIM_BASEL_CUST_ID AS PRIM_BASEL_CUST_ID 
      FROM &net_db..BASEL_REVLVNG_CR_MTH_SNAPSHOT
      WHERE MTH_TM_ID>=(&MTH_TM_ID.-440) AND MTH_TM_ID<=&MTH_TM_ID.
      GROUP BY PRIM_BASEL_CUST_ID 
      ORDER BY PRIM_BASEL_CUST_ID,MAX_BNS_DLQNT_DAY
      )L9
      on SQ1.PRIM_BASEL_CUST_ID = L9.PRIM_BASEL_CUST_ID

      /* ADDED: New Join for 3-month sum from BASEL_REVLVNG_CR_MTH_SNAPSHOT */
      LEFT JOIN
      (
            WITH CurrentMonthAccounts AS (
            SELECT DISTINCT PRIM_BASEL_CUST_ID, BASEL_ACCT_ID
            FROM &NET_DB..BASEL_REVLVNG_CR_MTH_SNAPSHOT
            WHERE MTH_TM_ID = &MTH_TM_ID.
            ),
            Last3MonthsData AS (
            SELECT T.PRIM_BASEL_CUST_ID,
                  T.BASEL_ACCT_ID,
                  T.CSH_ADVNC_CRNT_CYCL_BAL_AMT,
                  T.MTH_TM_ID
            FROM &NET_DB..BASEL_REVLVNG_CR_MTH_SNAPSHOT T
            WHERE T.MTH_TM_ID >= &MTH_TM_ID.-80 AND T.MTH_TM_ID <= &MTH_TM_ID.
            )
            SELECT L.PRIM_BASEL_CUST_ID,
                  SUM(L.CSH_ADVNC_CRNT_CYCL_BAL_AMT) AS CSH_AD_CRNT_C_BAL_KSCSUM3M
            FROM Last3MonthsData L
            JOIN CurrentMonthAccounts C
            ON L.PRIM_BASEL_CUST_ID = C.PRIM_BASEL_CUST_ID
            AND L.BASEL_ACCT_ID = C.BASEL_ACCT_ID
            GROUP BY L.PRIM_BASEL_CUST_ID
      ) L_NEW1
      on SQ1.PRIM_BASEL_CUST_ID = L_NEW1.PRIM_BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  MAX(A.DLQNT_DAY_CNT) AS MAX_DLQNT_DAY_CNT,  
                  A.BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM &net_db..BASEL_MORT_ACCT_DRVD_VARS A,&net_db..BASEL_MORT_MTH_SNAPSHOT B
      WHERE
/*--A.BASEL_ACCT_ID=B.BASEL_ACCT_ID AND*/
            B.PRIM_BASEL_CUST_ID = A.BASEL_CUST_ID AND A.MTH_TM_ID=(&MTH_TM_ID.) AND 
            B.MTH_TM_ID=A.MTH_TM_ID AND UPPER(TRIM(A.COMM_TP_CD))='RESIDENTIAL' 
            AND B.CRNT_BAL_AMT>0 AND TRIM(B.PD_OFF_F)='N'
      GROUP BY A.BASEL_CUST_ID
      ORDER BY  A.BASEL_CUST_ID 
/*-- ORDER BY BASEL_CUST_ID,MAX_DLQNT_DAY_CNT*/
      )L6
      on SQ1.PRIM_BASEL_CUST_ID = L6.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  MAX(A.DLQNT_DAY_CNT) AS MAX_DLQNT_DAY_CNT,  
                  A.BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM &net_db..BASEL_MORT_ACCT_DRVD_VARS A, &net_db..BASEL_MORT_MTH_SNAPSHOT B
      WHERE
/*--A.BASEL_ACCT_ID=B.BASEL_ACCT_ID AND*/
            B.PRIM_BASEL_CUST_ID = A.BASEL_CUST_ID AND
            A.MTH_TM_ID=(&MTH_TM_ID.-200) AND B.MTH_TM_ID=A.MTH_TM_ID AND
            UPPER(TRIM(A.COMM_TP_CD))='RESIDENTIAL' AND
            B.CRNT_BAL_AMT>0 AND TRIM(B.PD_OFF_F)='N'
      GROUP BY A.BASEL_CUST_ID
      ORDER BY  A.BASEL_CUST_ID )L7
      on SQ1.PRIM_BASEL_CUST_ID = L7.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  SUM(NVL(A.DLQNT_DAY_CNT,0)) AS SUM_DLQNT_DAY_CNT,  
                  A.BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM &net_db..BASEL_MORT_ACCT_DRVD_VARS A,&net_db..BASEL_MORT_MTH_SNAPSHOT B
      WHERE
            B.PRIM_BASEL_CUST_ID = A.BASEL_CUST_ID AND
            (A.MTH_TM_ID>(&MTH_TM_ID.-200) AND A.MTH_TM_ID<(&MTH_TM_ID.))
            AND B.MTH_TM_ID=A.MTH_TM_ID AND
            UPPER(TRIM(A.COMM_TP_CD))='RESIDENTIAL' AND
            B.CRNT_BAL_AMT>0 AND TRIM(B.PD_OFF_F)='N'
      GROUP BY A.BASEL_CUST_ID
      ORDER BY  A.BASEL_CUST_ID 
      )L8
      on SQ1.PRIM_BASEL_CUST_ID = L8.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  SUM(A.TOT_NSF_AMT) AS SUM_TOT_NSF_AMT,  
                  A.BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM &net_db..BASEL_CUST_MTH_DEP_TXN_SUM A
      JOIN 
      (
      SELECT DISTINCT A.BASEL_CUST_ID, A.MTH_TM_ID 
	FROM	&net_db..BASEL_CUST_ACCT_RLTNP_SNAPSHOT A
      WHERE (A.MTH_TM_ID >= &MTH_TM_ID.-440 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      )B
      ON A.BASEL_CUST_ID=B.BASEL_CUST_ID
      AND (A.MTH_TM_ID >= &MTH_TM_ID.-440 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      AND A.MTH_TM_ID=B.MTH_TM_ID
      GROUP BY A.BASEL_CUST_ID
      ORDER BY A.BASEL_CUST_ID 
      )L18
      on SQ1.PRIM_BASEL_CUST_ID = L18.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  COUNT(1) AS CNT_BASEL_CUST_ID_23,  
                  BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM (
      SELECT  DISTINCT  MTH_TM_ID,  BASEL_CUST_ID 
      FROM  &net_db..BASEL_CUST_ACCT_RLTNP_SNAPSHOT
	WHERE MTH_TM_ID>=&MTH_TM_ID.-440	AND   MTH_TM_ID<=&MTH_TM_ID.)  A
      GROUP BY  BASEL_CUST_ID ORDER BY BASEL_CUST_ID,CNT_BASEL_CUST_ID_23
      )L23
      on SQ1.PRIM_BASEL_CUST_ID = L23.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  NSS.MAX_BNKRPY_CNT AS MAX_BNKRPY_CNT,  
                  WSS.SUM_TRADE_NEVER_DLQNT_PC AS SUM_TRADE_NEVER_DLQNT_PC_12,  
                  NSS.MAX_INQRY_CNT AS MAX_INQRY_CNT,  
                  NSS.BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM (
      SELECT SUM(A.TRADE_NEVER_DLQNT_PC) AS SUM_TRADE_NEVER_DLQNT_PC,
                  A.BASEL_CUST_ID as BASEL_CUST_ID
      FROM &net_db..CR_BUREAU_DELI_MTH_SNAPSHOT A
      JOIN 
      (
      SELECT DISTINCT A.BASEL_CUST_ID, A.MTH_TM_ID 
      FROM &net_db..BASEL_CUST_ACCT_RLTNP_SNAPSHOT A
      WHERE (A.MTH_TM_ID >= &MTH_TM_ID.-920 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      )B
      ON A.BASEL_CUST_ID=B.BASEL_CUST_ID
      AND (A.MTH_TM_ID >= &MTH_TM_ID.-920 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      AND A.MTH_TM_ID=B.MTH_TM_ID
      GROUP BY A.BASEL_CUST_ID
      ORDER BY A.BASEL_CUST_ID)WSS
      RIGHT OUTER JOIN 
      (
      SELECT MAX(A.BNKRPY_CNT) as MAX_BNKRPY_CNT, 
                  MAX(A.INQRY_CNT) as MAX_INQRY_CNT, 
                  A.BASEL_CUST_ID as BASEL_CUST_ID
      FROM &net_db..CR_BUREAU_DELI_MTH_SNAPSHOT A
      WHERE (A.MTH_TM_ID >= &MTH_TM_ID.-920 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      GROUP BY A.BASEL_CUST_ID
      ORDER BY A.BASEL_CUST_ID ) NSS
      ON WSS.BASEL_CUST_ID=NSS.BASEL_CUST_ID
      ORDER BY NSS.BASEL_CUST_ID 
      )L12
      on SQ1.PRIM_BASEL_CUST_ID = L12.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  COUNT(1) AS CNT_BASEL_CUST_ID_24,  
                  BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM (
      SELECT  DISTINCT  MTH_TM_ID,  BASEL_CUST_ID 
      FROM  &net_db..BASEL_CUST_ACCT_RLTNP_SNAPSHOT
	WHERE MTH_TM_ID>=&MTH_TM_ID.-920	AND   MTH_TM_ID<=&MTH_TM_ID.)  A
      GROUP BY  BASEL_CUST_ID ORDER BY BASEL_CUST_ID,CNT_BASEL_CUST_ID_24
      )L24
      on SQ1.PRIM_BASEL_CUST_ID = L24.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  NSS.MAX_TRADE_NEVER_DLQNT_PC AS MAX_TRADE_NEVER_DLQNT_PC,  
                  WSS.SUM_INQRY_PAST_6_MTH_CNT AS SUM_INQRY_PAST_6_MTH_CNT,  
                  NSS.MAX_INQRY_PAST_6_MTH_CNT AS MAX_INQRY_PAST_6_MTH_CNT,  
                  /* ADDED: New 12 Month Min Var selected from subquery NSS */
                  NSS.BY34 AS BY34,
                  NSS.BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM (
      SELECT SUM(A.INQRY_PAST_6_MTH_CNT) AS SUM_INQRY_PAST_6_MTH_CNT,
                  A.BASEL_CUST_ID as BASEL_CUST_ID 
      FROM &net_db..CR_BUREAU_DELI_MTH_SNAPSHOT A
      JOIN 
      (
      SELECT DISTINCT A.BASEL_CUST_ID, A.MTH_TM_ID 
      FROM &net_db..BASEL_CUST_ACCT_RLTNP_SNAPSHOT A
      WHERE (A.MTH_TM_ID >= &MTH_TM_ID.-440 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      )B
      ON A.BASEL_CUST_ID=B.BASEL_CUST_ID
      AND (A.MTH_TM_ID >= &MTH_TM_ID.-440 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      AND A.MTH_TM_ID=B.MTH_TM_ID
      GROUP BY A.BASEL_CUST_ID
      ORDER BY A.BASEL_CUST_ID) WSS
      RIGHT OUTER JOIN
      (
      SELECT MAX(A.TRADE_NEVER_DLQNT_PC) as MAX_TRADE_NEVER_DLQNT_PC, 
                  MAX(A.INQRY_PAST_6_MTH_CNT) as MAX_INQRY_PAST_6_MTH_CNT, 
                  
                  /* MODIFIED START: Replaced MIN 12M with Current Month Value using Conditional Aggregation */
                  MAX(CASE WHEN A.MTH_TM_ID = &MTH_TM_ID. THEN A.TOT_UTLTN_BNK_REVLVNG_LINE_AMT ELSE NULL END) as BY34,
                  /* MODIFIED END */

                  A.BASEL_CUST_ID as BASEL_CUST_ID 
      FROM &net_db..CR_BUREAU_DELI_MTH_SNAPSHOT A
      WHERE (A.MTH_TM_ID >= &MTH_TM_ID.-440 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      GROUP BY A.BASEL_CUST_ID
      ORDER BY A.BASEL_CUST_ID) NSS
      ON WSS.BASEL_CUST_ID=NSS.BASEL_CUST_ID
      ORDER BY NSS.BASEL_CUST_ID 
      )L11
      on SQ1.PRIM_BASEL_CUST_ID = L11.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  MAX(OVDR_CNT) AS MAX_OVDR_CNT,  
                  BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM &net_db..BASEL_CUST_MTH_DEP_TXN_SUM
      WHERE MTH_TM_ID>=&MTH_TM_ID.-80  AND MTH_TM_ID<=&MTH_TM_ID.
      GROUP BY BASEL_CUST_ID
      ORDER BY BASEL_CUST_ID 
      )L13
      on SQ1.PRIM_BASEL_CUST_ID = L13.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  TRIM(CUST_CID) AS CUST_CID,  
                  BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM &net_db..BASEL_CUST_DIM 
      ORDER BY BASEL_CUST_ID,CUST_CID
      )L10
      on SQ1.PRIM_BASEL_CUST_ID = L10.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  SUM(A.NON_REGISTERED_INVSTMNT_BAL_AMT) AS SUM_NON_REGISTERED_INVSTMNT_BAL_AMT,  
                  A.BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM &net_db..BASEL_CUST_MTH_POSTN_SUM_FACT A
      JOIN 
      (
      SELECT DISTINCT A.BASEL_CUST_ID, A.MTH_TM_ID 
	FROM	&net_db..BASEL_CUST_ACCT_RLTNP_SNAPSHOT A
      WHERE (A.MTH_TM_ID >= &MTH_TM_ID.-200 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      )B
      ON A.BASEL_CUST_ID=B.BASEL_CUST_ID
      AND (A.MTH_TM_ID >= &MTH_TM_ID.-200 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      AND A.MTH_TM_ID=B.MTH_TM_ID
      GROUP BY A.BASEL_CUST_ID
      ORDER BY A.BASEL_CUST_ID 
      )L17
      on SQ1.PRIM_BASEL_CUST_ID = L17.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  COUNT(1) AS CNT_BASEL_CUST_ID_26,  
                  BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM (
      SELECT  DISTINCT  MTH_TM_ID,  BASEL_CUST_ID 
      FROM  &net_db..BASEL_CUST_ACCT_RLTNP_SNAPSHOT
      WHERE MTH_TM_ID>=&MTH_TM_ID.-200    AND   MTH_TM_ID<=&MTH_TM_ID.)  A
      GROUP BY  BASEL_CUST_ID 
      ORDER BY BASEL_CUST_ID,CNT_BASEL_CUST_ID_26

      )L26
      on SQ1.PRIM_BASEL_CUST_ID = L26.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  WSS.SUM_TRADE_NEVER_DLQNT_PC AS SUM_TRADE_NEVER_DLQNT_PC_19,  
            NSS.MAX_MAX_REVLVNG_CR_CRNT_UTLTN_AMT AS MAX_MAX_REVLVNG_CR_CRNT_UTLTN_AMT, 
            /* ADDED: New 3 Month Min Var selected from subquery NSS */
            NSS.AT147 AS AT147, 
            NSS.BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM (
      SELECT SUM(A.TRADE_NEVER_DLQNT_PC) AS SUM_TRADE_NEVER_DLQNT_PC,
                  A.BASEL_CUST_ID as BASEL_CUST_ID 
      FROM &net_db..CR_BUREAU_DELI_MTH_SNAPSHOT A
      JOIN 
      (
      SELECT DISTINCT A.BASEL_CUST_ID, A.MTH_TM_ID 
	FROM	&net_db..BASEL_CUST_ACCT_RLTNP_SNAPSHOT A
      WHERE (A.MTH_TM_ID >= &MTH_TM_ID.-80 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      )B
      ON A.BASEL_CUST_ID=B.BASEL_CUST_ID
      AND (A.MTH_TM_ID >= &MTH_TM_ID.-80 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      AND A.MTH_TM_ID=B.MTH_TM_ID
      GROUP BY A.BASEL_CUST_ID
      ORDER BY A.BASEL_CUST_ID
      ) WSS
      RIGHT OUTER JOIN
      (
      SELECT 
      MAX(A.MAX_REVLVNG_CR_CRNT_UTLTN_AMT) as MAX_MAX_REVLVNG_CR_CRNT_UTLTN_AMT, 
      
      /* MODIFIED START: Replaced MIN 3M with Current Month Value using Conditional Aggregation */
      MAX(CASE WHEN A.MTH_TM_ID = &MTH_TM_ID. THEN A.HIGHST_ACTV_UTLTN ELSE NULL END) as AT147,
      /* MODIFIED END */

      A.BASEL_CUST_ID as BASEL_CUST_ID 
      FROM &net_db..CR_BUREAU_DELI_MTH_SNAPSHOT A
	WHERE 	(A.MTH_TM_ID >= &MTH_TM_ID.-80 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      GROUP BY A.BASEL_CUST_ID
      ORDER BY A.BASEL_CUST_ID
      ) NSS
      ON WSS.BASEL_CUST_ID=NSS.BASEL_CUST_ID
      ORDER BY NSS.BASEL_CUST_ID 
      )L19
      on SQ1.PRIM_BASEL_CUST_ID = L19.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  COUNT(1) AS CNT_BASEL_CUST_ID_25,  
                  BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM (
      SELECT  DISTINCT  MTH_TM_ID,  BASEL_CUST_ID 
      FROM  &net_db..BASEL_CUST_ACCT_RLTNP_SNAPSHOT
      WHERE MTH_TM_ID>=&MTH_TM_ID.-80 AND   MTH_TM_ID<=&MTH_TM_ID.)  A
      GROUP BY  BASEL_CUST_ID 
      ORDER BY BASEL_CUST_ID,CNT_BASEL_CUST_ID_25

      )L25
      on SQ1.PRIM_BASEL_CUST_ID = L25.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  WSS.SUM_D2D_BAL_AMT AS SUM_D2D_BAL_AMT,  
                  NSS.MIN_D2D_BAL_AMT AS MIN_D2D_BAL_AMT,  
                  NSS.BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM (
      SELECT SUM(A.D2D_BAL_AMT)  AS SUM_D2D_BAL_AMT,
                  A.BASEL_CUST_ID AS BASEL_CUST_ID
      FROM &net_db..BASEL_CUST_MTH_POSTN_SUM_FACT A
      JOIN 
      (SELECT DISTINCT A.BASEL_CUST_ID, A.MTH_TM_ID 
	FROM	&net_db..BASEL_CUST_ACCT_RLTNP_SNAPSHOT A
      WHERE (A.MTH_TM_ID >= &MTH_TM_ID.-440 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      )B
      ON A.BASEL_CUST_ID=B.BASEL_CUST_ID
      AND (A.MTH_TM_ID >= &MTH_TM_ID.-440 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      AND A.MTH_TM_ID=B.MTH_TM_ID
      GROUP BY A.BASEL_CUST_ID
      ORDER BY A.BASEL_CUST_ID
      )WSS
      RIGHT OUTER JOIN 
      (
      SELECT MIN(A.D2D_BAL_AMT)  AS MIN_D2D_BAL_AMT,
                  A.BASEL_CUST_ID AS BASEL_CUST_ID
      FROM &net_db..BASEL_CUST_MTH_POSTN_SUM_FACT A
      WHERE (A.MTH_TM_ID >= &MTH_TM_ID.-440 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      GROUP BY A.BASEL_CUST_ID
      ORDER BY A.BASEL_CUST_ID
      ) NSS
      ON WSS.BASEL_CUST_ID=NSS.BASEL_CUST_ID
      ORDER BY NSS.BASEL_CUST_ID 
      )L20
      on SQ1.PRIM_BASEL_CUST_ID = L20.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  WSS.SUM_MTH_SINCE_LAST_60_DAY_DLQNT_CNT AS SUM_MTH_SINCE_LAST_60_DAY_DLQNT_CNT,  
                  NSS.MAX_TOT_BAL_TP_BANKCARD_AMT AS MAX_TOT_BAL_TP_BANKCARD_AMT,  
                  NSS.MAX_TRADE_NEVER_DLQNT_PC AS MAX_TRADE_NEVER_DLQNT_PC,  
                  WSS.SUM_TOT_UTLTN_AMT AS SUM_TOT_UTLTN_AMT_15,  
                  WSS.SUM_HIGHST_ACTV_UTLTN AS SUM_HIGHST_ACTV_UTLTN,  
                  WSS.SUM_TOT_AVL_CR_NOT_UTILIZED_AMT AS SUM_TOT_AVL_CR_NOT_UTILIZED_AMT,  
                  WSS.SUM_INQRY_CNT AS SUM_INQRY_CNT,  
                  NSS.MAX_INQRY_PAST_6_MTH_CNT AS MAX_INQRY_PAST_6_MTH_CNT,  
                  WSS.SUM_ACTV_INSTLMNT_TRDS_BAL_GT_ZERO_CNT AS SUM_ACTV_INSTLMNT_TRDS_BAL_GT_ZERO_CNT,  
                  NSS.BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM (
      SELECT 
          SUM(A.MTH_SINCE_LAST_60_DAY_DLQNT_CNT)  AS SUM_MTH_SINCE_LAST_60_DAY_DLQNT_CNT,
          SUM(A.TOT_UTLTN_AMT) AS SUM_TOT_UTLTN_AMT,
            SUM(A.HIGHST_ACTV_UTLTN) AS SUM_HIGHST_ACTV_UTLTN,
            SUM(A.TOT_AVL_CR_NOT_UTILIZED_AMT)  AS SUM_TOT_AVL_CR_NOT_UTILIZED_AMT,
            SUM(A.INQRY_CNT) AS SUM_INQRY_CNT,
            SUM(A.ACTV_INSTLMNT_TRDS_BAL_GT_ZERO_CNT) AS SUM_ACTV_INSTLMNT_TRDS_BAL_GT_ZERO_CNT,
            A.BASEL_CUST_ID AS BASEL_CUST_ID
      FROM &net_db..CR_BUREAU_DELI_MTH_SNAPSHOT A
      JOIN 
      (
      SELECT DISTINCT A.BASEL_CUST_ID, A.MTH_TM_ID 
	FROM	&net_db..BASEL_CUST_ACCT_RLTNP_SNAPSHOT A
      WHERE (A.MTH_TM_ID >= &MTH_TM_ID.-200 AND A.MTH_TM_ID <= &MTH_TM_ID.))B
      ON A.BASEL_CUST_ID=B.BASEL_CUST_ID
      AND (A.MTH_TM_ID >= &MTH_TM_ID.-200 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      AND A.MTH_TM_ID=B.MTH_TM_ID
      GROUP BY A.BASEL_CUST_ID
      ORDER BY A.BASEL_CUST_ID
      ) WSS
      RIGHT OUTER JOIN 
      (
      SELECT 
      MAX(A.TOT_BAL_TP_BANKCARD_AMT) AS MAX_TOT_BAL_TP_BANKCARD_AMT,
      MAX(A.TRADE_NEVER_DLQNT_PC) AS MAX_TRADE_NEVER_DLQNT_PC,
      MAX(A.INQRY_PAST_6_MTH_CNT) AS MAX_INQRY_PAST_6_MTH_CNT,
      A.BASEL_CUST_ID AS BASEL_CUST_ID
      FROM &net_db..CR_BUREAU_DELI_MTH_SNAPSHOT A
	WHERE 	(A.MTH_TM_ID >= &MTH_TM_ID.-200 AND A.MTH_TM_ID <= &MTH_TM_ID.)
      GROUP BY A.BASEL_CUST_ID
      ORDER BY A.BASEL_CUST_ID
      ) NSS
      ON WSS.BASEL_CUST_ID=NSS.BASEL_CUST_ID
      ORDER BY NSS.BASEL_CUST_ID 
      )L15
      on SQ1.PRIM_BASEL_CUST_ID = L15.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  SS.TOT_UTLTN_AMT AS CURR_TOT_UTLTN_AMT,  
                  S1.TOT_UTLTN_AMT  AS PREV_TOT_UTLTN_AMT,  
                  SM.SUM_TOT_UTLTN_AMT AS SUM_TOT_UTLTN_AMT_16,  
                  SS.TRADE_NEVER_DLQNT_PC AS CURR_TRADE_NEVER_DLQNT_PC,  
                  S1.TRADE_NEVER_DLQNT_PC AS PREV_TRADE_NEVER_DLQNT_PC,  
                  SM.SUM_TRADE_NEVER_DLQNT_PC AS SUM_TRADE_NEVER_DLQNT_PC_16,  
                  SS.BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM (SELECT S.TOT_UTLTN_AMT, S.TRADE_NEVER_DLQNT_PC, S.BASEL_CUST_ID 
      FROM &net_db..CR_BUREAU_DELI_MTH_SNAPSHOT S 
      WHERE S.MTH_TM_ID=&MTH_TM_ID.) SS
      JOIN 
      (
      SELECT TOT_UTLTN_AMT, TRADE_NEVER_DLQNT_PC, BASEL_CUST_ID 
      FROM &net_db..CR_BUREAU_DELI_MTH_SNAPSHOT
	WHERE MTH_TM_ID=&MTH_TM_ID.-200	)S1
      ON SS.BASEL_CUST_ID=S1.BASEL_CUST_ID
      LEFT OUTER JOIN 
      (
      SELECT SUM(NVL(TOT_UTLTN_AMT,0)) AS SUM_TOT_UTLTN_AMT, 
                  SUM(NVL(TRADE_NEVER_DLQNT_PC,0)) AS SUM_TRADE_NEVER_DLQNT_PC, 
                  BASEL_CUST_ID 
      FROM &net_db..CR_BUREAU_DELI_MTH_SNAPSHOT
      WHERE (MTH_TM_ID>&MTH_TM_ID.-200 AND MTH_TM_ID<&MTH_TM_ID.)
      GROUP BY BASEL_CUST_ID 
      )SM
      ON S1.BASEL_CUST_ID=SM.BASEL_CUST_ID
      ORDER BY SS.BASEL_CUST_ID 
      )L16
      on SQ1.PRIM_BASEL_CUST_ID = L16.BASEL_CUST_ID

      LEFT JOIN 
      (
      SELECT  SUM(TOT_NSF_AMT) AS SUM_TOT_NSF_AMT_21,  
                  BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM &net_db..BASEL_CUST_MTH_DEP_TXN_SUM
      WHERE MTH_TM_ID>=&MTH_TM_ID.-200  AND MTH_TM_ID<=&MTH_TM_ID.
      GROUP BY BASEL_CUST_ID
      ORDER BY  BASEL_CUST_ID 
      )L21
      on SQ1.PRIM_BASEL_CUST_ID = L21.BASEL_CUST_ID

      LEFT JOIN 
      (
      
      SELECT 
       NSS.MAX_D2D_BAL_AMT AS MAX_D2D_BAL_AMT,
       WSS.SUM_RGSTRD_INVSTMNT_BAL_ACCT_CNT AS SUM_RGSTRD_INVSTMNT_BAL_ACCT_CNT,
       NSS.MIN_NON_REGISTERED_INVSTMNT_ACCT_CNT AS MIN_NON_REGISTERED_INVSTMNT_ACCT_CNT,
       NSS.BASEL_CUST_ID AS BASEL_CUST_ID
      FROM 
      (
      SELECT 
               SUM(A.RGSTRD_INVSTMNT_BAL_ACCT_CNT) AS SUM_RGSTRD_INVSTMNT_BAL_ACCT_CNT,
               A.BASEL_CUST_ID AS BASEL_CUST_ID
        FROM 
               &net_db..BASEL_CUST_MTH_POSTN_SUM_FACT A

            JOIN 
            (
            SELECT DISTINCT A.BASEL_CUST_ID, A.MTH_TM_ID FROM
            &net_db..BASEL_CUST_ACCT_RLTNP_SNAPSHOT A
            WHERE 
            (A.MTH_TM_ID >= &MTH_TM_ID.-80 AND A.MTH_TM_ID <= &MTH_TM_ID.)
            )B
            ON A.BASEL_CUST_ID=B.BASEL_CUST_ID
            AND (A.MTH_TM_ID >= &MTH_TM_ID.-80 AND A.MTH_TM_ID <= &MTH_TM_ID.)
            AND A.MTH_TM_ID=B.MTH_TM_ID

      GROUP BY A.BASEL_CUST_ID
      ORDER BY A.BASEL_CUST_ID
      ) WSS

      RIGHT OUTER JOIN 
      (
      SELECT 
               MAX(A.D2D_BAL_AMT) AS MAX_D2D_BAL_AMT,
               MIN(A.NON_REGISTERED_INVSTMNT_ACCT_CNT) AS MIN_NON_REGISTERED_INVSTMNT_ACCT_CNT,
               A.BASEL_CUST_ID AS BASEL_CUST_ID
        FROM 
              &net_db..BASEL_CUST_MTH_POSTN_SUM_FACT A
      WHERE
            (A.MTH_TM_ID >= &MTH_TM_ID.-80 AND A.MTH_TM_ID <= &MTH_TM_ID.)
            
      GROUP BY A.BASEL_CUST_ID
      ORDER BY A.BASEL_CUST_ID
      ) NSS
      ON WSS.BASEL_CUST_ID=NSS.BASEL_CUST_ID
      ORDER BY NSS.BASEL_CUST_ID )L14
      
      on SQ1.PRIM_BASEL_CUST_ID = L14.BASEL_CUST_ID
      
      );
      disconnect from NZCON; 
QUIT;



/* INFA Workflow sql - lkp_CUST_XREF from OWSTAR Database */
proc sql;
      CONNECT USING NZRRAP  AS NZCON;
      create table LOOKUPXRF_0 as
    select *              
    from connection to NZCON
      (
      SELECT 
      A.CUST_ACCT_CNT AS CUST_BASE_KEY, A.POPN_DT,
      LTRIM(RTRIM(C.CUST_ID)) AS CUST_ID
      FROM 
      &net_db..IWD_CUST AS A, 
      &net_db..CUST_MODEL AS B, 
      &net_db..CUST_XREF C
      WHERE
      B.TIME_KEY = &MTH_TM_ID.
      AND A.CUST_KEY = B.CUST_KEY 
      AND C.CUST_BASE_KEY=A.CUST_BASE_KEY
      ORDER BY CUST_ID,CUST_BASE_KEY,POPN_DT desc

      );
      disconnect from NZCON; 
Quit;

/* Delete if any duplicate records  */
data LOOKUPXRF;
      set LOOKUPXRF_0;
      by CUST_ID CUST_BASE_KEY descending POPN_DT;
      if first.CUST_ID then output;
run;


/* Extract data from BASEL_CUST_DIM table to join with OWSTAR data */
proc sql;
      CONNECT USING NZRRAP AS NZCON;
      create table LOOKUP10 as
    select *              
    from connection to NZCON
      (
      SELECT  TRIM(CUST_CID) AS CUST_CID,  
                  BASEL_CUST_ID AS BASEL_CUST_ID 
      FROM &net_db..BASEL_CUST_DIM 
      ORDER BY BASEL_CUST_ID,CUST_CID
      );
      disconnect from NZCON; 
Quit;

/* Merging OWSTAR data with IIAS SOURCE Data table */
proc sql;
      create table SOURCE_Q2 as
      select a.*, coalesce(L10XRF.CUST_BASE_KEY,0) as CUST_BASE_KEY,L10XRF.POPN_DT
      FROM SOURCE_INPUT A

      LEFT JOIN (select LP10.CUST_CID, 
                                          LP10.BASEL_CUST_ID, 
                                          RR.POPN_DT,
                                          RR.CUST_BASE_KEY
                        from LOOKUP10 LP10  left join LOOKUPXRF RR
                        on STRIP(LP10.CUST_CID) = STRIP(RR.CUST_ID)) L10XRF
	on A.PRIM_BASEL_CUST_ID=	L10XRF.BASEL_CUST_ID;

QUIT;


/********* Create Final output table to load into DB table **********/
data LOAD (keep= MTH_TM_ID BASEL_CUST_ID CUST_ACCT_CNT DFT_TOT_BAL_AMT
                        LAST_12_MTH_AVG_CR_INQ_6_MTH_CNT LAST_12_MTH_AVG_DY_TO_DY_BAL_AMT
                        LAST_12_MTH_AVG_NOT_SUFF_FND_AMT LAST_12_MTH_MAX_CR_INQ_6_MTH_CNT
                        LAST_12_MTH_MAX_TRD_NVR_DLQNT_PC LAST_12_MTH_MIN_DY_TO_DY_BAL_AMT
                        LST12MTHRL_ACT_WST_DLQNT_DAY_CNT LAST24MTH_AVG_TR_NVR_DLQNT_AVGPC
                        LAST_24_MTH_MAX_BNKRPY_CNT LAST_24_MTH_MAX_CR_INQRY_CNT
                        LAST3MTH_AVG_RG_INV_BAL_ACCT_CNT LAST_3_MTH_AVG_TRAD_NVR_DLQNT_PC
                        LAST_3_MTH_MAX_BNK_REV_UTLTN_AMT LAST_3_MTH_MAX_DY_TO_DAY_BAL_AMT
                        LAST_3_MTH_MAX_OVDR_TXN_CNT LAST3MTH_MIN_NON_RG_INVT_ACT_CNT
                        LAST6MTH_AL_CR_CRD_MX_TOT_BALAMT LAST6MTH_AVG_AVL_CR_NOT_UTLZ_AMT
                        LAST_6_MTH_AVG_CR_INQRY_CNT LAST_6_MTH_AVG_HIGHST_UTLTN_AMT
                        LAST6MTH_AVG_SNE_60_DY_DLQNT_CNT LAST6MTH_AVG_NON_RGT_INV_BAL_AMT
                        LAST_6_MTH_AVG_TOT_UTLTN_AMT LAST6MTH_AVG_TRD_BAL_GT_T_ZRO_CN
                        LAST_6_MTH_MAX_CR_INQR_6_MTH_CNT LAST_6_MTH_MAX_NVR_DLQNT_TRAD_PC
                        LST6MTH_MORT_SLP_WRT_DY_DLQNT_CN LST6_MTH_SLP_TOT_BAL_TO_HCCL_RTO  
                        LST6_MTH_SLP_TRADE_NEVR_DLQNT_PC LAST_6_MTH_TOT_NOT_SUFF_FUND_AMT
                        INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP
                        /* ADDED 3 new variables to keep list */
                        CSH_AD_CRNT_C_BAL_KSCSUM3M BY34 AT147);

      set SOURCE_Q2;


	rename PRIM_BASEL_CUST_ID	=	BASEL_CUST_ID
/*			o_CUST_ACCT_CNT	=	CUST_ACCT_CNT*/
			MIN_D2D_BAL_AMT	=	LAST_12_MTH_MIN_DY_TO_DY_BAL_AMT
			MAX_BNKRPY_CNT	=	LAST_24_MTH_MAX_BNKRPY_CNT
			MAX_INQRY_CNT	=	LAST_24_MTH_MAX_CR_INQRY_CNT
			MAX_MAX_REVLVNG_CR_CRNT_UTLTN_AM	=	LAST_3_MTH_MAX_BNK_REV_UTLTN_AMT
			MAX_D2D_BAL_AMT	=	LAST_3_MTH_MAX_DY_TO_DAY_BAL_AMT
			MAX_OVDR_CNT	=	LAST_3_MTH_MAX_OVDR_TXN_CNT
			MIN_NON_REGISTERED_INVSTMNT_ACCT	=	LAST3MTH_MIN_NON_RG_INVT_ACT_CNT
			MAX_TOT_BAL_TP_BANKCARD_AMT	=	LAST6MTH_AL_CR_CRD_MX_TOT_BALAMT
			MAX_INQRY_PAST_6_MTH_CNT	=	LAST_6_MTH_MAX_CR_INQR_6_MTH_CNT
			MAX_TRADE_NEVER_DLQNT_PC	=	LAST_6_MTH_MAX_NVR_DLQNT_TRAD_PC
			SUM_TOT_NSF_AMT_21	=	LAST_6_MTH_TOT_NOT_SUFF_FUND_AMT; 

      format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP DATETIME25.6
                  BASEL_CUST_ID     20.  
                  CUST_ACCT_CNT           11.
                  DFT_TOT_BAL_AMT         19.3  
                  LAST_12_MTH_AVG_CR_INQ_6_MTH_CNT    13.4
                  LAST_12_MTH_AVG_DY_TO_DY_BAL_AMT    19.3
                  LAST_12_MTH_AVG_NOT_SUFF_FND_AMT    19.3
                  LAST_12_MTH_MAX_CR_INQ_6_MTH_CNT    13.4
                  LAST_12_MTH_MAX_TRD_NVR_DLQNT_PC    13.4
                  LAST_12_MTH_MIN_DY_TO_DY_BAL_AMT    19.3
                  LST12MTHRL_ACT_WST_DLQNT_DAY_CNT    11.
                  LAST24MTH_AVG_TR_NVR_DLQNT_AVGPC    13.4
                  LAST_24_MTH_MAX_BNKRPY_CNT                13.4
                  LAST_24_MTH_MAX_CR_INQRY_CNT        13.4
                  LAST3MTH_AVG_RG_INV_BAL_ACCT_CNT    13.4
                  LAST_3_MTH_AVG_TRAD_NVR_DLQNT_PC    13.4
                  LAST_3_MTH_MAX_BNK_REV_UTLTN_AMT    19.3
                  LAST_3_MTH_MAX_DY_TO_DAY_BAL_AMT    19.3
                  LAST_3_MTH_MAX_OVDR_TXN_CNT               11.
                  LAST3MTH_MIN_NON_RG_INVT_ACT_CNT    11.
                  LAST6MTH_AL_CR_CRD_MX_TOT_BALAMT    19.3
                  LAST6MTH_AVG_AVL_CR_NOT_UTLZ_AMT    19.3
                  LAST_6_MTH_AVG_CR_INQRY_CNT               13.4
                  LAST_6_MTH_AVG_HIGHST_UTLTN_AMT           19.3
                  LAST6MTH_AVG_SNE_60_DY_DLQNT_CNT    13.4
                  LAST6MTH_AVG_NON_RGT_INV_BAL_AMT    19.3
                  LAST_6_MTH_AVG_TOT_UTLTN_AMT        19.3
                  LAST6MTH_AVG_TRD_BAL_GT_T_ZRO_CN    13.4
                  LAST_6_MTH_MAX_CR_INQR_6_MTH_CNT    13.4
                  LAST_6_MTH_MAX_NVR_DLQNT_TRAD_PC    13.4
                  LST6MTH_MORT_SLP_WRT_DY_DLQNT_CN    13.4
                  LST6_MTH_SLP_TOT_BAL_TO_HCCL_RTO    13.4
                  LST6_MTH_SLP_TRADE_NEVR_DLQNT_PC    13.4
                  LAST_6_MTH_TOT_NOT_SUFF_FUND_AMT    19.3
                  /* ADDED formats for new variables */
                  CSH_AD_CRNT_C_BAL_KSCSUM3M          19.3
                  BY34                         19.3
                  AT147                         19.3;

/* Metrics calculation */ 

      if (SUM_RGSTRD_INVSTMNT_BAL_ACCT_CNT = . or CNT_BASEL_CUST_ID_25 = . or 
      CNT_BASEL_CUST_ID_25 = 0) then LAST3MTH_AVG_RG_INV_BAL_ACCT_CNT = . ;
      else  do ;
      LAST3MTH_AVG_RG_INV_BAL_ACCT_CNT = round(SUM_RGSTRD_INVSTMNT_BAL_ACCT_CNT/CNT_BASEL_CUST_ID_25,0.0001);
      end;
      
      if (SUM_INQRY_PAST_6_MTH_CNT = . or CNT_BASEL_CUST_ID_23 = . or 
      CNT_BASEL_CUST_ID_23 = 0) then LAST_12_MTH_AVG_CR_INQ_6_MTH_CNT = . ;
      else  do ;
      LAST_12_MTH_AVG_CR_INQ_6_MTH_CNT = round(SUM_INQRY_PAST_6_MTH_CNT/CNT_BASEL_CUST_ID_23,0.0001);
      end;
      
      if (SUM_TOT_AVL_CR_NOT_UTILIZED_AMT = . or CNT_BASEL_CUST_ID_26 = . or 
      CNT_BASEL_CUST_ID_26 = 0) then LAST6MTH_AVG_AVL_CR_NOT_UTLZ_AMT = . ;
      else  do ;
      LAST6MTH_AVG_AVL_CR_NOT_UTLZ_AMT = round(SUM_TOT_AVL_CR_NOT_UTILIZED_AMT/CNT_BASEL_CUST_ID_26,0.001);
      end;

      
      if (SUM_D2D_BAL_AMT = . or CNT_BASEL_CUST_ID_23 = . or 
      CNT_BASEL_CUST_ID_23 = 0) then LAST_12_MTH_AVG_DY_TO_DY_BAL_AMT = . ;
      else  do ;
      LAST_12_MTH_AVG_DY_TO_DY_BAL_AMT = round(SUM_D2D_BAL_AMT/CNT_BASEL_CUST_ID_23,0.001);
      end;


      
      if (SUM_TOT_NSF_AMT = . or CNT_BASEL_CUST_ID_23 = . or 
      CNT_BASEL_CUST_ID_23 = 0) then LAST_12_MTH_AVG_NOT_SUFF_FND_AMT = . ;
      else  do ;
      LAST_12_MTH_AVG_NOT_SUFF_FND_AMT = round(SUM_TOT_NSF_AMT/CNT_BASEL_CUST_ID_23,0.001);
      end;
      
      
      if (SUM_TRADE_NEVER_DLQNT_PC_12 = . or CNT_BASEL_CUST_ID_24 = . or 
      CNT_BASEL_CUST_ID_24 = 0) then LAST24MTH_AVG_TR_NVR_DLQNT_AVGPC = . ;
      else  do ;
      LAST24MTH_AVG_TR_NVR_DLQNT_AVGPC = round(SUM_TRADE_NEVER_DLQNT_PC_12/CNT_BASEL_CUST_ID_24,0.0001);
      end;
      
      
      if (SUM_TRADE_NEVER_DLQNT_PC_19 = . or CNT_BASEL_CUST_ID_25 = . or 
      CNT_BASEL_CUST_ID_25 = 0) then LAST_3_MTH_AVG_TRAD_NVR_DLQNT_PC = . ;
      else  do ;
      LAST_3_MTH_AVG_TRAD_NVR_DLQNT_PC = round(SUM_TRADE_NEVER_DLQNT_PC_19/CNT_BASEL_CUST_ID_25,0.0001);
      end;
      
      
      if (SUM_INQRY_CNT = . or CNT_BASEL_CUST_ID_26 = . or 
      CNT_BASEL_CUST_ID_26 = 0) then LAST_6_MTH_AVG_CR_INQRY_CNT = . ;
      else  do ;
      LAST_6_MTH_AVG_CR_INQRY_CNT = round(SUM_INQRY_CNT/CNT_BASEL_CUST_ID_26,0.0001);
      end;
      

      if (SUM_HIGHST_ACTV_UTLTN = . or CNT_BASEL_CUST_ID_26 = . or 
      CNT_BASEL_CUST_ID_26 = 0) then LAST_6_MTH_AVG_HIGHST_UTLTN_AMT = . ;
      else  do ;
      LAST_6_MTH_AVG_HIGHST_UTLTN_AMT = round(SUM_HIGHST_ACTV_UTLTN/CNT_BASEL_CUST_ID_26,0.001);
      end;  
      
      
      if (SUM_MTH_SINCE_LAST_60_DAY_DLQNT_ = . or CNT_BASEL_CUST_ID_26 = . or 
      CNT_BASEL_CUST_ID_26 = 0) then LAST6MTH_AVG_SNE_60_DY_DLQNT_CNT = . ;
      else  do ;
      LAST6MTH_AVG_SNE_60_DY_DLQNT_CNT = round(SUM_MTH_SINCE_LAST_60_DAY_DLQNT_/CNT_BASEL_CUST_ID_26,0.0001);
      end;        
      

      if (SUM_NON_REGISTERED_INVSTMNT_BAL_ = . or CNT_BASEL_CUST_ID_26 = . or 
      CNT_BASEL_CUST_ID_26 = 0) then LAST6MTH_AVG_NON_RGT_INV_BAL_AMT = . ;
      else  do ;
      LAST6MTH_AVG_NON_RGT_INV_BAL_AMT = round(SUM_NON_REGISTERED_INVSTMNT_BAL_/CNT_BASEL_CUST_ID_26,0.001);
      end;              
      
      
      if (SUM_TOT_UTLTN_AMT_15 = . or CNT_BASEL_CUST_ID_26 = . or 
      CNT_BASEL_CUST_ID_26 = 0) then LAST_6_MTH_AVG_TOT_UTLTN_AMT = . ;
      else  do ;
      LAST_6_MTH_AVG_TOT_UTLTN_AMT = round(SUM_TOT_UTLTN_AMT_15/CNT_BASEL_CUST_ID_26,0.001);
      end;  
      

      if (SUM_ACTV_INSTLMNT_TRDS_BAL_GT_ZE = . or CNT_BASEL_CUST_ID_26 = . or 
      CNT_BASEL_CUST_ID_26 = 0) then LAST6MTH_AVG_TRD_BAL_GT_T_ZRO_CN = . ;
      else  do ;
      LAST6MTH_AVG_TRD_BAL_GT_T_ZRO_CN = round(SUM_ACTV_INSTLMNT_TRDS_BAL_GT_ZE/CNT_BASEL_CUST_ID_26,0.0001);
      end;
/*	IF(ISNULL(i_OWSTAR_CUST_ID), 0, i_OWSTAR_CUST_ACCT_CNT)*/
      IF CUST_CID  = "" THEN DO ; CUST_ACCT_CNT = 0; END;
	ELSE IF CUST_BASE_KEY = . THEN DO ; CUST_ACCT_CNT = 0; END; 	/* formaulla added by VK on 11APR2022 to adress null value */
      ELSE DO CUST_ACCT_CNT = CUST_BASE_KEY; END; 

      if v_MAX_DLQNT_DAY_CNT_CurrMth=0 AND v_MAX_DLQNT_DAY_CNT_5Mth=0 THEN DO ;  LST6MTH_MORT_SLP_WRT_DY_DLQNT_CN = 0; END;
      ELSE IF MISSING(v_MAX_DLQNT_DAY_CNT_5Mth) AND v_MAX_DLQNT_DAY_CNT_CurrMth=0 THEN DO; LST6MTH_MORT_SLP_WRT_DY_DLQNT_CN = NULL; END;
      ELSE IF MISSING(v_MAX_DLQNT_DAY_CNT_5Mth) AND (( NOT MISSING(v_MAX_DLQNT_DAY_CNT_CurrMth)) AND v_MAX_DLQNT_DAY_CNT_CurrMth ne 0) THEN DO; LST6MTH_MORT_SLP_WRT_DY_DLQNT_CN = NULL; END;
      ELSE IF v_MAX_DLQNT_DAY_CNT_5Mth=0 AND  v_MAX_DLQNT_DAY_CNT_between_mont=0 AND (( NOT MISSING(v_MAX_DLQNT_DAY_CNT_CurrMth)) AND v_MAX_DLQNT_DAY_CNT_CurrMth ne 0) THEN DO; LST6MTH_MORT_SLP_WRT_DY_DLQNT_CN = v_MAX_DLQNT_DAY_CNT_CurrMth; END;
      ELSE IF MISSING(v_MAX_DLQNT_DAY_CNT_5Mth) OR MISSING(v_MAX_DLQNT_DAY_CNT_CurrMth) THEN DO; LST6MTH_MORT_SLP_WRT_DY_DLQNT_CN = NULL; END;
      ELSE DO; LST6MTH_MORT_SLP_WRT_DY_DLQNT_CN = (v_MAX_DLQNT_DAY_CNT_CurrMth-v_MAX_DLQNT_DAY_CNT_5Mth)/5;
      END ; 


      if PREV_TOT_UTLTN_AMT=0 AND CURR_TOT_UTLTN_AMT=0 THEN DO ; LST6_MTH_SLP_TOT_BAL_TO_HCCL_RTO= 0; END;
      ELSE IF MISSING((PREV_TOT_UTLTN_AMT) ) AND CURR_TOT_UTLTN_AMT=0 THEN DO; LST6_MTH_SLP_TOT_BAL_TO_HCCL_RTO= NULL; END;
      ELSE IF MISSING(PREV_TOT_UTLTN_AMT) AND CURR_TOT_UTLTN_AMT ne 0 THEN DO; LST6_MTH_SLP_TOT_BAL_TO_HCCL_RTO = NULL; END;
      ELSE IF PREV_TOT_UTLTN_AMT=0 AND SUM_TOT_UTLTN_AMT_16=0 AND CURR_TOT_UTLTN_AMT ne 0 THEN DO; LST6_MTH_SLP_TOT_BAL_TO_HCCL_RTO = CURR_TOT_UTLTN_AMT; END;
      ELSE IF MISSING(PREV_TOT_UTLTN_AMT) OR MISSING(CURR_TOT_UTLTN_AMT) THEN DO; LST6_MTH_SLP_TOT_BAL_TO_HCCL_RTO = NULL; END;
      ELSE DO; LST6_MTH_SLP_TOT_BAL_TO_HCCL_RTO = (CURR_TOT_UTLTN_AMT-PREV_TOT_UTLTN_AMT)/5;
      END ; 


      if PREV_TRADE_NEVER_DLQNT_PC=0 AND CURR_TRADE_NEVER_DLQNT_PC=0 THEN DO ; LST6_MTH_SLP_TRADE_NEVR_DLQNT_PC = 0; END;
      ELSE IF MISSING(PREV_TRADE_NEVER_DLQNT_PC) AND CURR_TRADE_NEVER_DLQNT_PC=0 THEN DO; LST6_MTH_SLP_TRADE_NEVR_DLQNT_PC = NULL; END;
      ELSE IF MISSING(PREV_TRADE_NEVER_DLQNT_PC) AND CURR_TRADE_NEVER_DLQNT_PC ne 0 THEN DO; LST6_MTH_SLP_TRADE_NEVR_DLQNT_PC = NULL; END;
      ELSE IF PREV_TRADE_NEVER_DLQNT_PC=0 AND SUM_TRADE_NEVER_DLQNT_PC_16=0 AND CURR_TRADE_NEVER_DLQNT_PC ne 0 THEN DO;LST6_MTH_SLP_TRADE_NEVR_DLQNT_PC =CURR_TRADE_NEVER_DLQNT_PC; END;
      ELSE IF MISSING(PREV_TRADE_NEVER_DLQNT_PC) OR MISSING(CURR_TRADE_NEVER_DLQNT_PC) THEN DO; LST6_MTH_SLP_TRADE_NEVR_DLQNT_PC= NULL; END;
      ELSE DO; LST6_MTH_SLP_TRADE_NEVR_DLQNT_PC = (CURR_TRADE_NEVER_DLQNT_PC-PREV_TRADE_NEVER_DLQNT_PC)/5;
      END ; 

      INSRT_PROCESS_TMSTMP="&SYSDATE9.:&SYSTIME."dt   ;
      UPDT_PROCESS_TMSTMP ="&SYSDATE9.:&SYSTIME."dt ;

run;

/* Delete temp table */ 
PROC SQL NOPRINT;
       CONNECT USING NZRRAP AS NZCON;
        EXECUTE (drop table &net_db..TEMP_BSL_CC_LOC_CUS_SCRCRD_DRVD if exists) BY NZCON;
QUIT;

/* Create temp db table to read columns having more than 32 characters */
/* data NZRRAP.TEMP_BSL_CC_LOC_CUS_SCRCRD_DRVD; */
/*       set LOAD; */
/* run; */

PROC APPEND BASE=NZRRAP.TEMP_BSL_CC_LOC_CUS_SCRCRD_DRVD (BULKLOAD=YES BL_METHOD=CLILOAD)
            DATA=LOAD;
RUN;

/* Delete records if already exisits for the run period */
PROC SQL NOPRINT;
             CONNECT USING NZRRAP AS NZCON;
             EXECUTE(DELETE FROM &net_db..BASEL_CC_LOC_CUST_SCORECRD_DRVD_VARS WHERE MTH_TM_ID=&MTH_TM_ID) BY NZCON;
 QUIT;

/** Insert data to DB2 final target table **/
 
PROC SQL ;
connect using NZRRAP as nzcon;      
          execute(
       INSERT INTO &net_db..BASEL_CC_LOC_CUST_SCORECRD_DRVD_VARS(
             MTH_TM_ID,
             BASEL_CUST_ID,
             CUST_ACCT_CNT,
             DFT_TOT_BAL_AMT,
             LAST_3_MTH_AVG_RGSTRD_INVSTMNT_BAL_ACCT_CNT,
             LAST_3_MTH_AVG_TRADE_NEVER_DLQNT_PC,
             LAST_3_MTH_MAX_BNK_REVLVNG_UTLTN_AMT,
             LAST_3_MTH_MAX_DAY_TO_DAY_BAL_AMT,
             LAST_3_MTH_MIN_NON_RGSTRD_INVSTMNT_ACCT_CNT,
             LAST_3_MTH_MAX_OVDR_TXN_CNT,
             LAST_6_MTH_ALL_CR_CRD_MAX_TOT_BAL_AMT,
             LAST_6_MTH_AVG_AVL_CR_NOT_UTILIZED_AMT,
             LAST_6_MTH_AVG_CR_INQRY_CNT,
             LAST_6_MTH_AVG_HIGHST_UTLTN_AMT,
             LAST_6_MTH_AVG_MTH_SINCE_LAST_60_DAY_DLQNT_CNT,
             LAST_6_MTH_AVG_NON_RGSTRD_INVSTMNT_BAL_AMT,
             LAST_6_MTH_AVG_TOT_UTLTN_AMT,
             LAST_6_MTH_AVG_TRADE_WITH_BAL_GREATER_THAN_ZERO_CN,
             LAST_6_MTH_MAX_CR_INQRY_6_MTH_CNT,
             LAST_6_MTH_MAX_NEVER_DLQNT_TRADE_PC,
             LAST_6_MTH_MORT_SLOPE_WORST_DAY_DLQNT_CNT,
             LAST_6_MTH_SLOPE_TOT_BAL_TO_HCCL_RTO,
             LAST_6_MTH_SLOPE_TRADE_NEVER_DLQNT_PC,
             LAST_6_MTH_TOT_NOT_SUFFCNT_FUNDS_AMT,
             LAST_12_MTH_AVG_CR_INQRY_6_MTH_CNT,
             LAST_12_MTH_AVG_DAY_TO_DAY_BAL_AMT,
             LAST_12_MTH_AVG_NOT_SUFFCNT_FUNDS_AMT,
             LAST_12_MTH_MAX_CR_INQRY_6_MTH_CNT,
             LAST_12_MTH_MAX_TRADE_NEVER_DLQNT_PC,
             LAST_12_MTH_MIN_DAY_TO_DAY_BAL_AMT,
             LAST_12_MTH_REVLVNG_ACCT_WORST_DLQNT_DAY_CNT,
             LAST_24_MTH_AVG_TRADE_NEVER_DLQNT_AVG_PC,
             LAST_24_MTH_MAX_BNKRPY_CNT,
             LAST_24_MTH_MAX_CR_INQRY_CNT,
             INSRT_PROCESS_TMSTMP,
             UPDT_PROCESS_TMSTMP,
             /* ADDED new columns to Insert list */
             CSH_AD_CRNT_C_BAL_KSCSUM3M,
             BY34,
             AT147)
       SELECT 
             MTH_TM_ID,        
             BASEL_CUST_ID,
             CUST_ACCT_CNT,
             DFT_TOT_BAL_AMT,
             LAST3MTH_AVG_RG_INV_BAL_ACCT_CNT    AS    LAST_3_MTH_AVG_RGSTRD_INVSTMNT_BAL_ACCT_CNT,
             LAST_3_MTH_AVG_TRAD_NVR_DLQNT_PC    AS    LAST_3_MTH_AVG_TRADE_NEVER_DLQNT_PC,
             LAST_3_MTH_MAX_BNK_REV_UTLTN_AMT    AS    LAST_3_MTH_MAX_BNK_REVLVNG_UTLTN_AMT,
             LAST_3_MTH_MAX_DY_TO_DAY_BAL_AMT    AS    LAST_3_MTH_MAX_DAY_TO_DAY_BAL_AMT,
             LAST3MTH_MIN_NON_RG_INVT_ACT_CNT    AS    LAST_3_MTH_MIN_NON_RGSTRD_INVSTMNT_ACCT_CNT,
             LAST_3_MTH_MAX_OVDR_TXN_CNT,  
             LAST6MTH_AL_CR_CRD_MX_TOT_BALAMT    AS    LAST_6_MTH_ALL_CR_CRD_MAX_TOT_BAL_AMT,
             LAST6MTH_AVG_AVL_CR_NOT_UTLZ_AMT    AS    LAST_6_MTH_AVG_AVL_CR_NOT_UTILIZED_AMT,
             LAST_6_MTH_AVG_CR_INQRY_CNT,  
             LAST_6_MTH_AVG_HIGHST_UTLTN_AMT,    
             LAST6MTH_AVG_SNE_60_DY_DLQNT_CNT    AS    LAST_6_MTH_AVG_MTH_SINCE_LAST_60_DAY_DLQNT_CNT,
             LAST6MTH_AVG_NON_RGT_INV_BAL_AMT    AS    LAST_6_MTH_AVG_NON_RGSTRD_INVSTMNT_BAL_AMT,
             LAST_6_MTH_AVG_TOT_UTLTN_AMT,
             LAST6MTH_AVG_TRD_BAL_GT_T_ZRO_CN    AS    LAST_6_MTH_AVG_TRADE_WITH_BAL_GREATER_THAN_ZERO_CN,
             LAST_6_MTH_MAX_CR_INQR_6_MTH_CNT    AS    LAST_6_MTH_MAX_CR_INQRY_6_MTH_CNT,
             LAST_6_MTH_MAX_NVR_DLQNT_TRAD_PC    AS    LAST_6_MTH_MAX_NEVER_DLQNT_TRADE_PC,
             LST6MTH_MORT_SLP_WRT_DY_DLQNT_CN    AS    LAST_6_MTH_MORT_SLOPE_WORST_DAY_DLQNT_CNT,
             LST6_MTH_SLP_TOT_BAL_TO_HCCL_RTO    AS    LAST_6_MTH_SLOPE_TOT_BAL_TO_HCCL_RTO,
             LST6_MTH_SLP_TRADE_NEVR_DLQNT_PC    AS    LAST_6_MTH_SLOPE_TRADE_NEVER_DLQNT_PC,
             LAST_6_MTH_TOT_NOT_SUFF_FUND_AMT    AS    LAST_6_MTH_TOT_NOT_SUFFCNT_FUNDS_AMT,
             LAST_12_MTH_AVG_CR_INQ_6_MTH_CNT    AS    LAST_12_MTH_AVG_CR_INQRY_6_MTH_CNT,
             LAST_12_MTH_AVG_DY_TO_DY_BAL_AMT    AS    LAST_12_MTH_AVG_DAY_TO_DAY_BAL_AMT,
             LAST_12_MTH_AVG_NOT_SUFF_FND_AMT    AS    LAST_12_MTH_AVG_NOT_SUFFCNT_FUNDS_AMT,
             LAST_12_MTH_MAX_CR_INQ_6_MTH_CNT    AS    LAST_12_MTH_MAX_CR_INQRY_6_MTH_CNT,
             LAST_12_MTH_MAX_TRD_NVR_DLQNT_PC    AS    LAST_12_MTH_MAX_TRADE_NEVER_DLQNT_PC,
             LAST_12_MTH_MIN_DY_TO_DY_BAL_AMT    AS    LAST_12_MTH_MIN_DAY_TO_DAY_BAL_AMT,
             LST12MTHRL_ACT_WST_DLQNT_DAY_CNT    AS    LAST_12_MTH_REVLVNG_ACCT_WORST_DLQNT_DAY_CNT,
             LAST24MTH_AVG_TR_NVR_DLQNT_AVGPC    AS    LAST_24_MTH_AVG_TRADE_NEVER_DLQNT_AVG_PC,
             LAST_24_MTH_MAX_BNKRPY_CNT,
             LAST_24_MTH_MAX_CR_INQRY_CNT,
             INSRT_PROCESS_TMSTMP,
             UPDT_PROCESS_TMSTMP,
             /* ADDED new columns to select list */
             CSH_AD_CRNT_C_BAL_KSCSUM3M,
             BY34,
             AT147

       FROM &net_db..TEMP_BSL_CC_LOC_CUS_SCRCRD_DRVD
       ) BY NZCON;
QUIT;

/* Drop temp tables */
PROC SQL NOPRINT;
       CONNECT USING NZRRAP AS NZCON;
        EXECUTE (drop table &net_db..TEMP_SOURCE_Q if exists) BY NZCON;
            EXECUTE (drop table &net_db..TEMP_BSL_CC_LOC_CUS_SCRCRD_DRVD if exists) BY NZCON;
QUIT;

/************************** CODE END ***************************/