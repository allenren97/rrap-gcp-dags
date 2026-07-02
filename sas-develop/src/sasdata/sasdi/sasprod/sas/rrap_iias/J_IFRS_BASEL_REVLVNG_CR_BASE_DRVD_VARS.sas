/**************************************************************************** 
 * Job:             J_RRII_KS10_2103_BASEL_REVLVNG_CR_BASE_DRVD_VARS	    * 
 *                                                                          * 
 * Source Table:    EDRTLRP1D.BASEL_REVLVNG_CR_MTH_SNAPSHOT  				* 
 * 				    EDRTLRP1D.TM_DIM						  				* 
 * 				    EDRTLRP1D.TRNST_EXCLSN_LKP				  				* 
 * 				    EDRTLRP1D.BLOCK_RECL_LKP				  				* 
 * 				    EDRTLRP1D.BLOCK_RECL_CLS_RSN_LKP		  				* 
 * 				    EDRTLRP1D.SRC_PRD_LKP					  				* 
 * 				    EDRTLRP1D.SRC_PRD_STDNT_LOAN_LKP		  				* 
 *				   	EDRTLRP1D.CHRG_OFF_LKP	 				  				* 
 *                                                                          * 
 *                                                                          * 
 * Target Table:    EDRTLRP1D.BASEL_REVLVNG_CR_BASE_DRVD_VARS				*                                                                      * 
 * Generated on:    Thursday, March 04, 2021 2:36:46 PM EDT                 * 
  *************************************************************************/ 

/*************************************************************************************
Added by Hadi Dimashkieh - 20220830

*** Tables identified as being in the RRAP schema will have a view created 
*** in the IFRS9 schema with the SAME NAME which will point to the RRAP table.

DDL Required:
IFRS9	AUDIT_JOB_TIMER_CHECK

**** SOURCES:
SCHEMA  TABLENAME

RRAP	TM_DIM
RRAP	BASEL_REVLVNG_CR_MTH_SNAPSHOT
RRAP	TRNST_EXCLSN_LKP
RRAP	BLOCK_RECL_LKP
RRAP	CHRG_OFF_LKP
RRAP	BLOCK_RECL_CLS_RSN_LKP
RRAP	SRC_PRD_LKP
RRAP	SRC_PRD_STDNT_LOAN_LKP


TARGET: 

IFRS9	BASEL_REVLVNG_CR_BASE_DRVD_VARS

****************************************************************************************/


%put WORK LOCATION: %sysfunc(getoption(work));
%include '/sasdata/sasdi/sasprod/macro/rrap_iias/rrap_iias_ifrs_autoexec.sas';
%rrap_ifrs9_autoexec(ENV=PROD);

/*%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);*/

DATA _NULL_;
	CALL SYMPUTX('PROCESSSTARTTIME',PUT(DATETIME(),DATETIME25.0));
RUN;
	proc sql;
		select 	TM_ID, put(tm_lvl_end_dt,date9.) as tm_lvl_end_dt ,TM_LVL_END_DT format yymmn6. as yrmth
		into 	:TM_ID, :tm_lvl_end_dt, :yrmth
		from 	nzrrap.TM_DIM
		where 	tm_id = &MTH_TM_ID.
		;
		select 	TM_ID, put(tm_lvl_end_dt,date9.) as tm_lvl_end_dt ,TM_LVL_END_DT format yymmn6. as yrmth
		into 	:PRV_TM_ID, :PRV_tm_lvl_end_dt, :Prv_yrmth
		from 	nzrrap.TM_DIM
		where tm_id = &MTH_TM_ID.-40
		;
	quit;

	%put tm_lvl_end_dt= &tm_lvl_end_dt.;
	%put yrmth= &yrmth.;
	%put PRV_tm_lvl_end_dt= &PRV_tm_lvl_end_dt.;
	%put DB= &net_db.;

			 PROC SQL ;
	           CONNECT USING NZRRAP AS NZCON;
            create table S_BASEL_REVLVNG_CR_MTH_SNAPSHOT as
            select *              
            from connection to NZCON
			(	WITH 
				       Snap  as
				       (
				       SELECT  MTH_TM_ID, 
								BASEL_ACCT_ID, 
								PRIM_BASEL_CUST_ID, 
								STEP_PLN_SNAPSHOT_ID, 
								ACCT_NUM, 
								PRD_CD, 
								SUB_PRD_CD, 
								A.BLOCK_RECL_CD, 
								TOT_NEW_BAL_AMT, 
								CR_LMT_AMT, 
								ACCT_CLS_RSN_CD, 
								CHRG_OFF_CD, 
								BNS_DLQNT_DAY, 
								TOT_UNPAID_FNCL_CHRG_AMT, 
								CRNT_BILL_CD, 
								SCRD_TP_CD, 
								SWITCH_XREF, 
								SCRTY_TP_CD, 
								TRNST_NUM,	EXCLUDED_TRNST_NUM,
						        PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,				       
						     	(CASE 
							        WHEN SUB_PRD_CD='RS' OR STEP_PLN_SNAPSHOT_ID NOT IN (-1,-2) OR
							          ( PRD_CD IN ( 'SCL', 'VIC') AND STEP_PLN_SNAPSHOT_ID IN (-1,-2) AND SUBSTR(CRNT_BILL_CD,1,2) in ('11', 'SB', 'SN', 'SP', 'SR', 'ST') )
							          THEN 'Y'
							         ELSE 'N'
							       END)  AS HELOC_F,
						       (CASE 
						         WHEN  LK_RECL.BLOCK_RECL_CD IS NULL THEN '1'
						         ELSE '0'
						       END) as  v_PT_STAT_BLCK_RECL_CD_LKP_CUR,				                                                  
				    	       (CASE 
							    WHEN CR_LMT_AMT > TOT_NEW_BAL_AMT then CR_LMT_AMT
							     ELSE TOT_NEW_BAL_AMT
							     END )  AS REVISED_EXPSR_AMT				     
							    FROM &net_db..BASEL_REVLVNG_CR_MTH_SNAPSHOT  a
									left join 	&net_db..TRNST_EXCLSN_LKP c on TRNST_NUM=EXCLUDED_TRNST_NUM
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
				    	A.BLOCK_RECL_CD as PREV_BLOCK_RECL_CD, 

						(CASE  WHEN TOT_NEW_BAL_AMT > 0 AND LK_RECL.BLOCK_RECL_CD IS NULL THEN '1'
				          		ELSE '0'
				       END ) as  v_PT_STAT_BLCK_RECL_CD_LKP_PRV, 
					   
                       (CASE WHEN LK_CHRG.CHRG_OFF_CD IS NULL THEN '1' ELSE '0' END) AS 	v_PT_STAT_CHRG_OFF_LKP_PREV2,
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
				       s.BASEL_ACCT_ID,
				       s.BASEL_CUST_ID,
					   S.PRIM_BASEL_CUST_ID, 
						S.STEP_PLN_SNAPSHOT_ID, 
						S.ACCT_NUM, 
						S.PRD_CD, 
						S.SUB_PRD_CD, 
						S.BLOCK_RECL_CD, 
						S.TOT_NEW_BAL_AMT, 
						S.CR_LMT_AMT, 
						S.ACCT_CLS_RSN_CD, 
						S.CHRG_OFF_CD, 
						S.BNS_DLQNT_DAY, 
						S.TOT_UNPAID_FNCL_CHRG_AMT, 
						S.CRNT_BILL_CD, 
						S.SCRD_TP_CD, 
						s.REVISED_EXPSR_AMT,
						S.SWITCH_XREF, 
						S.SCRTY_TP_CD, 
						S.TRNST_NUM,s.EXCLUDED_TRNST_NUM,
						s.HELOC_F,
						ps.PREV_TOT_NEW_BAL_AMT,
						ps.PREV_CHRG_OFF_CD,
						ps.PREV_BLOCK_RECL_CD,
						ps.PREV_TOT_UNPAID_FNCL_CHRG_AMT ,
				      (CASE 
				         WHEN  s.HELOC_F='N' THEN
				              CASE  
				               WHEN CHRG_OFF_CD ='1' THEN  'CHG' 
							   WHEN (  BNS_DLQNT_DAY<120 and NOT (TOT_NEW_BAL_AMT> 0 AND  CHRG_OFF_CD IN ('N','Q')) and  v_PT_STAT_BLCK_RECL_CD_LKP_CUR='1') THEN  'CUR'       
				               WHEN  ( 
									TOT_NEW_BAL_AMT>0 and TOT_NEW_BAL_AMT=TOT_UNPAID_FNCL_CHRG_AMT and v_PT_STAT_CHRG_OFF_LKP_PREV2='1' 
                                     and  NOT ( PREV_TOT_NEW_BAL_AMT>0 and PREV_CHRG_OFF_CD IN ('N','Q'))
                                     and (PREV_TOT_NEW_BAL_AMT>0 and v_PT_STAT_BLCK_RECL_CD_LKP_PRV='1') ) THEN 'CUR'
							   WHEN	(  TOT_NEW_BAL_AMT=0 and PREV_TOT_NEW_BAL_AMT=PREV_TOT_UNPAID_FNCL_CHRG_AMT and v_PT_STAT_CHRG_OFF_LKP_PREV2='1'
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
				        END) AS PIT_STAT_VER_2_CD
				    FROM SNAP s left JOIN PREV_SNAP ps ON s.BASEL_ACCT_ID = ps.BASEL_ACCT_ID AND s.BASEL_CUST_ID  = ps.BASEL_CUST_ID
				    );
            disconnect from NZCON; 
         quit;
					
		
		%let yrmth=&yrmth.;
		%let PRV_yrmth=&PRV_yrmth.;
		proc sql;
	
		CREATE TABLE BLOCKreclLKPgetCONSMscrcrdEXCLNf AS 
		SELECT DISTINCT STRIP(BNKRPY_F) as BNKRPY_F, 
		STRIP(BLOCK_RECL_CD) as BLOCK_RECL_CD ,
		STRIP(CONSM_SCORECRD_EXCLSN_F) as CONSM_SCORECRD_EXCLSN_F
		FROM NZRRAP.BLOCK_RECL_LKP 
		WHERE "&yrmth" BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH 
		;
	
		CREATE TABLE BLOCKreclLKPgetCONSMscrcrdEXCLfP AS 
		SELECT DISTINCT STRIP(BNKRPY_F) as BNKRPY_F, 
		STRIP(BLOCK_RECL_CD) as BLOCK_RECL_CD ,
		STRIP(CONSM_SCORECRD_EXCLSN_F) as CONSM_SCORECRD_EXCLSN_F
		FROM NZRRAP.BLOCK_RECL_LKP 
		WHERE "&PRV_TM_ID" BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH 
		;
		
		CREATE TABLE BLK_RECLclsRSNconsmScRDexclsn_f as 
		SELECT distinct 
		STRIP(CONSM_SCORECRD_EXCLSN_F) as CONSM_SCORECRD_EXCLSN_F, 
		STRIP(BLOCK_RECL_CD) as BLOCK_RECL_CD 
		FROM 
		NZRRAP.BLOCK_RECL_CLS_RSN_LKP
		WHERE "&yrmth" BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
		;

		CREATE TABLE CHRGoffLKPchrgOFFCDgetACCRLstatF as
		SELECT DISTINCT 
		STRIP(ACCRL_STAT_F) as ACCRL_STAT_F, 
		STRIP(CHRG_OFF_CD) as CHRG_OFF_CD 
		FROM 
		NZRRAP.CHRG_OFF_LKP
		WHERE  "&yrmth" BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH
		and STRIP(CHRG_OFF_STAT_F)='Y' and 
		CHRG_OFF_CD in (SELECT STRIP(CHRG_OFF_CD) 
						FROM NZRRAP.CHRG_OFF_LKP 
						WHERE STRIP(ACCRL_STAT_F) IN ('N', 'Q') and 
						"&yrmth" BETWEEN EFF_FROM_YR_MTH AND EFF_TO_YR_MTH)
		;
		
		CREATE TABLE S_SRC_PRD_LKP AS 
		SELECT DISTINCT STRIP(BASEL_PRD_CD) as BASEL_PRD_CD, 
				STRIP(BASEL_PRD_DESC) as BASEL_PRD_DESC, 
				STRIP(LTV_TP_CD) as LTV_TP_CD, 
				STRIP(SML_BUS_F) as SML_BUS_F, 
				STRIP(CONSM_SCORECRD_EXCLSN_F) as CONSM_SCORECRD_EXCLSN_F3, 
				STRIP(CONSM_PRD_TREATMNT_CD) as CONSM_PRD_TREATMNT_CD, 
				STRIP(SRC_PRD_CD) as SRC_PRD_CD, 
				STRIP(SRC_SUB_PRD_CD) as SRC_SUB_PRD_CD 
		FROM NZRRAP.SRC_PRD_LKP 
		WHERE STRIP(PRD_SYS_CD)='KS' 
		AND "&yrmth" BETWEEN EFF_FROM_YR_MTH 
		AND EFF_TO_YR_MTH ORDER BY SRC_PRD_CD,SRC_SUB_PRD_CD
		;
		
		CREATE TABLE S_STD_PRD_LKP AS 
		SELECT DISTINCT 
		STRIP(BASEL_PRD_CD) as BASEL_PRD_CD,
		STRIP(BASEL_PRD_DESC) as BASEL_PRD_DESC, 
		STRIP(SRC_PRD_CD) as SRC_PRD_CD, 
		STRIP(SRC_SUB_PRD_CD) as SRC_SUB_PRD_CD ,
		STRIP(BILL_CD_CHAR) as BILL_CD_CHAR
		FROM NZRRAP.SRC_PRD_STDNT_LOAN_LKP
		WHERE "&yrmth" BETWEEN  EFF_FROM_YR_MTH AND  EFF_TO_YR_MTH and TRIM(PRD_SYS_CD) = 'KS'
	 ;
		Create table CSEF_CONDITION_1 as 
		SELECT DISTINCT STRIP(CONSM_SCORECRD_EXCLSN_F) as CSEF_CONDITION_1,
				STRIP(BLOCK_RECL_CD) as BLOCK_RECL_CD 
		FROM NZRRAP.BLOCK_RECL_LKP 
		WHERE "&yrmth" BETWEEN  EFF_FROM_YR_MTH AND  EFF_TO_YR_MTH and STRIP(BLOCK_RECL_CD) ne ''
		 ;
		CREATE TABLE CSEF_CONDITION_2 AS 
		SELECT DISTINCT STRIP(CONSM_SCORECRD_EXCLSN_F) as CSEF_CONDITION_2, 
			STRIP(CLS_RSN_CD) as CLS_RSN_CD, 
			STRIP(BLOCK_RECL_CD) as BLOCK_RECL_CD 
		FROM NZRRAP.BLOCK_RECL_CLS_RSN_LKP 
		WHERE "&yrmth" BETWEEN  EFF_FROM_YR_MTH AND  EFF_TO_YR_MTH and (STRIP(CLS_RSN_CD) ne '' or STRIP(BLOCK_RECL_CD) ne '')
		;
		quit;

	
		Proc sql;
		CREATE TABLE S_BASEL_REVLVNG_CR_MTH_SNAP_CD AS 
		SELECT distinct a.MTH_TM_ID,
				       a.BASEL_ACCT_ID,
				       a.BASEL_CUST_ID,
					   a.PRIM_BASEL_CUST_ID, 
						a.STEP_PLN_SNAPSHOT_ID, 
						a.ACCT_NUM format $50., 
						a.PRD_CD, 
						a.SUB_PRD_CD, 
						a.BLOCK_RECL_CD, 
						a.TOT_NEW_BAL_AMT, 
						a.CR_LMT_AMT, 
						a.ACCT_CLS_RSN_CD, 
						a.CHRG_OFF_CD, 
						a.BNS_DLQNT_DAY, 
						a.TOT_UNPAID_FNCL_CHRG_AMT, 
						a.CRNT_BILL_CD, 
						a.SCRD_TP_CD, 
						a.SWITCH_XREF, 
						a.SCRTY_TP_CD, 
						a.TRNST_NUM,a.EXCLUDED_TRNST_NUM,
						a.HELOC_F,
						a.PREV_TOT_NEW_BAL_AMT,
						a.PREV_BLOCK_RECL_CD,
						a.PREV_TOT_UNPAID_FNCL_CHRG_AMT, 
						a.PIT_STAT_VER_2_CD,
						a.REVISED_EXPSR_AMT,
						bf.BNKRPY_F,
						bpf.BNKRPY_F as PREV_BNKRPY_F,
					cs.CONSM_SCORECRD_EXCLSN_F,
					sp.CONSM_SCORECRD_EXCLSN_F3,
					CC1.CSEF_CONDITION_1,
					CC2.CSEF_CONDITION_2,
					ACCRL_STAT_F,
					(case when SP.SRC_PRD_CD='SSL' then STD.BASEL_PRD_DESC else SP.BASEL_PRD_DESC end) as BASEL_PRD_DESC,
					SP.LTV_TP_CD, 
					SP.SML_BUS_F, 				
					SP.CONSM_PRD_TREATMNT_CD, 
					(case when SP.SRC_PRD_CD='SSL' then STD.BASEL_PRD_CD else sp.BASEL_PRD_CD end) as BASEL_PRD_CD,
					SP.SRC_SUB_PRD_CD,
					SUBSTR(strip(CRNT_BILL_CD), 3,1) as BILL_CD_CHAR1,
					BILL_CD_CHAR
					
		FROM S_BASEL_REVLVNG_CR_MTH_SNAPSHOT A 
		left join BLOCKreclLKPgetCONSMscrcrdEXCLNf as BF on a.BLOCK_RECL_CD=bf.BLOCK_RECL_CD
		left join BLOCKreclLKPgetCONSMscrcrdEXCLfP as BpF on a.PREV_BLOCK_RECL_CD=bpf.BLOCK_RECL_CD
		left join BLK_RECLclsRSNconsmScRDexclsn_f as cs on /*ACCT_CLS_RSN_CD=CLS_RSN_CD and*/ a.BLOCK_RECL_CD=cs.BLOCK_RECL_CD
		left join CHRGoffLKPchrgOFFCDgetACCRLstatF as asf on a.CHRG_OFF_CD=asf.CHRG_OFF_CD
		left join S_SRC_PRD_LKP SP ON A.PRD_CD=SP.SRC_PRD_CD AND A.SUB_PRD_CD=SP.SRC_SUB_PRD_CD
		left join S_STD_PRD_LKP std on A.PRD_CD=STD.SRC_PRD_CD AND A.SUB_PRD_CD=STD.SRC_SUB_PRD_CD 
										and SUBSTR(strip(CRNT_BILL_CD), 3,1)=BILL_CD_CHAR
		LEFT JOIN CSEF_CONDITION_1 AS CC1 ON A.BLOCK_RECL_CD=CC1.BLOCK_RECL_CD
		LEFT JOIN CSEF_CONDITION_2 AS CC2 ON A.BLOCK_RECL_CD=CC2.BLOCK_RECL_CD AND A.acct_CLS_RSN_CD=CC2.CLS_RSN_CD
		;
		QUIT;


	data LD_BASEL_REVL_CR_BASE_DRVD_VARS2 
/*	(keep=MTH_TM_ID BASEL_ACCT_ID BASEL_CUST_ID ACCT_NUM SML_BUS_F STEP_CD TRNST_EXCLSN_F ACCRL_STAT_F LTV_TP_CD BNKRPY_F INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP BASEL_PRD_CD BASEL_PRD_DESC CONSM_SCORECRD_EXCLSN_F CONSM_PRD_TREATMNT_CD HELOC_F PIT_STAT_VER_2_CD REVISED_EXPSR_AMT RS_F )*/
	;
	set S_BASEL_REVLVNG_CR_MTH_SNAP_CD;
	format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.6  ACCT_NUM $50. PIT_STAT_VER_2_CD $10. STEP_CD $10.
	Note1 Note2 Note3 Note4  $100.;

	/*IIF(ISNULL(i_SML_BUS_F), &#xD;&#xA;ABORT(&apos;PRD_CD:  &apos; || PRD_CD || &apos;  and SUB_PRD_CD:  &apos; || SUB_PRD_CD || &apos;could not find a match in the SOURCE PRODUCT LOOKUP&apos;)&#xD;&#xA;, i_SML_BUS_F)*/
	if strip(SML_BUS_F) eq '' then do;
		Note1= 'NOTE: PRD_CD:  '|| PRD_CD ||' and SUB_PRD_CD:  '|| SUB_PRD_CD || 'could not find a match in the SOURCE PRODUCT LOOKUP';
		put Note1;
		abort cancel;
	end;
/*	IIF(ISNULL(BASEL_PRD_CD), ABORT(&apos;PRD_CD:  &apos; || PRD_CD || &apos;  and SUB_PRD_CD:  &apos; || SUB_PRD_CD || &apos;could not find a match in the SOURCE PRODUCT LOOKUP&apos;)&#xD;&#xA;,i_BASEL_PRD_CD)*/
    	if strip(BASEL_PRD_CD) eq '' then do;
		Note2= 'NOTE: PRD_CD:  '|| PRD_CD ||' and SUB_PRD_CD:  '|| SUB_PRD_CD || 'could not find a match in the SOURCE PRODUCT LOOKUP';
		put Note2;
		abort cancel;
	end;
/*IIF(ISNULL(i_BASEL_PRD_DESC), &#xD;&#xA;ABORT(&apos;PRD_CD:  &apos; || PRD_CD || &apos;  and SUB_PRD_CD:  &apos; || SUB_PRD_CD || &apos;could not find a match in the SOURCE PRODUCT LOOKUP&apos;)&#xD;&#xA;, i_BASEL_PRD_DESC)" */
	if strip(BASEL_PRD_DESC) eq '' then do;
		Note3= 'NOTE: PRD_CD:  '|| PRD_CD ||' and SUB_PRD_CD:  '|| SUB_PRD_CD || 'could not find a match in the SOURCE PRODUCT LOOKUP';
		put Note3;
		abort cancel;
	end;


	if strip(EXCLUDED_TRNST_NUM) eq '' then do; TRNST_EXCLSN_F='N'; end;
	else do; TRNST_EXCLSN_F='Y'; end;

	CRNT_BILL_CD2=SUBSTR(CRNT_BILL_CD,1,2);

	if strip(STEP_PLN_SNAPSHOT_ID) notin ('-1','-2') then do; STEP_CD='Y'; end;

	if strip(STEP_PLN_SNAPSHOT_ID) in ('-1','-2') then do;
		if strip(PRD_CD) in ('SCL', 'VIC') then do;
			if 	strip(SCRD_TP_CD)='U' then do; STEP_CD='U'; end;
			else if SUBSTR(CRNT_BILL_CD,1,1)='U' then do; STEP_CD= 'U'; end;
			else if CRNT_BILL_CD2 in ('11', 'SB', 'SN', 'SP', 'SR', 'ST') then do; STEP_CD= 'R'; end;
			else if strip(SCRD_TP_CD) in ('S') then do; STEP_CD= 'O'; end;
			Else do; STEP_CD= 'O'; end;
		end;
/*		else if strip(PRD_CD) notin ('SCL', 'VIC') then do; STEP_CD= 'O'; end;*/
	end;
	if strip(STEP_PLN_SNAPSHOT_ID) in ('-1','-2') and strip(PRD_CD) notin ('SCL', 'VIC') then do; STEP_CD= 'N';
	end;

	IF STRIP(SUB_PRD_CD)='RS' then do; RS_F= 'Y'; end; 						else do; RS_F= 'N'; end;


	CSEF_CONDITION_3=CONSM_SCORECRD_EXCLSN_F3;
	

	IF CSEF_CONDITION_1='Y' THEN DO; CONSM_SCORECRD_EXCLSN_F= 'Y'; END;
	ELSE IF CSEF_CONDITION_2='Y' THEN DO; CONSM_SCORECRD_EXCLSN_F= 'Y'; END;
	ELSE IF CSEF_CONDITION_3='Y' THEN DO; CONSM_SCORECRD_EXCLSN_F='Y'; END;
	ELSE IF (TOT_NEW_BAL_AMT le 0 AND CR_LMT_AMT le 0) THEN DO; CONSM_SCORECRD_EXCLSN_F= 'Y'; END;
	ELSE IF PIT_STAT_VER_2_CD='CHG' THEN DO; CONSM_SCORECRD_EXCLSN_F='Y'; ;END;
	ELSE DO;CONSM_SCORECRD_EXCLSN_F='N'; ;END;
		
	INSRT_PROCESS_TMSTMP="&SYSDATE9.:&SYSTIME."dt   ;
	UPDT_PROCESS_TMSTMP ="&SYSDATE9.:&SYSTIME."dt ;

	If (CR_LMT_AMT<=0 AND TOT_NEW_BAL_AMT<=0) then do; CONSM_PRD_TREATMNT_CD='Z'; end;
	else do;	CONSM_PRD_TREATMNT_CD=CONSM_PRD_TREATMNT_CD; end;

	if CONSM_PRD_TREATMNT_CD eq '' then do;
		Note4= 'NOTE: PRD_CD:  '|| PRD_CD ||' and SUB_PRD_CD:  '|| SUB_PRD_CD || 'could not find a match in the SOURCE PRODUCT LOOKUP';
		put Note4;
		abort cancel;

	end;


	run;

	data LD_BASEL_REVL_CR_BASE_DRVD_VARS
	(keep=MTH_TM_ID BASEL_ACCT_ID BASEL_CUST_ID ACCT_NUM SML_BUS_F STEP_CD TRNST_EXCLSN_F ACCRL_STAT_F LTV_TP_CD BNKRPY_F INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP BASEL_PRD_CD BASEL_PRD_DESC CONSM_SCORECRD_EXCLSN_F CONSM_PRD_TREATMNT_CD HELOC_F PIT_STAT_VER_2_CD REVISED_EXPSR_AMT RS_F)
	;
	set LD_BASEL_REVL_CR_BASE_DRVD_VARS2;

	run;
	
	 PROC SQL NOPRINT;
	         	CONNECT USING NZRRAP AS NZCON;
	         	EXECUTE(DELETE FROM &net_db..BASEL_REVLVNG_CR_BASE_DRVD_VARS WHERE MTH_TM_ID=&tm_id.) BY NZCON;
	 QUIT;

	 proc append base=NZRRAP.BASEL_REVLVNG_CR_BASE_DRVD_VARS  (BULKLOAD=YES BL_METHOD=CLILOAD)
				data=LD_BASEL_REVL_CR_BASE_DRVD_VARS force; run;
	

		data _null_;
			if 0 then set S_BASEL_REVLVNG_CR_MTH_SNAPSHOT Nobs=Number_of_Obs; 
			call symputx('SourceRec',Number_of_Obs); 
			stop; 
		run;
		%put Source=&SourceRec.;
		data _null_;
			if 0 then set LD_BASEL_REVL_CR_BASE_DRVD_VARS Nobs=Number_of_Obs; 
			call symputx('TargetRec',Number_of_Obs); 
			stop; 
		run;
			%put Target=&TargetRec.;
DATA _NULL_;
		CALL SYMPUTX('PROCESSENDTIME',PUT(DATETIME(),DATETIME25.0));
		CALL SYMPUTX('PROCESSRUNTIME',PUT((DATETIME()-"&PROCESSSTARTTIME"dt),TIME.));
	RUN;
PROC SQL NOPRINT;
	 insert into NZRRAP.AUDIT_JOB_TIMER_CHECK (Job_name, MTH_TM_ID, START_Time, End_Time, Source_count, Target_Count)
	 values(		'J_RRII_KS10_2103_BASEL_REVLVNG_CR_BASE_DRVD_VARS',
			&TM_ID.,
			"&PROCESSSTARTTIME"dt,
			"&PROCESSENDTIME"dt,
			&SourceRec.,
			&TargetRec.
		);
	 QUIT;

%Put Info:-----------------------------------------------------------;
%Put Info: Jobs Strated at: &PROCESSSTARTTIME;
%Put Info: Jobs Completed at: &PROCESSENDTIME;
%Put Info: Job execution Time: &PROCESSRUNTIME.;
