/**************************************************************************************** 
* INFA Job Name			:wf_DM_RRAP_Load_BASEL_MORT_ACCT_DRVD_VARS						*
* SAS Job Name			:J_RRII_KS10_2107_BASEL_MORT_ACCT_DRVD_VARS						* 
* Description			:INFA to SAS Convertion											*
* Source Database/Schema: BLUDBPRD / EDRTLRP1D                                 			* 
* Source Table Name		: BASEL_MORT_MTH_SNAPSHOT,BASEL_ACCT_DIM,TRNST_EXCLSN_LKP,TM_DIM*				
* Target Database/Schema: BLUDBPRD / EDRTLRP1D											*
* Target Table Name 	: BASEL_MORT_ACCT_DRVD_VARS										*
* SAS Code Location		: /sasdata/sasdi/sasprod/sas/rrap_iias							* 
* Created on			: Thursday, August 12, 2021 2:36:46 PM EDT             			* 
* Created by			: owprdsas                                              		* 
* Updated on			: Wednesday, March 2, 2022										*
* Updated by			: Vijay Kadiyala												*
* Version				: SAS Enterprise Guide 7.1										*	
****************************************************************************************/ 

%put WORK LOCATION: %sysfunc(getoption(work));
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

DATA _NULL_;
	CALL SYMPUTX('PROCESSSTARTTIME',PUT(DATETIME(),DATETIME25.0));
RUN;

%PUT NOTE: ******* PROCESS START TIME: &PROCESSSTARTTIME. *******;
proc sql;
	select 	TM_ID, tm_lvl_ST_dt,put(tm_lvl_ST_dt,date9.) as tm_lvl_ST_dtt,tm_lvl_end_dt, put(tm_lvl_end_dt,date9.) as tm_lvl_end_dtt ,TM_LVL_END_DT format yymmn6. as yrmth
	into 	:TM_ID, :tm_lvl_ST_dt,:tm_lvl_ST_dtt ,:tm_lvl_end_dt,:tm_lvl_end_dtt, :yrmth
	from 	NZRRAP.TM_DIM
	where tm_id = &MTH_TM_ID.;
	SELECT 
		put(MAX(tgt.DAY_DT),date9.) as LAST_BUSINESS_DAYt,
		MAX(tgt.DAY_DT)  as LAST_BUSINESS_DAY,
		tmp.TM_ID AS TM_ID 
	INTO :LAST_BUSINESS_DAYt, :lbd_tm_id, :LAST_BUSINESS_DAY
	FROM NZRRAP.TM_DIM tgt
	INNER JOIN 
	(SELECT TM_ID, CLNDR_YR, MTH_CLNDR_CD 
		FROM NZRRAP.TM_DIM 
		WHERE TM_ID=&TM_ID) tmp	ON tgt.CLNDR_YR = tmp.CLNDR_YR  AND tgt.MTH_CLNDR_CD = tmp.MTH_CLNDR_CD
		WHERE 	tgt.TM_LVL='Day'
		AND		tgt.DAY_OF_WK_DESC
		IN 		('Monday','Tuesday','Wednesday','Thursday',	'Friday')
		GROUP BY tmp.TM_ID;
quit;

%put LAST_BUSINESS_DAYT=&LAST_BUSINESS_DAYT.;
%put LAST_BUSINESS_DAYT=&LAST_BUSINESS_DAY.;


Proc sql;
	Create table IN_BASEL_MORT_MTH_SNAPSHOT as
	SELECT A.*,
			STRIP(ACCT_NUM) as ACCT_NUM,
			EXCLUDED_TRNST_NUM
	FROM NZRRAP.BASEL_MORT_MTH_SNAPSHOT A 
	LEFT JOIN NZRRAP.TRNST_EXCLSN_LKP BR on a.SERV_BR_TRNST_NUM=BR.EXCLUDED_TRNST_NUM 
	LEFT JOIN NZRRAP.BASEL_ACCT_DIM BA ON A.BASEL_ACCT_ID = BA.BASEL_ACCT_ID  AND SRC_APP_CD = 'MO'
	WHERE A.MTH_TM_ID=&TM_ID. ;
quit;

DATA LD_BASEL_MORT_MTH_SNAPSHOT 
	(keep= MTH_TM_ID BASEL_ACCT_ID BASEL_CUST_ID ACCT_NUM COMM_TP_CD CONSM_PRD_TREATMNT_CD DLQNT_DAY_CNT DLQNT_MTH_CNT  LAND_RGSTRN_ACT_STAT_F OS_BAL_AMT PIT_STAT_VER_1_CD STEP_F TRNST_EXCLSN_F  INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP);
	SET IN_BASEL_MORT_MTH_SNAPSHOT;	
	FORMAT INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.6  COMM_TP_CD $12. DLQNT_DAY_CNT DLQNT_DAY_CNT_TEMP 9.
			FRST_UNPAID_DT1 WK_FRST_UNPAID_DT1 tm_lvl_ST_dt1  LAST_BUSINESS_DAY1 date9.
			CONSM_PRD_TREATMNT_CD $10. PIT_STAT_VER_1_CD $10.;	
	if SCRTY_TP_2 ne '' then do;
		TST=INPUT(SUBSTR(STRIP(SCRTY_TP_2),LENGTH(STRIP(SCRTY_TP_2))-2, 3),6.);
	end; 

	BASEL_CUST_ID=PRIM_BASEL_CUST_ID;

	LAST_BUSINESS_DAY1=input("&LAST_BUSINESS_DAYT.",date9.);

	tm_lvl_ST_dt1="&tm_lvl_ST_dt."d;
	if WK_FRST_UNPAID_DT ne . 
	then do;
		WK_FRST_UNPAID_DT1= intnx('day',WK_FRST_UNPAID_DT,0,"BEGINNING");
		FRST_UNPAID_Diff2=intck('MONTH',tm_lvl_ST_dt1, WK_FRST_UNPAID_DT1)+1; 
		DLQNT_DAY_CNT_TEMPw1=intck('day',LAST_BUSINESS_DAY1, WK_FRST_UNPAID_DT); 
	end;
	if FRST_UNPAID_DT ne . 
	then do;
		FRST_UNPAID_DT1=intnx('day',FRST_UNPAID_DT,0,"BEGINNING");
		FRST_UNPAID_Diff=intck('MONTH',tm_lvl_ST_dt1, FRST_UNPAID_DT1)+1; 
		DLQNT_DAY_CNT_TEMPd1=intck('day',LAST_BUSINESS_DAY1, FRST_UNPAID_DT);
		DLQNT_DAY_CNT_TEMPd2=LAST_BUSINESS_DAY1-FRST_UNPAID_DT ;

	end;
	

	IF PD_OFF_DT NE . OR STRIP(PD_OFF_F)='Y' 
		THEN DO; DLQNT_DAY_CNT_TEMP=0; 
	END;
	ELSE DO;
		IF STRIP(FLOAT_CD) IN ( 'W', 'B', 'S') /*and WK_FRST_UNPAID_DT ne .*/ 
		THEN DO;
			DLQNT_DAY_CNT_TEMP=LAST_BUSINESS_DAY1- WK_FRST_UNPAID_DT; 
		END;
		ELSE /*if FRST_UNPAID_DT ne . then */
			DO; DLQNT_DAY_CNT_TEMP=LAST_BUSINESS_DAY1-FRST_UNPAID_DT; 
		END;
	 END;


	IF DLQNT_DAY_CNT_TEMP EQ .  
	THEN DO;  DLQNT_DAY_CNT=.; 
	END;
	ELSE DO; 
			IF DLQNT_DAY_CNT_TEMP<0 
			THEN DO; DLQNT_DAY_CNT= 0; 
			END;
			ELSE DO; DLQNT_DAY_CNT=DLQNT_DAY_CNT_TEMP; 
			END;
	END;


	IF DLQNT_DAY_CNT=0 
	THEN DO ; 
		DLQNT_MTH_CNT_TEMP= 0; DLQ=1;
	END;
	ELSE DO;
		IF(STRIP(FLOAT_CD)) IN ( 'W', 'B', 'S') /*and WK_FRST_UNPAID_DT ne .*/ 
		THEN DO; 
/*			DLQNT_MTH_CNT_TEMP=intck('MONTH',tm_lvl_ST_dt1, WK_FRST_UNPAID_DT1)+1; */
			DLQNT_MTH_CNT_TEMP=intck('MONTH', WK_FRST_UNPAID_DT1,tm_lvl_ST_dt1)+1;
			DLQ=2; 

	END;
		ELSE /*if FRST_UNPAID_DT ne . then */
			DO; 	
			DLQNT_MTH_CNT_TEMP=intck('MONTH', FRST_UNPAID_DT1,tm_lvl_ST_dt1)+1; DLQ=3;
		END;
	END;

/*v_DLQNT_MTH_CNT= IIF(ISNULL(v_DLQNT_MTH_CNT_TEMP) OR v_DLQNT_MTH_CNT_TEMP<0, 0, v_DLQNT_MTH_CNT_TEMP)*/
	IF DLQNT_MTH_CNT_TEMP EQ . OR DLQNT_MTH_CNT_TEMP LT 0 
	THEN DO; 		DLQNT_MTH_CNT= 0; DLQT=1;	END;
	ELSE DO; 		DLQNT_MTH_CNT=DLQNT_MTH_CNT_TEMP; DLQT=2;	END;

	os_bal_amt=CRNT_BAL_AMT+INTR_ACCR_AMT;


	
			IF SUBSTR(STRIP(SCRTY_TP_2),1,1)='6' OR INPUT(SUBSTR(STRIP(SCRTY_TP_2),LENGTH(STRIP(SCRTY_TP_2))-2, 3),6.)>=5 
			THEN DO; COMM_TP_CD='COMMERCIAL'; 
			END;
			ELSE DO; COMM_TP_CD= 'RESIDENTIAL'; 
			END;

	FUND_CD1= INPUT(STRIP(FUND_CD),9.);
	IF 	 	(INPUT(STRIP(FUND_CD),9.) GE 2000 AND INPUT(STRIP(FUND_CD),9.) LE 2199)
		OR 	(INPUT(STRIP(FUND_CD),9.) GE 2202 AND INPUT(STRIP(FUND_CD),9.) LE 2249)	 
		OR 	(INPUT(STRIP(FUND_CD),9.) GE 6490 AND INPUT(STRIP(FUND_CD),9.) LE 6499) 
	THEN DO; LAND_RGSTRN_ACT_STAT_F='Y'; END;
	ELSE DO; LAND_RGSTRN_ACT_STAT_F='N'; 
	END;


	IF EXCLUDED_TRNST_NUM ne '' 
	then do ; EXCLUDED_TRNST_f= 'Y'; 
	end;
	else do; EXCLUDED_TRNST_f='N'; 
	end;



	IF 	COMM_TP_CD='RESIDENTIAL' AND CRNT_BAL_AMT NE 0.000
		AND (PD_OFF_DT EQ . OR STRIP(PD_OFF_DT) EQ '' ) 
		AND (DLQNT_MTH_CNT LE 3 OR DLQNT_MTH_CNT EQ . )
		AND (STRIP(FRCLSR_F) NE 'Y' OR strip(FRCLSR_F) EQ '' )
		AND (strip(LAND_RGSTRN_ACT_STAT_F) EQ ''  OR  strip(LAND_RGSTRN_ACT_STAT_F)='N') 
		
	THEN DO; 
		PIT_STAT_VER_1_CD='CUR'; 
	END;
	else do;
	IF 	CRNT_BAL_AMT NE 0.000 AND COMM_TP_CD='RESIDENTIAL' 
		AND (PD_OFF_DT EQ . OR PD_OFF_DT EQ '' ) 
			AND ((DLQNT_MTH_CNT EQ . OR DLQNT_MTH_CNT gt 3 ) 
			OR (strip(FRCLSR_F) EQ '' OR STRIP(FRCLSR_F) = 'Y')
			OR (strip(LAND_RGSTRN_ACT_STAT_F) EQ '' OR strip(LAND_RGSTRN_ACT_STAT_F) ='Y') )
			
	THEN DO;
			PIT_STAT_VER_1_CD= 'DEF'; 
	END;
	end;

	IF STEP_PLN_SNAPSHOT_ID IN ( -1, -2) 
	THEN DO; 
		STEP_F= 'N'; 
	END;
	ELSE DO; 
		STEP_F='Y'; 
	END;

	IF COMM_TP_CD NE 'RESIDENTIAL' OR PD_OFF_DT NE . OR OS_BAL_AMT LE 0 
	THEN DO;
		CONSM_PRD_TREATMNT_CD= 'Z'; 
	END;
	ELSE DO; 
		CONSM_PRD_TREATMNT_CD='A'; 
	END;

	if EXCLUDED_TRNST_NUM eq '' 
	then do; 
		TRNST_EXCLSN_F='N'; 
	end; 
	else do; 
		TRNST_EXCLSN_F='Y'; 
	end;

	INSRT_PROCESS_TMSTMP="&SYSDATE9.:&SYSTIME."dt   ;
	UPDT_PROCESS_TMSTMP ="&SYSDATE9.:&SYSTIME."dt ;
RUN;


PROC SQL NOPRINT;
	CONNECT USING NZRRAP AS NZCON;
	EXECUTE(DELETE FROM &net_db..BASEL_MORT_ACCT_DRVD_VARS WHERE MTH_TM_ID=&TM_ID.) BY NZCON;	 									
QUIT;


proc append base=NZRRAP.BASEL_MORT_ACCT_DRVD_VARS  (BULKLOAD=YES BL_METHOD=CLILOAD)
			data=LD_BASEL_MORT_MTH_SNAPSHOT  force ; run;

DATA _NULL_;
	CALL SYMPUTX('PROCESSENDTIME',PUT(DATETIME(),DATETIME25.0));
	CALL SYMPUTX('PROCESSRUNTIME',PUT((DATETIME()-"&PROCESSSTARTTIME"dt),TIME.));
RUN;

data _null_;
	if 0 then set IN_BASEL_MORT_MTH_SNAPSHOT Nobs=Number_of_Obs; 
	call symputx('SourceRec',Number_of_Obs); 
	stop; 
run;
%put Source=&SourceRec.;

data _null_;
	if 0 then set LD_BASEL_MORT_MTH_SNAPSHOT Nobs=Number_of_Obs; 
	call symputx('TargetRec',Number_of_Obs); 
	stop; 
run;


PROC SQL NOPRINT;
	 insert into NZRRAP.AUDIT_JOB_TIMER_CHECK (Job_name, MTH_TM_ID, START_Time, End_Time, Source_count, Target_Count)
	values(		'J_RRII_KS10_2107_BASEL_MORT_ACCT_DRVD_VARS',
			&TM_ID.,
			"&PROCESSSTARTTIME"dt,
			"&PROCESSENDTIME"dt,
			&SourceRec.,
			&TargetRec.
		);
QUIT;


/**************************** CODE END ******************************/
