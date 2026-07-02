***************************************************************************************************************************;

%let etls_jobname = J_RRAP_MTH_SRC_IND_COST.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  	MTH_SRC_IND_COST 
*					  
*  Source: 	IIAS source:EDRTLRP1D.MBR_INDRCT_COST_MTH_SNAPSHOT  (for Trnst: 07112)
			FLAT FILE: TOT_NON_INT_EXP_TRANSIT.csv  (for transit 36442, 78808)  
*
*  Frequency: 		Month End runs
*
*	Change Log:
*	2024-05-10: 	Ganesh Patro - Initial Development
*
*
*
***************************************************************************************************************************;

%rrap_autoexec();

*################ TABLE1:		MTH_SRC_IND_COST		###########################*;
%put &MTH_TM_ID;
%let sourcefile=/owpftp/TOT_NON_INT_EXP_TRANSIT.csv;

PROC IMPORT DATAFILE="&sourcefile."
OUT=temp
DBMS=CSV
REPLACE;
GETNAMES=YES;
guessingrows=max;
RUN;

proc sql;
connect using NZRRAP as iiascon;
execute(delete from &net_db..MTH_SRC_IND_COST where MTH_TM_ID = &mth_tm_id.;) by iiascon;
execute(commit;) by iiascon;
quit;


proc sql;
connect using NZRRAP as iiascon;
execute(INSERT into &net_db..MTH_SRC_IND_COST 
(MTH_TM_ID,PROCESS_MTH,TRNST_NUM,TOT_NON_INTR_EXPNS_AMT,DATA_SOURCE,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
) 
(SELECT A.MTH_TM_ID,VARCHAR_FORMAT(B.TM_LVL_END_DT , 'YYYYMM') as PROCESS_MTH,
A.TRNST_NUM,A.TOT_NON_INTR_EXPNS_AMT,'MBR_INDRCT_COST_MTH_SNAPSHOT' AS DATA_SOURCE, 
CURRENT TIMESTAMP as INSRT_PROCESS_TMSTMP, 
CURRENT TIMESTAMP as UPDT_PROCESS_TMSTMP
from &net_db..MBR_INDRCT_COST_MTH_SNAPSHOT  A
inner join &net_db..TM_DIM B
on A.MTH_TM_ID=B.TM_ID
WHERE A.MTH_TM_ID =&mth_tm_id.);) by iiascon;
execute(CALL SYSPROC.ADMIN_CMD (%nrbquote('RUNSTATS ON TABLE &net_db..MTH_SRC_IND_COST on KEY COLUMNS and INDEXES ALL'));) by iiascon;
execute(COMMIT;) by iiascon;
quit;


proc sql;
	insert into NZRRAP.MTH_SRC_IND_COST (MTH_TM_ID,PROCESS_MTH,TRNST_NUM,
			TOT_NON_INTR_EXPNS_AMT,DATA_SOURCE,INSRT_PROCESS_TMSTMP,UPDT_PROCESS_TMSTMP)
	(select MTH_TM_ID,PROCESS_MONTH,PUT(TRNST_NUM,5.) AS TRNST_NUM,TOT_NON_INTR_EXPNS_AMT,
	'TOT_NON_INT_EXP_TRANSIT.csv' as DATA_SOURCE,
	"&SYSDATE9.:&SYSTIME."dt as INSRT_PROCESS_TMSTMP length = 8,
	"&SYSDATE9.:&SYSTIME."dt as UPDT_PROCESS_TMSTMP length = 8
from work.temp where mth_tm_id = &mth_tm_id.);
quit;


