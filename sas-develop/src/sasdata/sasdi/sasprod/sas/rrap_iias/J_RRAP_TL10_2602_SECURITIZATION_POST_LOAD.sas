%put WORK LOCATION: %sysfunc(getoption(work));
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

/*---- Start of User Written Code  ----*/ 
data _null_;
	infile "&rrap_dir./params/rrap/cc_securitization_email_list.txt";
	input;
	if _N_ =2 then do;
	CALL SYMPUT("to_email_list",_infile_);
	end;
	if _N_ =3 then do;
	CALL SYMPUT("cc_email_list",_infile_);
	end;
	CALL SYMPUT('MonthYear', CATX(' ',PUT(INTNX('month',"&MTH_END_DT"d,0,'E'),monname10.),PUT(YEAR(INTNX('month',"&MTH_END_DT"d,0,'E')),4.)));
run;

%put &to_email_list;
%put &cc_email_list;

/*INITIALIZE PARAMETERS*/
%GLOBAL MV_DELTA_FOR_DRAWN_AMT;
%GLOBAL MV_DELTA_FOR_CREDIT_LIMIT_AMT;


PROC SQL NOPRINT;
CREATE TABLE DRAPT.AUTO_POST_LOAD_VALIDATIONS AS 
SELECT MTH_TM_ID, 
SUM(BEFORE_ZERO_NET_DRAWN_AMT) AS TOT_BF_0_NET_DR_AMT, 
SUM(ADJUSTED_OS_BAL_AMT) AS TOT_ADJ_OS_BAL_AMT,
SUM(AUTH_AMT) AS TOT_AUTH_AMT,
SUM(CR_LMT_AMT) AS TOT_CR_LMT_AMT,
SUM(BEFORE_ZERO_NET_DRAWN_AMT)-SUM(ADJUSTED_OS_BAL_AMT) AS DELTA_FOR_DRAWN_AMT, 
SUM(AUTH_AMT)-SUM(CR_LMT_AMT) AS DELTA_FOR_CREDIT_LIMIT_AMT
FROM EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT
WHERE SRC_SYS_CD='SPL' AND MTH_TM_ID=&MTH_TM_ID
GROUP BY MTH_TM_ID;
QUIT;

PROC SQL NOPRINT;
SELECT DELTA_FOR_DRAWN_AMT FORMAT=15.2, DELTA_FOR_CREDIT_LIMIT_AMT FORMAT=15.
INTO :MV_DELTA_FOR_DRAWN_AMT, :MV_DELTA_FOR_CREDIT_LIMIT_AMT
FROM DRAPT.AUTO_POST_LOAD_VALIDATIONS WHERE MTH_TM_ID=&MTH_TM_ID;
QUIT;

PROC SQL NOPRINT;
SELECT TOT_OS_BAL_MATCHED, OS_BAL_AMT_MATCHED_PCTG, TOT_SECRTZTN_AMT_TO_ADJUST FORMAT=15.2, SECRTZTN_OS_ADJ_FACTR FORMAT=15.10,
CASE WHEN OS_BAL_AMT_MATCHED_PCTG>=95 THEN 'YES' ELSE 'NO, AUTHORIZATION REQUIRED' END AS THRESHOLD
INTO :TGT_TOT_OS_BAL, :TGT_TOT_OS_BAL_MATCH_RT, :TGT_TOT_SECRTZTN_AMT_TO_ADJUST, :TGT_SECURI_ADJ_RT, :TGT_THRESHOLD
FROM DRAPT.SPL_ACCOUNT_MATCH_RESULTS WHERE MTH_TM_ID=&MTH_TM_ID;
QUIT;

/*EXTRACT TIME DESCRIPTION FOR THE TIME ID*/
PROC SQL NOPRINT;
SELECT TM_DESC FORMAT=$18. INTO :TM_DESC FROM NZRRAP.TM_DIM WHERE TM_ID=&MTH_TM_ID;
QUIT;

/*
PROC SQL NOPRINT;
SELECT SECURITIZEDAMOUNT FORMAT=15.2 INTO :SECURI_AMT 
FROM DRAPT.SPL_SOURCE_FILE_SECURITIZATION;
QUIT;
*/
/*---- End of User Written Code  ----*/ 

/*---- Start of User Written Code  ----*/ 



%MACRO SECUREMAIL1;
FILENAME OUTMAIL EMAIL
SUBJECT= "[RRAP] AUTO Securitization Match Rates - &MonthYear. : post update Delta";

DATA _NULL_;
FILE OUTMAIL
TO=(&to_email_list)
CC=(&cc_email_list);
PUT "Hi,";
PUT " ";
PUT "Please see below monthly AUTO securitization post update results";
PUT "The month time id for this run is &TM_DESC.";
PUT " ";
PUT "1. Securitization amount received	: &TGT_TOT_SECRTZTN_AMT_TO_ADJUST";
PUT "2. Securitization amount applied	: &TGT_TOT_SECRTZTN_AMT_TO_ADJUST";
PUT "3. Post update delta for drawn amount	: &MV_DELTA_FOR_DRAWN_AMT";
PUT " ";
PUT "Please note: This is a system generated email and responses aren't monitored.";
run;

%MEND SECUREMAIL1;
%SECUREMAIL1;

/*COPY AUTO LOAN FILE*/
filename src "&OUTPATH/securitization/cc/incoming/autoloan_securitization_acct_mthly_&YEARMONTH..csv";
filename dest "&OUTPATH/cmf/outgoing/autoloan_securitization_acct_mthly_&YEARMONTH..csv";

data _null_;
   length msg $ 384;
   rc=fcopy('src', 'dest');
   if rc=0 then
      put 'Copied SRC to DEST.';
   else do;
      msg=sysmsg();
      put rc= msg=;
   end;
run;



