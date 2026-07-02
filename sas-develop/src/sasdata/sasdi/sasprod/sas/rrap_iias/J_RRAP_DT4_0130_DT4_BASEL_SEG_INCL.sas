***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_BASEL_SEG_INCL
*  
*  Purpose: Load DT4_BASEL_SEG_INCL 
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  		  
*
*	Change Log:
*	2021-11-02: Hadi Dimashkieh - Initial Development
*
*
*
***************************************************************************************************************************;

%rrap_dt4_autoexec();

%let sourcefile=&owftp./rrm/DT4_BASEL_SEG_INCL.csv;

%let TARGET=DT4_BASEL_SEG_INCL;

%let key_fields = BASEL_SEG_ID ;
%let digest_fields = MODEL_TYPE, SRC_SYS_CD, MODEL_NM, SEG_NUM, BASEL_MODEL_ID, BASEL_SEG_DESC, SEG_CRTRIA_DESC, INCL_F;



%let surrogate_key_flag = N;
%let surrogate_key = ;
%let initial_surrogate_key_value = ;


proc sql;
create table DT4_BASEL_SEG_INCL as 
SELECT 	
CASE 	
	WHEN b.LGD_NON_DEFAULTER_F = 'Y' THEN 'LGD-ND'
	WHEN b.PD_F = 'Y' THEN 'PD'
	WHEN b.LGD_DEFAULTER_F = 'Y' THEN 'LGD-D'
	WHEN b.EAD_F = 'Y' THEN 'EAD'
	ELSE '' 
 END AS MODEL_TYPE	
,a.SRC_SYS_CD, b.MODEL_NM, a.SEG_NUM, a.BASEL_SEG_ID, a.BASEL_MODEL_ID,  a.BASEL_SEG_DESC, a.SEG_CRTRIA_DESC	
FROM NZRRAP.basel_seg a LEFT JOIN NZRRAP.basel_model b 	
ON a.BASEL_MODEL_ID =b.BASEL_MODEL_ID 	
ORDER BY 1,BASEL_SEG_ID;
quit;

data DT4_BASEL_SEG_INCL;
	set DT4_BASEL_SEG_INCL;

	if SEG_NUM in (90,98,99) then do;
		INCL_F = 0;
	end;
	
	else if model_type = 'LGD-D' then do;	
			 if (MODEL_NM IN ('MOR LGD-D')	AND SEG_NUM IN (13,14))			then INCL_F = 0;
		else if (MODEL_NM IN ('TNG-MOR LGD-D') AND SEG_NUM IN (10,11))		then INCL_F = 0;
		else if (MODEL_NM IN ('DTL LGD-D') AND SEG_NUM IN (6,7,8)) 			then INCL_F = 0;
		else if (MODEL_NM IN ('ITL LGD-D') AND SEG_NUM IN (6,7,8)) 			then INCL_F = 0;
		else INCL_F = 1;
	end;
	
	else if model_type = 'LGD-ND' then do;
			 if (MODEL_NM IN ('MOR LGD-ND')	AND SEG_NUM IN (13)) 			then INCL_F = 0;
		else if (MODEL_NM IN ('TNG-MOR LGD-ND') AND SEG_NUM IN (10))		then INCL_F = 0;
		else if (MODEL_NM IN ('DTL LGD-ND') AND SEG_NUM IN (8))				then INCL_F = 0;
		else if (MODEL_NM IN ('ITL LGD-ND') AND SEG_NUM IN (7,8))			then INCL_F = 0;
		else INCL_F = 1;
	end;
	
	else if model_type = 'PD' then do;
		 	 if (MODEL_NM in ('DTL PD') and SEG_NUM in (7)) 				then INCL_F = 0;
		else if (MODEL_NM in ('ITL PD') and SEG_NUM in (10,11))				then INCL_F = 0;
		else INCL_F = 1;	
	end;
	
	else INCL_F = 1;
run;



proc sort data=DT4_BASEL_SEG_INCL; by MODEL_TYPE MODEL_NM SEG_NUM; run;


proc export data=DT4_BASEL_SEG_INCL dbms=csv outfile="&sourcefile." replace; run;

data varnames;
length vars $ 20;
input vars $;
datalines;
MODEL_TYPE
SRC_SYS_CD
MODEL_NM
SEG_NUM
BASEL_MODEL_ID
BASEL_SEG_DESC
SEG_CRTRIA_DESC
INCL_F
EFF_FROM_YR_MTH
EFF_TO_YR_MTH
; 
run;

/********************************************************************************************************************/
/********************************************************************************************************************/

%DT4_ETL_SCD2_LOAD();

/********************************************************************************************************************/
/********************************************************************************************************************/

