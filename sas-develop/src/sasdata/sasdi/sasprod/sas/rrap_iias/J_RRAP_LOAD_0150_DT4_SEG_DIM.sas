***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_SEG_DIM
*  
*  Purpose: Load DT4_SEG_DIM 
*
*  Frequency: Quarter End runs
*
*  Notes: 
*  		  
*
*	Change Log:
*	2021-12-08: Hadi Dimashkieh - Initial Development
*
*
*
***************************************************************************************************************************;

%rrap_dt4_autoexec();


%let sourcefile=&owftp./rrm/rma_ncr_dt4_segment.del;
/*%let sourcefile=/u/s2809211/projects/rrap/dimensions/opsfiles/rma_ncr_dt4_segment.del;*/



%let TARGET=DT4_SEG_DIM;

%let key_fields = MODEL_TYPE RRAP_SEG_NUM;
%let digest_fields = DT4_SEG_KEY_VAL, DT4_SEG_DESC;

%let surrogate_key_flag = N;
%let surrogate_key = ;
%let initial_surrogate_key_value = ;


DATA &TARGET./*(where=(input(DT4_EXPSR_CL_KEY_VAL,4.) GE 5501 or input(DT4_EXPSR_CL_KEY_VAL,4.) EQ 599))*/;
    LENGTH
        MODEL_TYPE           $ 6
        RRAP_SEG_NUM 		8
        DT4_SEG_KEY_VAL  $ 4
        DT4_SEG_DESC     $ 255;
    FORMAT
        MODEL_TYPE           $6.
        RRAP_SEG_NUM 4.
        DT4_SEG_KEY_VAL  $4.
        DT4_SEG_DESC      $CHAR255. ;
    INFORMAT
        MODEL_TYPE           $6.
        RRAP_SEG_NUM 4.
        DT4_SEG_KEY_VAL  $4.
        DT4_SEG_DESC      $CHAR255. ;
   INFILE "&sourcefile."
        LRECL=32767
        FIRSTOBS=2
        ENCODING="LATIN1"
        DLM='2c'x
        MISSOVER
        DSD ;
    INPUT
        MODEL_TYPE           : $CHAR6.
        RRAP_SEG_NUM : ?? BEST4.
        DT4_SEG_KEY_VAL  : $CHAR4.
        DT4_SEG_DESC      : $CHAR255. ;
RUN;
/*data &target.;*/
/*	set &target.;*/
/*	MODEL_TYPE=compress(MODEL_TYPE);*/
/*run;*/


proc sort data=&target. dupout=duplicates nodupkey; by &key_fields.; run;

proc sql noprint;
	select count(1) into :count_dups from duplicates;
quit;

%macro abort_process;
	%if &count_dups. NE 0 %then %do;
		%PUT;
		%PUT DUPLICATES IN SOURCE FILE. PLEASE FIX BEFORE PROCEEDING.;
		%PUT;
		%abort;
	%end;
%mend abort_process;
%abort_process;

data varnames;
length vars $ 20;
input vars $;
datalines;
DT4_SEG_KEY_VAL
DT4_SEG_DESC
EFF_FROM_YR_MTH
EFF_TO_YR_MTH
; 
run;

/********************************************************************************************************************/
/********************************************************************************************************************/

%DT4_ETL_SCD2_LOAD();

/********************************************************************************************************************/
/********************************************************************************************************************/

/*
DROP TABLE EDRTLRD3D.DT4_SEG_DIM IF EXISTS; COMMIT;

CREATE TABLE EDRTLRD3D.DT4_SEG_DIM (
		MODEL_TYPE VARCHAR(6 OCTETS) NOT NULL, 
		RRAP_SEG_NUM INTEGER NOT NULL, 
		DT4_SEG_KEY_VAL CHAR(4 OCTETS) NOT NULL, 
		DT4_SEG_DESC VARCHAR(255 OCTETS) NOT NULL, 
		EFF_FROM_YR_MTH CHAR(6 OCTETS) NOT NULL, 
		EFF_TO_YR_MTH CHAR(6 OCTETS) NOT NULL, 
		CRNT_F CHAR(1 OCTETS) NOT NULL, 
		INSRT_PROCESS_TMSTMP TIMESTAMP NOT NULL, 
		UPDT_PROCESS_TMSTMP TIMESTAMP,
		PRIMARY KEY (DT4_SEG_KEY_VAL) 
	)
	ORGANIZE BY COLUMN
	DATA CAPTURE NONE 
	DISTRIBUTE BY HASH (DT4_SEG_KEY_VAL); COMMIT;
CREATE UNIQUE INDEX MYINDEX
   ON EDRTLRD3D.DT4_SEG_DIM (DT4_SEG_KEY_VAL); COMMIT;

*/
