

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_RW_INSURER
*  
*  Purpose: Load DT4_RW_INSURER
*
*  Frequency: Quarter End runs
*
*  Notes:  
*  		  
*
*	Change Log:
*	2021-09-21: Hadi Dimashkieh - Initial Development
*   2023-01-13: Hadi Dimashkieh - Add 5 additional columns
*	2023-05-23: Hadi Dimashkieh - Removed end of line character specification.
*
***************************************************************************************************************************;



%rrap_dlgd_autoexec();

%let sourcefile=&owftp./rrap/lookup/RW_Insurer.csv;

%let TARGET=DT4_RW_INSURER;

%let key_fields = NAME;
%let digest_fields = RW_INSURER, IG_CODE, PD, LGD, RW_INSURER_A1, CORRELATION_PMI, MATURITY_ADJUSTMENT_PMI;

%let surrogate_key_flag = N;
%let surrogate_key = ;
%let initial_surrogate_key_value = ;

DATA &TARGET.;
    LENGTH
	    NAME				 $  6
        RW_INSURER   			8
        IG_CODE    			  $ 4
        PD			 			8
        LGD			 			8
        RW_INSURER_A1 			8
        CORRELATION_PMI 		8
        MATURITY_ADJUSTMENT_PMI 8
 ;

    INFILE "&sourcefile."
        LRECL=32767 firstobs=2
        ENCODING="LATIN1"
        /*TERMSTR=CRLF*/
        DLM=','
        MISSOVER
        DSD ;
    INPUT
	    NAME 					: $CHAR6.
        RW_INSURER   			: ?? COMMA12.
        IG_CODE    			    : $CHAR4.
        PD			 			: ?? COMMA12.
        LGD			 			: ?? COMMA12.
        RW_INSURER_A1 			: ?? COMMA12.
        CORRELATION_PMI 		: ?? COMMA12.
        MATURITY_ADJUSTMENT_PMI : ?? COMMA12.
;
RUN;


/********************************************************************************************************************/
proc sort data=&target. dupout=duplicates nodupkey; by &key_fields.; run;



proc sql noprint;
	select count(1) into :count_dups from duplicates;
quit;

%macro abort_process;
	%if &count_dups. NE 0 %then %do;
		%PUT;
		%PUT DUPLICATES IN SOURCE FILE. PLEASE FIX BEFORE PROCEEDING.;
		%PUT;
		%abort abend;
	%end;
%mend abort_process;
%abort_process;
/********************************************************************************************************************/

data varnames;
length vars $ 25;
input vars $;
datalines;
RW_INSURER
IG_CODE
PD
LGD
RW_INSURER_A1
CORRELATION_PMI
MATURITY_ADJUSTMENT_PMI
EFF_FROM_YR_MTH
EFF_TO_YR_MTH
; 
run;

/********************************************************************************************************************/
/********************************************************************************************************************/

%DT4_ETL_SCD2_LOAD();

/********************************************************************************************************************/
/********************************************************************************************************************/

