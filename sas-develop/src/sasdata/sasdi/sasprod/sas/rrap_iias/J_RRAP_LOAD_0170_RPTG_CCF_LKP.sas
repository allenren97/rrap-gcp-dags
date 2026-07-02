***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  RPTG_CCF_LKP
*  
*  Purpose: Load RPTG_CCF_LKP 
*
*  Frequency: Adhoc
*
*  Notes: 
*  		  
*
*	Change Log:
*	2023-01-10: Hadi Dimashkieh - Initial Development
*
*
*
***************************************************************************************************************************;

%rrap_dlgd_autoexec();


%let sourcefile=&owftp./rrap/lookup/CCF_Lookup_Basel_III.csv;




%let TARGET=RPTG_CCF_LKP;

%let key_fields = BASEL_PRD_TP_CD;
%let digest_fields = CCF;

%let surrogate_key_flag = N;
%let surrogate_key = ;
%let initial_surrogate_key_value = ;



DATA &TARGET.;
    LENGTH
        BASEL_PRD_TP_CD $ 20
        CCF   8
     ;

    INFILE "&sourcefile."
        LRECL=32767 firstobs=2
        ENCODING="LATIN1"
        TERMSTR=CRLF
        DLM=','
        MISSOVER
        DSD ;
    INPUT
        BASEL_PRD_TP_CD : $CHAR20.
        CCF : ?? COMMA8.
       ;
RUN;

DATA &TARGET.;
set &TARGET.;
BASEL_PRD_TP_CD=upcase(BASEL_PRD_TP_CD);
run;



data varnames;
length vars $ 28;
input vars $;
datalines;
CCF
EFF_FROM_YR_MTH
EFF_TO_YR_MTH
; 
run;

/********************************************************************************************************************/
/********************************************************************************************************************/

%DT4_ETL_SCD2_LOAD();

/********************************************************************************************************************/
/********************************************************************************************************************/


