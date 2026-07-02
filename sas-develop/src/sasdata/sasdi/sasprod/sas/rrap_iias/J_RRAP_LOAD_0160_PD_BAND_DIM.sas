***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  PD_BAND_DIM
*  
*  Purpose: Load PD_BAND_DIM 
*
*  Frequency: Adhoc
*
*  Notes: 
*  		  
*
*	Change Log:
*	2023-01-26: Hadi Dimashkieh - Initial Development
*
*
*
***************************************************************************************************************************;


options mprint;
%rrap_dlgd_autoexec();


%let sourcefile=&owftp./rrap/lookup/rma_pd_band_f.csv;




%let TARGET=PD_BAND_DIM;

%let key_fields = NCR_PD_BAND_KEY_VAL NCR_EXPSR_CL_KEY_VAL TRANSACTOR_F CMHC_F;
%let digest_fields = 
PD_MIN_VAL
,PD_MAX_VAL
,PD_MIN_VAL_DESC
,PD_MAX_VAL_DESC
,PD_BAND_EXPSR_CL_DESC
,NCR_PD_BAND_DESC
,FRS_CD
,PD_BAND;

%let surrogate_key_flag = Y;
%let surrogate_key = NCR_PD_BAND_ID;
%let initial_surrogate_key_value = 17000;



DATA &TARGET.;
    LENGTH
        NCR_EXPSR_CL_KEY_VAL $ 4
        PD_MIN_VAL   8
        PD_MAX_VAL   8
        PD_MIN_VAL_DESC $ 10
        PD_MAX_VAL_DESC $ 10
        NCR_PD_BAND_KEY_VAL $ 4
        PD_BAND_EXPSR_CL_DESC $ 36
        NCR_PD_BAND_DESC $ 16
        FRS_CD         $ 2
        PD_BAND          $ 2
        TRANSACTOR_F  $ 1
        CMHC_F        $ 1 ;

    INFILE "&sourcefile."
        LRECL=32767 firstobs=2
        ENCODING="LATIN1"
        TERMSTR=CRLF
        DLM=','
        MISSOVER
        DSD ;
    INPUT
        NCR_EXPSR_CL_KEY_VAL : $CHAR4.
        PD_MIN_VAL : ?? COMMA8.
        PD_MAX_VAL : ?? COMMA8.
        PD_MIN_VAL_DESC : $CHAR10.
        PD_MAX_VAL_DESC : $CHAR10.
        NCR_PD_BAND_KEY_VAL : $CHAR4.
        PD_BAND_EXPSR_CL_DESC : $CHAR36.
        NCR_PD_BAND_DESC : $CHAR16.
        FRS_CD         : $CHAR2.
        PD_BAND          : $CHAR2.
        TRANSACTOR_F  : $CHAR1.
        CMHC_F        : $CHAR1. ;
RUN;


** variables shouldn't be longer than 28 chars;
data varnames;
length vars $ 32;
input vars $;
datalines;
PD_MIN_VAL
PD_MAX_VAL
PD_MIN_VAL_DESC
PD_MAX_VAL_DESC
PD_BAND_EXPSR_CL_DESC
NCR_PD_BAND_DESC
FRS_CD
PD_BAND
EFF_FROM_YR_MTH
EFF_TO_YR_MTH
; 
run;

/********************************************************************************************************************/
/********************************************************************************************************************/

%DT4_ETL_SCD2_LOAD();

/********************************************************************************************************************/
/********************************************************************************************************************/

