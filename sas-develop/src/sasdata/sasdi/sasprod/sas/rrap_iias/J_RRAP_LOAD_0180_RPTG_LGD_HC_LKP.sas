***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  RPTG_LGD_HC_LKP
*  
*  Purpose: Load RPTG_LGD_HC_LKP 
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



%let sourcefile=&owftp./rrap/lookup/rma_basel3_airb_lgd_formulaic_floors_haircut_lookup_f.csv;



%let TARGET=RPTG_LGD_HC_LKP;

%let key_fields = COLLATERAL_TYPE;
%let digest_fields = 
 H_C
,LGD_S
,LGD_U
,H_E;

%let surrogate_key_flag = N;
%let surrogate_key = ;
%let initial_surrogate_key_value = ;



DATA &TARGET.;
    LENGTH
        COLLATERAL_TYPE $ 20
        H_C   8
		LGD_S 8
		LGD_U 8
		H_E   8
     ;

    INFILE "&sourcefile."
        LRECL=32767 firstobs=2
        ENCODING="LATIN1"
        TERMSTR=CRLF
        DLM=','
        MISSOVER
        DSD ;
    INPUT
        COLLATERAL_TYPE : $CHAR20.
        H_C : ?? COMMA8.
        LGD_S : ?? COMMA8.
        LGD_U : ?? COMMA8.
        H_E : ?? COMMA8.
       ;
RUN;



data varnames;
length vars $ 20;
input vars $;
datalines;
H_C
LGD_S
LGD_U
H_E
EFF_FROM_YR_MTH
EFF_TO_YR_MTH
; 
run;

/********************************************************************************************************************/
/********************************************************************************************************************/

%DT4_ETL_SCD2_LOAD();

/********************************************************************************************************************/
/********************************************************************************************************************/

