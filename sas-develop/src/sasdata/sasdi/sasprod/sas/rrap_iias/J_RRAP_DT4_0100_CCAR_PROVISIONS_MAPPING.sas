***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  CCAR_PROVISIONS_MAPPING
*  
*  Purpose: Load CCAR_PROVISIONS_MAPPING
*
*  Frequency: Quarter End runs
*
*  Notes:  
*  		  
*
*	Change Log:
*	2021-09-21: Hadi Dimashkieh - Initial Development
*
*
*
***************************************************************************************************************************;
 

%rrap_dt4_autoexec();


%let sourcefile=&owftp./CCAR_PROVISIONS_MAP.csv;



%let TARGET=CCAR_PROVISIONS_MAPPING;

%let key_fields = Provisions_Product Basel_Product_Type Security_Type_Desc;
%let digest_fields = 1,1;

%let surrogate_key_flag = N;
%let surrogate_key = ;
%let initial_surrogate_key_value = ;

PROC IMPORT DATAFILE="&sourcefile."
OUT=&TARGET.(rename=('Provisions Product'n=Provisions_Product 'Basel Product Type'n=Basel_Product_Type 'Security Type Desc'n=Security_Type_Desc))
DBMS=CSV
REPLACE;
GETNAMES=YES;
GUESSINGROWS=MAX;
RUN;


data varnames;
length vars $ 20;
input vars $;
datalines;
EFF_FROM_YR_MTH
EFF_TO_YR_MTH
; 
run;


/*data hadi;*/
/*set CCAR_PROVISIONS_MAPPING;*/
/*if _n_=1;*/
/*Provisions_Product='Hadi'; Basel_Product_Type='Hadi'; Security_Type_Desc='Hadi';*/
/*run;*/
/**/
/*proc append base=CCAR_PROVISIONS_MAPPING data=hadi; run;*/

/********************************************************************************************************************/
/********************************************************************************************************************/

%DT4_ETL_SCD2_LOAD();

/********************************************************************************************************************/
/********************************************************************************************************************/

