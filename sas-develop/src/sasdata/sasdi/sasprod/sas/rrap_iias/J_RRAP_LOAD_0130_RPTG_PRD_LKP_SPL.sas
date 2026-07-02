***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  RPTG_PRD_LKP_SPL
*  
*  Purpose: Load RPTG_PRD_LKP_SPL 
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

%let sourcefile=&owftp./rrm/rrm_edwext_personal_ln_reporting_product_lookup_f_adhoc.csv;

%let TARGET=RPTG_PRD_LKP_SPL;

%let key_fields = SRC_SYS_CD PRD_ID;
/*%let digest_fields = DT4_RISK_RT_KEY_VAL, DT4_EXPSR_CL_KEY_VAL;*/
%let digest_fields =
 PRD_CD  				
,BASEL_PRD_CD 			
,BASEL_PRD_TP_CD 		
,ASST_CL_NUM 			
,ASST_CL_DESC 			
,PORTFL_NM 				
,PRD_NM  				
,SUB_PRD_NM 				
,BASEL_SUB_PRD_NM 		
,BCAR_SCHED_NUM 			
,NCR_EXPSR_CL_KEY_VAL 	
,CCAR_BASEL_PRD_TP_NM 	
,CCAR_SHT_NM 			
,CRNCY_OF_ACCT 			
,NCR_RT_KEY_VAL 			
,BASEL_PRD    			
,BASEL_PRD_ABR  			
,SCRTY_TP 				
,PD_BAND_EXPSR_CL_KEY_VAL
,PD_BAND_EXPSR_CL_DESC 	
,NCR_EXPSR_CL_DESC 		
,DT4_RISK_RT_KEY_VAL 	
,DT4_EXPSR_CL_KEY_VAL 
;

%let surrogate_key_flag = N;
%let surrogate_key = ;
%let initial_surrogate_key_value = ;

DATA &TARGET.;
    LENGTH
		SRC_SYS_CD   				$ 10    		
		PRD_ID           			$ 10
		PRD_CD  					$ 60
		BASEL_PRD_CD 				$ 10
		BASEL_PRD_TP_CD 			$ 20
		ASST_CL_NUM 				8
		ASST_CL_DESC 				$ 255
		PORTFL_NM 					$ 50
		PRD_NM  					$ 50
		SUB_PRD_NM 					$ 255
		BASEL_SUB_PRD_NM 			$ 255
		BCAR_SCHED_NUM 			 	$ 3
		NCR_EXPSR_CL_KEY_VAL 		$ 4
		CCAR_BASEL_PRD_TP_NM 		$ 50
		CCAR_SHT_NM 				$ 10
		CRNCY_OF_ACCT 				$ 10
		NCR_RT_KEY_VAL 				$ 4
		BASEL_PRD    				$ 50
		BASEL_PRD_ABR  				$ 50
		SCRTY_TP 					$ 50
		PD_BAND_EXPSR_CL_KEY_VAL	$ 4
		PD_BAND_EXPSR_CL_DESC 		$ 255
		NCR_EXPSR_CL_DESC 			$ 255
		DT4_RISK_RT_KEY_VAL 	 	$ 4
		DT4_EXPSR_CL_KEY_VAL 		$ 4
;
/*    KEEP*/
/*        SRC_SYS_CD*/
/*        PRD_ID*/
/*        DT4_RISK_RT_KEY_VAL*/
/*        DT4_EXPSR_CL_KEY_VAL ;*/
/*    LABEL*/
/*        SRC_SYS_CD       = "Source System Code"*/
/*        PRD_ID           = "Product ID"*/
/*        DT4_RISK_RT_KEY_VAL = "DT4 Risk Rating Key Value"*/
/*        DT4_EXPSR_CL_KEY_VAL = "DT4 Exposure Class Key Value" ;*/
    FORMAT
		SRC_SYS_CD   				$10.    		
		PRD_ID           			$10.
		PRD_CD  					$60.
		BASEL_PRD_CD 				$10.
		BASEL_PRD_TP_CD 			$20.
		ASST_CL_NUM 				8.
		ASST_CL_DESC 				$255.
		PORTFL_NM 					$50.
		PRD_NM  					$50.
		SUB_PRD_NM 					$255.
		BASEL_SUB_PRD_NM 			$255.
		BCAR_SCHED_NUM 			 	$3.
		NCR_EXPSR_CL_KEY_VAL 		$4.
		CCAR_BASEL_PRD_TP_NM 		$50.
		CCAR_SHT_NM 				$10.
		CRNCY_OF_ACCT 				$10.
		NCR_RT_KEY_VAL 				$4.
		BASEL_PRD    				$50.
		BASEL_PRD_ABR  				$50.
		SCRTY_TP 					$50.
		PD_BAND_EXPSR_CL_KEY_VAL	$4.
		PD_BAND_EXPSR_CL_DESC 		$255.
		NCR_EXPSR_CL_DESC 			$255.
		DT4_RISK_RT_KEY_VAL 	 	$4.
		DT4_EXPSR_CL_KEY_VAL 		$4.
;
    INFORMAT
		SRC_SYS_CD   				$10.    		
		PRD_ID           			$10.
		PRD_CD  					$60.
		BASEL_PRD_CD 				$10.
		BASEL_PRD_TP_CD 			$20.
		ASST_CL_NUM 				8.
		ASST_CL_DESC 				$255.
		PORTFL_NM 					$50.
		PRD_NM  					$50.
		SUB_PRD_NM 					$255.
		BASEL_SUB_PRD_NM 			$255.
		BCAR_SCHED_NUM 			 	$3.
		NCR_EXPSR_CL_KEY_VAL 		$4.
		CCAR_BASEL_PRD_TP_NM 		$50.
		CCAR_SHT_NM 				$10.
		CRNCY_OF_ACCT 				$10.
		NCR_RT_KEY_VAL 				$4.
		BASEL_PRD    				$50.
		BASEL_PRD_ABR  				$50.
		SCRTY_TP 					$50.
		PD_BAND_EXPSR_CL_KEY_VAL	$4.
		PD_BAND_EXPSR_CL_DESC 		$255.
		NCR_EXPSR_CL_DESC 			$255.
		DT4_RISK_RT_KEY_VAL 	 	$4.
		DT4_EXPSR_CL_KEY_VAL 		$4.
;
    INFILE "&sourcefile."
        LRECL=32767
        FIRSTOBS=2
        ENCODING="LATIN1"
        DLM='2c'x
        MISSOVER
        DSD ;
    INPUT
        SRC_SYS_CD       			: $10.
        PRD_ID           			: $10.
        PRD_CD  					: $60.
        BASEL_PRD_CD 				: $10.
        BASEL_PRD_TP_CD 			: $20.
        ASST_CL_NUM 				: 8.
        ASST_CL_DESC 				: $255.
        PORTFL_NM 					: $50.
        PRD_NM  					: $50.
        SUB_PRD_NM 					: $255.
        BASEL_SUB_PRD_NM 			: $255.
        BCAR_SCHED_NUM 				: $3.
        NCR_EXPSR_CL_KEY_VAL 		: $4.
        CCAR_BASEL_PRD_TP_NM 		: $50.
        CCAR_SHT_NM 				: $10.
        CRNCY_OF_ACCT 				: $10.
        NCR_RT_KEY_VAL 				: $4.
        BASEL_PRD    				: $50.
        BASEL_PRD_ABR  				: $50.
        SCRTY_TP 					: $50.
        PD_BAND_EXPSR_CL_KEY_VAL 	: $4.
        PD_BAND_EXPSR_CL_DESC 		: $255.
        NCR_EXPSR_CL_DESC 			: $255.
        DT4_RISK_RT_KEY_VAL 		: $4.
        DT4_EXPSR_CL_KEY_VAL 		: $4.
;
RUN;

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
length vars $ 50;
input vars $;
datalines;
PRD_CD  				
BASEL_PRD_CD 			
BASEL_PRD_TP_CD 		
ASST_CL_NUM 			
ASST_CL_DESC 			
PORTFL_NM 				
PRD_NM  				
SUB_PRD_NM 				
BASEL_SUB_PRD_NM 		
BCAR_SCHED_NUM 			
NCR_EXPSR_CL_KEY_VAL 	
CCAR_BASEL_PRD_TP_NM 	
CCAR_SHT_NM 			
CRNCY_OF_ACCT 			
NCR_RT_KEY_VAL 			
BASEL_PRD    			
BASEL_PRD_ABR  			
SCRTY_TP 				
PD_BAND_EXPSR_CL_KEY_VAL
PD_BAND_EXPSR_CL_DESC 	
NCR_EXPSR_CL_DESC 		
DT4_RISK_RT_KEY_VAL 	
DT4_EXPSR_CL_KEY_VAL    
EFF_FROM_YR_MTH         
EFF_TO_YR_MTH           
; 
run;



/********************************************************************************************************************/
/********************************************************************************************************************/

%DT4_ETL_SCD2_LOAD();

/********************************************************************************************************************/
/********************************************************************************************************************/



