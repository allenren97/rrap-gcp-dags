***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  RPTG_PRD_LKP_MOR
*  
*  Purpose: Load RPTG_PRD_LKP_MOR 
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

%let sourcefile=&owftp./rrm/rrm_edwext_mortgage_reporting_product_lookup_f_adhoc.csv;


%let TARGET=RPTG_PRD_LKP_MOR;

%let key_fields = SOURCE_SYSTEM_CODE BASEL_MORTGAGE_INSURER_GROUP_DES BULK_INDICATOR PRODUCT_ID;
/*%let digest_fields = DT4_RISK_RT_KEY_VAL, DT4_EXPSR_CL_KEY_VAL;*/
%let digest_fields = 
 BASEL_PRODUCT_CODE     			
,BASEL_PRODUCT_TYPE_CODE  		
,ASSET_CLASS_NUMBER      		
,ASSET_CLASS_DESCRIPTION     	      			
,PORTFOLIO_NAME        			
,PRODUCT_NAME           			
,SUB_PRODUCT_NAME       			
,BASEL_SUB_PRODUCT_NAME 			
,BCAR_SCHEDULE_NUMBER   			
,NCR_EXPOSURE_CLASS_KEY_VALUE 	
,CCAR_BASEL_PRODUCT_TYPE_NAME 	
,CCAR_SHORT_NAME      			
,CURRENCY_OF_ACCOUNT    			
,BASEL_PRODUCT        			
,BASEL_PROD_ABBR    				
,SECURITY_TYPE         			
,PD_BAND_EXPOSURE_CLASS_KEY_VALUE
,PD_BAND_EXPOSURE_CLASS_DESCRIPTI
,NCR_EXPOSURE_CLASS_DESCRIPTION 	
,DT4_RISK_RT_KEY_VAL 			
,DT4_EXPSR_CL_KEY_VAL 	
;

%let surrogate_key_flag = N;
%let surrogate_key = ;
%let initial_surrogate_key_value = ;

DATA &TARGET.;
    LENGTH
		SOURCE_SYSTEM_CODE 					  $ 7
		BASEL_MORTGAGE_INSURER_GROUP_DES 	  $ 39
		BULK_INDICATOR   					  $ 1
		BASEL_PRODUCT_CODE     				  $ 7
		BASEL_PRODUCT_TYPE_CODE  			  $ 10
		ASSET_CLASS_NUMBER      			  8 
		ASSET_CLASS_DESCRIPTION     		  $ 19
		PRODUCT_ID           				  $ 3
		PORTFOLIO_NAME        				  $ 8
		PRODUCT_NAME           				  $ 14
		SUB_PRODUCT_NAME       				  $ 24
		BASEL_SUB_PRODUCT_NAME 				  $ 46
		BCAR_SCHEDULE_NUMBER   				  8 
		NCR_EXPOSURE_CLASS_KEY_VALUE 		  $ 4
		CCAR_BASEL_PRODUCT_TYPE_NAME 		  $ 18
		CCAR_SHORT_NAME      				  $ 8
		CURRENCY_OF_ACCOUNT    				  $ 3
		BASEL_PRODUCT        				  $ 31
		BASEL_PROD_ABBR    					  $ 23
		SECURITY_TYPE         				  $ 9
		PD_BAND_EXPOSURE_CLASS_KEY_VALUE 	  $ 4
		PD_BAND_EXPOSURE_CLASS_DESCRIPTI 	  $ 9
		NCR_EXPOSURE_CLASS_DESCRIPTION 		  $ 23
		DT4_RISK_RT_KEY_VAL 				  $ 4
		DT4_EXPSR_CL_KEY_VAL 				  $ 4
;

    FORMAT
		SOURCE_SYSTEM_CODE 					 $7.
		BASEL_MORTGAGE_INSURER_GROUP_DES 	 $39.
		BULK_INDICATOR   					 $1.
		BASEL_PRODUCT_CODE     				 $7.
		BASEL_PRODUCT_TYPE_CODE  			 $10.
		ASSET_CLASS_NUMBER      			 8.
		ASSET_CLASS_DESCRIPTION     		 $19.
		PRODUCT_ID           				 $3.
		PORTFOLIO_NAME        				 $8.
		PRODUCT_NAME           				 $14.
		SUB_PRODUCT_NAME       				 $24.
		BASEL_SUB_PRODUCT_NAME 				 $46.
		BCAR_SCHEDULE_NUMBER   				 8.
		NCR_EXPOSURE_CLASS_KEY_VALUE 		 $4.
		CCAR_BASEL_PRODUCT_TYPE_NAME 		 $18.
		CCAR_SHORT_NAME      				 $8.
		CURRENCY_OF_ACCOUNT    				 $3.
		BASEL_PRODUCT        				 $31.
		BASEL_PROD_ABBR    					 $23.
		SECURITY_TYPE         				 $9.
		PD_BAND_EXPOSURE_CLASS_KEY_VALUE 	 $4.
		PD_BAND_EXPOSURE_CLASS_DESCRIPTI 	 $9.
		NCR_EXPOSURE_CLASS_DESCRIPTION 		 $23.
		DT4_RISK_RT_KEY_VAL 				 $4.
		DT4_EXPSR_CL_KEY_VAL 				 $4. 
;
    INFORMAT
		SOURCE_SYSTEM_CODE 					 $7.
		BASEL_MORTGAGE_INSURER_GROUP_DES 	 $39.
		BULK_INDICATOR   					 $1.
		BASEL_PRODUCT_CODE     				 $7.
		BASEL_PRODUCT_TYPE_CODE  			 $10.
		ASSET_CLASS_NUMBER      			 8.
		ASSET_CLASS_DESCRIPTION     		 $19.
		PRODUCT_ID           				 $3.
		PORTFOLIO_NAME        				 $8.
		PRODUCT_NAME           				 $14.
		SUB_PRODUCT_NAME       				 $24.
		BASEL_SUB_PRODUCT_NAME 				 $46.
		BCAR_SCHEDULE_NUMBER   				 8.
		NCR_EXPOSURE_CLASS_KEY_VALUE 		 $4.
		CCAR_BASEL_PRODUCT_TYPE_NAME 		 $18.
		CCAR_SHORT_NAME      				 $8.
		CURRENCY_OF_ACCOUNT    				 $3.
		BASEL_PRODUCT        				 $31.
		BASEL_PROD_ABBR    					 $23.
		SECURITY_TYPE         				 $9.
		PD_BAND_EXPOSURE_CLASS_KEY_VALUE 	 $4.
		PD_BAND_EXPOSURE_CLASS_DESCRIPTI 	 $9.
		NCR_EXPOSURE_CLASS_DESCRIPTION 		 $23.
		DT4_RISK_RT_KEY_VAL 				 $4.
		DT4_EXPSR_CL_KEY_VAL 				 $4. 
;
    INFILE "&sourcefile."
        LRECL=32767
        FIRSTOBS=2
        ENCODING="LATIN1"
        DLM='2c'x
        MISSOVER
        DSD ;
    INPUT
        SOURCE_SYSTEM_CODE 					: $7.
        BASEL_MORTGAGE_INSURER_GROUP_DES 	: $39.
        BULK_INDICATOR   					: $1.
        BASEL_PRODUCT_CODE     				: $7.
        BASEL_PRODUCT_TYPE_CODE  			: $10.
        ASSET_CLASS_NUMBER      			: 8.
        ASSET_CLASS_DESCRIPTION     		: $19.
        PRODUCT_ID           				: $3.
        PORTFOLIO_NAME        				: $8.
        PRODUCT_NAME           				: $14.
        SUB_PRODUCT_NAME       				: $24.
        BASEL_SUB_PRODUCT_NAME 				: $46.
        BCAR_SCHEDULE_NUMBER   				: 8.
        NCR_EXPOSURE_CLASS_KEY_VALUE 		: $4.
        CCAR_BASEL_PRODUCT_TYPE_NAME 		: $18.
        CCAR_SHORT_NAME      				: $8.
        CURRENCY_OF_ACCOUNT    				: $3.
        BASEL_PRODUCT        				: $31.
        BASEL_PROD_ABBR    					: $23.
        SECURITY_TYPE         				: $9.
        PD_BAND_EXPOSURE_CLASS_KEY_VALUE 	: $4.
        PD_BAND_EXPOSURE_CLASS_DESCRIPTI 	: $9.
        NCR_EXPOSURE_CLASS_DESCRIPTION 		: $23.
        DT4_RISK_RT_KEY_VAL 				: $4.
        DT4_EXPSR_CL_KEY_VAL 				: $4. 
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
BASEL_PRODUCT_CODE     			
BASEL_PRODUCT_TYPE_CODE  		
ASSET_CLASS_NUMBER      		
ASSET_CLASS_DESCRIPTION     	
PORTFOLIO_NAME        			
PRODUCT_NAME           			
SUB_PRODUCT_NAME       			
BASEL_SUB_PRODUCT_NAME 			
BCAR_SCHEDULE_NUMBER   			
NCR_EXPOSURE_CLASS_KEY_VALUE 	
CCAR_BASEL_PRODUCT_TYPE_NAME 	
CCAR_SHORT_NAME      			
CURRENCY_OF_ACCOUNT    			
BASEL_PRODUCT        			
BASEL_PROD_ABBR    				
SECURITY_TYPE         			
PD_BAND_EXPOSURE_CLASS_KEY_VALUE
PD_BAND_EXPOSURE_CLASS_DESCRIPTI
NCR_EXPOSURE_CLASS_DESCRIPTION 	
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

