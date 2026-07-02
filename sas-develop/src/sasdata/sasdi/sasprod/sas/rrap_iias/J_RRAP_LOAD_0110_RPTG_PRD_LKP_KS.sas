***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  RPTG_PRD_LKP_KS
*  
*  Purpose: Load RPTG_PRD_LKP_KS 
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

%let sourcefile=&owftp./rrm/rrm_edwext_reporting_product_lookup_f_adhoc.del;

%let TARGET=RPTG_PRD_LKP_KS;

%let key_fields = SRC_SYS_CD PRD_CD SUB_PRD_CD REVISED_EXPSR_OV_125K_F HELOC_F BASEL_PRD_CD PRD_ID;
%let digest_fields = 
 BASEL_PRD_TP_CD 		
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
,ACCT_CRNCY_CD 			
,RT_KEY_VAL 				
,BASEL_PRD_NM    		
,BASEL_PRD_ABR  			
,SCRTY_TP_CD 			
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
		SRC_SYS_CD       				 $ 10
		PRD_CD           				 $ 10
		SUB_PRD_CD       				 $ 10
		REVISED_EXPSR_OV_125K_F 		 $ 1
		HELOC_F          				 $ 1
		BASEL_PRD_CD     				 $ 10
		BASEL_PRD_TP_CD 				 $ 10
		ASST_CL_NUM 					 8 
		ASST_CL_DESC 					 $ 255
		PRD_ID    						 $ 10
		PORTFL_NM 						 $ 50
		PRD_NM  						 $ 50
		SUB_PRD_NM 						 $ 255
		BASEL_SUB_PRD_NM 				 $ 255
		BCAR_SCHED_NUM 					 $ 3
		NCR_EXPSR_CL_KEY_VAL 			 $ 4
		CCAR_BASEL_PRD_TP_NM 			 $ 50
		CCAR_SHT_NM 					 $ 4
		ACCT_CRNCY_CD 					 $ 10
		RT_KEY_VAL 						 $ 4
		BASEL_PRD_NM    				 $ 50
		BASEL_PRD_ABR  					 $ 255
		SCRTY_TP_CD 					 $ 50
		PD_BAND_EXPSR_CL_KEY_VAL 		 $ 4
		PD_BAND_EXPSR_CL_DESC 			 $ 255
		NCR_EXPSR_CL_DESC 				 $ 255
		DT4_RISK_RT_KEY_VAL 			 $ 4
		DT4_EXPSR_CL_KEY_VAL 			 $ 4 
;

    FORMAT
		SRC_SYS_CD       				 $10.
		PRD_CD           				 $10.
		SUB_PRD_CD       				 $10.
		REVISED_EXPSR_OV_125K_F 		 $1.
		HELOC_F          				 $1.
		BASEL_PRD_CD     				 $10.
		BASEL_PRD_TP_CD 				 $10.
		ASST_CL_NUM 					 8.
		ASST_CL_DESC 					 $255.
		PRD_ID    						 $10.
		PORTFL_NM 						 $50.
		PRD_NM  						 $50.
		SUB_PRD_NM 						 $255.
		BASEL_SUB_PRD_NM 				 $255.
		BCAR_SCHED_NUM 					 $3.
		NCR_EXPSR_CL_KEY_VAL 			 $4.
		CCAR_BASEL_PRD_TP_NM 			 $50.
		CCAR_SHT_NM 					 $4.
		ACCT_CRNCY_CD 					 $10.
		RT_KEY_VAL 						 $4.
		BASEL_PRD_NM    				 $50.
		BASEL_PRD_ABR  					 $255.
		SCRTY_TP_CD 					 $50.
		PD_BAND_EXPSR_CL_KEY_VAL 		 $4.
		PD_BAND_EXPSR_CL_DESC 			 $255.
		NCR_EXPSR_CL_DESC 				 $255.
		DT4_RISK_RT_KEY_VAL 			 $4.
		DT4_EXPSR_CL_KEY_VAL 			 $4. 
;
    INFORMAT
		SRC_SYS_CD       				 $10.
		PRD_CD           				 $10.
		SUB_PRD_CD       				 $10.
		REVISED_EXPSR_OV_125K_F 		 $1.
		HELOC_F          				 $1.
		BASEL_PRD_CD     				 $10.
		BASEL_PRD_TP_CD 				 $10.
		ASST_CL_NUM 					 8.
		ASST_CL_DESC 					 $255.
		PRD_ID    						 $10.
		PORTFL_NM 						 $50.
		PRD_NM  						 $50.
		SUB_PRD_NM 						 $255.
		BASEL_SUB_PRD_NM 				 $255.
		BCAR_SCHED_NUM 					 $3.
		NCR_EXPSR_CL_KEY_VAL 			 $4.
		CCAR_BASEL_PRD_TP_NM 			 $50.
		CCAR_SHT_NM 					 $4.
		ACCT_CRNCY_CD 					 $10.
		RT_KEY_VAL 						 $4.
		BASEL_PRD_NM    				 $50.
		BASEL_PRD_ABR  					 $255.
		SCRTY_TP_CD 					 $50.
		PD_BAND_EXPSR_CL_KEY_VAL 		 $4.
		PD_BAND_EXPSR_CL_DESC 			 $255.
		NCR_EXPSR_CL_DESC 				 $255.
		DT4_RISK_RT_KEY_VAL 			 $4.
		DT4_EXPSR_CL_KEY_VAL 			 $4. 
;
    INFILE "&sourcefile."
        LRECL=32767
        FIRSTOBS=2
        ENCODING="LATIN1"
        DLM='2c'x
        MISSOVER
        DSD ;
    INPUT
        SRC_SYS_CD       				: $10.
        PRD_CD           				: $10.
        SUB_PRD_CD       				: $10.
        REVISED_EXPSR_OV_125K_F 		: $1.
        HELOC_F          				: $1.
        BASEL_PRD_CD     				: $10.
        BASEL_PRD_TP_CD 				: $10.
        ASST_CL_NUM 					: 8.
        ASST_CL_DESC 					: $255.
        PRD_ID    						: $10.
        PORTFL_NM 						: $50.
        PRD_NM  						: $50.
        SUB_PRD_NM 						: $255.
        BASEL_SUB_PRD_NM 				: $255.
        BCAR_SCHED_NUM 					: $3.
        NCR_EXPSR_CL_KEY_VAL 			: $4.
        CCAR_BASEL_PRD_TP_NM 			: $50.
        CCAR_SHT_NM 					: $4.
        ACCT_CRNCY_CD 					: $10.
        RT_KEY_VAL 						: $4.
        BASEL_PRD_NM    				: $50.
        BASEL_PRD_ABR  					: $255.
        SCRTY_TP_CD 					: $50.
        PD_BAND_EXPSR_CL_KEY_VAL 		: $4.
        PD_BAND_EXPSR_CL_DESC 			: $255.
        NCR_EXPSR_CL_DESC 				: $255.
        DT4_RISK_RT_KEY_VAL 			: $4.
        DT4_EXPSR_CL_KEY_VAL 			: $4. 
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
ACCT_CRNCY_CD 				
RT_KEY_VAL 					
BASEL_PRD_NM    			
BASEL_PRD_ABR  				
SCRTY_TP_CD 				
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


