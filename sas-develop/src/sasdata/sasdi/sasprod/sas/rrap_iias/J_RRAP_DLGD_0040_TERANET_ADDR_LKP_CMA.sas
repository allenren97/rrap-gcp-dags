

***************************************************************************************************************************;
***************************************************************************************************************************;
*  Job Name : J_RRAP_DLGD_0040_TERANET_ADDR_LKP_CMA.sas
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  TERANET_ADDR_LKP_CMA  
*  
*  Purpose: Load TERANET_ADDR_LKP_CMA
*
*  Frequency: Monthly
*
*  Notes:  
*  		  
*
*	Change Log:
*
*   2016-XX-XX: Hadi Dimashkieh - Initial Development
*	2022-11-04: Hadi Dimashkieh - Load CMA version. Pick up standardized CMA values
*	2023-02-27: Hadi Dimashkieh - Update expiry logic and initial assignment of mth_tm_id as &mth_tm_id +40
*
***************************************************************************************************************************;


%rrap_dlgd_autoexec;


%let mth_tm_id=%eval(&mth_tm_id. +40);
%put MTH_TM_ID used for this job is MTH_TM_ID+40 = &mth_tm_id.;

%macro translate(dset);
	STRIP(COMPBL(compress((translate(lowcase(&dset),"aaaceeeeiiouuuy    ","àâäçèéêëîïôùûüÿ,-'/")),,'kadt')))
%mend translate;



proc sql noprint;
	select put(tm_lvl_end_dt,yymmn6.), put(intnx('Month',tm_lvl_end_dt,-1,'e'),yymmn6.)  into :yyyymm , :yrmth_prev
		from nzrrap.tm_dim
	where tm_lvl='Month' and tm_id = &mth_tm_id.;
quit;
%put yyyymm = &yyyymm.;
%put yrmth_prev = &yrmth_prev.;


proc sort data=nzrrap.metrpl_city_lkp_CMA out=cma_lkp(rename=(prov=LOCTN_LABEL_1 metrpl_area_nm=LOCTN_LABEL_2 city_nm=PRPTY_LOCTN_NM));
	by prov metrpl_area_nm city_nm;
	where "&yyyymm." GE EFF_FROM_YR_MTH and  "&yyyymm." LT  EFF_TO_YR_MTH;
run;

data newfile;
	set cma_lkp;
	prov=%translate(LOCTN_LABEL_1); cma_new=%translate(LOCTN_LABEL_2); city=%translate(PRPTY_LOCTN_NM);
/*	prov=LOCTN_LABEL_1; cma_new=LOCTN_LABEL_2; city=PRPTY_LOCTN_NM;*/

	LOCTN_LABEL_2_new=LOCTN_LABEL_2;
	keep prov cma_new city LOCTN_LABEL_1 LOCTN_LABEL_2 LOCTN_LABEL_2_new PRPTY_LOCTN_NM;
run;
proc sort data=newfile; by prov city; run;

data existing;
set NZRRAP.TERANET_ADDR_LKP_CMA (drop=INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP);
	where "&yyyymm." GE EFF_FROM_YR_MTH and  "&yyyymm." LT  EFF_TO_YR_MTH;
	prov=%translate(LOCTN_LABEL_1); cma_existing=%translate(LOCTN_LABEL_2); city=%translate(PRPTY_LOCTN_NM);
/*	prov=LOCTN_LABEL_1; cma_existing=LOCTN_LABEL_2; city=PRPTY_LOCTN_NM;*/
run;
proc sort data=existing; by prov city; run;






data new close stay;
	merge newfile(in=new) existing(in=exist);
	by prov city;
	if new and not exist then do;
		EFF_FROM_YR_MTH = "&yyyymm.";
		EFF_TO_YR_MTH = '999912';
		INSRT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
		UPDT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
		CRNT_F = 'Y';
		output new;
	end;

	if exist and new then do;
		IF cma_existing NE cma_new then do;

			EFF_TO_YR_MTH = "&yrmth_prev.";
			CRNT_F = 'N';
			output close;

			LOCTN_LABEL_2 = LOCTN_LABEL_2_new;
			EFF_FROM_YR_MTH = "&yyyymm.";
			EFF_TO_YR_MTH = '999912';
			INSRT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
			UPDT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
			CRNT_F = 'Y';
			output new;

		end;
		IF cma_existing = cma_new then do;
			output stay;
		end;
	end;
	drop LOCTN_LABEL_2_new prov cma_new cma_existing city;
run;


proc sql;
update NZRRAP.TERANET_ADDR_LKP_CMA AA

     set EFF_TO_YR_MTH = (
        select EFF_TO_YR_MTH
           from close
           where PRPTY_LOCTN_NM =  AA.PRPTY_LOCTN_NM
           and LOCTN_LABEL_1 = AA.LOCTN_LABEL_1
		   and LOCTN_LABEL_2 = AA.LOCTN_LABEL_2)
     where exists ( select 1
           from close
           where PRPTY_LOCTN_NM =  AA.PRPTY_LOCTN_NM
           and LOCTN_LABEL_1 = AA.LOCTN_LABEL_1
		   and LOCTN_LABEL_2 = AA.LOCTN_LABEL_2);

update NZRRAP.TERANET_ADDR_LKP_CMA AA
     set CRNT_F = 'N'
     where exists ( select 1
           from close
           where PRPTY_LOCTN_NM =  AA.PRPTY_LOCTN_NM
           and LOCTN_LABEL_1 = AA.LOCTN_LABEL_1
		   and LOCTN_LABEL_2 = AA.LOCTN_LABEL_2);

update NZRRAP.TERANET_ADDR_LKP_CMA AA
     set UPDT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt
     where exists ( select 1
           from close
           where PRPTY_LOCTN_NM =  AA.PRPTY_LOCTN_NM
           and LOCTN_LABEL_1 = AA.LOCTN_LABEL_1
		   and LOCTN_LABEL_2 = AA.LOCTN_LABEL_2);
quit;  
  

proc append base=NZRRAP.TERANET_ADDR_LKP_CMA(BULKLOAD=YES BL_METHOD=CLILOAD) data=new force; run;


