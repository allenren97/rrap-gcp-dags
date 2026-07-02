
***************************************************************************************************************************;
***************************************************************************************************************************;
*  Job Name : J_RRAP_DLGD_0030_METRPL_CITY_LKP_CMA.sas
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  METRPL_CITY_LKP_CMA  
*  
*  Purpose: Load METRPL_CITY_LKP_CMA
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

%let file=rrm_edwext_CMA32_City_lookup_f_adhoc_CMA32;


%let mth_tm_id=%eval(&mth_tm_id. +40);
%put MTH_TM_ID used for this job is MTH_TM_ID+40 = &mth_tm_id.;


proc sql noprint;
	select put(tm_lvl_end_dt,yymmn6.), put(intnx('Month',tm_lvl_end_dt,-1,'e'),yymmn6.)  into :yyyymm , :yrmth_prev
		from nzrrap.tm_dim
	where tm_lvl='Month' and tm_id = &mth_tm_id.;


	select 
		case when count(*) = 0 then 'Y'
		else 'N' end as initial_load into :initial_load_f
	from nzrrap.METRPL_CITY_LKP_CMA;
quit;
%put initial_load_f = &initial_load_f;
%put yyyymm = &yyyymm.;
%put yrmth_prev = &yrmth_prev.;


DATA CMA_CITY_LKP1;
    LENGTH
        METRPL_AREA_NM1     $ 32
        CITY_NM             $ 64 ;

    INFILE "&outpath./rrm/&file..csv"
        LRECL=100
        ENCODING="LATIN1"
        TERMSTR=CRLF
        DLM=','
        MISSOVER
		FIRSTOBS=2
        DSD ;
    INPUT
        METRPL_AREA_NM1 : $CHAR32.
        CITY_NM         : $CHAR64. 
		PROV 			: $CHAR2.;

/*	If METRPL_AREA_NM1='Clagary' then METRPL_AREA_NM1='Calgary';*/

	retain METRPL_AREA_NM_ORIG;
	if not missing(METRPL_AREA_NM1) then METRPL_AREA_NM_ORIG=METRPL_AREA_NM1;

	format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP;
	INSRT_PROCESS_TMSTMP=datetime();
	UPDT_PROCESS_TMSTMP=datetime();
	CRNT_F = 'Y';
	drop METRPL_AREA_NM1;
RUN;
proc sort nodupkey; by METRPL_AREA_NM_ORIG CITY_NM; run;


data CMA_CITY_LKP;
attrib METRPL_AREA_NM length=$50.;
	if _n_=1 then do;
	     declare hash h(dataset: "NZRRAP.STANDARDIZED_CMA(rename=(SOURCE_CMA=METRPL_AREA_NM_ORIG STANDARDIZED_CMA=METRPL_AREA_NM))"); 
	     h.defineKey("METRPL_AREA_NM_ORIG");
	     h.defineData("METRPL_AREA_NM");
	     h.defineDone();
	     call missing (METRPL_AREA_NM); 
	 end;  
	set CMA_CITY_LKP1;

	if /*METRPL_AREA_NM_ORIG not in ('11','6') and*/ h.find(key:METRPL_AREA_NM_ORIG) ne 0 then do;
		put 'ERROR: Missing lookup value';
		put METRPL_AREA_NM_ORIG= 'does not exist in the STANDARDIZED_CMA table.';
		put 'Please add this value then rerun this job.';
		abort;
	end;

run;



data existing;
set NZRRAP.METRPL_CITY_LKP_CMA (drop=INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP);
	where "&yyyymm." GE EFF_FROM_YR_MTH and  "&yyyymm." LT  EFF_TO_YR_MTH;
run;
proc sort data=existing; by METRPL_AREA_NM CITY_NM; run;


data newfile;
	set CMA_CITY_LKP;
	keep CITY_NM METRPL_AREA_NM METRPL_AREA_NM_ORIG PROV;
run;
proc sort nodupkey; by METRPL_AREA_NM CITY_NM PROV; run;


%macro new_close_stay();

data new close stay;
	merge newfile(in=new) existing(in=exist);
	by METRPL_AREA_NM CITY_NM PROV;
	if new and not exist then do;
		%if &initial_load_f EQ Y %then %do;
			EFF_FROM_YR_MTH = '200001';
		%end;
		%else %do;
			EFF_FROM_YR_MTH = "&yyyymm.";
		%end;
		EFF_TO_YR_MTH = '999912';
		INSRT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
		UPDT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt;
		CRNT_F = 'Y';
		output new;
	end;
/*The CANSIM file should always include all CMA cities. Any cities no longer in a CMA will get expired.*/
	if exist and not new then do;
		EFF_TO_YR_MTH = "&yrmth_prev.";
		CRNT_F = 'N';
		output close;
	end;

	if exist and new then output stay;
run;
%mend new_close_stay;
%new_close_stay;



proc sql;
update NZRRAP.METRPL_CITY_LKP_CMA AA

     set EFF_TO_YR_MTH = (
        select EFF_TO_YR_MTH
           from close
           where METRPL_AREA_NM =  AA.METRPL_AREA_NM
           and CITY_NM = AA.CITY_NM
			and PROV = AA.PROV)
     where exists ( select 1
           from close
           where METRPL_AREA_NM =  AA.METRPL_AREA_NM
           and CITY_NM = AA.CITY_NM
			and PROV = AA.PROV  );

update NZRRAP.METRPL_CITY_LKP_CMA AA
     set CRNT_F = 'N'
     where exists ( select 1
           from close
           where METRPL_AREA_NM =  AA.METRPL_AREA_NM
           and CITY_NM = AA.CITY_NM
			and PROV = AA.PROV  );

update NZRRAP.METRPL_CITY_LKP_CMA AA
     set UPDT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME"dt
     where exists ( select 1
           from close
           where METRPL_AREA_NM =  AA.METRPL_AREA_NM
           and CITY_NM = AA.CITY_NM
			and PROV = AA.PROV  );
quit;  
  

proc append base=NZRRAP.METRPL_CITY_LKP_CMA(BULKLOAD=YES BL_METHOD=CLILOAD) data=new force; run;




/*
DROP TABLE TNGSTD3D.METRPL_CITY_LKP_CMA_CMA IF exists; COMMIT;

CREATE TABLE TNGSTD3D.METRPL_CITY_LKP_CMA_CMA (
		CITY_NM VARCHAR(64 OCTETS) NOT NULL, 
		METRPL_AREA_NM VARCHAR(32 OCTETS) NOT NULL, 
		METRPL_AREA_NM_ORIG VARCHAR(32 OCTETS) NOT NULL,
		PROV CHAR(2 OCTETS) NOT NULL, 
		EFF_FROM_YR_MTH CHAR(6 OCTETS) NOT NULL,
		EFF_TO_YR_MTH CHAR(6 OCTETS), 
		CRNT_F CHAR(1 OCTETS) NOT NULL, 
		INSRT_PROCESS_TMSTMP TIMESTAMP NOT NULL, 
		UPDT_PROCESS_TMSTMP TIMESTAMP
	)
	ORGANIZE BY ROW
	DATA CAPTURE NONE 
	DISTRIBUTE BY HASH (EFF_FROM_YR_MTH, METRPL_AREA_NM, INSRT_PROCESS_TMSTMP); COMMIT;
