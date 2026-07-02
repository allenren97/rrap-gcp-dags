


options mprint errorabend;

***************************************************************************************************************************;

%let etls_jobname = RRAP_MOR_SCRCD_50_LOAD_LTV_DATA_G_CMA.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS FRG_USER_DATA
*  Target Table:  LTV_VAR_CLUS_LTV_FINAL_CMA
*  
*  Purpose: Lookup CMA for each property address and calculate HPI index and LTV
*
*  Frequency: Monthly
*
*  Notes: Rewrite of the legacy RRAP_MOR_SCRCD_50_LOAD_LTV_DATA_G job. 
*  		  Changed to process monthly and leverage TERANET_ADDR_LKP table and dynamic lookup logic.
*
*	Change Log:
*	2022-11-04: Hadi Dimashkieh - Initial Development
*
***************************************************************************************************************************;

options mprint;
%rrap_mor_bns_autoexec;

%get_model_period_dates(product=mor);   

%put Start and End Dates for Mortgage Models:;

%put start_period_dt: &start_period_dt ;
%put end_period_dt: &end_period_dt;


data _null_;
	set NZUSER.tm_dim;
	where tm_lvl = 'Month' and tm_lvl_end_dt = "&end_period_dt."d;
	call symputx('mth_tm_id',put(tm_id,5.));
	call symputx('mth_end_dt',put(tm_lvl_end_dt,date9.));
	call symputx('mth_end_dt_nz',put(tm_lvl_end_dt,yymmdd10.));
	call symputx('mth_end_dt_1_nz',put(intnx('Month',tm_lvl_end_dt,-12,'e'),yymmdd10.));
	call symputx('yrmth',put(tm_lvl_end_dt,yymmn6.));
	call symputx('yrmth_prev',put(intnx('Month',tm_lvl_end_dt,-1,'e'),yymmn6.));
run;

%put &mth_tm_id. &mth_end_dt. &mth_end_dt_nz. &mth_end_dt_1_nz. &yrmth.;


proc sql;  
connect using NZUSER as nzcon;
create table SNAPSHOT_01 as select * from connection to nzcon( 
 SELECT date(process_date) AS process_date
		,mortgage_no
		,case when made_date is null or made_date > process_date then date(INT_ADJ_DATE)
			else date(made_date)
		 end as made_date
		,COALESCE(CURRENT_BAL,0) AS CURRENT_BAL
		,Int_Accr_Amt
		,yymth
		,date(INT_ADJ_DATE) AS INT_ADJ_DATE
		,total_suspense 
		,b.PRPTY_DESC_1
		,b.PRPTY_DESC_2
		,b.PRPTY_DESC_3
		,c.PROVINCE_CD as PROV
		,b.LEND_VAL2


FROM &FRG_DB..mortgage_hist b LEFT JOIN &FRG_DB..PROVINCE_REF c
on cast(b.PROVINCE as integer)=c.PROVINCE_ID
WHERE date(b.PROCESS_DATE) = %nrbquote('&mth_end_dt_nz.'));                     
quit;
                        
data SNAPSHOT;	
	set snapshot_01(keep=process_date mortgage_no PROV PRPTY_DESC_1 PRPTY_DESC_2 PRPTY_DESC_3);
    length PRPTY_DESC_11 PRPTY_DESC_22 PRPTY_DESC_33 $200.;         
/*	PRPTY_DESC_11 = STRIP(COMPBL(compress((translate(lowcase(PRPTY_DESC_1),"aaceeeeiiouu","àâçéèêëîïôùû,-")),,'kadst')));*/
/*	PRPTY_DESC_22 = STRIP(COMPBL(compress((translate(lowcase(PRPTY_DESC_2),"aaceeeeiiouu  ","àâçéèêëîïôùû,-")),,'kadst')));*/
/*	PRPTY_DESC_33 = STRIP(COMPBL(compress((translate(lowcase(PRPTY_DESC_3),"aaceeeeiiouu  ","àâçéèêëîïôùû,-")),,'kadst')));*/

	PRPTY_DESC_11 = STRIP(COMPBL(compress((translate(lowcase(PRPTY_DESC_1),"aaaceeeeiiouuuy    ","àâäçèéêëîïôùûüÿ,-'/")),,'kadst')));
	PRPTY_DESC_22 = STRIP(COMPBL(compress((translate(lowcase(PRPTY_DESC_2),"aaaceeeeiiouuuy    ","àâäçèéêëîïôùûüÿ,-'/")),,'kadst')));
	PRPTY_DESC_33 = STRIP(COMPBL(compress((translate(lowcase(PRPTY_DESC_3),"aaaceeeeiiouuuy    ","àâäçèéêëîïôùûüÿ,-'/")),,'kadst')));
run;                        
proc sort; by PROV mortgage_no; run;               

/********************************************         
PRPTY_LOCTN_NM: City name or Postal Code
LOCTN_LABEL_1:  Province 
LOCTN_LABEL_2:  CMA
*********************************************/        
data TERANET_ADDR_LKP;
	set nzrrap.TERANET_ADDR_LKP_CMA(where=( eff_to_yr_mth GE put("&mth_end_dt."d,yymmn6.) and eff_from_yr_mth LE put("&mth_end_dt."d,yymmn6.)));
	keep loctn_label_1 loctn_label_2 prpty_loctn_nm;
run;
proc sort data=TERANET_ADDR_LKP nodupkey; by PRPTY_LOCTN_NM LOCTN_LABEL_1; run;
         
data substrings (keep=PRPTY_LOCTN_NM PRPTY_LOCTN_NM2 PROV CMA SORT ) ;
	SET TERANET_ADDR_LKP;
	LENGTH PRPTY_LOCTN_NM2 $112.;
	PRPTY_LOCTN_NM = input(PRPTY_LOCTN_NM,$112.); 
/*	PRPTY_LOCTN_NM2 = STRIP(COMPBL(compress((translate(lowcase(PRPTY_LOCTN_NM),"aaceeeeiiouu  ","àâçéèêëîïôùû,-")),,'kadst')));*/
	PRPTY_LOCTN_NM2 = STRIP(COMPBL(compress((translate(lowcase(PRPTY_LOCTN_NM),"aaaceeeeiiouuuy    ","àâäçèéêëîïôùûüÿ,-'/")),,'kadst')));                      
	PROV = LOCTN_LABEL_1;
	CMA = LOCTN_LABEL_2 ;
	if length(PRPTY_LOCTN_NM)=3 then SORT = 1;
		else SORT = 2;
RUN;

proc sort data=substrings nodupkey; by PROV SORT PRPTY_LOCTN_NM; run; 


%macro provinces();

	 proc sql;
	 create table provinces as
	 select distinct PROV as prov from snapshot order by 1;
	 quit;

	 data _null_;
	     set provinces;
	     call symputx('prov'||trim(left(_n_)),prov);
	     call symputx('nobs',_n_);
	 run;

	%do i = 1 %to &nobs;

	data final_&&prov&i. ;
	 attrib PROV length=$2 PRPTY_LOCTN_NM PRPTY_LOCTN_NM2 length=$50 CMA length=$40;
	 if _n_=1 then do;
	     declare hash h(dataset: "SUBSTRINGS(where=(prov='&&prov&i.'))", multidata: 'y'); 
	     declare hiter iter('h');
	     h.defineKey("PROV");
	     h.defineData("CMA","PRPTY_LOCTN_NM","PRPTY_LOCTN_NM2");
	     h.defineDone();
	     call missing (PRPTY_LOCTN_NM, CMA); 
	 end;  
	 
	 set  SNAPSHOT(where=(PROV="&&prov&i."));

	     if 1 = 1 then do;
	         rc3 = iter.first();
	         do while (rc3 = 0);
	         	F3=-1;
	         		F3=FINDW(PRPTY_DESC_33,PRPTY_LOCTN_NM2,' ',"ir");
	         		if F3 > 0 then do;
	         			CMA_NEW=CMA; PROPERTY=PRPTY_LOCTN_NM;
	         			leave;
	         		end;
	         		rc3 = iter.next();
	         end;
	     end;
	     if F3 = 0 then do;
	         rc2 = iter.first();
	         do while (rc2 = 0);
	         	F2=-1;				
	         		F2=FINDW(PRPTY_DESC_22,PRPTY_LOCTN_NM2,' ',"ir");
	         		if F2 > 0 then do;
	         			CMA_NEW=CMA; PROPERTY=PRPTY_LOCTN_NM;
	         			leave;
	         		end;
	         	rc2 = iter.next();
	         end;
	     end;
	     if F2 = 0 and F3 = 0 then do;
	         rc1 = iter.first();
	         do while (rc1 = 0);
	         		F=-1;F1=-1;
	         		F1=FINDW(PRPTY_DESC_11,PRPTY_LOCTN_NM2,' ',"ir");
	         		if F1 > 0 then do;
	         			CMA_NEW=CMA; PROPERTY=PRPTY_LOCTN_NM;
	         			leave;
	         		end;
	         	rc1 = iter.next();
	         end;
	     end;
	     if F1 = 0 and F2 = 0 and F3 = 0 then do;
	         rc33 = iter.first();
	         do while (rc33 = 0);
	         	F33=-1;		
	         		F33=FIND(PRPTY_DESC_33,PRPTY_LOCTN_NM2,"it");
	         		if F33 > 0 then do;
	         			CMA_NEW=CMA; PROPERTY=PRPTY_LOCTN_NM;
	         			leave;
	         		end;
	         	rc33 = iter.next();
	         end;
	     end;

	     F=MAX(f1,f2,f3,f33);

	     if F = 0 then do;
	         PRPTY_LOCTN_NM=""; CMA=""; CMA_NEW=""; PRPTY_LOCTN_NM="";
	     end;

	 drop PRPTY_LOCTN_NM PRPTY_LOCTN_NM2 CMA rc1-rc3 rc33 f f1-f3 f33 PRPTY_DESC_11 PRPTY_DESC_22 PRPTY_DESC_33;
	 rename CMA_NEW = CMA;
	run;



	proc append base=SNAPSHOT_CMA data=final_&&prov&i. force; run;

	%end;
%mend provinces;

%provinces;


proc sql;
	create table SNAPSHOT_CMA_01 as
	select 
		 a.PROCESS_DATE
		,a.MORTGAGE_NO
		,b.CMA 
		,b.PROPERTY
		,intnx('Month',a.MADE_DATE,0,'e') format=date9. as MADE_DATE
		,a.CURRENT_BAL
		,a.INT_ACCR_AMT
		,a.YYMTH
		,a.INT_ADJ_DATE
		,a.TOTAL_SUSPENSE
		,a.PRPTY_DESC_1
		,a.PRPTY_DESC_2
		,a.PRPTY_DESC_3
		,a.PROV
		,a.LEND_VAL2
		,MAX((a.CURRENT_BAL + a.INT_ACCR_AMT) , -a.TOTAL_SUSPENSE) as TOTAL_BAL
	from SNAPSHOT_01 a , SNAPSHOT_CMA b
	where a.process_date=b.process_date and a.mortgage_no = b.mortgage_no
	order by a.process_date, a.mortgage_no;
quit;

proc sql;
	create table teranet_data as
		select a.mth_tm_id, t.tm_lvl_end_dt as month_end_dt, a.label_1, a.label_2, a.INDEX
			,case when p.HOUSE_INDEX_RTO = 0 then . else p.HOUSE_INDEX_RTO end as PROVNCL_INDEX
		from NZRRAP.TERANET_HOUSE_PRC_INDEX_CMA a 
			LEFT JOIN NZRRAP.PROVNCL_HOUSE_INDEX_SUM_CMA p 
			ON a.MTH_TM_ID = p.MTH_TM_ID AND a.LABEL_1 = (CASE WHEN p.PROV_CD='CO' THEN 'COMPOSITE' ELSE p.PROV_CD end )
			LEFT JOIN NZRRAP.tm_dim t
				on a.mth_tm_id = t.tm_id and t.tm_lvl='Month'
		order by label_1, label_2, mth_tm_id desc;
quit;

data teranet;
	set teranet_data;
	retain newindex newPROVNCL_INDEX;
	by label_1 label_2 descending mth_tm_id;
		if first.label_2 then do;
			newindex=index;
			newPROVNCL_INDEX=PROVNCL_INDEX;
		end;
		else do;
	 		if not missing(index) then newindex=index;
	 		if not missing(PROVNCL_INDEX) then newPROVNCL_INDEX=PROVNCL_INDEX;
		end;
run;
proc sort nodupkey; by mth_tm_id label_1 label_2; run;






proc sql;
	create table LTV_VAR_CLUS_LTV_FINAL as
	select a.MORTGAGE_NO, a.YYMTH, a.LEND_VAL2, a.CURRENT_BAL, a.INT_ACCR_AMT, a.TOTAL_SUSPENSE, a.CMA as PROP_CITY 
		,coalesce(b.newindex,/*c.newPROVNCL_INDEX,*/d.COMP11) as current_HPI
		,coalesce(b2.newindex,/*c2.newPROVNCL_INDEX,*/d2.COMP11) as madedate_HPI

		,a.LEND_VAL2 * (calculated current_HPI/calculated madedate_HPI) as index_teranetV

		,case when calculated index_teranetV NE 0 then a.TOTAL_BAL/(calculated index_teranetV)
			  else .
		 end as LTV
		,a.*
/*		,b.newindex as Rmonthly_HPI   ,c.newPROVNCL_INDEX as Pmonthly_HPI , d.COMP11 as Cmonthly_HPI*/
/*		,b2.newindex as RAppraisal_HPI ,c2.newPROVNCL_INDEX as PAppraisal_HPI , d2.COMP11 as CAppraisal_HPI*/
		,datetime() as INSRT_PROCESS_TMSTMP, datetime() as UPDT_PROCESS_TMSTMP

	from SNAPSHOT_CMA_01 as a 
		left join  TERANET as b
			on a.process_date=b.month_end_dt and a.CMA=b.LABEL_2 and a.PROV = b.LABEL_1
		left join  TERANET as b2
			on a.made_date=b2.month_end_dt and a.CMA=b2.LABEL_2 and a.PROV = b2.LABEL_1

		left join  (select distinct month_end_dt, label_1, newPROVNCL_INDEX from TERANET) as c
			on a.process_date=c.month_end_dt and a.PROV=c.LABEL_1
		left join  (select distinct month_end_dt, label_1, newPROVNCL_INDEX from TERANET) as c2
			on a.made_date=c2.month_end_dt and a.PROV=c2.LABEL_1

		left join  (select distinct month_end_dt, label_1, newINDEX as COMP11 from TERANET where LABEL_1='COMPOSITE' and LABEL_2='11') as d
			on a.process_date=d.month_end_dt 
		left join  (select distinct month_end_dt, label_1, newINDEX as COMP11 from TERANET where LABEL_1='COMPOSITE' and LABEL_2='11') as d2
			on a.made_date=d2.month_end_dt 
	;
quit;


proc sql;
connect using NZUSER as nzcon;
execute(delete from &FRG_DB..LTV_VAR_CLUS_LTV_FINAL_CMA where YYMTH = &yrmth.; commit;) by nzcon;
quit;


proc append base=NZUSER.LTV_VAR_CLUS_LTV_FINAL_CMA(BULKLOAD=YES BL_METHOD=CLILOAD) data=LTV_VAR_CLUS_LTV_FINAL force; run;
          
