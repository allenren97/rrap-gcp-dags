

options errorabend validvarname=ANY;

***************************************************************************************************************************;
***************************************************************************************************************************;
*  Job Name : rrap_ccar_pd_curve_extr.sas
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  BASEL_CCAR_PD_CURVE  
*  
*  Purpose: Load the PD Curve Table
*
*  Frequency: Monthly
*
*  Notes:  
*  		   
*
*	Change Log:
*
*   2022-12-04: Hadi Dimashkieh - Initial Development
*   2023-02-02: Hadi Dimashkieh - Refresh BASEL_NCR_HIERARCHY_LKP from DB2
*	2023-04-04: Hadi Dimashkieh - Addition of TOT_EXPSR_ABOVE_1500K_LMT_F
*	2023-05-12: Hadi Dimashkieh - Change creation of INSTR_FACT_0599
***************************************************************************************************************************;




%rrap_dlgd_autoexec;

data _null_;
	set nzrrap.tm_dim;
	where tm_id = &mth_tm_id.;
	call symputx('mth_end_dt',put(tm_lvl_end_dt,date9.));
	call symputx('mth_end_dt_nz',put(tm_lvl_end_dt,yymmdd10.));
	call symputx('mth_end_dt_prev',put(intnx('Month',tm_lvl_end_dt,-1,'e'),date9.));
	call symputx('yyyymmdd',put(tm_lvl_end_dt,yymmddn8.));
	call symputx('yyyymm',put(tm_lvl_end_dt,yymmn6.));
run;

%put &mth_end_dt. &yyyymmdd. &mth_end_dt_prev  &yyyymm.;


**************************************  REFRESH BASEL_NCR_HIERARCHY_LKP from DB2 ****************************************************;
proc sql;
connect using EDRRAPT as dbcon;
create table BASEL_NCR_HIERARCHY_LKP as select * from connection to dbcon(
select * from EDRRAPT.BASEL_NCR_HIERARCHY_LKP);
quit;


proc sql;
connect using nzuser as nzcon;
execute(drop table &EDRTLRFRGP1D..BASEL_NCR_HIERARCHY_LKP if exists; commit;) by nzcon;
quit;

proc append base=NZUSER.BASEL_NCR_HIERARCHY_LKP(bulkload=yes BL_METHOD=CLILOAD) data=BASEL_NCR_HIERARCHY_LKP force nowarn; run;
*************************************************************************************************************************************;

** Prep instrument fact table with 0599 PD Bands.;
proc sql;
connect using nzrrap as nzcon;
execute(DROP TABLE &EDRTLRFRGP1D..INSTR_FACT_0599 IF EXISTS; COMMIT;) by nzcon;
/*execute(
CREATE TABLE &EDRTLRFRGP1D..INSTR_FACT_0599 (
		BASEL_ACCT_ID INTEGER, 
		MTH_TM_ID INTEGER, 
		SRC_SYS_CD VARCHAR(10 OCTETS), 
		CCAR_BASEL_PRD_TP_NM VARCHAR(50 OCTETS), 
		TOT_EXPSR_ABOVE_1500K_LMT_F CHAR(1 OCTETS), 
		PD_FLRD_RPTG_RTO DOUBLE, 
		NCR_EXPSR_CL_KEY_VAL VARCHAR(4 OCTETS), 
		TRANSACTOR_FLAG_QRR VARCHAR(1 OCTETS), 
		CMHC_F VARCHAR(1 OCTETS), 
		PD_BAND_EXPSR_CL_KEY_VAL VARCHAR(4 OCTETS), 
		PD_BAND VARCHAR(2 OCTETS), 
		NCR_PD_BAND_KEY_VAL VARCHAR(4 OCTETS), 
		CCAR_F INTEGER, 
		PD_BASEL_SEG_NUM DECIMAL(11 , 0), 
		LGD_BASEL_SEG_NUM DECIMAL(11 , 0)
	)
	ORGANIZE BY COLUMN
	DATA CAPTURE NONE 
	DISTRIBUTE BY HASH (BASEL_ACCT_ID, MTH_TM_ID); COMMIT;) by nzcon;*/

execute( 
/*insert into &EDRTLRFRGP1D..INSTR_FACT_0599 */
create table &EDRTLRFRGP1D..INSTR_FACT_0599 as (
select 
	 a.BASEL_ACCT_ID
	,a.mth_tm_id
	,a.src_sys_cd
	,a.CCAR_BASEL_PRD_TP_NM
	,a.TOT_EXPSR_ABOVE_1500K_LMT_F
	,a.PD_FLRD_RPTG_RTO 

	,CASE WHEN a.TOT_EXPSR_ABOVE_1500K_LMT_F = 'Y' and a.ASST_CL_NUM <> 1 THEN '0599' ELSE a.NCR_EXPSR_CL_KEY_VAL END AS NCR_EXPSR_CL_KEY_VAL

	,a.TRANSACTOR_FLAG_QRR
	,a.CMHC_F
	,a.PD_BAND_EXPSR_CL_KEY_VAL 

	,COALESCE(c.pd_band,b.pd_band) AS pd_band
	,COALESCE(c.NCR_PD_BAND_KEY_VAL, b.NCR_PD_BAND_KEY_VAL) AS NCR_PD_BAND_KEY_VAL
	,a.CCAR_F
	,a.pd_basel_seg_num 
	,a.lgd_basel_seg_num 

	from &RRAP_DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT a 
		left join 
	&RRAP_DB..PD_BAND_DIM b
		on a.PD_BAND_EXPSR_CL_KEY_VAL = b.NCR_EXPSR_CL_KEY_VAL and a.PD_FLRD_RPTG_RTO between b.PD_MIN_VAL and b.PD_MAX_VAL
		and coalesce(upper(a.CMHC_F),'z') = coalesce(upper(b.CMHC_F),'z') and coalesce(upper(a.TRANSACTOR_FLAG_QRR),'z') = coalesce(upper(b.TRANSACTOR_F),'z')
		and  &yyyymm. between cast(b.eff_from_yr_mth AS integer) and cast(b.eff_to_yr_mth AS integer)
		
		left join 
	&RRAP_DB..PD_BAND_DIM c
		on '0599'  = c.NCR_EXPSR_CL_KEY_VAL AND a.TOT_EXPSR_ABOVE_1500K_LMT_F = 'Y' and a.ASST_CL_NUM <> 1 and a.PD_FLRD_RPTG_RTO between c.PD_MIN_VAL and c.PD_MAX_VAL
		and  &yyyymm. between cast(c.eff_from_yr_mth AS integer) and cast(c.eff_to_yr_mth AS integer)
WHERE a.mth_tm_id = &mth_tm_id. and a.ccar_f = 1) with data; COMMIT;) by nzcon;
quit;


** Get all combinations of CCAR_BASEL_PRD_TP_NM, PD_BAND, PD_MIN_VAL, PD_MAX_VAL from the data.;
proc sql;
connect using NZRRAP as nzcon;
create table combos as select * from connection to nzcon(

	
	WITH combos AS (
			SELECT DISTINCT a.CCAR_BASEL_PRD_TP_NM, a.NCR_EXPSR_CL_KEY_VAL, a.TRANSACTOR_FLAG_QRR AS TRANSACTOR_FLAG, a.CMHC_F, b.PD_BAND, b.NCR_PD_BAND_KEY_VAL
				,CASE WHEN a.NCR_EXPSR_CL_KEY_VAL = '0599' then 'Y' else '' end as TOT_EXPSR_ABOVE_1500K_LMT_F
			FROM
/*				&RRAP_DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT a ,*/
				&EDRTLRFRGP1D..INSTR_FACT_0599 a INNER JOIN
				&RRAP_DB..PD_BAND_DIM b

			ON (
				(COALESCE(a.CMHC_F,'z') = COALESCE(b.CMHC_F,'z') AND COALESCE(a.TRANSACTOR_FLAG_QRR,'z') = COALESCE(b.TRANSACTOR_F,'z') AND b.pd_band IS NOT NULL AND a.pd_band IS NOT NULL )
					OR
				(a.NCR_EXPSR_CL_KEY_VAL = '0599' AND  b.NCR_EXPSR_CL_KEY_VAL IN ('0599') AND b.pd_band IS NOT NULL)
					AND 
				(&yyyymm. BETWEEN cast(b.EFF_FROM_YR_MTH as integer) AND cast(b.EFF_TO_YR_MTH as integer)) AND b.pd_band IS NOT NULL 
				)

			WHERE
				a.MTH_TM_ID = &mth_tm_id. AND a.CCAR_F = 1  
				AND b.pd_band <> '' AND a.pd_band IS NOT NULL 
				and a.pd_basel_seg_num is not null and a.lgd_basel_seg_num is not null
			ORDER BY a.CCAR_BASEL_PRD_TP_NM, b.NCR_PD_BAND_KEY_VAL, a.TRANSACTOR_FLAG_QRR, a.CMHC_F , b.NCR_PD_BAND_KEY_VAL
	)



	SELECT  a.CCAR_BASEL_PRD_TP_NM,a.NCR_EXPSR_CL_KEY_VAL, a.TOT_EXPSR_ABOVE_1500K_LMT_F, a.TRANSACTOR_FLAG, a.CMHC_F, b.PD_BAND as pd_band1, b.PD_MIN_VAL, b.PD_MAX_VAL

	FROM COMBOS a 

		LEFT JOIN &EDRTLRFRGP1D..BASEL_NCR_HIERARCHY_LKP h
			ON a.NCR_EXPSR_CL_KEY_VAL = h.NCR_KEY_VAL AND h.NCR_PRNT_KEY_VAL IN ('0502', '0503', '0504', '0507')

		LEFT JOIN &RRAP_DB..PD_BAND_DIM b
			ON ((h.NCR_PRNT_KEY_VAL = b.NCR_EXPSR_CL_KEY_VAL AND a.NCR_PD_BAND_KEY_VAL = b.NCR_PD_BAND_KEY_VAL AND b.NCR_EXPSR_CL_KEY_VAL IN ('0502', '0503', '0504', '0507') 
					AND b.pd_band IS NOT NULL 
					AND COALESCE(a.CMHC_F,'z') = COALESCE(b.CMHC_F,'z') AND COALESCE(a.TRANSACTOR_FLAG,'z') = COALESCE(b.TRANSACTOR_F,'z')
				)
			OR
				(a.NCR_EXPSR_CL_KEY_VAL = '0599' AND a.NCR_PD_BAND_KEY_VAL = b.NCR_PD_BAND_KEY_VAL AND b.NCR_EXPSR_CL_KEY_VAL IN ('0599') AND b.pd_band IS NOT NULL))
			AND (&yyyymm. BETWEEN cast(b.EFF_FROM_YR_MTH as integer) AND cast(b.EFF_TO_YR_MTH as integer)) AND b.pd_band IS NOT NULL 

	WHERE b.NCR_PD_BAND_KEY_VAL IS NOT NULL 
	ORDER BY a.CCAR_BASEL_PRD_TP_NM, a.TRANSACTOR_FLAG, a.CMHC_F, a.TOT_EXPSR_ABOVE_1500K_LMT_F, b.NCR_PD_BAND_KEY_VAL

);
quit;

data combos;
	set combos;
	pd_band=input(pd_band1,2.); drop pd_band1;
run;

** Extract the actual PD_BANDs, their associated ranges, and the average of the PD Ratio for each CCAR_BASEL_PRD_TP_NM.;
proc sql;
connect using NZRRAP as nzcon;
create table pd_values as select * from connection to nzcon(

	SELECT a.CCAR_BASEL_PRD_TP_NM, a.TRANSACTOR_FLAG_QRR as TRANSACTOR_FLAG, a.cmhc_f
		,CASE WHEN a.NCR_EXPSR_CL_KEY_VAL = '0599' then 'Y' else '' end as TOT_EXPSR_ABOVE_1500K_LMT_F
		,cast(a.pd_band as integer) as PD_BAND, b.PD_MIN_VAL, b.PD_MAX_VAL, avg(a.PD_FLRD_RPTG_RTO) AS PD_VAL

/*	FROM &RRAP_DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT a*/
	FROM &EDRTLRFRGP1D..INSTR_FACT_0599 a

	LEFT JOIN &EDRTLRFRGP1D..BASEL_NCR_HIERARCHY_LKP h 
		ON a.NCR_EXPSR_CL_KEY_VAL = h.NCR_KEY_VAL and h.NCR_PRNT_KEY_VAL IN ('0502', '0503', '0504', '0507','0599')

	LEFT JOIN &RRAP_DB..PD_BAND_DIM b 
		ON b.NCR_EXPSR_CL_KEY_VAL = h.NCR_PRNT_KEY_VAL AND a.NCR_PD_BAND_KEY_VAL = b.NCR_PD_BAND_KEY_VAL 
		AND COALESCE(a.CMHC_F,'Z') = COALESCE(b.CMHC_F,'Z') AND COALESCE(a.TRANSACTOR_FLAG_QRR,'Z') = COALESCE(b.TRANSACTOR_F,'Z')
		AND (&yyyymm. BETWEEN cast(b.EFF_FROM_YR_MTH as integer) AND cast(b.EFF_TO_YR_MTH as integer)) AND b.pd_band IS NOT NULL 

	WHERE a.MTH_TM_ID = &mth_tm_id. AND a.ccar_f = 1
		and a.pd_band is not null 
		and pd_basel_seg_num is not null and lgd_basel_seg_num is not null
		
	
GROUP BY a.CCAR_BASEL_PRD_TP_NM, a.TRANSACTOR_FLAG_QRR, a.cmhc_f, 
			CASE WHEN a.NCR_EXPSR_CL_KEY_VAL = '0599' then 'Y' else '' end, cast(a.pd_band as integer) , b.PD_MIN_VAL, b.PD_MAX_VAL		
ORDER BY 1,5,2,3	

);
quit;



** Combine together to get the complete picture in one output.;
proc sql;
	create table BASEL_CCAR_PD_CURVE_EXTR as
	select 
		"&mth_end_dt."d format=date9. as MTH_END_DT
		,a.CCAR_BASEL_PRD_TP_NM
		,'DOM-BANK-ALONE' as LEGAL_ENTITY
		,a.CMHC_F
		,a.TRANSACTOR_FLAG
		,a.TOT_EXPSR_ABOVE_1500K_LMT_F
		,compress(put(a.pd_band,best3.)) as PD_BAND
		,coalesce(b.PD_MIN_VAL,a.PD_MIN_VAL) as PD_MIN_VAL
		,coalesce(b.PD_MAX_VAL,a.pd_MAX_VAL) as PD_MAX_VAL
		,coalesce(b.pd_VAL,0) as PD_VAL
		,"&SYSDATE9.:&SYSTIME"dt as INSRT_PROCESS_TMSTMP
	    ,"&SYSDATE9.:&SYSTIME"dt as UPDT_PROCESS_TMSTMP
	from 
	combos a left join pd_values b
	on a.CCAR_BASEL_PRD_TP_NM = b.CCAR_BASEL_PRD_TP_NM and a.pd_band=b.pd_band
	and coalesce(a.TRANSACTOR_FLAG,'Z')=coalesce(b.TRANSACTOR_FLAG,'Z') and coalesce(a.CMHC_F,'Z')=coalesce(b.CMHC_F,'Z')
	and coalesce(a.TOT_EXPSR_ABOVE_1500K_LMT_F,'Z')=coalesce(b.TOT_EXPSR_ABOVE_1500K_LMT_F,'Z')
	order by 1,2,3,a.TOT_EXPSR_ABOVE_1500K_LMT_F,a.TRANSACTOR_FLAG, a.PD_BAND;
quit;


******************************************************************************************************************************;


proc sql;
create table diff as 
	SELECT  distinct CCAR_BASEL_PRD_TP_NM  FROM NZRRAP.BASEL_CCAR_EXPSR_EXTR_ACAP where mth_end_dt = "&mth_end_dt."d   
		EXCEPT
	SELECT distinct CCAR_BASEL_PRD_TP_NM FROM BASEL_CCAR_PD_CURVE_EXTR   ;
quit;


data _null_;
	set diff end=last;
	if _n_=1 then do;
		put '****************************************************************************';
		put "The following CCAR_BASEL_PRD_TP_NM will be added from &mth_end_dt_prev.:";
	end;
	put CCAR_BASEL_PRD_TP_NM;
	if last then do;
		put '****************************************************************************';
	end;
run;


proc sql;

create table diff_add_pd_curve as 
	select 
		"&mth_end_dt."d format=date9. as MTH_END_DT
		,CCAR_BASEL_PRD_TP_NM
		,LEGAL_ENTITY
		,CMHC_F
		,TRANSACTOR_FLAG
		,TOT_EXPSR_ABOVE_1500K_LMT_F
		,PD_BAND
		,PD_MIN_VAL
		,PD_MAX_VAL
		,PD_VAL 
		,"&SYSDATE9.:&SYSTIME"dt as INSRT_PROCESS_TMSTMP
	    ,"&SYSDATE9.:&SYSTIME"dt as UPDT_PROCESS_TMSTMP
	from NZRRAP.BASEL_CCAR_PD_CURVE
	where mth_end_dt = "&mth_end_dt_prev."d and CCAR_BASEL_PRD_TP_NM in (select CCAR_BASEL_PRD_TP_NM from diff);
quit;

proc append base=BASEL_CCAR_PD_CURVE_EXTR data=diff_add_pd_curve force; run;

******************************************************************************************************************************;

proc sql;
connect using NZRRAP as nzcon;
execute(delete from &RRAP_DB..BASEL_CCAR_PD_CURVE where mth_end_dt = %nrbquote('&mth_end_dt_nz.')) by nzcon;
quit;

proc append base=NZRRAP.BASEL_CCAR_PD_CURVE(bulkload=yes BL_METHOD=CLILOAD) data=BASEL_CCAR_PD_CURVE_EXTR force nowarn; run;


/********************************************************************************************************************************************************
**************************************        GENERATE CSV FILE              ****************************************************************************
********************************************************************************************************************************************************/


libname DRAPT "&rrap_dir./data/rrap_iias/reporting";

%let reqvars=
	CCAR_BASEL_PRD_TP_NM
	TRANSACTOR_FLAG
	TOT_EXPSR_ABOVE_1500K_LMT_F
	PD_BAND
	PD_VAL
	PD_MIN_VAL
	PD_MAX_VAL
;


data BASEL_CCAR_PD_CURVE_EXTR_TMP0;
	retain  &reqvars.;
	set Basel_ccar_pd_curve_extr
		(keep= mth_end_dt &reqvars. 
		rename=(PD_VAL=pdval PD_MIN_VAL = PDMINVAL PD_MAX_VAL = PDMAXVAL)) nobs = lastobs;
	dt = compress(put(mth_end_dt,yymmdd10.),'-');
	rundt = compress(put(today(),yymmdd10.),'-');
	tm = put(time(),tod8.);
	PD_VAL =put(pdval*100,12.6)||"%";
	PD_MIN_VAL =put(pdMINval*100,12.6);
	PD_MAX_VAL =put(pdMAXval*100,12.6);
	call symput('YYYYMMDD',compress(dt));
	call symput('rundt',compress(rundt));
	call symput('nobs',_n_);
run;


%put pd_curve >> YYYYMMDD = &YYYYMMDD.;

proc sql noprint;
	select distinct
		sum(PDVAL) as hash_total_pd_Curve into :hashtot from BASEL_CCAR_PD_CURVE_EXTR_TMP0;
quit;


%put >>> pd_Curve hash_total = &hashtot.;
%put >>> pd_curve nobs = &nobs.;

DATA BASEL_CCAR_PD_CURVE_EXTR_TMP0;
	SET BASEL_CCAR_PD_CURVE_EXTR_TMP0;
	keep &reqvars.;
RUN;


DATA DRAPT.DR_AIRB_PD_CURVE_&YYYYMMDD._ctl;
	length 
		YYYYMMDD 
		rundt $10
		nobs $6
		hashtot $15 n1 8;
		file "&OUTPATH./cmf/outgoing/CCAR_ACAP/DR-AIRB-PD-CURVE-&YYYYMMDD..ctl" ; 


	YYYYMMDD = "&YYYYMMDD.,";
	rundt = "&rundt. ,";
	nobs = left(put(&nobs. ,6.));
	n1= 100*&hashtot.;
	hashtot = left(put(n1, 15.2));
	DLM=",";
	mtxtx=compress((rundt||nobs||DLM||hashtot),' ');
	put
		@1 YYYYMMDD
		@10 mtxtx
	;

	output;
run;


data DR_AIRB_PD_CURVE_&YYYYMMDD.;
	set BASEL_CCAR_PD_CURVE_EXTR_TMP0;
	rename
		CCAR_BASEL_PRD_TP_NM = 'Basel Product Type'n
		TRANSACTOR_FLAG = 'Transactor Flag'n
			PD_BAND = 'PD Band'n
		PD_VAL  = 'Pd Value'n
		PD_MIN_VAL = 'PD Min'n
		PD_MAX_VAL = 'PD Max'n
	;
	pd_bandn = input(PD_BAND,8.);
run;
proc sort out=DRAPT.DR_AIRB_PD_CURVE_&YYYYMMDD.(drop=pd_bandn); by 'Basel Product Type'n TOT_EXPSR_ABOVE_1500K_LMT_F 'Transactor Flag'n PD_BANDn; run;

proc export data = DRAPT.DR_AIRB_PD_CURVE_&YYYYMMDD. 

		outfile="&OUTPATH./cmf/outgoing/CCAR_ACAP/DR-AIRB-PD-CURVE-&YYYYMMDD..csv" 

dbms=csv replace; run;

DATA _NULL_;
	CALL SYSTEM("openssl dgst -sha256 &OUTPATH./cmf/outgoing/CCAR_ACAP/DR-AIRB-PD-CURVE-&YYYYMMDD..csv | sed -e 's/^.*= //g' >> &OUTPATH./cmf/outgoing/CCAR_ACAP/DR-AIRB-PD-CURVE-&YYYYMMDD._chk.ctl");
run;


