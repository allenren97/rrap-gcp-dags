

/*DECLARE THE PATH TO DEFINE AUTOCALL MACRO*/
/*INITIALIZATION OF VARIABLES AND EXECUTE AUTOCALL MACRO*/
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

%GLOBAL YEARMONTH;
%GLOBAL PRELOAD_DATA_COUNT;
%GLOBAL SRCCOUNT00;
%GLOBAL TGTCOUNT00;
%GLOBAL ALLBANK_COUNT;
%GLOBAL SUBSIDIARY_COUNT;
%LET INPUT00=EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT;
%LET TGT00=EDRRAPT.BASEL_CCAR_EXPSR_FACT;

proc sql NOPRINT;
delete from EDRRAPT.BASEL_CCAR_EXPSR_FACT  where  MTH_TM_ID= &MTH_TM_ID;
quit;

%macro rrap_db2yearmonth_initialize;
proc sql noprint;
create table _temp_yearmonth as 
select clndr_yr, tm_yr_seq_num, '00' as charmonth, '000000' as yearmonth from 
EDRTLRT.TM_DIM where TM_ID = &MTH_TM_ID;
quit;
/*Convert the integer values to char values as needed*/
data _temp_yearmonth;
set _temp_yearmonth;
if tm_yr_seq_num < 10 then do;
	charmonth='0'||put(tm_yr_seq_num,1.);
end;
else do;
charmonth=tm_yr_seq_num;
end;
	yearmonth=clndr_yr||charmonth;
	format yearmonth $char6.;
run;
/*Save the string YYYYMM for the latest MTH_TM_ID fetched from TM_DIM into a 
variable*/
proc sql noprint;
select yearmonth into :yearmonth from _temp_yearmonth;
quit;
%mend rrap_db2yearmonth_initialize;


/*CLEANUP MACRO*/
/*THIS MACRO DELETES ALL TEMPORARY SAS DATASETS LISTED BY THE USER AT THE SPECIFIED PATH ON AIX PLATFORM*/
/*LIBREF SHOULD BE THE LIBREF DECLARED FOR A UNIX PATH IN THE PROGRAM*/
/*DATASETS SHOULD BE THE LIST OF DATASETS SEPARATED BY A SINGLE SPACE*/
%MACRO SAS_DATASET_CLEANUP (LIBREF=, DATASETS=);
/*DELETE TEMPORARY DATASETS*/
PROC DATASETS LIBRARY=&LIBREF;
DELETE &DATASETS;
RUN;
QUIT;
%MEND;

/*Macro for fetching the yearmonth in an integer format*/
%rrap_db2yearmonth_initialize;
%PUT YEARMONTH IS &YEARMONTH;

/*Create the base table*/


%macro getRRAPreportingExtractFact(indata=, outdata=);
	proc sql noprint;
	DROP TABLE &outdata.;
	CREATE table &outdata. as 
		select 	a.ACCT_NUM,
				a.ADJUSTED_OS_BAL_AMT,
				a.CCAR_BASEL_PRD_TP_NM,
				a.CONS_DFT_MTH_CNT,
				a.EAD_BASEL_SEG_ID,
				(case when a.EAD_BASEL_SEG_NUM eq . then -1 else a.EAD_BASEL_SEG_NUM end ) as EAD_BASEL_SEG_NUM,
				a.EAD_FINAL_RPTG_RTO,
				a.LEGAL_ENTITY,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then LGD_BASEL_SEG_ID 
/*		when a.SRC_SYS_CD in ('SPL') and a.CONS_DFT_MTH_CNT >= 24 then .*/
/*	Changes by Khalid July 10, 2015 as per BSTM rev 13*/
/*SPL Accounts: (IF source system Code = 'SPL')*/
/*		IF 'LGD_BASEL_SEG_ID' IS NULL THEN -1*/
/*ELSE 'LGD_BASEL_SEG_ID'*/
					when a.SRC_SYS_CD in ('SPL')  and a.LGD_BASEL_SEG_ID eq . THEN -1
					else a.LGD_BASEL_SEG_ID end) as  LGD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') and a.LGD_BASEL_SEG_NUM eq . then -1
/*		when a.SRC_SYS_CD in ('SPL') and a.CONS_DFT_MTH_CNT >= 24 then -1*/
	/*	Changes by Khalid July 10, 2015 as per BSTM rev 13*/
		/*SPL Accounts: (IF source system Code = 'SPL')
		IF 'LGD_BASEL_SEG_NUM' IS NULL THEN -1
		ELSE 'LGD_BASEL_SEG_NUM'
	 */
					when a.SRC_SYS_CD in ('SPL') and a.LGD_BASEL_SEG_NUM eq . then -1
					else a.LGD_BASEL_SEG_NUM end) as LGD_BASEL_SEG_NUM,
				a.LGD_FINAL_RPTG_RTO,
				a.MTH_TM_ID,
				a.NCR_EXPSR_CL_KEY_VAL,
				a.PD_BAND,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.PD_BASEL_SEG_ID
	/*	Changes by Khalid July 10, 2015 as per BSTM rev 13*/
/*		when a.SRC_SYS_CD in ('SPL') then .*/
					else a.PD_BASEL_SEG_ID end) as PD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.PD_BASEL_SEG_NUM
	/*	Changes by Khalid July 10, 2015 as per BSTM rev 13*/
/*when a.SRC_SYS_CD in ('SPL') then .*/
					else a.PD_BASEL_SEG_NUM end) as PD_BASEL_SEG_NUM,
				a.PD_FINAL_RPTG_RTO,
				a.SRC_SYS_CD,
				a.UNADJUSTED_ADD_ON_BAL_AMT,
				a.BEFORE_ZERO_NET_UNDRAWN_AMT,
				a.AF_ZERO_NET_UNDRAWN_AMT,
				a.UNQ_ACCT_ID, A.SCRTY_TP_DESC ,A.BASEL_PRD_TP_CD,
				(case when A.SRC_SYS_CD = 'KS' AND A.INSUR_F='YES' THEN 'YES'
					when A.SRC_SYS_CD in ('MOR','TNG-MOR') AND A.SCRTY_TP_DESC='Insured' THEN 'YES' ELSE 'NO' end ) as INSURER_FLAG 

				,DLGD_RPTG_RTO as WGHTD_DLGD_RTO, DLGD_F, LTV_PERCENTAGE
/*Hadi*/
				
		from &lib..BASEL_ANALYTCL_BL_INSTRMNT_FACT as A
		where a.SRC_SYS_CD in ('TNG-MOR','KS','MOR','SPL') and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N'
/*THIS CONDITION IS REMOVED AS PART OF INTEGRATION TESTING DATED AUGUST 30 2014 AND BSTM v0.9*/
/*			and (ADJUSTED_OS_BAL_AMT <> 0 or UNDRAWN_AMT <> 0)*/  
			and a.PIT_STAT_CD in ('CUR','DEF')
			and a.TRNST_EXCLSN_F='N'
			and a.MTH_TM_ID = &MTH_TM_ID
			and DLGD_F='N';
	quit;
%mend;

/*STEP01 - Extract current month data;*/
%getRRAPreportingExtractFact(indata=&input00.,outdata=INPATH.RRAP_Rpt_Extract00);

/*STEP02 - Split extract by source system code;*/
data INPATH.RRAP_Rpt_Extract_KS
	INPATH.RRAP_Rpt_Extract_MOR
	INPATH.RRAP_Rpt_Extract_SPL
	INPATH.RRAP_Rpt_Extract_TNG;
	set INPATH.RRAP_Rpt_Extract00;
/*Create LGS Basel Segment Number equal to missing for MOR/SPL. This helps in grouping and aggregation. ;*/
/*This will later change to -1 at the final step;*/
/*	if SRC_SYS_CD in ('MOR','SPL') and CONS_DFT_MTH_CNT >= 24 then*/
/*	if SRC_SYS_CD in ('SPL') and CONS_DFT_MTH_CNT >= 24 then*/
/**/
/*		do;*/
/*			LGD_BASEL_SEG_NUM=.;*/
/*			LGD_BASEL_SEG_ID=.;*/
/*		end;*/
/*if SRC_SYS_CD in ('MOR','SPL') then do; PD_BASEL_SEG_NUM=.; PD_BASEL_SEG_ID=.; */
/*end;*/
	*** Split into 3 output files;
	if SRC_SYS_CD in ('KS') then output INPATH.RRAP_Rpt_Extract_KS;
	else if SRC_SYS_CD in ('MOR') then output INPATH.RRAP_Rpt_Extract_MOR;
	else if SRC_SYS_CD in ('SPL') then output INPATH.RRAP_Rpt_Extract_SPL;
	else if SRC_SYS_CD in ('TNG-MOR') then output INPATH.RRAP_Rpt_Extract_TNG;
	else delete;
run;

%macro getRRAPreportingExtract(SRC_SYS_CD=, indata=, NCRdata=, outdata=);
	/*Calculate first set of transformation rules using the base data.*/
	proc sql noprint;
		create table INPATH._temp_aggr_base as
			select 	PD_BAND,
				CCAR_BASEL_PRD_TP_NM,
				MTH_TM_ID,
				SRC_SYS_CD,
				LGD_BASEL_SEG_NUM,
				EAD_BASEL_SEG_NUM, 
				PD_BASEL_SEG_NUM, 
				PD_BASEL_SEG_ID, 
				LGD_BASEL_SEG_ID,
				EAD_BASEL_SEG_ID,
				INSURER_FLAG,
			case 
				when mean(PD_FINAL_RPTG_RTO) >=1 then 'T' 
				else 'F' 
			end 
		as PD_90_DAY_F 
			format $1.,
			%if &SRC_SYS_CD=KS or &SRC_SYS_CD=MOR or &SRC_SYS_CD=TNG %then

			%do;
				sum(ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
				sum
				(case 
					when ADJUSTED_OS_BAL_AMT <=0 then 0 
					else ADJUSTED_OS_BAL_AMT 
				end )
				as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
			%end;
%else %if &SRC_SYS_CD=SPL %then
	%do;
		sum(ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
		sum
		(case 
			when (ADJUSTED_OS_BAL_AMT) <=0 then 0 
			else (ADJUSTED_OS_BAL_AMT) 
		end)
		as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
	%end;

	case 
		when mean(PD_FINAL_RPTG_RTO) >=1 then max(WGHTD_DLGD_RTO) 
		else . 
	end 														
as EXPCTD_LOSS_RTO format 28.8,
	%if &SRC_SYS_CD=KS %then

		%do;
			sum(BEFORE_ZERO_NET_UNDRAWN_AMT) as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
			sum(AF_ZERO_NET_UNDRAWN_AMT) as AF_ZERO_NET_UNDRAWN_AMT format 17.3,
		%end;
%else %if &SRC_SYS_CD ne KS  %then
	%do;
		0 as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
		0 as AF_ZERO_NET_UNDRAWN_AMT format 17.3,
	%end;

	max(LGD_FINAL_RPTG_RTO) as LGD_FINAL_RPTG_RATIO format 28.8,
	%if &SRC_SYS_CD=KS %then

		%do;
			case 
				when mean(PD_FINAL_RPTG_RTO) >=1 then 0 
				else max(EAD_FINAL_RPTG_RTO) 
			end 
		as EAD_FINAL_RPTG_RATIO format 28.8
		%end;
%else
	%do;
		0 as EAD_FINAL_RPTG_RATIO format 28.8
	%end;

	,AVG(WGHTD_DLGD_RTO) as WGHTD_DLGD_RTO, LTV_PERCENTAGE, count(1) as OBLIGORS
		/*Hadi*/
	from &indata.
	where DLGD_F='N'
		group by PD_BAND, CCAR_BASEL_PRD_TP_NM, MTH_TM_ID, SRC_SYS_CD, 
			LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, PD_BASEL_SEG_NUM, 
			PD_BASEL_SEG_ID, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID ,INSURER_FLAG, LTV_PERCENTAGE
			/*hadi*/
	order by PD_BAND, CCAR_BASEL_PRD_TP_NM, MTH_TM_ID, SRC_SYS_CD, 
		LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, PD_BASEL_SEG_NUM, 
		PD_BASEL_SEG_ID, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID ,INSURER_FLAG, LTV_PERCENTAGE
		/*hadi*/
	;
	quit;





/*step 1 of last transformation*/
/*Get base NCR data - aggregate level irrespective of src_sys_code; */
/*So the source dataset is the primary dataset which ensures the same aggr_NCR_balance is available for all SRC_SYS_CD */
proc sql noprint;
create table INPATH._temp_aggr_NCR_balance as
select 	MTH_TM_ID,
NCR_EXPSR_CL_KEY_VAL,
/*INSURER_FLAG,*/
sum(
	case 	when (SRC_SYS_CD='KS' or SRC_SYS_CD='MOR') then ADJUSTED_OS_BAL_AMT
when SRC_SYS_CD='SPL' then ADJUSTED_OS_BAL_AMT
	end
)
as total_outstanding_balance,
sum(case when (SRC_SYS_CD='KS' or SRC_SYS_CD='MOR') THEN CASE WHEN ADJUSTED_OS_BAL_AMT < 0 then 0 ELSE ADJUSTED_OS_BAL_AMT END
 when SRC_SYS_CD='SPL' THEN CASE WHEN (ADJUSTED_OS_BAL_AMT) < 0 THEN 0 ELSE (ADJUSTED_OS_BAL_AMT) END
end
)
as total_outstanding_balance_net
from INPATH.RRAP_Rpt_Extract00
where PD_FINAL_RPTG_RTO >=1
and SRC_SYS_CD in ('KS','MOR','SPL')
group by MTH_TM_ID, NCR_EXPSR_CL_KEY_VAL 
/*,INSURER_FLAG*/
order by MTH_TM_ID, NCR_EXPSR_CL_KEY_VAL
;
quit;

/*step 2a of last transformation*/
/*Get NCR data - NCR Key Value level;*/
proc sql noprint;
create table INPATH._temp_dir_NCR as
select	distinct	
MTH_TM_ID, 
NCR_EXPSR_CL_KEY,
CR_LOSS_ALWNC_AMT
from &NCRdata. where MTH_TM_ID=&MTH_TM_ID
;
quit;

/*step 2b and 2c of last transformation rule*/
/*Get balance data - account level;*/
proc sql noprint;
create table INPATH._temp_acct_balance as

select 	MTH_TM_ID,
SRC_SYS_CD,
CCAR_BASEL_PRD_TP_NM, 	
PD_BAND,
PD_BASEL_SEG_NUM, 
EAD_BASEL_SEG_NUM, 
LGD_BASEL_SEG_NUM, 
ADJUSTED_OS_BAL_AMT,
UNADJUSTED_ADD_ON_BAL_AMT,
CONS_DFT_MTH_CNT,
NCR_EXPSR_CL_KEY_VAL,
ACCT_NUM,
INSURER_FLAG, LTV_PERCENTAGE,
case 
	when SRC_SYS_CD in ('KS','MOR') then ADJUSTED_OS_BAL_AMT
	when SRC_SYS_CD in ('SPL') then ADJUSTED_OS_BAL_AMT
	else 0
end as outstanding_balance, 
case 
when SRC_SYS_CD in ('KS','MOR') and ADJUSTED_OS_BAL_AMT>0 then  
ADJUSTED_OS_BAL_AMT
when SRC_SYS_CD in ('KS','MOR') and ADJUSTED_OS_BAL_AMT<=0 then 0
when SRC_SYS_CD in ('SPL') and 
(ADJUSTED_OS_BAL_AMT)>0 then 
(ADJUSTED_OS_BAL_AMT)
when SRC_SYS_CD in ('SPL') and 
(ADJUSTED_OS_BAL_AMT)<=0 then 0
	else 0
end as outstanding_balance_after_net 
from INPATH.RRAP_Rpt_Extract00
where PD_FINAL_RPTG_RTO >=1
order by MTH_TM_ID, SRC_SYS_CD,CCAR_BASEL_PRD_TP_NM, PD_BAND, 
PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM, NCR_EXPSR_CL_KEY_VAL, 
ACCT_NUM
;
quit;

/*step 3 of last transformation rule*/
/*Combine account level PRORATED_CR_LOSS_ALLOW_AMT;*/
proc sql noprint;
	create table INPATH._temp_acct_prorated_loss as
		select 	a.*, 
			b.total_outstanding_balance,
			b.total_outstanding_balance_net,
			c.CR_LOSS_ALWNC_AMT,
			(a.outstanding_balance/b.total_outstanding_balance)*c.CR_LOSS_ALWNC_AMT as 
			PRORATED_CR_LOSS_ALLOW_AMT,
			(a.outstanding_balance_after_net/b.total_outstanding_balance_net)*c.CR_LOSS_ALWNC_AMT 
		as PRORATED_CR_LOSS_ALLOW_AMT_AFTER 
			from INPATH._temp_acct_balance as a 
				left join INPATH._temp_aggr_NCR_balance as b 
					on a.MTH_TM_ID=b.MTH_TM_ID and a.NCR_EXPSR_CL_KEY_VAL=b.NCR_EXPSR_CL_KEY_VAL 
				left join INPATH._temp_dir_NCR as c
					on a.MTH_TM_ID=c.MTH_TM_ID and a.NCR_EXPSR_CL_KEY_VAL=c.NCR_EXPSR_CL_KEY 
				order by MTH_TM_ID, SRC_SYS_CD, CCAR_BASEL_PRD_TP_NM, PD_BAND,
					PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM, NCR_EXPSR_CL_KEY_VAL, 
					ACCT_NUM
	;
quit;

/*step 4 of last transformation rule*/
/*Calculate ACL at CCAR aggregated level;*/
proc sql noprint;
create table INPATH._temp_aggr_ACL as
select  MTH_TM_ID,
SRC_SYS_CD,
CCAR_BASEL_PRD_TP_NM,
PD_BAND,
PD_BASEL_SEG_NUM, 
EAD_BASEL_SEG_NUM, 
LGD_BASEL_SEG_NUM,INSURER_FLAG,LTV_PERCENTAGE,
sum(PRORATED_CR_LOSS_ALLOW_AMT) as ACL,
sum(PRORATED_CR_LOSS_ALLOW_AMT_AFTER) as After_Netting_ACL 
from INPATH._temp_acct_prorated_loss
group by MTH_TM_ID,SRC_SYS_CD, CCAR_BASEL_PRD_TP_NM, PD_BAND,INSURER_FLAG, LTV_PERCENTAGE,
PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM
order by MTH_TM_ID, SRC_SYS_CD, CCAR_BASEL_PRD_TP_NM, PD_BAND, 
PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM
;
quit;

/*Generate final output;*/
proc sql noprint;
create table &outdata. as
select 'DOM-BANK-BNS' as LEGAL_ENTITY format $40.,
a.PD_BAND,
a.CCAR_BASEL_PRD_TP_NM,
a.MTH_TM_ID,
a.SRC_SYS_CD,
COALESCE(a.LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM,
COALESCE(a.EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM,
a.INSURER_FLAG as INSUR_F,
a.PD_BASEL_SEG_NUM, 
a.PD_BASEL_SEG_ID, 
a.LGD_BASEL_SEG_ID,
a.EAD_BASEL_SEG_ID, 
a.PD_90_DAY_F,
a.BEFR_ZERO_NET_ADJ_OS_BAL_AMT,
a.AF_ZERO_NET_ADJUSTED_OS_BAL_AMT,
a.EXPCTD_LOSS_RTO,
a.BEFORE_ZERO_NET_UNDRAWN_AMT,
a.AF_ZERO_NET_UNDRAWN_AMT,
case when a.PD_90_DAY_F='T' then b.ACL else 0 end as BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
case when a.PD_90_DAY_F='T' then b.After_Netting_ACL else 0 end as 
AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
a.LGD_FINAL_RPTG_RATIO AS LGD_FINAL_RPTG_RTO,
a.EAD_FINAL_RPTG_RATIO AS EAD_FINAL_RPTG_RTO,
'CAD' as CRNCY_CD format $10.,
0 as PRTL_WRITE_OFF_AMT format 17.3,
'TRUE' as UNCONDTNLY_CNCLBL format $10.,
0 as ACCR_INTR_AMT format 17.3,
A.WGHTD_DLGD_RTO, a.LTV_PERCENTAGE, a.OBLIGORS,
/*Hadi*/
&SESSIONTIME AS INSRT_PROCESS_TMSTMP,
&SESSIONTIME AS UPDT_PROCESS_TMSTMP
from INPATH._temp_aggr_base as a 
left join INPATH._temp_aggr_ACL as b
	on a.MTH_TM_ID=b.MTH_TM_ID
	and a.SRC_SYS_CD=b.SRC_SYS_CD
	and a.CCAR_BASEL_PRD_TP_NM=b.CCAR_BASEL_PRD_TP_NM
	and a.PD_BAND=b.PD_BAND 
	and a.PD_BASEL_SEG_NUM=b.PD_BASEL_SEG_NUM
	and a.EAD_BASEL_SEG_NUM=b.EAD_BASEL_SEG_NUM
	and a.LGD_BASEL_SEG_NUM=b.LGD_BASEL_SEG_NUM
	and a.INSURER_FLAG=b.INSURER_FLAG
	and a.LTV_PERCENTAGE=b.LTV_PERCENTAGE
;
quit;
%mend getRRAPreportingExtract;
options mprint ;
/*STEP03 - Generate output datasets per SRC_SYS_CD;*/
%getRRAPreportingExtract(SRC_SYS_CD=KS,
	indata=INPATH.RRAP_Rpt_Extract_KS,
	NCRdata=INPATH.BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=INPATH.RRAP_Rpt_Extract_KS_Final);
%getRRAPreportingExtract(SRC_SYS_CD=MOR,
	indata=INPATH.RRAP_Rpt_Extract_MOR,
	NCRdata=INPATH.BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=INPATH.RRAP_Rpt_Extract_MOR_Final);
%getRRAPreportingExtract(SRC_SYS_CD=SPL,
	indata=INPATH.RRAP_Rpt_Extract_SPL,
	NCRdata=INPATH.BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=INPATH.RRAP_Rpt_Extract_SPL_Final);

/*STEP04 - Concatenate output files for KS MOR SPL;*/
%getRRAPreportingExtract(SRC_SYS_CD=TNG,
	indata=INPATH.RRAP_Rpt_Extract_TNG,
	NCRdata=INPATH.BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=INPATH.RRAP_Rpt_Extract_TNG_Final);


data INPATH.RRAP_Rpt_Extract_AllBank;
format legal_entity $40. crncy_cd $10. UNCONDTNLY_CNCLBL $10.;
	set INPATH.RRAP_Rpt_Extract_KS_Final
		INPATH.RRAP_Rpt_Extract_MOR_Final
		INPATH.RRAP_Rpt_Extract_SPL_Final
		INPATH.RRAP_Rpt_Extract_TNG_Final;
		drop src_sys_cd;
run;

proc sort data= &INPATH..RRAP_Rpt_Extract_AllBank nodupkey; 
by 
MTH_TM_ID LEGAL_ENTITY CCAR_BASEL_PRD_TP_NM PD_BAND LGD_BASEL_SEG_NUM EAD_BASEL_SEG_NUM INSUR_F LTV_PERCENTAGE; 
quit;


PROC SQL NOPRINT;
INSERT INTO EDRRAPT.BASEL_CCAR_EXPSR_FACT  (
LEGAL_ENTITY
,PD_BAND
,CCAR_BASEL_PRD_TP_NM
,MTH_TM_ID
,LGD_BASEL_SEG_NUM
,EAD_BASEL_SEG_NUM
,PD_BASEL_SEG_NUM
,PD_BASEL_SEG_ID
,LGD_BASEL_SEG_ID
,EAD_BASEL_SEG_ID
,PD_90_DAY_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FINAL_RPTG_RTO
,EAD_FINAL_RPTG_RTO
,CRNCY_CD
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,INSUR_F
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
,OBLIGORS)
SELECT 
legal_entity
,PD_BAND
,CCAR_BASEL_PRD_TP_NM
,MTH_TM_ID
,LGD_BASEL_SEG_NUM
,EAD_BASEL_SEG_NUM
,PD_BASEL_SEG_NUM
,PD_BASEL_SEG_ID
,LGD_BASEL_SEG_ID
,EAD_BASEL_SEG_ID
,PD_90_DAY_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FINAL_RPTG_RTO
,EAD_FINAL_RPTG_RTO
,crncy_cd
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,TRIM(INSUR_F)
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
,OBLIGORS
FROM &INPATH..RRAP_Rpt_Extract_AllBank;
QUIT;

/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/

%macro getRRAPreportingExtractFact_Sub(indata=, outdata=);
/*Applying initial filter conditions*/
proc sql noprint;
    CREATE table &outdata. as 
	select A.ACCT_NUM,
A.ADJUSTED_OS_BAL_AMT,
A.CCAR_BASEL_PRD_TP_NM,
A.CONS_DFT_MTH_CNT,
A.EAD_BASEL_SEG_ID,

(case
		when A.EAD_BASEL_SEG_NUM eq . then -1
		else A.EAD_BASEL_SEG_NUM end
) as EAD_BASEL_SEG_NUM,

A.EAD_FINAL_RPTG_RTO,
A.LEGAL_ENTITY,
A.LGD_BASEL_SEG_ID,
(case
		when A.LGD_BASEL_SEG_NUM eq . then -1
		else A.LGD_BASEL_SEG_NUM end
) as LGD_BASEL_SEG_NUM,

A.LGD_FINAL_RPTG_RTO,
A.MTH_TM_ID,
A.NCR_EXPSR_CL_KEY_VAL,
A.PD_BAND,
A.PD_BASEL_SEG_ID,
A.PD_BASEL_SEG_NUM,
A.PD_FINAL_RPTG_RTO,
A.SRC_SYS_CD,
A.UNADJUSTED_ADD_ON_BAL_AMT,
/*UNDRAWN_AMT,*/
A.UNQ_ACCT_ID,
(case 	when A.SCRTY_TP_DESC = 'Insured' THEN  'YES' ELSE 'NO' end ) as INSURER_FLAG 
,DLGD_RPTG_RTO as WGHTD_DLGD_RTO, DLGD_F, LTV_PERCENTAGE
/*Hadi*/
from &lib..BASEL_ANALYTCL_BL_INSTRMNT_FACT as A
where A.LEGAL_ENTITY in ('DOM-SUB-DUNDEE','DOM-SUB-MTCC','DOM-SUB-NT','DOM-SUB-MAPLE','DOM-SUB-SMC', 'DOM-SUB-TNG') 
	and A.SRC_SYS_CD in ('MOR', 'TNG-MOR') and A.CONSM_PRD_TREATMNT_CD='A' and A.SML_BUS_F='N'
/*THIS CONDITION IS REMOVED AS PART OF INTEGRATION TESTING DATED AUGUST 30 2014 AND BSTM v0.9*/
/*				and (ADJUSTED_OS_BAL_AMT <> 0 or UNDRAWN_AMT <> 0)*/
	and A.PIT_STAT_CD in ('CUR','DEF')
	and A.TRNST_EXCLSN_F='N'
	and A.MTH_TM_ID = &MTH_TM_ID;
quit;
%mend getRRAPreportingExtractFact_Sub;

/*STEP01 - Extract current month data;*/
%getRRAPreportingExtractFact_Sub(indata=&input00.,outdata=INPATH.RRAP_Rpt_Sub_Extract00);

/*Calculate first set of transformation rules using the base data.*/
%macro getRRAPreportingExtractSubs(indata=, ACLlookup=, outdata=);
/*Generate LGD Base Segment equal to blank for defaulters over 24 months;*/
data &indata;
set &indata;
/*if CONS_DFT_MTH_CNT >= 24 then do;*/
/*	LGD_BASEL_SEG_NUM=.;*/
/*    LGD_BASEL_SEG_ID=.;*/
/*end;*/
RUN;

/*Get base aggregated data;*/

proc sql noprint;
create table INPATH._temp_aggr_base as
select 	MTH_TM_ID,
		LEGAL_ENTITY,
		CCAR_BASEL_PRD_TP_NM,
		PD_BAND,
		PD_BASEL_SEG_NUM,
		PD_BASEL_SEG_ID,
		LGD_BASEL_SEG_NUM,
 		EAD_BASEL_SEG_NUM, 	
		LGD_BASEL_SEG_ID,
		EAD_BASEL_SEG_ID, 
		INSURER_FLAG,
	case 
			when mean(PD_FINAL_RPTG_RTO) >=1 then 'T'
			else 'F' 
		end as PD_90_DAY_F format $1.,
		sum (ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
		sum(
			case
				when ADJUSTED_OS_BAL_AMT <=0 then 0
				else ADJUSTED_OS_BAL_AMT
			end
			) as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
		max(LGD_FINAL_RPTG_RTO) as LGD_FINAL_RPTG_RATIO format 28.8,
		case 
			when mean(PD_FINAL_RPTG_RTO) >=1 then max(WGHTD_DLGD_RTO)
			else . 
		end as EXPCTD_LOSS_RTO format 28.8 


		,AVG(WGHTD_DLGD_RTO) as WGHTD_DLGD_RTO, LTV_PERCENTAGE, count(1) as OBLIGORS
		/*Hadi*/

from &indata.
where DLGD_F='N'
group by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, PD_BAND,
 		 LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID,PD_BASEL_SEG_NUM,
		PD_BASEL_SEG_ID,INSURER_FLAG, LTV_PERCENTAGE


order by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, PD_BAND, 
 		 LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID, LTV_PERCENTAGE

		 ;
quit;


/*step 1 of last transformation rule*/
/*Get total outstanding balances - aggregate level by LEGAL_ENTITY;*/
proc sql noprint;
create table INPATH._temp_aggr_balance as
select 	MTH_TM_ID,
		LEGAL_ENTITY,
		sum(ADJUSTED_OS_BAL_AMT) as total_os_balance,
		sum(
			case
				when ADJUSTED_OS_BAL_AMT <=0 then 0
				else ADJUSTED_OS_BAL_AMT
			end
			) as total_os_balance_after_net 
from &indata.
where PD_FINAL_RPTG_RTO >=1
group by MTH_TM_ID, LEGAL_ENTITY
order by MTH_TM_ID, LEGAL_ENTITY 
;
quit;

/*step 2a of last transformation rule*/
/*Get ACL Lookup data;*/
proc sql noprint;
    create table INPATH._temp_GL_ACL_balance as 
  	select &MTH_TM_ID as MTH_TM_ID,	
		LEGAL_ENTITY,
		EFF_FROM_YR_MTH,
		EFF_TO_YR_MTH,
		GENL_LEDGER_ENTITY_ACL_AMT
	from 	&ACLlookup.
where input(EFF_FROM_YR_MTH,6.) <= &yearmonth and &yearmonth <= 
input(EFF_TO_YR_MTH,6.) 
/*	and crnt_f='Y'*/
and LEGAL_ENTITY in ('DOM-SUB-DUNDEE','DOM-SUB-MTCC','DOM-SUB-NT',
'DOM-SUB-MAPLE','DOM-SUB-SMC');
quit;

/*step 2b and 2c of last transformation rule*/
/*Get Adjusted Outstanding Balance - account level;*/
proc sql noprint; 
create table INPATH._temp_acct_balance as
select 	MTH_TM_ID,
		LEGAL_ENTITY,
		CCAR_BASEL_PRD_TP_NM,
		PD_BAND,
		LGD_BASEL_SEG_NUM,
 		EAD_BASEL_SEG_NUM, 	
		ADJUSTED_OS_BAL_AMT,
		PD_FINAL_RPTG_RTO,
		case
			when ADJUSTED_OS_BAL_AMT>0 then ADJUSTED_OS_BAL_AMT
			when ADJUSTED_OS_BAL_AMT<=0 then 0
			else 0
			end as ADJUSTED_OS_BAL_AMT_after_net format 20.8, 
		CONS_DFT_MTH_CNT,
		ACCT_NUM, INSURER_FLAG, LTV_PERCENTAGE
from &indata.  
where PD_FINAL_RPTG_RTO >=1
order by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM,
		PD_BAND, LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, ACCT_NUM;
quit;

/*step 3 of last transformation rule*/
/*Combine account level data with ACL GL and total outstanding balances;*/
proc sql noprint;
create table INPATH._temp_acct_prorated_loss as
select 	a.*,
		b.total_os_balance,
		b.total_os_balance_after_net,
		c.GENL_LEDGER_ENTITY_ACL_AMT,
(a.ADJUSTED_OS_BAL_AMT/b.total_os_balance)*c.GENL_LEDGER_ENTITY_ACL_AMT as 
PRORATED_CR_LOSS_ALLOW_AMT,
(a.ADJUSTED_OS_BAL_AMT_after_net/b.total_os_balance_after_net)*c.GENL_LEDGER_ENTITY_ACL_AMT 
as PRORATED_CR_LOSS_ALLOW_AMT_after
from 	INPATH._temp_acct_balance as a 
left join INPATH._temp_aggr_balance as b on a.MTH_TM_ID=b.MTH_TM_ID and 
a.LEGAL_ENTITY=b.LEGAL_ENTITY 
left join INPATH._temp_GL_ACL_balance as c on a.MTH_TM_ID=c.MTH_TM_ID and 
a.LEGAL_ENTITY=c.LEGAL_ENTITY
order by MTH_TM_ID,LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM,
		PD_BAND, LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, ACCT_NUM;
quit;

/*step 4 of last transformation rule*/
/*Calculate ACL at CCAR aggregated level;*/
proc sql noprint;
create table INPATH._temp_aggr_ACL as
select  MTH_TM_ID,
		LEGAL_ENTITY,
 		CCAR_BASEL_PRD_TP_NM,
		PD_BAND,	
 		EAD_BASEL_SEG_NUM, 
 		LGD_BASEL_SEG_NUM, 
		INSURER_FLAG, LTV_PERCENTAGE,
		sum(PRORATED_CR_LOSS_ALLOW_AMT) as ACL,
		sum(PRORATED_CR_LOSS_ALLOW_AMT_after) as After_Netting_ACL 
from 	INPATH._temp_acct_prorated_loss
group by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, INSURER_FLAG,LTV_PERCENTAGE,
		PD_BAND, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM
order by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM,
		PD_BAND, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM
		;
quit;

/*Generate final output;*/
proc sql noprint; 
create table &outdata. as
select  
a.LEGAL_ENTITY,
a.PD_BAND,
a.CCAR_BASEL_PRD_TP_NM,
a.MTH_TM_ID,
COALESCE(a.LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM,
COALESCE(a.EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM,
a.PD_BASEL_SEG_NUM,
a.PD_BASEL_SEG_ID, 
a.LGD_BASEL_SEG_ID,
a.EAD_BASEL_SEG_ID,
a.PD_90_DAY_F,
a.INSURER_FLAG as INSUR_F,
a.BEFR_ZERO_NET_ADJ_OS_BAL_AMT,
a.AF_ZERO_NET_ADJUSTED_OS_BAL_AMT,
a.EXPCTD_LOSS_RTO,
0 as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
0 as AF_ZERO_NET_UNDRAWN_AMT format 17.3, 
case when a.PD_90_DAY_F='T' then b.ACL else 0 end as 
BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
case when a.PD_90_DAY_F='T' then b.After_Netting_ACL else 0 end as AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
a.LGD_FINAL_RPTG_RATIO as LGD_FINAL_RPTG_RTO,
0 as EAD_FINAL_RPTG_RTO format 28.8,
'CAD' as CRNCY_CD format $10.,
0 as PRTL_WRITE_OFF_AMT format 17.3,
'TRUE' as UNCONDTNLY_CNCLBL format $10.,
0 as ACCR_INTR_AMT format 17.3,
a.WGHTD_DLGD_RTO, a.LTV_PERCENTAGE, a.OBLIGORS,
/*Hadi*/
&SESSIONTIME AS INSRT_PROCESS_TMSTMP,
&SESSIONTIME AS UPDT_PROCESS_TMSTMP
from INPATH._temp_aggr_base as a 
left join INPATH._temp_aggr_ACL as b
on	a.MTH_TM_ID=b.MTH_TM_ID
	and a.LEGAL_ENTITY=b.LEGAL_ENTITY
	and a.CCAR_BASEL_PRD_TP_NM=b.CCAR_BASEL_PRD_TP_NM
	and a.PD_BAND=b.PD_BAND 
	and a.EAD_BASEL_SEG_NUM=b.EAD_BASEL_SEG_NUM
	and a.LGD_BASEL_SEG_NUM=b.LGD_BASEL_SEG_NUM
	and a.INSURER_FLAG=b.INSURER_FLAG
	and a.LTV_PERCENTAGE=b.LTV_PERCENTAGE
;
quit; 
%mend getRRAPreportingExtractSubs;
 
/*Generate output datasets;*/
%getRRAPreportingExtractSubs(indata=INPATH.RRAP_Rpt_Sub_Extract00,
							 ACLlookup=INPATH.BASEL_SUBSIDIARY_ACL_LKP, 
							 outdata=INPATH.RRAP_Rpt_Extract_Subsidiary);

/*PROC APPEND BASE=EDRRAPT.BASEL_CCAR_EXPSR_FACT */
/*DATA=INPATH.RRAP_Rpt_Extract_Subsidiary FORCE;*/
/*RUN;*/

PROC SQL NOPRINT;
INSERT INTO EDRRAPT.BASEL_CCAR_EXPSR_FACT  (
LEGAL_ENTITY
,PD_BAND
,CCAR_BASEL_PRD_TP_NM
,MTH_TM_ID
,LGD_BASEL_SEG_NUM
,EAD_BASEL_SEG_NUM
,PD_BASEL_SEG_NUM
,PD_BASEL_SEG_ID
,LGD_BASEL_SEG_ID
,EAD_BASEL_SEG_ID
,PD_90_DAY_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FINAL_RPTG_RTO
,EAD_FINAL_RPTG_RTO
,CRNCY_CD
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,INSUR_F
,WGHTD_DLGD_RTO,
LTV_PERCENTAGE
,OBLIGORS)
SELECT 
legal_entity
,PD_BAND
,CCAR_BASEL_PRD_TP_NM
,MTH_TM_ID
,LGD_BASEL_SEG_NUM
,EAD_BASEL_SEG_NUM
,PD_BASEL_SEG_NUM
,PD_BASEL_SEG_ID
,LGD_BASEL_SEG_ID
,EAD_BASEL_SEG_ID
,PD_90_DAY_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FINAL_RPTG_RTO
,EAD_FINAL_RPTG_RTO
,crncy_cd
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,TRIM(INSUR_F)
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
,OBLIGORS
FROM INPATH.RRAP_Rpt_Extract_Subsidiary;
QUIT;


/*UPDATE NULL VALUES TO 0 FOR TNG PRODUCTS*/
PROC SQL NOPRINT;
UPDATE EDRRAPT.BASEL_CCAR_EXPSR_FACT 
SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT IS NULL AND CCAR_BASEL_PRD_TP_NM LIKE '%TNG%'
AND MTH_TM_ID=&MTH_TM_ID;
UPDATE EDRRAPT.BASEL_CCAR_EXPSR_FACT 
SET AF_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE AF_ZERO_NET_ALWBL_CR_LOSS_AMT IS NULL AND CCAR_BASEL_PRD_TP_NM LIKE '%TNG%'
AND MTH_TM_ID=&MTH_TM_ID;
QUIT;

%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Extract00 RRAP_Rpt_Extract_KS RRAP_Rpt_Extract_MOR RRAP_Rpt_Extract_SPL _temp_aggr_base);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=_temp_aggr_NCR_balance _temp_dir_NCR _temp_acct_balance _temp_acct_prorated_loss _temp_aggr_ACL);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Extract_KS_Final RRAP_Rpt_Extract_MOR_Final RRAP_Rpt_Extract_SPL_Final RRAP_Rpt_Extract_AllBank);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Sub_Extract00 _temp_aggr_base _temp_aggr_balance _temp_GL_ACL_balance _temp_acct_balance);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=_temp_acct_prorated_loss _temp_aggr_ACL RRAP_Rpt_Extract_Subsidiary);


/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;
/****************** DLGD = Y *******************************************/;


/*DECLARE THE PATH TO DEFINE AUTOCALL MACRO*/
/*INITIALIZATION OF VARIABLES AND EXECUTE AUTOCALL MACRO*/
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);


%GLOBAL YEARMONTH;
%GLOBAL PRELOAD_DATA_COUNT;
%GLOBAL SRCCOUNT00;
%GLOBAL TGTCOUNT00;
%GLOBAL ALLBANK_COUNT;
%GLOBAL SUBSIDIARY_COUNT;
%LET INPUT00=EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT;
%LET TGT00=EDRRAPT.BASEL_CCAR_EXPSR_FACT;

/*proc sql NOPRINT;*/
/*delete from EDRRAPT.BASEL_CCAR_EXPSR_FACT  where  MTH_TM_ID= &MTH_TM_ID;*/
/*quit;*/

%macro rrap_db2yearmonth_initialize;
proc sql noprint;
create table _temp_yearmonth as 
select clndr_yr, tm_yr_seq_num, '00' as charmonth, '000000' as yearmonth from 
EDRTLRT.TM_DIM where TM_ID = &MTH_TM_ID;
quit;
/*Convert the integer values to char values as needed*/
data _temp_yearmonth;
set _temp_yearmonth;
if tm_yr_seq_num < 10 then do;
	charmonth='0'||put(tm_yr_seq_num,1.);
end;
else do;
charmonth=tm_yr_seq_num;
end;
	yearmonth=clndr_yr||charmonth;
	format yearmonth $char6.;
run;
/*Save the string YYYYMM for the latest MTH_TM_ID fetched from TM_DIM into a 
variable*/
proc sql noprint;
select yearmonth into :yearmonth from _temp_yearmonth;
quit;
%mend rrap_db2yearmonth_initialize;


/*CLEANUP MACRO*/
/*THIS MACRO DELETES ALL TEMPORARY SAS DATASETS LISTED BY THE USER AT THE SPECIFIED PATH ON AIX PLATFORM*/
/*LIBREF SHOULD BE THE LIBREF DECLARED FOR A UNIX PATH IN THE PROGRAM*/
/*DATASETS SHOULD BE THE LIST OF DATASETS SEPARATED BY A SINGLE SPACE*/
%MACRO SAS_DATASET_CLEANUP (LIBREF=, DATASETS=);
/*DELETE TEMPORARY DATASETS*/
PROC DATASETS LIBRARY=&LIBREF;
DELETE &DATASETS;
RUN;
QUIT;
%MEND;

/*Macro for fetching the yearmonth in an integer format*/
%rrap_db2yearmonth_initialize;
%PUT YEARMONTH IS &YEARMONTH;

/*Create the base table*/


%macro getRRAPreportingExtractFact(indata=, outdata=);
	proc sql noprint;
	DROP TABLE &outdata.;
	CREATE table &outdata. as 
		select 	a.ACCT_NUM,
				a.ADJUSTED_OS_BAL_AMT,
				a.CCAR_BASEL_PRD_TP_NM,
				a.CONS_DFT_MTH_CNT,
				a.EAD_BASEL_SEG_ID,
				(case when a.EAD_BASEL_SEG_NUM eq . then -1 else a.EAD_BASEL_SEG_NUM end ) as EAD_BASEL_SEG_NUM,
				a.EAD_FINAL_RPTG_RTO,
				a.LEGAL_ENTITY,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then LGD_BASEL_SEG_ID 
/*		when a.SRC_SYS_CD in ('SPL') and a.CONS_DFT_MTH_CNT >= 24 then .*/
/*	Changes by Khalid July 10, 2015 as per BSTM rev 13*/
/*SPL Accounts: (IF source system Code = 'SPL')*/
/*		IF 'LGD_BASEL_SEG_ID' IS NULL THEN -1*/
/*ELSE 'LGD_BASEL_SEG_ID'*/
					when a.SRC_SYS_CD in ('SPL')  and a.LGD_BASEL_SEG_ID eq . THEN -1
					else a.LGD_BASEL_SEG_ID end) as  LGD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') and a.LGD_BASEL_SEG_NUM eq . then -1
/*		when a.SRC_SYS_CD in ('SPL') and a.CONS_DFT_MTH_CNT >= 24 then -1*/
	/*	Changes by Khalid July 10, 2015 as per BSTM rev 13*/
		/*SPL Accounts: (IF source system Code = 'SPL')
		IF 'LGD_BASEL_SEG_NUM' IS NULL THEN -1
		ELSE 'LGD_BASEL_SEG_NUM'
	 */
					when a.SRC_SYS_CD in ('SPL') and a.LGD_BASEL_SEG_NUM eq . then -1
					else a.LGD_BASEL_SEG_NUM end) as LGD_BASEL_SEG_NUM,
				a.LGD_FINAL_RPTG_RTO,
				a.MTH_TM_ID,
				a.NCR_EXPSR_CL_KEY_VAL,
				a.PD_BAND,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.PD_BASEL_SEG_ID
	/*	Changes by Khalid July 10, 2015 as per BSTM rev 13*/
/*		when a.SRC_SYS_CD in ('SPL') then .*/
					else a.PD_BASEL_SEG_ID end) as PD_BASEL_SEG_ID,
				(case
					when a.SRC_SYS_CD in ('TNG-MOR','KS','MOR') then a.PD_BASEL_SEG_NUM
	/*	Changes by Khalid July 10, 2015 as per BSTM rev 13*/
/*when a.SRC_SYS_CD in ('SPL') then .*/
					else a.PD_BASEL_SEG_NUM end) as PD_BASEL_SEG_NUM,
				a.PD_FINAL_RPTG_RTO,
				a.SRC_SYS_CD,
				a.UNADJUSTED_ADD_ON_BAL_AMT,
				a.BEFORE_ZERO_NET_UNDRAWN_AMT,
				a.AF_ZERO_NET_UNDRAWN_AMT,
				a.UNQ_ACCT_ID, A.SCRTY_TP_DESC ,A.BASEL_PRD_TP_CD,
				(case when A.SRC_SYS_CD = 'KS' AND A.INSUR_F='YES' THEN 'YES'
					when A.SRC_SYS_CD in ('MOR','TNG-MOR') AND A.SCRTY_TP_DESC='Insured' THEN 'YES' ELSE 'NO' end ) as INSURER_FLAG 

				,DLGD_RPTG_RTO as WGHTD_DLGD_RTO, DLGD_F, LTV_PERCENTAGE
/*Hadi*/
				
		from &lib..BASEL_ANALYTCL_BL_INSTRMNT_FACT as A
		where a.SRC_SYS_CD in ('TNG-MOR','KS','MOR','SPL') and a.CONSM_PRD_TREATMNT_CD='A' and a.SML_BUS_F='N'
/*THIS CONDITION IS REMOVED AS PART OF INTEGRATION TESTING DATED AUGUST 30 2014 AND BSTM v0.9*/
/*			and (ADJUSTED_OS_BAL_AMT <> 0 or UNDRAWN_AMT <> 0)*/  
			and a.PIT_STAT_CD in ('CUR','DEF')
			and a.TRNST_EXCLSN_F='N'
			and a.MTH_TM_ID = &MTH_TM_ID
			and DLGD_F='Y';
	quit;
%mend;

/*STEP01 - Extract current month data;*/
%getRRAPreportingExtractFact(indata=&input00.,outdata=INPATH.RRAP_Rpt_Extract00);

/*STEP02 - Split extract by source system code;*/
data INPATH.RRAP_Rpt_Extract_KS
	INPATH.RRAP_Rpt_Extract_MOR
	INPATH.RRAP_Rpt_Extract_SPL
	INPATH.RRAP_Rpt_Extract_TNG;
	set INPATH.RRAP_Rpt_Extract00;
/*Create LGS Basel Segment Number equal to missing for MOR/SPL. This helps in grouping and aggregation. ;*/
/*This will later change to -1 at the final step;*/
/*	if SRC_SYS_CD in ('MOR','SPL') and CONS_DFT_MTH_CNT >= 24 then*/
/*	if SRC_SYS_CD in ('SPL') and CONS_DFT_MTH_CNT >= 24 then*/
/**/
/*		do;*/
/*			LGD_BASEL_SEG_NUM=.;*/
/*			LGD_BASEL_SEG_ID=.;*/
/*		end;*/
/*if SRC_SYS_CD in ('MOR','SPL') then do; PD_BASEL_SEG_NUM=.; PD_BASEL_SEG_ID=.; */
/*end;*/
	*** Split into 3 output files;
	if SRC_SYS_CD in ('KS') then output INPATH.RRAP_Rpt_Extract_KS;
	else if SRC_SYS_CD in ('MOR') then output INPATH.RRAP_Rpt_Extract_MOR;
	else if SRC_SYS_CD in ('SPL') then output INPATH.RRAP_Rpt_Extract_SPL;
	else if SRC_SYS_CD in ('TNG-MOR') then output INPATH.RRAP_Rpt_Extract_TNG;
	else delete;
run;

%macro getRRAPreportingExtract(SRC_SYS_CD=, indata=, NCRdata=, outdata=);
	/*Calculate first set of transformation rules using the base data.*/



	/*DLGD RECORDS USE DIFFERENT AGGREGATION.*/
	proc sql noprint;
		create table INPATH._temp_aggr_base as
			select 	PD_BAND,
				CCAR_BASEL_PRD_TP_NM,
				MTH_TM_ID,
				SRC_SYS_CD,
				LGD_BASEL_SEG_NUM,
				EAD_BASEL_SEG_NUM, 
				PD_BASEL_SEG_NUM, 
				PD_BASEL_SEG_ID, 
				LGD_BASEL_SEG_ID,
				EAD_BASEL_SEG_ID,
				INSURER_FLAG,
			case 
				when mean(PD_FINAL_RPTG_RTO) >=1 then 'T' 
				else 'F' 
			end 
		as PD_90_DAY_F 
			format $1.,
			%if &SRC_SYS_CD=KS or &SRC_SYS_CD=MOR or &SRC_SYS_CD=TNG %then

			%do;
				sum(ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
				sum
				(case 
					when ADJUSTED_OS_BAL_AMT <=0 then 0 
					else ADJUSTED_OS_BAL_AMT 
				end )
				as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
			%end;
%else %if &SRC_SYS_CD=SPL %then
	%do;
		sum(ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
		sum
		(case 
			when (ADJUSTED_OS_BAL_AMT) <=0 then 0 
			else (ADJUSTED_OS_BAL_AMT) 
		end)
		as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
	%end;

	case 
		when mean(PD_FINAL_RPTG_RTO) >=1 then max(WGHTD_DLGD_RTO) 
		else . 
	end 														
as EXPCTD_LOSS_RTO format 28.8,
	%if &SRC_SYS_CD=KS %then

		%do;
			sum(BEFORE_ZERO_NET_UNDRAWN_AMT) as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
			sum(AF_ZERO_NET_UNDRAWN_AMT) as AF_ZERO_NET_UNDRAWN_AMT format 17.3,
		%end;
%else %if &SRC_SYS_CD ne KS  %then
	%do;
		0 as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
		0 as AF_ZERO_NET_UNDRAWN_AMT format 17.3,
	%end;

	max(LGD_FINAL_RPTG_RTO) as LGD_FINAL_RPTG_RATIO format 28.8,
	%if &SRC_SYS_CD=KS %then

		%do;
			case 
				when mean(PD_FINAL_RPTG_RTO) >=1 then 0 
				else max(EAD_FINAL_RPTG_RTO) 
			end 
		as EAD_FINAL_RPTG_RATIO format 28.8
		%end;
%else
	%do;
		0 as EAD_FINAL_RPTG_RATIO format 28.8
	%end;

	,WGHTD_DLGD_RTO, LTV_PERCENTAGE, count(1) as OBLIGORS
		/*Hadi*/
	from &indata.
	where DLGD_F='Y'
		group by PD_BAND, CCAR_BASEL_PRD_TP_NM, MTH_TM_ID, SRC_SYS_CD, 
			LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, PD_BASEL_SEG_NUM, 
			PD_BASEL_SEG_ID, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID ,INSURER_FLAG, WGHTD_DLGD_RTO, LTV_PERCENTAGE
			/*hadi*/
	order by PD_BAND, CCAR_BASEL_PRD_TP_NM, MTH_TM_ID, SRC_SYS_CD, 
		LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, PD_BASEL_SEG_NUM, 
		PD_BASEL_SEG_ID, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID ,INSURER_FLAG, WGHTD_DLGD_RTO, LTV_PERCENTAGE
		/*hadi*/
	;
	quit;
	

/*step 1 of last transformation*/
/*Get base NCR data - aggregate level irrespective of src_sys_code; */
/*So the source dataset is the primary dataset which ensures the same aggr_NCR_balance is available for all SRC_SYS_CD */
proc sql noprint;
	create table INPATH._temp_aggr_NCR_balance as
		select 	MTH_TM_ID,
			NCR_EXPSR_CL_KEY_VAL,
			/*INSURER_FLAG,*/
	sum
	(
	case 	
		when (SRC_SYS_CD='KS' or SRC_SYS_CD='MOR') then ADJUSTED_OS_BAL_AMT
		when SRC_SYS_CD='SPL' then ADJUSTED_OS_BAL_AMT
	end
		)
		as total_outstanding_balance,
			sum
			(case 
				when (SRC_SYS_CD='KS' or SRC_SYS_CD='MOR') THEN 
				CASE 
					WHEN ADJUSTED_OS_BAL_AMT < 0 then 0 
					ELSE ADJUSTED_OS_BAL_AMT 
				END
				when SRC_SYS_CD='SPL' THEN 
				CASE 
					WHEN (ADJUSTED_OS_BAL_AMT) < 0 THEN 0 
					ELSE (ADJUSTED_OS_BAL_AMT) 
				END
				end
				)
				as total_outstanding_balance_net
					from INPATH.RRAP_Rpt_Extract00
						where PD_FINAL_RPTG_RTO >=1
							and SRC_SYS_CD in ('KS','MOR','SPL')
							group by MTH_TM_ID, NCR_EXPSR_CL_KEY_VAL 
								/*,INSURER_FLAG*/
	order by MTH_TM_ID, NCR_EXPSR_CL_KEY_VAL
	;
quit;

/*step 2a of last transformation*/
/*Get NCR data - NCR Key Value level;*/
proc sql noprint;
create table INPATH._temp_dir_NCR as
select	distinct	
MTH_TM_ID, 
NCR_EXPSR_CL_KEY,
CR_LOSS_ALWNC_AMT
from &NCRdata. where MTH_TM_ID=&MTH_TM_ID
;
quit;

/*step 2b and 2c of last transformation rule*/
/*Get balance data - account level;*/
proc sql noprint;
	create table INPATH._temp_acct_balance as
		select 	MTH_TM_ID,
			SRC_SYS_CD,
			CCAR_BASEL_PRD_TP_NM, 	
			PD_BAND,
			PD_BASEL_SEG_NUM, 
			EAD_BASEL_SEG_NUM, 
			LGD_BASEL_SEG_NUM, 
			ADJUSTED_OS_BAL_AMT,
			UNADJUSTED_ADD_ON_BAL_AMT,
			CONS_DFT_MTH_CNT,
			NCR_EXPSR_CL_KEY_VAL,
			ACCT_NUM,
			INSURER_FLAG,
			WGHTD_DLGD_RTO, LTV_PERCENTAGE,
		case 
			when SRC_SYS_CD in ('KS','MOR') then ADJUSTED_OS_BAL_AMT
			when SRC_SYS_CD in ('SPL') then ADJUSTED_OS_BAL_AMT
			else 0
		end 
	as outstanding_balance, 
		case 
			when SRC_SYS_CD in ('KS','MOR') and ADJUSTED_OS_BAL_AMT>0 then  
			ADJUSTED_OS_BAL_AMT
			when SRC_SYS_CD in ('KS','MOR') and ADJUSTED_OS_BAL_AMT<=0 then 0
			when SRC_SYS_CD in ('SPL') and 
			(ADJUSTED_OS_BAL_AMT)>0 then 
			(ADJUSTED_OS_BAL_AMT)
			when SRC_SYS_CD in ('SPL') and 
			(ADJUSTED_OS_BAL_AMT)<=0 then 0
			else 0
		end 
	as outstanding_balance_after_net 
		from INPATH.RRAP_Rpt_Extract00
			where PD_FINAL_RPTG_RTO >=1
				order by MTH_TM_ID, SRC_SYS_CD,CCAR_BASEL_PRD_TP_NM, PD_BAND, 
					PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM, NCR_EXPSR_CL_KEY_VAL, 
					ACCT_NUM
	;
quit;

/*step 3 of last transformation rule*/
/*Combine account level PRORATED_CR_LOSS_ALLOW_AMT;*/
proc sql noprint;
	create table INPATH._temp_acct_prorated_loss as
		select 	a.*, 
			b.total_outstanding_balance,
			b.total_outstanding_balance_net,
			c.CR_LOSS_ALWNC_AMT,
			(a.outstanding_balance/b.total_outstanding_balance)*c.CR_LOSS_ALWNC_AMT as 
			PRORATED_CR_LOSS_ALLOW_AMT,
			(a.outstanding_balance_after_net/b.total_outstanding_balance_net)*c.CR_LOSS_ALWNC_AMT 
		as PRORATED_CR_LOSS_ALLOW_AMT_AFTER 
			from INPATH._temp_acct_balance as a 
				left join INPATH._temp_aggr_NCR_balance as b 
					on a.MTH_TM_ID=b.MTH_TM_ID and a.NCR_EXPSR_CL_KEY_VAL=b.NCR_EXPSR_CL_KEY_VAL 
				left join INPATH._temp_dir_NCR as c
					on a.MTH_TM_ID=c.MTH_TM_ID and a.NCR_EXPSR_CL_KEY_VAL=c.NCR_EXPSR_CL_KEY 
				order by MTH_TM_ID, SRC_SYS_CD, CCAR_BASEL_PRD_TP_NM, PD_BAND,
					PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM, NCR_EXPSR_CL_KEY_VAL, 
					ACCT_NUM
	;
quit;

/*step 4 of last transformation rule*/
/*Calculate ACL at CCAR aggregated level;*/
proc sql noprint;
	create table INPATH._temp_aggr_ACL as
		select  MTH_TM_ID,
			SRC_SYS_CD,
			CCAR_BASEL_PRD_TP_NM,
			PD_BAND,
			PD_BASEL_SEG_NUM, 
			EAD_BASEL_SEG_NUM, 
			LGD_BASEL_SEG_NUM,INSURER_FLAG,WGHTD_DLGD_RTO,LTV_PERCENTAGE,
			sum(PRORATED_CR_LOSS_ALLOW_AMT) as ACL,
			sum(PRORATED_CR_LOSS_ALLOW_AMT_AFTER) as After_Netting_ACL 
		from INPATH._temp_acct_prorated_loss
			group by MTH_TM_ID,SRC_SYS_CD, CCAR_BASEL_PRD_TP_NM, PD_BAND,INSURER_FLAG, WGHTD_DLGD_RTO,LTV_PERCENTAGE,
				PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM
			order by MTH_TM_ID, SRC_SYS_CD, CCAR_BASEL_PRD_TP_NM, PD_BAND, 
				PD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM
	;
quit;
/*Generate final output;*/
proc sql noprint;
create table &outdata. as
select 'DOM-BANK-BNS' as LEGAL_ENTITY format $40.,
a.PD_BAND,
a.CCAR_BASEL_PRD_TP_NM,
a.MTH_TM_ID,
a.SRC_SYS_CD,
COALESCE(a.LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM,
COALESCE(a.EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM,
a.INSURER_FLAG as INSUR_F,
a.PD_BASEL_SEG_NUM, 
a.PD_BASEL_SEG_ID, 
a.LGD_BASEL_SEG_ID,
a.EAD_BASEL_SEG_ID, 
a.PD_90_DAY_F,
a.BEFR_ZERO_NET_ADJ_OS_BAL_AMT,
a.AF_ZERO_NET_ADJUSTED_OS_BAL_AMT,
a.EXPCTD_LOSS_RTO,
a.BEFORE_ZERO_NET_UNDRAWN_AMT,
a.AF_ZERO_NET_UNDRAWN_AMT,
case when a.PD_90_DAY_F='T' then b.ACL else 0 end as BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
case when a.PD_90_DAY_F='T' then b.After_Netting_ACL else 0 end as 
AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
a.LGD_FINAL_RPTG_RATIO AS LGD_FINAL_RPTG_RTO,
a.EAD_FINAL_RPTG_RATIO AS EAD_FINAL_RPTG_RTO,
'CAD' as CRNCY_CD format $10.,
0 as PRTL_WRITE_OFF_AMT format 17.3,
'TRUE' as UNCONDTNLY_CNCLBL format $10.,
0 as ACCR_INTR_AMT format 17.3,
A.WGHTD_DLGD_RTO, a.LTV_PERCENTAGE, A.OBLIGORS,
/*Hadi*/
&SESSIONTIME AS INSRT_PROCESS_TMSTMP,
&SESSIONTIME AS UPDT_PROCESS_TMSTMP
from INPATH._temp_aggr_base as a 
left join INPATH._temp_aggr_ACL as b
	on a.MTH_TM_ID=b.MTH_TM_ID
	and a.SRC_SYS_CD=b.SRC_SYS_CD
	and a.CCAR_BASEL_PRD_TP_NM=b.CCAR_BASEL_PRD_TP_NM
	and a.PD_BAND=b.PD_BAND 
	and a.PD_BASEL_SEG_NUM=b.PD_BASEL_SEG_NUM
	and a.EAD_BASEL_SEG_NUM=b.EAD_BASEL_SEG_NUM
	and a.LGD_BASEL_SEG_NUM=b.LGD_BASEL_SEG_NUM
	and a.INSURER_FLAG=b.INSURER_FLAG
	and a.WGHTD_DLGD_RTO = b.WGHTD_DLGD_RTO
	and a.LTV_PERCENTAGE = b.LTV_PERCENTAGE
;
quit;
%mend getRRAPreportingExtract;

/*STEP03 - Generate output datasets per SRC_SYS_CD;*/
%getRRAPreportingExtract(SRC_SYS_CD=KS,
	indata=INPATH.RRAP_Rpt_Extract_KS,
	NCRdata=INPATH.BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=INPATH.RRAP_Rpt_Extract_KS_Final);
%getRRAPreportingExtract(SRC_SYS_CD=MOR,
	indata=INPATH.RRAP_Rpt_Extract_MOR,
	NCRdata=INPATH.BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=INPATH.RRAP_Rpt_Extract_MOR_Final);
%getRRAPreportingExtract(SRC_SYS_CD=SPL,
	indata=INPATH.RRAP_Rpt_Extract_SPL,
	NCRdata=INPATH.BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=INPATH.RRAP_Rpt_Extract_SPL_Final);

/*STEP04 - Concatenate output files for KS MOR SPL;*/
%getRRAPreportingExtract(SRC_SYS_CD=TNG,
	indata=INPATH.RRAP_Rpt_Extract_TNG,
	NCRdata=INPATH.BASEL_CCAR_BUS_AGGRTD_FACT,
	outdata=INPATH.RRAP_Rpt_Extract_TNG_Final);


data INPATH.RRAP_Rpt_Extract_AllBank;
format legal_entity $40. crncy_cd $10. UNCONDTNLY_CNCLBL $10.;
	set INPATH.RRAP_Rpt_Extract_KS_Final
		INPATH.RRAP_Rpt_Extract_MOR_Final
		INPATH.RRAP_Rpt_Extract_SPL_Final
		INPATH.RRAP_Rpt_Extract_TNG_Final;
		drop src_sys_cd;
run;

proc sort data= &INPATH..RRAP_Rpt_Extract_AllBank nodupkey; 
by 
MTH_TM_ID LEGAL_ENTITY CCAR_BASEL_PRD_TP_NM PD_BAND LGD_BASEL_SEG_NUM EAD_BASEL_SEG_NUM INSUR_F WGHTD_DLGD_RTO LTV_PERCENTAGE; 
quit;


PROC SQL NOPRINT;
INSERT INTO EDRRAPT.BASEL_CCAR_EXPSR_FACT  (
LEGAL_ENTITY
,PD_BAND
,CCAR_BASEL_PRD_TP_NM
,MTH_TM_ID
,LGD_BASEL_SEG_NUM
,EAD_BASEL_SEG_NUM
,PD_BASEL_SEG_NUM
,PD_BASEL_SEG_ID
,LGD_BASEL_SEG_ID
,EAD_BASEL_SEG_ID
,PD_90_DAY_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FINAL_RPTG_RTO
,EAD_FINAL_RPTG_RTO
,CRNCY_CD
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,INSUR_F
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
,OBLIGORS)
SELECT 
legal_entity
,PD_BAND
,CCAR_BASEL_PRD_TP_NM
,MTH_TM_ID
,LGD_BASEL_SEG_NUM
,EAD_BASEL_SEG_NUM
,PD_BASEL_SEG_NUM
,PD_BASEL_SEG_ID
,LGD_BASEL_SEG_ID
,EAD_BASEL_SEG_ID
,PD_90_DAY_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FINAL_RPTG_RTO
,EAD_FINAL_RPTG_RTO
,crncy_cd
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,TRIM(INSUR_F)
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
,OBLIGORS
FROM &INPATH..RRAP_Rpt_Extract_AllBank;
QUIT;

/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/
/*SUBSIDIARIES LOGIC FOLLOW*/


%macro getRRAPreportingExtractFact_Sub(indata=, outdata=);
/*Applying initial filter conditions*/
proc sql noprint;
    CREATE table &outdata. as 
	select A.ACCT_NUM,
A.ADJUSTED_OS_BAL_AMT,
A.CCAR_BASEL_PRD_TP_NM,
A.CONS_DFT_MTH_CNT,
A.EAD_BASEL_SEG_ID,

(case
		when A.EAD_BASEL_SEG_NUM eq . then -1
		else A.EAD_BASEL_SEG_NUM end
) as EAD_BASEL_SEG_NUM,

A.EAD_FINAL_RPTG_RTO,
A.LEGAL_ENTITY,
A.LGD_BASEL_SEG_ID,
(case
		when A.LGD_BASEL_SEG_NUM eq . then -1
		else A.LGD_BASEL_SEG_NUM end
) as LGD_BASEL_SEG_NUM,

A.LGD_FINAL_RPTG_RTO,
A.MTH_TM_ID,
A.NCR_EXPSR_CL_KEY_VAL,
A.PD_BAND,
A.PD_BASEL_SEG_ID,
A.PD_BASEL_SEG_NUM,
A.PD_FINAL_RPTG_RTO,
A.SRC_SYS_CD,
A.UNADJUSTED_ADD_ON_BAL_AMT,
/*UNDRAWN_AMT,*/
A.UNQ_ACCT_ID,
(case 	when A.SCRTY_TP_DESC = 'Insured' THEN  'YES' ELSE 'NO' end ) as INSURER_FLAG 
,DLGD_RPTG_RTO as WGHTD_DLGD_RTO, DLGD_F, LTV_PERCENTAGE
/*Hadi*/
from &lib..BASEL_ANALYTCL_BL_INSTRMNT_FACT as A
where A.LEGAL_ENTITY in ('DOM-SUB-DUNDEE','DOM-SUB-MTCC','DOM-SUB-NT','DOM-SUB-MAPLE','DOM-SUB-SMC', 'DOM-SUB-TNG') 
	and A.SRC_SYS_CD in ('MOR', 'TNG-MOR') and A.CONSM_PRD_TREATMNT_CD='A' and A.SML_BUS_F='N'
/*THIS CONDITION IS REMOVED AS PART OF INTEGRATION TESTING DATED AUGUST 30 2014 AND BSTM v0.9*/
/*				and (ADJUSTED_OS_BAL_AMT <> 0 or UNDRAWN_AMT <> 0)*/
	and A.PIT_STAT_CD in ('CUR','DEF')
	and A.TRNST_EXCLSN_F='N'
	and A.MTH_TM_ID = &MTH_TM_ID;
quit;
%mend getRRAPreportingExtractFact_Sub;

/*STEP01 - Extract current month data;*/
%getRRAPreportingExtractFact_Sub(indata=&input00.,outdata=INPATH.RRAP_Rpt_Sub_Extract00);

/*Calculate first set of transformation rules using the base data.*/
%macro getRRAPreportingExtractSubs(indata=, ACLlookup=, outdata=);
/*Generate LGD Base Segment equal to blank for defaulters over 24 months;*/
data &indata;
set &indata;
/*if CONS_DFT_MTH_CNT >= 24 then do;*/
/*	LGD_BASEL_SEG_NUM=.;*/
/*    LGD_BASEL_SEG_ID=.;*/
/*end;*/
RUN;

/*Get base aggregated data;*/



proc sql noprint;
create table INPATH._temp_aggr_base as
select 	MTH_TM_ID,
		LEGAL_ENTITY,
		CCAR_BASEL_PRD_TP_NM,
		PD_BAND,
		PD_BASEL_SEG_NUM,
		PD_BASEL_SEG_ID,
		LGD_BASEL_SEG_NUM,
 		EAD_BASEL_SEG_NUM, 	
		LGD_BASEL_SEG_ID,
		EAD_BASEL_SEG_ID, 
		INSURER_FLAG,
	case 
			when mean(PD_FINAL_RPTG_RTO) >=1 then 'T'
			else 'F' 
		end as PD_90_DAY_F format $1.,
		sum (ADJUSTED_OS_BAL_AMT) as BEFR_ZERO_NET_ADJ_OS_BAL_AMT format 20.8,
		sum(
			case
				when ADJUSTED_OS_BAL_AMT <=0 then 0
				else ADJUSTED_OS_BAL_AMT
			end
			) as AF_ZERO_NET_ADJUSTED_OS_BAL_AMT format 20.8,
		max(LGD_FINAL_RPTG_RTO) as LGD_FINAL_RPTG_RATIO format 28.8,
		case 
			when mean(PD_FINAL_RPTG_RTO) >=1 then max(WGHTD_DLGD_RTO)
			else . 
		end as EXPCTD_LOSS_RTO format 28.8 


		,WGHTD_DLGD_RTO, LTV_PERCENTAGE, count(1) as OBLIGORS
		/*Hadi*/

from &indata.
where DLGD_F='Y'
group by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, PD_BAND,
 		 LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID,PD_BASEL_SEG_NUM,
		PD_BASEL_SEG_ID,INSURER_FLAG, WGHTD_DLGD_RTO, LTV_PERCENTAGE


order by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, PD_BAND, 
 		 LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_ID, EAD_BASEL_SEG_ID, LTV_PERCENTAGE

		 ;
quit;
/*step 1 of last transformation rule*/
/*Get total outstanding balances - aggregate level by LEGAL_ENTITY;*/
proc sql noprint;
create table INPATH._temp_aggr_balance as
select 	MTH_TM_ID,
		LEGAL_ENTITY,
		sum(ADJUSTED_OS_BAL_AMT) as total_os_balance,
		sum(
			case
				when ADJUSTED_OS_BAL_AMT <=0 then 0
				else ADJUSTED_OS_BAL_AMT
			end
			) as total_os_balance_after_net 
from &indata.
where PD_FINAL_RPTG_RTO >=1
group by MTH_TM_ID, LEGAL_ENTITY
order by MTH_TM_ID, LEGAL_ENTITY 
;
quit;

/*step 2a of last transformation rule*/
/*Get ACL Lookup data;*/
proc sql noprint;
    create table INPATH._temp_GL_ACL_balance as 
  	select &MTH_TM_ID as MTH_TM_ID,	
		LEGAL_ENTITY,
		EFF_FROM_YR_MTH,
		EFF_TO_YR_MTH,
		GENL_LEDGER_ENTITY_ACL_AMT
	from 	&ACLlookup.
where input(EFF_FROM_YR_MTH,6.) <= &yearmonth and &yearmonth <= 
input(EFF_TO_YR_MTH,6.) 
/*	and crnt_f='Y'*/
and LEGAL_ENTITY in ('DOM-SUB-DUNDEE','DOM-SUB-MTCC','DOM-SUB-NT',
'DOM-SUB-MAPLE','DOM-SUB-SMC');
quit;

/*step 2b and 2c of last transformation rule*/
/*Get Adjusted Outstanding Balance - account level;*/
proc sql noprint; 
create table INPATH._temp_acct_balance as
select 	MTH_TM_ID,
		LEGAL_ENTITY,
		CCAR_BASEL_PRD_TP_NM,
		PD_BAND,
		LGD_BASEL_SEG_NUM,
 		EAD_BASEL_SEG_NUM, 	
		ADJUSTED_OS_BAL_AMT,
		PD_FINAL_RPTG_RTO,
		case
			when ADJUSTED_OS_BAL_AMT>0 then ADJUSTED_OS_BAL_AMT
			when ADJUSTED_OS_BAL_AMT<=0 then 0
			else 0
			end as ADJUSTED_OS_BAL_AMT_after_net format 20.8, 
		CONS_DFT_MTH_CNT,
		ACCT_NUM, INSURER_FLAG, WGHTD_DLGD_RTO, LTV_PERCENTAGE
from &indata.  
where PD_FINAL_RPTG_RTO >=1
order by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM,
		PD_BAND, LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, ACCT_NUM;
quit;

/*step 3 of last transformation rule*/
/*Combine account level data with ACL GL and total outstanding balances;*/
proc sql noprint;
	create table INPATH._temp_acct_prorated_loss as
		select 	a.*,
			b.total_os_balance,
			b.total_os_balance_after_net,
			c.GENL_LEDGER_ENTITY_ACL_AMT,
			(a.ADJUSTED_OS_BAL_AMT/b.total_os_balance)*c.GENL_LEDGER_ENTITY_ACL_AMT as 
			PRORATED_CR_LOSS_ALLOW_AMT,
			(a.ADJUSTED_OS_BAL_AMT_after_net/b.total_os_balance_after_net)*c.GENL_LEDGER_ENTITY_ACL_AMT 
		as PRORATED_CR_LOSS_ALLOW_AMT_after
			from 	INPATH._temp_acct_balance as a 
				left join INPATH._temp_aggr_balance as b on a.MTH_TM_ID=b.MTH_TM_ID and 
					a.LEGAL_ENTITY=b.LEGAL_ENTITY 
				left join INPATH._temp_GL_ACL_balance as c on a.MTH_TM_ID=c.MTH_TM_ID and 
					a.LEGAL_ENTITY=c.LEGAL_ENTITY
				order by MTH_TM_ID,LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM,
					PD_BAND, LGD_BASEL_SEG_NUM, EAD_BASEL_SEG_NUM, ACCT_NUM;
quit;
/*step 4 of last transformation rule*/
/*Calculate ACL at CCAR aggregated level;*/
proc sql noprint;
create table INPATH._temp_aggr_ACL as
select  MTH_TM_ID,
		LEGAL_ENTITY,
 		CCAR_BASEL_PRD_TP_NM,
		PD_BAND,	
 		EAD_BASEL_SEG_NUM, 
 		LGD_BASEL_SEG_NUM, 
		INSURER_FLAG, /*ADDED AS PART OF Q4*/
		WGHTD_DLGD_RTO,LTV_PERCENTAGE,
		sum(PRORATED_CR_LOSS_ALLOW_AMT) as ACL,
		sum(PRORATED_CR_LOSS_ALLOW_AMT_after) as After_Netting_ACL 
from 	INPATH._temp_acct_prorated_loss
group by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM, INSURER_FLAG,WGHTD_DLGD_RTO,LTV_PERCENTAGE,
		PD_BAND, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM
order by MTH_TM_ID, LEGAL_ENTITY, CCAR_BASEL_PRD_TP_NM,
		PD_BAND, EAD_BASEL_SEG_NUM, LGD_BASEL_SEG_NUM
		;
quit;

/*Generate final output;*/
proc sql noprint; 
create table &outdata. as
select  
a.LEGAL_ENTITY,
a.PD_BAND,
a.CCAR_BASEL_PRD_TP_NM,
a.MTH_TM_ID,
COALESCE(a.LGD_BASEL_SEG_NUM,.,-1) AS LGD_BASEL_SEG_NUM,
COALESCE(a.EAD_BASEL_SEG_NUM,.,-1) AS EAD_BASEL_SEG_NUM,
a.PD_BASEL_SEG_NUM,
a.PD_BASEL_SEG_ID, 
a.LGD_BASEL_SEG_ID,
a.EAD_BASEL_SEG_ID,
a.PD_90_DAY_F,
a.INSURER_FLAG as INSUR_F,
a.BEFR_ZERO_NET_ADJ_OS_BAL_AMT,
a.AF_ZERO_NET_ADJUSTED_OS_BAL_AMT,
a.EXPCTD_LOSS_RTO,
0 as BEFORE_ZERO_NET_UNDRAWN_AMT format 17.3,
0 as AF_ZERO_NET_UNDRAWN_AMT format 17.3, 
case when a.PD_90_DAY_F='T' then b.ACL else 0 end as 
BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
case when a.PD_90_DAY_F='T' then b.After_Netting_ACL else 0 end as AF_ZERO_NET_ALWBL_CR_LOSS_AMT format 17.3,
a.LGD_FINAL_RPTG_RATIO as LGD_FINAL_RPTG_RTO,
0 as EAD_FINAL_RPTG_RTO format 28.8,
'CAD' as CRNCY_CD format $10.,
0 as PRTL_WRITE_OFF_AMT format 17.3,
'TRUE' as UNCONDTNLY_CNCLBL format $10.,
0 as ACCR_INTR_AMT format 17.3,
a.WGHTD_DLGD_RTO, a.LTV_PERCENTAGE, A.OBLIGORS,
/*Hadi*/
&SESSIONTIME AS INSRT_PROCESS_TMSTMP,
&SESSIONTIME AS UPDT_PROCESS_TMSTMP
from INPATH._temp_aggr_base as a 
left join INPATH._temp_aggr_ACL as b
on	a.MTH_TM_ID=b.MTH_TM_ID
	and a.LEGAL_ENTITY=b.LEGAL_ENTITY
	and a.CCAR_BASEL_PRD_TP_NM=b.CCAR_BASEL_PRD_TP_NM
	and a.PD_BAND=b.PD_BAND 
	and a.EAD_BASEL_SEG_NUM=b.EAD_BASEL_SEG_NUM
	and a.LGD_BASEL_SEG_NUM=b.LGD_BASEL_SEG_NUM
	and a.INSURER_FLAG=b.INSURER_FLAG
	and a.WGHTD_DLGD_RTO=b.WGHTD_DLGD_RTO
	and a.LTV_PERCENTAGE=b.LTV_PERCENTAGE
;
quit; 
%mend getRRAPreportingExtractSubs;
 
/*Generate output datasets;*/
%getRRAPreportingExtractSubs(indata=INPATH.RRAP_Rpt_Sub_Extract00,
							 ACLlookup=INPATH.BASEL_SUBSIDIARY_ACL_LKP, 
							 outdata=INPATH.RRAP_Rpt_Extract_Subsidiary);

/*PROC APPEND BASE=EDRRAPT.BASEL_CCAR_EXPSR_FACT */
/*DATA=INPATH.RRAP_Rpt_Extract_Subsidiary FORCE;*/
/*RUN;*/

PROC SQL NOPRINT;
INSERT INTO EDRRAPT.BASEL_CCAR_EXPSR_FACT  (
LEGAL_ENTITY
,PD_BAND
,CCAR_BASEL_PRD_TP_NM
,MTH_TM_ID
,LGD_BASEL_SEG_NUM
,EAD_BASEL_SEG_NUM
,PD_BASEL_SEG_NUM
,PD_BASEL_SEG_ID
,LGD_BASEL_SEG_ID
,EAD_BASEL_SEG_ID
,PD_90_DAY_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FINAL_RPTG_RTO
,EAD_FINAL_RPTG_RTO
,CRNCY_CD
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,INSUR_F
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
,OBLIGORS)
SELECT 
legal_entity
,PD_BAND
,CCAR_BASEL_PRD_TP_NM
,MTH_TM_ID
,LGD_BASEL_SEG_NUM
,EAD_BASEL_SEG_NUM
,PD_BASEL_SEG_NUM
,PD_BASEL_SEG_ID
,LGD_BASEL_SEG_ID
,EAD_BASEL_SEG_ID
,PD_90_DAY_F
,BEFR_ZERO_NET_ADJ_OS_BAL_AMT
,AF_ZERO_NET_ADJUSTED_OS_BAL_AMT
,EXPCTD_LOSS_RTO
,BEFORE_ZERO_NET_UNDRAWN_AMT
,AF_ZERO_NET_UNDRAWN_AMT
,BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT
,AF_ZERO_NET_ALWBL_CR_LOSS_AMT
,LGD_FINAL_RPTG_RTO
,EAD_FINAL_RPTG_RTO
,crncy_cd
,PRTL_WRITE_OFF_AMT
,UNCONDTNLY_CNCLBL
,ACCR_INTR_AMT
,INSRT_PROCESS_TMSTMP
,UPDT_PROCESS_TMSTMP
,TRIM(INSUR_F)
,WGHTD_DLGD_RTO
,LTV_PERCENTAGE
,OBLIGORS
FROM INPATH.RRAP_Rpt_Extract_Subsidiary;
QUIT;


/*UPDATE NULL VALUES TO 0 FOR TNG PRODUCTS*/
PROC SQL NOPRINT;
UPDATE EDRRAPT.BASEL_CCAR_EXPSR_FACT 
SET BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE BEFR_ZERO_NET_ALWBL_CR_LOSS_AMT IS NULL AND CCAR_BASEL_PRD_TP_NM LIKE '%TNG%'
AND MTH_TM_ID=&MTH_TM_ID;
UPDATE EDRRAPT.BASEL_CCAR_EXPSR_FACT 
SET AF_ZERO_NET_ALWBL_CR_LOSS_AMT=0 WHERE AF_ZERO_NET_ALWBL_CR_LOSS_AMT IS NULL AND CCAR_BASEL_PRD_TP_NM LIKE '%TNG%'
AND MTH_TM_ID=&MTH_TM_ID;
QUIT;

%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Extract00 RRAP_Rpt_Extract_KS RRAP_Rpt_Extract_MOR RRAP_Rpt_Extract_SPL _temp_aggr_base);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=_temp_aggr_NCR_balance _temp_dir_NCR _temp_acct_balance _temp_acct_prorated_loss _temp_aggr_ACL);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Extract_KS_Final RRAP_Rpt_Extract_MOR_Final RRAP_Rpt_Extract_SPL_Final RRAP_Rpt_Extract_AllBank);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=RRAP_Rpt_Sub_Extract00 _temp_aggr_base _temp_aggr_balance _temp_GL_ACL_balance _temp_acct_balance);
%SAS_DATASET_CLEANUP(LIBREF=INPATH,DATASETS=_temp_acct_prorated_loss _temp_aggr_ACL RRAP_Rpt_Extract_Subsidiary);
