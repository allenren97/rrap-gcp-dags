proc sql;
connect using NZWRK as nzcon;
execute(drop table &RRAP_WRK..CIS_DATA_POP_03 if exists) by nzcon;
quit;
proc sql;
connect using NZWRK as nzcon;
execute( create table &RRAP_WRK..CIS_DATA_POP_03 as (
	select 
		a.*
		,b.acct_base_key
		,b.acct_lcst
/*		,cast(cid as bigint) cid_num*/
/*		,cast(a.account as bigint) acct_numeric*/
		,case when b.ACCT_LCST='A' and lend_prods=0 then 1 else 0 end as non_lend_prods_ACT
		,case when b.ACCT_LCST='C' and lend_prods=0 then 1 else 0 end as non_lend_prods_PCLO
		,case when b.ACCT_LCST='D' and lend_prods=0 then 1 else 0 end as non_lend_prods_DOR
		,case when b.ACCT_LCST='I' and lend_prods=0 then 1 else 0 end as non_lend_prods_INACT
		,case when b.ACCT_LCST='P' and lend_prods=0 then 1 else 0 end as non_lend_prods_PND
		,case when b.ACCT_LCST='S' and lend_prods=0 then 1 else 0 end as non_lend_prods_STO
		,case when b.ACCT_LCST='V' and lend_prods=0 then 1 else 0 end as non_lend_prods_CLO
		,case when b.ACCT_LCST IS NULL and lend_prods=0 then 1 else 0 end as non_lend_prods_WO
	from &RRAP_WRK..CIS_DATA_POP_02 a left join &RRAP_WRK..status b
	on a.account=b.account
	) WITH DATA
	) by nzcon;
quit;

proc sql;
connect using NZWRK as nzcon;
execute(drop table &RRAP_WRK..CUST_BASE_06 if exists) by nzcon;
quit;
proc sql;
connect using NZWRK as nzcon;
execute( 
	create table &RRAP_WRK..CUST_BASE_06 as (
		select a.*, num_nonlend_prods_ACT, num_nonlend_prods_PCLO, num_nonlend_prods_DOR, 
					num_nonlend_prods_INACT, num_nonlend_prods_PND, num_nonlend_prods_STO, 
					num_nonlend_prods_CLO, num_nonlend_prods_WO, num_nonlend_prods
		from &RRAP_WRK..CUST_BASE_05 a 
			left join (select 
						cid
						,cid_num
						,mth_tm_id
						,process_date
						,sum(non_lend_prods_ACT) as num_nonlend_prods_ACT
						,sum(non_lend_prods_PCLO) as num_nonlend_prods_PCLO
						,sum(non_lend_prods_DOR) as num_nonlend_prods_DOR
						,sum(non_lend_prods_INACT) as num_nonlend_prods_INACT
						,sum(non_lend_prods_PND) as num_nonlend_prods_PND
						,sum(non_lend_prods_STO) as num_nonlend_prods_STO
						,sum(non_lend_prods_CLO) as num_nonlend_prods_CLO
						,sum(non_lend_prods_WO) as num_nonlend_prods_WO
						,(sum(non_lend_prods_ACT)+sum(non_lend_prods_DOR)+sum(non_lend_prods_INACT)) as num_nonlend_prods
					from &RRAP_WRK..CIS_DATA_POP_03
					group by cid,cid_num,mth_tm_id,process_date
         	) b on a.cid=b.cid and a.mth_tm_id=b.mth_tm_id
	) WITH DATA
) by nzcon;
quit;







proc sql;
	connect using NZWRK as nzcon;
	execute(drop table &RRAP_WRK..CUST_BASE_07 if exists) by nzcon;
quit;

proc sql;
connect using NZWRK as nzcon;
execute(create table &RRAP_WRK..CUST_BASE_07 as (
	select a.*
		
	
			,case
				when num_lend_prods=0 and num_nonlend_prods>0 then 1
				else 0
			end as DEP_FLG
			,case
				when num_lend_prods=0 and num_nonlend_prods=0 then 'Y'
				else 'N'
			end as noacct_excl
			,BNKRPTCY_FLAG as bankruptcy_excl
			,UNDER_18_FLAG as under_age_excl
			,case
				when (NUM_LEND_PRODS_CUR>0 or NUM_LEND_PRODS_CLO>0) and NUM_LEND_PRODS_DEF<=0 then 'CUR'
				when (NUM_LEND_PRODS_CUR>=0 or NUM_LEND_PRODS_CLO>=0) and NUM_LEND_PRODS_DEF>0 then 'DEF'
			end as CUST_PIT_STAT
			from &RRAP_WRK..cust_base_06 a
	) WITH DATA
		) by nzcon;
quit;


*Attach customer definitions to account table;
proc sql;
	connect using NZWRK as nzcon;
	execute(drop table &RRAP_WRK..CIS_DATA_POP_04 if exists) by nzcon;
quit;
proc sql noprint;
select "a."||compress(name) into :fields separated by ', ' from sashelp.vcolumn 
where libname='NZWRK' and memname='CIS_DATA_POP_03' 
and upcase(name) not in  (
						 'CUST_TYPE'
						,'CIFKEY'
						,'CAB'
						,'FILE_YR_MTH'
						,'LOAD_DATE_TM'
						,'MTH_TM_ID'
						,'LRA_STATUS_MOR'
						,'FRCLSR_F_MOR'
						,'FUND_CD_MOR'
						,'TRNST_NUM_REV'
						,'BLOCK_RECL_CD'
						,'PRD_CD_REV'
						,'ACCT_NUMERIC'
						,'CHECK_IND'
						,'PROD_TREAT'
						,'ACCT_BASE_KEY'
						,'ACCT_LCST'
						)

;
quit;

%put &fields.;
/*drop=cust_type!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!*/
proc sql;
connect using NZWRK as nzcon;
execute(create table &RRAP_WRK..CIS_DATA_POP_04 as (
	select 
				&fields.,
				b.cust_type,
				b.cust_status,
				b.DEP_FLG as CUST_DEP_ONLY_FLG,
				b.DECEASED_IND as CUST_DECEASED_IND,
				b.prime_ind as CUST_PRIME_IND,
				b.secondary_ind as CUST_SECONDARY_IND,
				b.noacct_excl as cust_noacct_excl,
				b.bankruptcy_excl as cust_bankruptcy_excl,
				b.under_age_excl as cust_under_age_excl
/*				,b.age as cust_age */
			from &RRAP_WRK..CIS_DATA_POP_03 as a
				left join &RRAP_WRK..CUST_BASE_07 as b on a.cid=b.cid
	) WITH DATA
	) by nzcon;
quit;





proc sql;
connect using NZWRK as nzcon;
execute(drop table &RRAP_WRK..cbs_ltv_data if exists) by nzcon;
execute(drop table &RRAP_WRK..CIS_DATA_SMPL if exists) by nzcon;
execute(drop table &RRAP_WRK..CUST_BASE_08 if exists) by nzcon;

quit;


proc sql;
	connect using EDRRAPT as dbcon;
	create table NZWRK.cbs_ltv_data as select * from connection to dbcon(
		select 
			mth_tm_id,
			basel_acct_id,
			amort,
			SRC_SYS_CD,
			INDEXED_PRPTY_VAL_AMT,
			INDEXED_LOAN_TO_VAL_RTO,
			ORIG_PRPTY_VAL_AMT,
			INSUR_F,
			CRNT_LTV_RTO
		from EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT as a
			where INDEXED_LOAN_TO_VAL_RTO>0 and mth_tm_id>=&tm_id. and mth_tm_id<=&tm_id.
				);
quit;


proc sql;
connect using NZWRK as nzcon;
execute(

create table &RRAP_WRK..CIS_DATA_SMPL as (

select aa.*, bb.src_sys_cd, bb.amort, bb.INDEXED_PRPTY_VAL_AMT, bb.INDEXED_LOAN_TO_VAL_RTO, bb.ORIG_PRPTY_VAL_AMT, bb.insur_f, bb.crnt_ltv_rto

from (select a.*, cast(b.mort_no as INT) as MORT_NUM,
	b.basel_acct_id, b.product, b.account, b.MORT_NO, /*b.cab,*/ b.loan_no, b.days_dlq
	from &RRAP_WRK..CUST_BASE_07 a left join &RRAP_WRK..CIS_DATA_POP_04 b
	on a.cid=b.cid and a.process_date=b.process_date) as aa
left join &RRAP_WRK..cbs_ltv_data as bb on aa.mth_tm_id=bb.mth_tm_id and aa.basel_acct_id=bb.basel_acct_id

) WITH DATA
) by nzcon;
quit;




proc sql;
connect using NZWRK as nzcon;
execute(
create table &RRAP_WRK..CUST_BASE_08 as (
select a.*, b.min_LTV, b.max_LTV, b.avg_LTV, 
	c.min_LTV_heloc, c.max_LTV_heloc, c.avg_LTV_heloc
from &RRAP_WRK..CUST_BASE_07 as a 
left join 
(
	select
	process_date,
	cid,
	mth_tm_id,
	min(INDEXED_LOAN_TO_VAL_RTO) as min_LTV,
	max(INDEXED_LOAN_TO_VAL_RTO) as max_LTV,
	avg(INDEXED_LOAN_TO_VAL_RTO) as avg_LTV
	from &RRAP_WRK..CIS_DATA_SMPL
	where SRC_SYS_CD='MOR' 
	group by process_date, mth_tm_id,cid
) as b on a.cid=b.cid and a.process_date=b.process_date
left join 
(
	select
	process_date,
	cid,
	mth_tm_id,
	min(INDEXED_LOAN_TO_VAL_RTO) as min_LTV_heloc,
	max(INDEXED_LOAN_TO_VAL_RTO) as max_LTV_heloc,
	avg(INDEXED_LOAN_TO_VAL_RTO) as avg_LTV_heloc
	from &RRAP_WRK..CIS_DATA_SMPL
	where SRC_SYS_CD='KS' 
	group by process_date, mth_tm_id, cid
) as c on a.cid=c.cid and a.process_date=c.process_date
order by b.cid, b.process_date
) WITH DATA
) by nzcon;

quit;

