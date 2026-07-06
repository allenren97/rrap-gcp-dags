data cbs.CIS_DATA_POP;
/*set NZWRK.CIS_DATA_POP_04(bulkunload=yes);*/
set NZWRK.CIS_DATA_POP_04;
run;



data CIS_DATA_POP_DEP;
set cbs.CIS_DATA_POP(where=(cust_type='Retail' and CUST_DEP_ONLY_FLG=1 and LEND_PRODS=0 and cust_noacct_excl='N'));
run;

data CIS_DATA_POP_LEND;
set cbs.CIS_DATA_POP(where=(cust_type='Retail' and CUST_DEP_ONLY_FLG=0 and LEND_PRODS=1 and cust_noacct_excl='N'));
run;

data CUST_BASE_DEP;
/*set NZWRK.CUST_BASE_08(bulkunload=yes where=(cust_type='Retail' and DEP_FLG=1 and noacct_excl='N') );*/
set NZWRK.CUST_BASE_08(where=(cust_type='Retail' and DEP_FLG=1 and noacct_excl='N') );
run;

data CUST_BASE_LEND;
/*set NZWRK.CUST_BASE_08(bulkunload=yes where=(cust_type='Retail' and DEP_FLG=0 and noacct_excl='N') );*/
set NZWRK.CUST_BASE_08(where=(cust_type='Retail' and DEP_FLG=0 and noacct_excl='N') );
run;

/*drop=ESTAB_DT DEP_AGENT_FLAG FINCL_SERVICE_PROVIDER_FLAG ORG_TYPE_CD*/
/*INDUSTRY_CD ASSOCIATION_AFFILIATION_CD BUS_SERVICE_METH_CD CRA_INSTITUTION_CD NON_PROFIT_ORG_FLAG ACT_MORE_THAN_50_FLAG HI_RISK_BUS_CD COMPANY_STRUCTURE_CD FRANCHISE_CD*/

proc sql;
create table CUST_BASE_LEND as 
select 
a.*,
b.worst_days_dlq_cust,
c.worst_days_dlq_ks_cust,
d.worst_days_dlq_mor_cust,
e.worst_days_dlq_spl_cust,
f.num_delq_acct
from CUST_BASE_LEND as a
left join (
	select
	cid,
	max(days_dlq) as  worst_days_dlq_cust
	from CIS_DATA_POP_LEND
	where pit_stat ='CUR'
	group by cid) as b on a.cid=b.cid
left join (
	select
	cid,
	max(days_dlq) as  worst_days_dlq_ks_cust
	from CIS_DATA_POP_LEND
	where pit_stat ='CUR' and rev_ind=1
	group by cid) as c on a.cid=c.cid
left join (
	select
	cid,
	max(days_dlq) as  worst_days_dlq_mor_cust
	from CIS_DATA_POP_LEND
	where pit_stat ='CUR' and mor_ind=1
	group by cid) as d on a.cid=d.cid
left join (
	select
	cid,
	max(days_dlq) as  worst_days_dlq_spl_cust
	from CIS_DATA_POP_LEND
	where pit_stat ='CUR' and spl_ind=1
	group by cid) as e on a.cid=e.cid
left join (
	select
	cid,
	sum(case when days_dlq>0 then 1 else 0 end) as num_delq_acct
	from CIS_DATA_POP_LEND
	where pit_stat ='CUR'
	group by cid) as f on a.cid=f.cid
;
quit;
proc sql;
create table CUST_BASE_DEP as 
select 
a.*,
b.worst_days_dlq_cust,
c.worst_days_dlq_ks_cust,
d.worst_days_dlq_mor_cust,
e.worst_days_dlq_spl_cust,
f.num_delq_acct
from CUST_BASE_DEP as a
left join (
	select
	cid,
	max(days_dlq) as  worst_days_dlq_cust
	from CIS_DATA_POP_DEP
	where pit_stat ='CUR'
	group by cid) as b on a.cid=b.cid
left join (
	select
	cid,
	max(days_dlq) as  worst_days_dlq_ks_cust
	from CIS_DATA_POP_DEP
	where pit_stat ='CUR' and rev_ind=1
	group by cid) as c on a.cid=c.cid
left join (
	select
	cid,
	max(days_dlq) as  worst_days_dlq_mor_cust
	from CIS_DATA_POP_DEP
	where pit_stat ='CUR' and mor_ind=1
	group by cid) as d on a.cid=d.cid
left join (
	select
	cid,
	max(days_dlq) as  worst_days_dlq_spl_cust
	from CIS_DATA_POP_DEP
	where pit_stat ='CUR' and spl_ind=1
	group by cid) as e on a.cid=e.cid
left join (
	select
	cid,
	sum(case when days_dlq>0 then 1 else 0 end) as num_delq_acct
	from CIS_DATA_POP_DEP
	where pit_stat ='CUR'
	group by cid) as f on a.cid=f.cid
;
quit;


proc sql;
	create table CUST_BASE_LEND as
		select
			a.*,
			b.default_date as cust_default_date format=date9.,
			case when b.default_ind=. then 0 else b.default_ind end as cust_default_ind,
			case when c.CORP_COMM_EXCL='' then 'N' else c.CORP_COMM_EXCL end as CORP_COMM_EXCL,
			case when d.STAFF_EXCL='' then 'N' else d.STAFF_EXCL end as STAFF_EXCL
		from CUST_BASE_LEND as a
			left join (
			select 
				cid,
				min(default_date) as default_date,
				max(default_ind) as default_ind
			from CIS_DATA_POP_LEND
				where default_ind=1 and PIT_STAT='CUR' and MODEL_EXCL='N'
					group by cid

						) as b on a.cid=b.cid


			left join (
				select 
					distinct cid,
					'Y' as CORP_COMM_EXCL
				from CIS_DATA_POP_LEND 
					where CORP_COMM_EXCL='Y'
					group by cid
						) as c on a.cid=c.cid

			left join (
				select 
					distinct cid,
					'Y' as STAFF_EXCL
				from CIS_DATA_POP_LEND
					where STAFF_EXCL='Y'
					group by cid
						) as d on a.cid=d.cid

;
quit;



data cbs.CUST_BASE;
set CUST_BASE_LEND CUST_BASE_DEP;
format CID_NUM 32.;  
run;


