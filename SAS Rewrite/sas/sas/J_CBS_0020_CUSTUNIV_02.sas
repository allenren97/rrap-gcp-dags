proc sql;
connect using NZWRK as nzcon;
execute(drop table &RRAP_WRK..CUST_BASE_01 if exists) by nzcon;
execute(
	create table &RRAP_WRK..CUST_BASE_01 as (
		select 
			cid,
			cid_num,
			mth_tm_id,
			process_date,
			count(1) as num_prods,
			sum(lend_prods_CUR) as num_lend_prods_CUR,
			sum(lend_prods_CLO) as num_lend_prods_CLO,
			sum(lend_prods_bnk) as num_lend_prods_bnk,
			sum(lend_prods_DEF) as num_lend_prods_DEF,
			sum(lend_prods_CHG) as num_lend_prods_CHG,
			sum(lend_prods_WO) as num_lend_prods_WO,

			sum(lend_prods_COMM) as num_lend_prods_COMM,

			sum(lend_prods_COMM_CUR) as num_lend_prods_COMM_CUR,
			sum(lend_prods_COMM_CLO) as num_lend_prods_COMM_CLO,
			sum(lend_prods_COMM_CHG) as num_lend_prods_COMM_CHG,
			sum(lend_prods_COMM_DEF) as num_lend_prods_COMM_DEF,
			sum(lend_prods_COMM_WO) as num_lend_prods_COMM_WO,

			sum(PRIVATE_BANK_IND) as PRIVATE_BANK_IND,

			sum(mor_ind) as num_mor,
			sum(spl_ind) as num_spl,
			sum(rev_ind) as num_rev,
			sum(ssl_ind) as num_ssl,
/*			(calculated num_lend_prods_CUR+calculated num_lend_prods_DEF+calculated num_lend_prods_COMM_CUR+calculated num_lend_prods_COMM_DEF) as num_lend_prods,*/
			sum(lend_prods_CUR)+sum(lend_prods_DEF)+sum(lend_prods_COMM_CUR)+sum(lend_prods_COMM_DEF) as num_lend_prods,
			max(days_dlq) as worst_dlq_days
		from &RRAP_WRK..CIS_DATA_POP_02
			group by cid, cid_num, mth_tm_id,process_date 
		) WITH DATA	
	) by nzcon;
quit;



proc sql;
	connect using NZWRK as nzcon;
	execute(drop table &RRAP_WRK..CUST_BASE_02 if exists) by nzcon;
	execute (create table &RRAP_WRK..CUST_BASE_02 as (

	select * from (
		select 
			a.*,
			c.basel_cust_id,
			c.cust_cid,
			c.HIT_NOHIT_EDIT_REJCT_CD,
			c.FICO_08_SCORE,
			c.FICO_08_EXCLSN_CD,
			case 
				when cust_cid is not null then 1
				else 0
			end as bureau_exist
			,row_number() over (partition by a.cid order by c.SCORE_LAST_RECVD_DT desc) as row_num

		from &RRAP_WRK..CUST_BASE_01 as a
			left join &RRAP_DB..CR_BUREAU_DELI_MTH_SNAPSHOT as c 
			on a.cid=c.cust_cid and a.mth_tm_id=c.mth_tm_id and c.mth_tm_id=&tm_id.

		) aa where aa.row_num=1
	) WITH DATA
				) by nzcon;
	disconnect from nzcon;
quit;


/*Using MDMflags*/
proc sql;
connect using NZWRK as nzcon;
execute(drop table &RRAP_WRK..CUST_BASE_04 if exists) by nzcon;
execute (create table &RRAP_WRK..CUST_BASE_04 as (
	select distinct a.*,b.cust_type,b.cust_status,b.deceased_ind,b.BNKRPTCY_FLAG,b.UNDER_18_FLAG
	,case 
		when b.cust_type='Retail' then 1
		else null
	end as retail_ind

	from &RRAP_WRK..cust_base_02 a 
		left join &RRAP_WRK..CBS_MDM_FLAGS b 
		on 
		LENGTH(RTRIM(TRANSLATE(b.party_id, '*', ' 0123456789'))) = 0 /*Make sure party_id is numeric*/
		and to_number(a.cid,'999999999999999')=to_number(b.party_id,'999999999999999')
		and b.EFF_DT=&string.
	) WITH DATA
) by nzcon;
quit;

proc sql;
connect using nzwrk as nzcon;
execute(drop table &RRAP_WRK..CUST_BASE_05 if exists) by nzcon;
execute(
	create table &RRAP_WRK..CUST_BASE_05 as (
		select
			a.*,
		case 
			when b.PRIMARY_FLAG = 'Y' then 1 
			else 0 
		end 
	as prime_ind,
		case 
			when c.PRIMARY_FLAG = 'N' then 1 
			else 0 
		end 
	as secondary_ind,
		case 
			when d.PRIMARY_FLAG = 'Y' then 1 
			else 0 
		end 
	as prime_ind_lend,
		case 
			when e.PRIMARY_FLAG = 'N' then 1 
			else 0 
		end 
	as secondary_ind_lend

		from &RRAP_WRK..CUST_BASE_04 as a
			left join (select distinct cid,PRIMARY_FLAG from &RRAP_WRK..CIS_DATA_POP_02 where PRIMARY_FLAG='Y') as b  on a.cid=b.cid
			left join (select distinct cid,PRIMARY_FLAG from &RRAP_WRK..CIS_DATA_POP_02 where PRIMARY_FLAG='N') as c  on a.cid=c.cid
			left join (select distinct cid,PRIMARY_FLAG from &RRAP_WRK..CIS_DATA_POP_02 where PRIMARY_FLAG='Y' and LEND_PRODS=1) as d  on a.cid=d.cid
			left join (select distinct cid,PRIMARY_FLAG from &RRAP_WRK..CIS_DATA_POP_02 where PRIMARY_FLAG='N' and LEND_PRODS=1) as e  on a.cid=e.cid
	) WITH DATA
	) by nzcon;
quit;

