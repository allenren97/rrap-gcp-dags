

proc sql;
connect using NZWRK as nzcon;
create table acct_list as select * from connection to nzcon(
	select distinct lpad(cast(account as bigint),18,'0') as acct_id, a.account, a.product
	from &RRAP_WRK..CIS_DATA_POP_02 a left join &RRAP_WRK..CUST_BASE_05 b
	on a.cid=b.cid
	where b.cust_type='Retail'
	order by lpad(cast(account as bigint),18,'0') desc
);
quit;

proc sql;
connect using cbsdb2 as dbcon;
execute(truncate table sas.Z_CBS_ACCNTS immediate) by dbcon;
quit;

proc append base=cbsdb2.Z_CBS_ACCNTS(BULKLOAD=YES BL_METHOD=CLILOAD) data=acct_list force; run;


proc sql;
connect using NZWRK as nzcon;
execute(drop table &RRAP_WRK..STATUS if exists) by nzcon;
quit;

proc sql;
connect using cbsdb2 as dbcon;
create table NZWRK.STATUS(bulkload=yes BL_METHOD=CLILOAD) as select * from connection to dbcon(
	select ACCT_ID, ACCOUNT, PRODUCT, ACCT_TYP, ACCT_BASE_KEY, ACCT_LCST
	from (
	    select a.*, b.ACCT_TYP,b.ACCT_BASE_KEY, c.acct_lcst,(rownumber() over (partition by account)) as rn 
	    from SAS.Z_CBS_ACCNTS a
	      left join owtact.acct_xref b on a.ACCT_ID=b.acct_id
	      left join OWSTAR.IWF_CUST_ACCT c on b.acct_base_key=c.acct_base_key and c.time_key=&tm_id. and c.PRIM_CUST_F='P'
	      ) where rn=1
);
quit;
