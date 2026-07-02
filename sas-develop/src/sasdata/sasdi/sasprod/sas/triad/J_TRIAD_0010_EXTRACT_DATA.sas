
proc sql noprint;
	connect using EDRRAPT as dbcon;
	select count into :db2_count from connection to dbcon(
	select count(1) as count from 
	EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT 
			where src_sys_cd in ('KS','SPL','MOR') and CONSM_PRD_TREATMNT_CD='A' 
				and SML_BUS_F='N'
				and PIT_STAT_CD in ('CUR','DEF')
				and TRNST_EXCLSN_F='N'
				and MTH_TM_ID = &mth_tm_id.);
quit;


proc sql;
	connect using EDRRAPT as dbcon;
	create table TRIAD.BASEL_ANALYTCL_BL_INSTRMNT_FACT as select * from connection to dbcon(
	select basel_acct_id
			,MTH_TM_ID 
			,src_sys_cd 
			,CONSM_PRD_TREATMNT_CD 
			,SML_BUS_F 
			,PIT_STAT_CD 
			,TRNST_EXCLSN_F 
			,unq_acct_id 
			,PD_MODEL_NM 
			,PD_ACCT_SCORE
			,PD_BASEL_SEG_NUM
			,LGD_MODEL_NM 
			,LGD_ACCT_SCORE 
			,EAD_MODEL_NM 
			,EAD_ACCT_SCORE 
			,PRIM_CUST_CID as INST_FACT_PRIM_CUST_CID
			,SCRTY_TP_DESC
from EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT 
where src_sys_cd in ('KS','SPL','MOR') and CONSM_PRD_TREATMNT_CD='A' 
	and SML_BUS_F='N'
	and PIT_STAT_CD in ('CUR','DEF')
	and TRNST_EXCLSN_F='N'
	and MTH_TM_ID = &mth_tm_id.);
quit;

proc sql noprint;
	select count(1) into :sas_count
	from TRIAD.BASEL_ANALYTCL_BL_INSTRMNT_FACT;
quit;

data _null_;
	sas_count=&sas_count.; 
	db2_count=&db2_count.;
	PUT 'The following counts were exported from DB2 and imported into SAS:';
	PUT DB2_COUNT= SAS_COUNT=;
	if sas_count ne db2_count then abort;
run;

proc sql;
connect USING nzrrap as nzcon;
create table triad.BASEL_CUST_ACCT_RLTNP_SNAPSHOT as select * from connection to nzcon 
(select t.TM_LVL_END_DT, a.*, b.CUST_CID as PRIM_CUST_CID, b.CIF_KEY, c.acct_num as acct_num_dim
	from EDRTLRP1D.BASEL_CUST_ACCT_RLTNP_SNAPSHOT a 
		left join EDRTLRP1D.BASEL_CUST_DIM b on a.BASEL_CUST_ID=b.BASEL_CUST_ID 
		left join EDRTLRP1D.BASEL_ACCT_DIM c on a.BASEL_ACCT_ID=c.BASEL_ACCT_ID
		left join EDRTLRP1D.TM_DIM t on a.MTH_TM_ID=t.TM_ID
	where a.MTH_TM_ID=&mth_tm_id. and a.PRIM_CUST_F='Y'
	order by a.BASEL_ACCT_ID);
quit;

proc datasets lib=triad;
	modify BASEL_CUST_ACCT_RLTNP_SNAPSHOT;
	index create basel_acct_id / unique;
run;

proc datasets lib=triad;
	modify BASEL_ANALYTCL_BL_INSTRMNT_FACT;
	index create basel_acct_id / unique;
run;