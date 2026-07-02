%coll_autoexec;
%include "&coll_dir./sas/collection_models/email_communication.sas";

/*Stage 1*/
/*Creating snapshots for each table for the defined attributes*/

proc sql noprint;
select max(eff_tm_id) into :itlc_spl_dly from sascoll.itlc_spl_dly_snapshot_attr_check;
connect using db2prod as dbcon;
create table itlc_spl_dly_snapshot_attr_check as 
select * from connection to dbcon(
SELECT eff_tm_Id, tm_lvl_end_dt as process_dt, COUNT(CASE WHEN day_overdue between 1 and 89 then 1 ELSE NULL END) AS day_overdue_count,
count(CASE WHEN replace(commercial_loan_cd,' ','') = '0' then 1 ELSE NULL END) AS commercial_loan_cd_count,
count(CASE WHEN replace(recd_stat_cd,' ','') = '4' THEN 1 ELSE NULL END)  AS recd_stat_cd_count,
count(CASE WHEN prncpl_bal_amt+accr_intr_amt > 0 then 1 ELSE NULL END) AS total_amount_count,
count(CASE when scrty_cd <> 18 THEN 1 ELSE NULL END) AS scrty_cd_count,
COUNT(CASE WHEN (SUBSTR(TO_CHAR(scrty_cd), LENGTH(TO_CHAR(scrty_cd)), 1)) in ('7','8') THEN 1 ELSE NULL END) AS last_digit_scrty_cd_count
FROM edrtlr.PSNL_LOAN_DLY_SNAPSHOT
left outer join edrtlr.tm_dim on eff_tm_Id=tm_id
where eff_tm_id>&itlc_spl_dly
GROUP BY eff_tm_Id, tm_lvl_end_dt
ORDER BY eff_tm_Id, tm_lvl_end_dt);
disconnect from dbcon;
quit;

proc append base=sascoll.itlc_spl_dly_snapshot_attr_check data=itlc_spl_dly_snapshot_attr_check force;run;

proc sql noprint;
select max(mth_tm_Id) into :itlc_custacct_rltnp from sascoll.itlc_cust_acct_rltnp_attr_check;
connect using db2prod as dbcon;
create table itlc_cust_acct_rltnp_attr_check as 
select * from connection to dbcon(
select mth_tm_Id, tm_lvl_end_dt as process_dt, count(case when prim_cust_f='Y' then 1 else null end) as prim_cust_count
from edrtlr.risk_cust_acct_rltnp_snapshot 
left outer join edrtlr.tm_dim on mth_tm_Id=tm_Id and tm_lvl='Month'
where mth_tm_Id>&itlc_custacct_rltnp
group by mth_tm_Id, tm_lvl_end_dt
order by mth_tm_id, tm_lvl_end_dt
);
disconnect from dbcon;
quit;

proc append base=sascoll.itlc_cust_acct_rltnp_attr_check data=itlc_cust_acct_rltnp_attr_check force;run;

proc sql noprint;
select max(mth_tm_id) into :itlc_deli from sascoll.itlc_deli_file_attr_check;
connect using db2prod as dbcon;
create table itlc_deli_file_attr_check as 
select * from connection to dbcon(
select mth_tm_Id, tm_lvl_end_dt as process_dt, count(*) as deli_count
from edrtlr.RISK_CR_BUREAU_DELI_MTH_SNAPSHOT 
left outer join edrtlr.tm_dim on mth_tm_Id=tm_Id and tm_lvl='Month'
where mth_tm_Id>&itlc_deli
group by mth_tm_Id, tm_lvl_end_dt
order by mth_tm_Id, tm_lvl_end_dt
);
disconnect from dbcon;
quit;

proc append base=sascoll.itlc_deli_file_attr_check data=itlc_deli_file_attr_check force;run;


proc sql noprint;
select max(eff_tm_id) into :ccc_ks_dly from sascoll.ccc_revl_cr_dly_snap_attr_check;
connect using db2prod as dbcon;
create table ccc_revl_cr_dly_snap_attr_check as 
select * from connection to dbcon(
select eff_tm_id, tm_lvl_end_dt as process_dt, 
count(case when substr(PRD_CD, 1, 1) in ('A','V') then 1 else null end) as prd_cd_count,
count(case when BNS_DLQNT_DAY > 0  and  BNS_DLQNT_DAY <=89 then 1 else null end) as day89dlqnt_count,
count(case when BNS_DLQNT_DAY > 0  and  BNS_DLQNT_DAY <=119 then 1 else null end) as day119dlqnt_count,
count(case when coalesce(PRD_CD,'') <> 'VIC' then 1 else null end) as nonVIC_prd_cd_count
from edrtlr.risk_revlvng_cr_dly_snapshot 
left outer join edrtlr.tm_dim on eff_tm_Id=tm_Id
where eff_tm_Id>&ccc_ks_dly
group by eff_tm_Id, tm_lvl_end_dt
order by eff_tm_Id, tm_lvl_end_dt
);
disconnect from dbcon;
quit;

proc append base=sascoll.ccc_revl_cr_dly_snap_attr_check data=ccc_revl_cr_dly_snap_attr_check force;run;


proc sql noprint;
select max(mth_tm_Id) into :ccc_instr_fact from sascoll.ccc_basel_instr_fact_attr_check;
connect using db2prod as dbcon;
create table ccc_basel_instr_fact_attr_check as 
select * from connection to dbcon(
select mth_tm_Id, tm_lvl_end_dt as process_dt, count(*) as KS_count
from edrrapt.basel_analytcl_bl_instrmnt_fact
left outer join edrtlr.tm_dim on mth_tm_Id=tm_Id and tm_lvl='Month'
where src_sys_cd='KS' and mth_tm_Id>&ccc_instr_fact
group by mth_tm_Id, tm_lvl_end_dt
order by mth_tm_Id, tm_lvl_end_dt
);
disconnect from dbcon;
quit;

proc append base=sascoll.ccc_basel_instr_fact_attr_check data=ccc_basel_instr_fact_attr_check force;run;

proc sql noprint;
connect using db2prod as dbcon;
select max(eff_tm_Id) into :ulocc_ks_dly from sascoll.ulocc_rev_cr_dly_snap_attr_check;
create table ulocc_rev_cr_dly_snap_attr_check as 
select * from connection to dbcon(
select eff_tm_id, tm_lvl_end_dt as process_dt,
count(case when (CR_LMT_AMT >0 or TOT_NEW_BAL_AMT >0) then 1 else null end) as Total_amount,
count(case when prd_cd not in 'BLV' then 1 else null end) as prod_code_count,
count(case when replace(sub_prd_cd,' ','') <> 'CC' then 1 else null end) as sub_prd_cd_count,
count(case when prd_cd in ('VIC','SCL') then 1 else null end) as delq_prd_cd_count,
count(case when dlqnt_day between 30 and 119 then 1 else null end) as delq_dlqnt_day_count,
count(case when sub_prd_cd = 'RS' or STEP_IND in ('Y','R') then null else 1 end) as ind_heloc_count,
count(case when replace(CHRG_OFF_CD,' ','') not in ('1','2','C','N', 'P','Q') then 1 else null end) as chrg_off_code_count,
count(case when ltrim(COALESCE(BLOCK_CD,NULL,' '))||ltrim(COALESCE(RECL_CD,NULL,' ')) not in ('B4','SF','S','SS','XS','SP','B5','D','SR') then 1 else null end) as block_recl_count
from edrtlrt.ULOC_REVLVNG_CR_DLY_SNAPSHOT
left outer join edrtlr.tm_dim on eff_tm_Id=tm_Id
where eff_tm_Id>&ulocc_ks_dly
group by eff_tm_Id, tm_lvl_end_dt
order by eff_tm_Id, tm_lvl_end_dt
);
disconnect from dbcon;
quit;

proc append base=sascoll.ulocc_rev_cr_dly_snap_attr_check data=ulocc_rev_cr_dly_snap_attr_check force;run;

proc sql noprint;
select max(eff_tm_Id) into :ulocc_spl_dly from sascoll.ulocc_psnl_loan_dly_attr_check;
connect using db2prod as dbcon;
create table ulocc_psnl_loan_dly_attr_check as 
select * from connection to dbcon(
select eff_tm_Id, tm_lvl_end_dt as process_dt, 
count(case when replace(PROC_TRANSIT,' ','') not in ('4036','71506','95042','99432') then 1 else null end) as proc_transit_count,
count(case when replace(RECD_STAT_CD,' ','') in ('4','7','8') then 1 else null end) as recd_stat_cd_count
from edrtlr.psnl_loan_dly_snapshot
left outer join edrtlr.tm_dim on eff_tm_Id=tm_Id
where eff_tm_Id>&ulocc_spl_dly
group by eff_tm_Id, tm_lvl_end_dt
order by eff_tm_Id, tm_lvl_end_dt
);
disconnect from dbcon;
quit;

proc append base=sascoll.ulocc_psnl_loan_dly_attr_check data=ulocc_psnl_loan_dly_attr_check force;run;

proc sql noprint;
select max(eff_tm_id) into :ulocc_mort from sascoll.ulocc_basel_mort_attr_check;
connect using db2prod as dbcon;
create table ulocc_basel_mort_attr_check as 
select * from connection to dbcon(
select eff_tm_Id, tm_lvl_end_dt as process_dt, 
count(case when comm_tp = 'Residential' then 1 else null end) as comm_tp_count,
count(case when Crnt_Bal >0 then 1 else null end) as crnt_bal_count,
count(case when pd_off_f='N' then 1 else null end) as pd_off_f_count
from edrtlr.basel_mort
left outer join edrtlr.tm_dim on eff_tm_Id=tm_Id
where eff_tm_Id>&ulocc_mort
group by eff_tm_Id, tm_lvl_end_dt
order by eff_tm_Id, tm_lvl_end_dt
);
disconnect from dbcon;
quit;

proc append base=sascoll.ulocc_basel_mort_attr_check data=ulocc_basel_mort_attr_check force;run;

proc sql noprint;
select max(posted_dt_tm_key) into :ulocc_sav_txn from sascoll.ulocc_sav_dda_txn_attr_check;
connect using db2star as dbcon;
create table ulocc_sav_dda_txn_attr_check as 
select * from connection to dbcon(
select POSTED_DT_TM_KEY,
count(case when txn_cd in ('804','894') then 1 else null end) as txn_cd_set1_count,
count(case when txn_cd in ('110','120','140','141','202','220','292','321','552','730','740','895','898',
'900','901','903','905','933','934','935','941','942','943','944','946','947','951','952',
'953','954','959','960','975','983','988','990','991','992','993','994','995') then 1 else null end) as txn_cd_set2_count
from ededtt.saving_and_dda_txn_fact
where POSTED_DT_TM_KEY>&ulocc_sav_txn
group by POSTED_DT_TM_KEY
order by POSTED_DT_TM_KEY
);
disconnect from dbcon;
quit;

/*NEEDED AN EXTRA MERGE AS OWSTAR DOESNT HAVE A TIME DIMENSION DEDICATED FOR DATAMART PROCESSING*/
proc sql noprint;
create table ulocc_sav_dda_txn_attr_check1 as 
select a.*, b.tm_lvl_end_dt as process_dt from ulocc_sav_dda_txn_attr_check a
left outer join db2prod.tm_dim b on a.POSTED_DT_TM_KEY=b.tm_id;
quit;

proc append base=sascoll.ulocc_sav_dda_txn_attr_check data=ulocc_sav_dda_txn_attr_check1 force;run;


/*custom delta process is needed for dimensions */
proc sql noprint;
connect using db2prod as dbcon;
create table ulocc_acct_dim_attr_check as 
select * from connection to dbcon(
select current date as snapshot_date, count(acct_id) as account_count
from ededw.acct_dim where cis_prd_cd in ('SCL','VIC') and del_tmstmp is null
);
disconnect from dbcon;
quit;

proc append base=sascoll.ulocc_acct_dim_attr_check data=ulocc_acct_dim_attr_check force;run;

proc sql noprint;
connect using db2prod as dbcon;
create table ulocc_sav_acct_dim_attr_check as 
select * from connection to dbcon(
select current date as snapshot_date,
count(*) as account_count,
count(case when acct_stat_cd='A' then 1 else null end) as acct_stat_cd_count
from edrds.sav_acct_dim
);
disconnect from dbcon;
quit;

proc append base=sascoll.ulocc_sav_acct_dim_attr_check data=ulocc_sav_acct_dim_attr_check force;run;

/*Stage 2*/
/*Aggregation for each attribute*/

proc sort data=sascoll.itlc_spl_dly_snapshot_attr_check;by eff_tm_id;run;

data sascoll.itlc_spl_dly_attr_check_aggr;
  set sascoll.itlc_spl_dly_snapshot_attr_check;
  DAY_OVERDUE_COUNT_delta=DAY_OVERDUE_COUNT-lag1(DAY_OVERDUE_COUNT);
  COMMERCIAL_LOAN_CD_COUNT_delta=COMMERCIAL_LOAN_CD_COUNT-lag1(COMMERCIAL_LOAN_CD_COUNT);
  RECD_STAT_CD_COUNT_delta=RECD_STAT_CD_COUNT-lag1(RECD_STAT_CD_COUNT);
  TOTAL_AMOUNT_COUNT_delta=TOTAL_AMOUNT_COUNT-lag1(TOTAL_AMOUNT_COUNT);
  SCRTY_CD_COUNT_delta=SCRTY_CD_COUNT-lag1(SCRTY_CD_COUNT);
  LAST_DIGIT_SCRTY_CD_COUNT_delta=LAST_DIGIT_SCRTY_CD_COUNT-lag1(LAST_DIGIT_SCRTY_CD_COUNT);
  DAY_OVERDUE_COUNT_chng=round(DAY_OVERDUE_COUNT_delta/lag1(DAY_OVERDUE_COUNT),0.00001);
  COMMERCIAL_LOAN_CD_COUNT_chng=round(COMMERCIAL_LOAN_CD_COUNT_delta/lag1(COMMERCIAL_LOAN_CD_COUNT),0.00001);
  RECD_STAT_CD_COUNT_chng=round(RECD_STAT_CD_COUNT_delta/lag1(RECD_STAT_CD_COUNT),0.00001);
  TOTAL_AMOUNT_COUNT_chng=round(TOTAL_AMOUNT_COUNT_delta/lag1(TOTAL_AMOUNT_COUNT),0.00001);
  SCRTY_CD_COUNT_chng=round(SCRTY_CD_COUNT_delta/lag1(SCRTY_CD_COUNT),0.00001);
  LAST_DIGIT_SCRTY_CD_COUNT_chng=round(LAST_DIGIT_SCRTY_CD_COUNT_delta/lag1(LAST_DIGIT_SCRTY_CD_COUNT),0.00001);
  COMMERCIAL_LOAN_CD_CYCLE_chng=COMMERCIAL_LOAN_CD_COUNT-lag30(COMMERCIAL_LOAN_CD_COUNT);
  SCRTY_CD_CYCLE_chng=SCRTY_CD_COUNT-lag30(SCRTY_CD_COUNT);
  LAST_DIGIT_SCRTY_CD_CYCLE_chng=LAST_DIGIT_SCRTY_CD_COUNT-lag30(LAST_DIGIT_SCRTY_CD_COUNT);
format DAY_OVERDUE_COUNT_chng percent10.2;
format COMMERCIAL_LOAN_CD_COUNT_chng percent10.2;
format RECD_STAT_CD_COUNT_chng percent10.2;
format TOTAL_AMOUNT_COUNT_chng percent10.2;
format SCRTY_CD_COUNT_chng percent10.2;
format LAST_DIGIT_SCRTY_CD_COUNT_chng percent10.2;
run;

proc sort data=sascoll.itlc_cust_acct_rltnp_attr_check; by mth_tm_id;run;

data sascoll.itlc_cust_acct_rltnp_aggr;
set sascoll.itlc_cust_acct_rltnp_attr_check;
prim_cust_count_delta=prim_cust_count-lag1(prim_cust_count);
prim_cust_count_chng=round(prim_cust_count_delta/lag1(prim_cust_count),0.00001);
format prim_cust_count_chng percent10.2;
run;

proc sort data=sascoll.ulocc_rev_cr_dly_snap_attr_check; by eff_tm_id;run;

data sascoll.ulocc_rev_cr_dly_snap_aggr;
set sascoll.ulocc_rev_cr_dly_snap_attr_check;
Total_amount_delta=Total_amount-lag1(Total_amount);
prod_code_count_delta=prod_code_count-lag1(prod_code_count);
sub_prd_cd_count_delta=sub_prd_cd_count-lag1(sub_prd_cd_count);
delq_prd_cd_count_delta=delq_prd_cd_count-lag1(delq_prd_cd_count);
delq_dlqnt_day_count_delta=delq_dlqnt_day_count-lag1(delq_dlqnt_day_count);
ind_heloc_count_delta=ind_heloc_count-lag1(ind_heloc_count);
chrg_off_code_count_delta=chrg_off_code_count-lag1(chrg_off_code_count);
block_recl_count_delta=block_recl_count-lag1(block_recl_count);

Total_amount_chng=round(Total_amount_delta/lag1(Total_amount),0.00001);
prod_code_count_chng=round(prod_code_count_delta/lag1(prod_code_count),0.00001);
sub_prd_cd_count_chng=round(sub_prd_cd_count_delta/lag1(sub_prd_cd_count),0.00001);
delq_prd_cd_count_chng=round(delq_prd_cd_count_delta/lag1(delq_prd_cd_count),0.00001);
delq_dlqnt_day_count_chng=round(delq_dlqnt_day_count_delta/lag1(delq_dlqnt_day_count),0.00001);
ind_heloc_count_chng=round(ind_heloc_count_delta/lag1(ind_heloc_count),0.00001);
chrg_off_code_count_chng=round(chrg_off_code_count_delta/lag1(chrg_off_code_count),0.00001);
block_recl_count_chng=round(block_recl_count_delta/lag1(block_recl_count),0.00001);

Total_amount_cycle_chng=Total_amount-lag7(Total_amount);
prod_code_cycle_chng=prod_code_count-lag7(prod_code_count);
sub_prd_cd_cycle_chng=sub_prd_cd_count-lag7(sub_prd_cd_count);
delq_prd_cd_cycle_chng=delq_prd_cd_count-lag30(delq_prd_cd_count);
ind_heloc_cycle_chng=ind_heloc_count-lag7(ind_heloc_count);
chrg_off_code_cycle_chng=chrg_off_code_count-lag7(chrg_off_code_count);
block_recl_cycle_chng=block_recl_count-lag7(block_recl_count);

format Total_amount_chng percent10.2;
format prod_code_count_chng percent10.2;
format sub_prd_cd_count_chng percent10.2;
format delq_prd_cd_count_chng percent10.2;
format delq_dlqnt_day_count_chng percent10.2;
format ind_heloc_count_chng percent10.2;
format chrg_off_code_count_chng percent10.2;
format block_recl_count_chng percent10.2;

run;

proc sort data=sascoll.ulocc_psnl_loan_dly_attr_check; by eff_tm_id; run;

data sascoll.ulocc_psnl_loan_dly_aggr;
set sascoll.ulocc_psnl_loan_dly_attr_check;
proc_transit_count_delta=proc_transit_count-lag1(proc_transit_count);
recd_stat_cd_count_delta=recd_stat_cd_count-lag1(recd_stat_cd_count);
proc_transit_count_chng=round(proc_transit_count_delta/lag1(proc_transit_count),0.00001);
recd_stat_cd_count_chng=round(recd_stat_cd_count_delta/lag1(recd_stat_cd_count),0.00001);
proc_transit_cycle_chng=proc_transit_count-lag35(proc_transit_count);
format proc_transit_count_chng percent10.2;
format recd_stat_cd_count_chng percent10.2;
run;

proc sort data=sascoll.ulocc_basel_mort_attr_check; by eff_tm_id;run;

data sascoll.ulocc_basel_mort_aggr;
set sascoll.ulocc_basel_mort_attr_check;
comm_tp_count_delta=comm_tp_count-lag1(comm_tp_count);
crnt_bal_count_delta=crnt_bal_count-lag1(crnt_bal_count);
pd_off_f_count_delta=pd_off_f_count-lag1(pd_off_f_count);
comm_tp_count_chng=round(comm_tp_count_delta/lag1(comm_tp_count),0.00001);
crnt_bal_count_chng=round(crnt_bal_count_delta/lag1(crnt_bal_count),0.00001);
pd_off_f_count_chng=round(pd_off_f_count_delta/lag1(pd_off_f_count),0.00001);
format comm_tp_count_chng percent10.2;
format crnt_bal_count_chng percent10.2;
format pd_off_f_count_chng percent10.2;
run;

proc sort data=sascoll.ulocc_sav_dda_txn_attr_check; by POSTED_DT_TM_KEY;run;

data sascoll.ulocc_sav_dda_txn_aggr;
set sascoll.ulocc_sav_dda_txn_attr_check;
/*process_dt=datepart(EFF_DT_TM);*/
txn_cd_set1_count_delta=txn_cd_set1_count-lag1(txn_cd_set1_count);
txn_cd_set2_count_delta=txn_cd_set2_count-lag1(txn_cd_set2_count);
txn_cd_set1_count_chng=round(txn_cd_set1_count_delta/lag1(txn_cd_set1_count),0.00001);
txn_cd_set2_count_chng=round(txn_cd_set2_count_delta/lag1(txn_cd_set2_count),0.00001);
/*format process_dt date10.;*/
format txn_cd_set1_count_chng percent10.2;
format txn_cd_set2_count_chng percent10.2;
run;

proc sort data=sascoll.itlc_deli_file_attr_check; by mth_tm_id;run;

data sascoll.itlc_deli_file_aggr;
set sascoll.itlc_deli_file_attr_check;
deli_count_delta=deli_count-lag1(deli_count);
deli_count_chng=round(deli_count_delta/lag1(deli_count),0.00001);
format deli_count_chng percent10.2;
run;

proc sort data=sascoll.ccc_revl_cr_dly_snap_attr_check; by eff_tm_id;run;

data sascoll.ccc_revl_cr_dly_snap_aggr;
set sascoll.ccc_revl_cr_dly_snap_attr_check;
prd_cd_count_delta=prd_cd_count-lag1(prd_cd_count);
day89dlqnt_count_delta=day89dlqnt_count-lag1(day89dlqnt_count);
day119dlqnt_count_delta=day119dlqnt_count-lag1(day119dlqnt_count);
nonvic_prd_cd_count_delta=nonvic_prd_cd_count-lag1(nonvic_prd_cd_count);
prd_cd_count_chng=round(prd_cd_count_delta/lag1(prd_cd_count),0.00001);
day89dlqnt_count_chng=round(day89dlqnt_count_delta/lag1(day89dlqnt_count),0.00001);
day119dlqnt_count_chng=round(day119dlqnt_count_delta/lag1(day119dlqnt_count),0.00001);
nonvic_prd_cd_count_chng=round(nonvic_prd_cd_count_delta/lag1(nonvic_prd_cd_count),0.00001);
prd_cd_count_cycle_chng=prd_cd_count-lag30(prd_cd_count);
nonvic_prd_cd_count_cycle_chng=nonvic_prd_cd_count-lag30(nonvic_prd_cd_count);
format prd_cd_count_chng percent10.2;
format day89dlqnt_count_chng percent10.2;
format day119dlqnt_count_chng percent10.2;
format nonvic_prd_cd_count_chng percent10.2;
run;

proc sort data=sascoll.ccc_basel_instr_fact_attr_check; by mth_tm_id;run;

data sascoll.ccc_basel_instr_fact_aggr;
set sascoll.ccc_basel_instr_fact_attr_check;
ks_count_delta=ks_count-lag1(ks_count);
ks_count_chng=round(ks_count_delta/lag1(ks_count),0.00001);
format ks_count_chng percent10.2;
run;

/*Stage 3*/
/*Prep for reporting*/
/*BRING TABLE FROM DB2*/

DATA SASCOLL.COLLECTION_MODELS_THRESHOLD;
SET DB2PROD.COLLECTION_MODELS_THRESHOLD;
RUN;

/*Step 1 - define the time key that is being checked - latest time key in that table*/

PROC SQL;
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET 
 TIME_ID = (SELECT MAX(EFF_TM_ID) FROM SASCOLL.ccc_revl_cr_dly_snap_aggr), 
 EFFECTIVE_DATE = (SELECT MAX(PROCESS_DT) FROM SASCOLL.ccc_revl_cr_dly_snap_aggr)
WHERE TABLE_NAME='EDRTLRT.risk_revlvng_cr_dly_snapshot';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 TIME_ID = (SELECT MAX(MTH_TM_ID) FROM SASCOLL.ccc_basel_instr_fact_aggr),
 EFFECTIVE_DATE = (SELECT MAX(PROCESS_DT) FROM SASCOLL.ccc_basel_instr_fact_aggr)
WHERE TABLE_NAME='EDRRAPT.basel_analytcl_bl_instrmnt_fact';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 TIME_ID = (SELECT MAX(EFF_TM_ID) FROM SASCOLL.itlc_spl_dly_attr_check_aggr),
 EFFECTIVE_DATE = (SELECT MAX(PROCESS_DT) FROM SASCOLL.itlc_spl_dly_attr_check_aggr)
WHERE TABLE_NAME='EDRTLRT.psnl_loan_dly_snapshot';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 TIME_ID = (SELECT MAX(MTH_TM_ID) FROM SASCOLL.itlc_cust_acct_rltnp_aggr),
 EFFECTIVE_DATE = (SELECT MAX(PROCESS_DT) FROM SASCOLL.itlc_cust_acct_rltnp_aggr)
WHERE TABLE_NAME='EDRTLRT.risk_cust_acct_rltnp_snapshot';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 TIME_ID = (SELECT MAX(EFF_TM_ID) FROM SASCOLL.ulocc_rev_cr_dly_snap_aggr),
 EFFECTIVE_DATE = (SELECT MAX(PROCESS_DT) FROM SASCOLL.ulocc_rev_cr_dly_snap_aggr)
WHERE TABLE_NAME='EDRTLRT.uloc_revlvng_cr_dly_snapshot';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 TIME_ID = (SELECT MAX(EFF_TM_ID) FROM SASCOLL.ulocc_basel_mort_aggr),
 EFFECTIVE_DATE = (SELECT MAX(PROCESS_DT) FROM SASCOLL.ulocc_basel_mort_aggr)
WHERE TABLE_NAME='EDRTLRT.basel_mort';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 TIME_ID = (SELECT MAX(MTH_TM_ID) FROM SASCOLL.itlc_deli_file_aggr),
 EFFECTIVE_DATE = (SELECT MAX(PROCESS_DT) FROM SASCOLL.itlc_deli_file_aggr)
WHERE TABLE_NAME='EDRTLRT.risk_cr_bureau_deli_mth_snapshot';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 TIME_ID = (SELECT MAX(POSTED_DT_TM_KEY) FROM SASCOLL.ulocc_sav_dda_txn_aggr),
 EFFECTIVE_DATE = (SELECT MAX(PROCESS_DT) FROM SASCOLL.ulocc_sav_dda_txn_aggr)
WHERE TABLE_NAME='EDEDTT.saving_and_dda_txn_fact';

/*DIMENSION TABLES DONT HAVE A TIME KEY CHECK STRATEGY*/

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 EFFECTIVE_DATE = "&SYSDATE"D
WHERE TABLE_NAME IN ('EDEDW.acct_dim','EDRDS.sav_acct_dim');
QUIT;

/*UPDATE THRESHOLD TABLE WITH EXPECTED DATES OF DATA FOR ALL TABLES EXCEPT DIMENSIONS*/
data A;
/*birthd='20oct2018'd;*/
  birthd="&SYSDATE"D;
  DAYOFMONTH=DAY(BIRTHD);
  DAY=PUT(birthd,DOWNAME.);
  CURRENT_DATE = COMPRESS(PUT(birthd,yymmdd10.),'-');
  	IF STRIP(DAY)='Monday' then ITLC_DATE=INTNX('DAY',birthd,-2);
		ELSE ITLC_DATE=INTNX('DAY',birthd,-1);
	IF STRIP(DAY)='Monday' then ULOC_DATE=INTNX('DAY',birthd,-3);
        ELSE ULOC_DATE=INTNX('DAY',birthd,-1);
	IF DAYOFMONTH<=20 THEN MONTH_END_DATE=INTNX('MONTH',birthd,-2,'END');
		ELSE MONTH_END_DATE=INTNX('MONTH',birthd,-1,'END');
  FORMAT ITLC_DATE DATE9.;
  FORMAT ULOC_DATE DATE9.;
  FORMAT MONTH_END_DATE DATE9.;
  FORMAT DAY $15.;
run;

PROC SQL NOPRINT;
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET EXPECTED_DATE = (SELECT ITLC_DATE FROM A) WHERE PORTFOLIO='ITLC' AND TABLE_NAME LIKE '%dly%';
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET EXPECTED_DATE = (SELECT ULOC_DATE FROM A) WHERE PORTFOLIO='ULOCC' AND TABLE_NAME LIKE '%uloc%dly%';
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET EXPECTED_DATE = (SELECT ITLC_DATE FROM A) WHERE PORTFOLIO='ULOCC' AND (TABLE_NAME LIKE '%psnl_loan%' OR TABLE_NAME LIKE '%saving%');
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET EXPECTED_DATE = (SELECT ULOC_DATE FROM A) WHERE PORTFOLIO='CCC' AND TABLE_NAME LIKE '%dly%';
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET EXPECTED_DATE = (SELECT MONTH_END_DATE FROM A) WHERE 
	TABLE_NAME LIKE 'EDRRAPT.%' OR TABLE_NAME IN ('EDRTLRT.risk_cust_acct_rltnp_snapshot','EDRTLRT.risk_cr_bureau_deli_mth_snapshot',
	'EDRTLRT.basel_mort');
QUIT;

/*ITLC ATTRIBUTES CHANGE UPDATE*/

PROC SQL NOPRINT;
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT DAY_OVERDUE_COUNT_chng FROM SASCOLL.itlc_spl_dly_attr_check_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.psnl_loan_dly_snapshot' AND ATTRIBUTE_NAME='day_overdue' AND PORTFOLIO='ITLC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT COMMERCIAL_LOAN_CD_COUNT_chng FROM SASCOLL.itlc_spl_dly_attr_check_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.psnl_loan_dly_snapshot' AND ATTRIBUTE_NAME='commercial_loan_cd' AND PORTFOLIO='ITLC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT RECD_STAT_CD_COUNT_chng FROM SASCOLL.itlc_spl_dly_attr_check_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.psnl_loan_dly_snapshot' AND ATTRIBUTE_NAME='recd_stat_code' AND PORTFOLIO='ITLC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT TOTAL_AMOUNT_COUNT_chng FROM SASCOLL.itlc_spl_dly_attr_check_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.psnl_loan_dly_snapshot' AND ATTRIBUTE_NAME='bal_intr_amount' AND PORTFOLIO='ITLC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT SCRTY_CD_COUNT_chng FROM SASCOLL.itlc_spl_dly_attr_check_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.psnl_loan_dly_snapshot' AND ATTRIBUTE_NAME='scrty_cd' AND PORTFOLIO='ITLC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT LAST_DIGIT_SCRTY_CD_COUNT_chng FROM SASCOLL.itlc_spl_dly_attr_check_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.psnl_loan_dly_snapshot' AND ATTRIBUTE_NAME='last_digit_scrty_cd' AND PORTFOLIO='ITLC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT prim_cust_count_chng FROM SASCOLL.itlc_cust_acct_rltnp_aggr HAVING MTH_TM_iD=MAX(MTH_TM_iD))
WHERE TABLE_NAME='EDRTLRT.risk_cust_acct_rltnp_snapshot' AND ATTRIBUTE_NAME='prim_cust' AND PORTFOLIO='ITLC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT deli_count_chng FROM SASCOLL.itlc_deli_file_aggr HAVING MTH_TM_iD=MAX(MTH_TM_iD))
WHERE TABLE_NAME='EDRTLRT.risk_cr_bureau_deli_mth_snapshot' AND ATTRIBUTE_NAME='deli count' AND PORTFOLIO='ITLC';
QUIT;


/*CCC ATTRIBUTES CHANGE UPDATE*/
PROC SQL NOPRINT;
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT prd_cd_count_chng FROM SASCOLL.ccc_revl_cr_dly_snap_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.risk_revlvng_cr_dly_snapshot' AND ATTRIBUTE_NAME='prd_cd' AND PORTFOLIO='CCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT day89dlqnt_count_chng FROM SASCOLL.ccc_revl_cr_dly_snap_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))/*17444 FOR TESTING*/
WHERE TABLE_NAME='EDRTLRT.risk_revlvng_cr_dly_snapshot' AND ATTRIBUTE_NAME='day89dlqnt' AND PORTFOLIO='CCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT day119dlqnt_count_chng FROM SASCOLL.ccc_revl_cr_dly_snap_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.risk_revlvng_cr_dly_snapshot' AND ATTRIBUTE_NAME='day119dlqnt' AND PORTFOLIO='CCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT nonvic_prd_cd_count_chng FROM SASCOLL.ccc_revl_cr_dly_snap_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))/*17082 FOR TESTING*/
WHERE TABLE_NAME='EDRTLRT.risk_revlvng_cr_dly_snapshot' AND ATTRIBUTE_NAME='nonvic_prd_cd' AND PORTFOLIO='CCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT ks_count_chng FROM SASCOLL.ccc_basel_instr_fact_aggr HAVING MTH_TM_iD=MAX(MTH_TM_iD))
WHERE TABLE_NAME='EDRRAPT.basel_analytcl_bl_instrmnt_fact' AND ATTRIBUTE_NAME='ks_count' AND PORTFOLIO='CCC';

QUIT;

/*ULOCC ATTRIBUTES CHANGE UPDATE*/
PROC SQL NOPRINT;
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT Total_amount_chng FROM SASCOLL.ulocc_rev_cr_dly_snap_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.uloc_revlvng_cr_dly_snapshot' AND ATTRIBUTE_NAME='Total_amount' AND PORTFOLIO='ULOCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT prod_code_count_chng FROM SASCOLL.ulocc_rev_cr_dly_snap_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.uloc_revlvng_cr_dly_snapshot' AND ATTRIBUTE_NAME='prod_code' AND PORTFOLIO='ULOCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT sub_prd_cd_count_chng FROM SASCOLL.ulocc_rev_cr_dly_snap_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.uloc_revlvng_cr_dly_snapshot' AND ATTRIBUTE_NAME='sub_prd_cd' AND PORTFOLIO='ULOCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT delq_prd_cd_count_chng FROM SASCOLL.ulocc_rev_cr_dly_snap_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.uloc_revlvng_cr_dly_snapshot' AND ATTRIBUTE_NAME='delq_prd_cd' AND PORTFOLIO='ULOCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT delq_dlqnt_day_count_chng FROM SASCOLL.ulocc_rev_cr_dly_snap_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.uloc_revlvng_cr_dly_snapshot' AND ATTRIBUTE_NAME='delq_dlqnt_day' AND PORTFOLIO='ULOCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT ind_heloc_count_chng FROM SASCOLL.ulocc_rev_cr_dly_snap_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.uloc_revlvng_cr_dly_snapshot' AND ATTRIBUTE_NAME='ind_heloc' AND PORTFOLIO='ULOCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT chrg_off_code_count_chng FROM SASCOLL.ulocc_rev_cr_dly_snap_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.uloc_revlvng_cr_dly_snapshot' AND ATTRIBUTE_NAME='chrg_off_code' AND PORTFOLIO='ULOCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT block_recl_count_chng FROM SASCOLL.ulocc_rev_cr_dly_snap_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.uloc_revlvng_cr_dly_snapshot' AND ATTRIBUTE_NAME='block_recl' AND PORTFOLIO='ULOCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT proc_transit_count_chng FROM SASCOLL.ulocc_psnl_loan_dly_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.psnl_loan_dly_snapshot' AND ATTRIBUTE_NAME='proc_transit' AND PORTFOLIO='ULOCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT recd_stat_cd_count_chng FROM SASCOLL.ulocc_psnl_loan_dly_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.psnl_loan_dly_snapshot' AND ATTRIBUTE_NAME='recd_stat_cd' AND PORTFOLIO='ULOCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT comm_tp_count_chng FROM SASCOLL.ulocc_basel_mort_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.basel_mort' AND ATTRIBUTE_NAME='comm_tp' AND PORTFOLIO='ULOCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT crnt_bal_count_chng FROM SASCOLL.ulocc_basel_mort_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.basel_mort' AND ATTRIBUTE_NAME='crnt_bal' AND PORTFOLIO='ULOCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT pd_off_f_count_chng FROM SASCOLL.ulocc_basel_mort_aggr HAVING EFF_TM_iD=MAX(EFF_TM_iD))
WHERE TABLE_NAME='EDRTLRT.basel_mort' AND ATTRIBUTE_NAME='pd_off_f' AND PORTFOLIO='ULOCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT txn_cd_set1_count_chng FROM SASCOLL.ulocc_sav_dda_txn_aggr HAVING POSTED_DT_TM_KEY=MAX(POSTED_DT_TM_KEY))
WHERE TABLE_NAME='EDEDTT.saving_and_dda_txn_fact' AND ATTRIBUTE_NAME='txn_cd_set1' AND PORTFOLIO='ULOCC';

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD
SET
 CHANGE = (SELECT txn_cd_set2_count_chng FROM SASCOLL.ulocc_sav_dda_txn_aggr HAVING POSTED_DT_TM_KEY=MAX(POSTED_DT_TM_KEY))
WHERE TABLE_NAME='EDEDTT.saving_and_dda_txn_fact' AND ATTRIBUTE_NAME='txn_cd_set2' AND PORTFOLIO='ULOCC';


QUIT;


/*STAGE 4*/
/*REPORTING*/

/*TO PREVENT ERROR IN UPDATE STATEMENT - AS WE ARE READING AND UPDATING THE SAME DATASET*/
DATA COLLECTION_MODELS_THRESHOLD;
SET SASCOLL.COLLECTION_MODELS_THRESHOLD;
RUN;

/*ITLC COUNT CHECK UPDATE*/

PROC SQL NOPRINT;
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.DAY_OVERDUE_COUNT>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ITLC_SPL_DLY_ATTR_CHECK_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='day_overdue'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='day_overdue'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.RECD_STAT_CD_COUNT>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ITLC_SPL_DLY_ATTR_CHECK_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='recd_stat_code'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='recd_stat_code'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.TOTAL_AMOUNT_COUNT>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ITLC_SPL_DLY_ATTR_CHECK_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='bal_intr_amount'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='bal_intr_amount'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.COMMERCIAL_LOAN_CD_CYCLE_chng>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK 
FROM SASCOLL.ITLC_SPL_DLY_ATTR_CHECK_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='commercial_loan_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='commercial_loan_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.SCRTY_CD_CYCLE_chng > 0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ITLC_SPL_DLY_ATTR_CHECK_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='scrty_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='scrty_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.LAST_DIGIT_SCRTY_CD_CYCLE_chng > 0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ITLC_SPL_DLY_ATTR_CHECK_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='last_digit_scrty_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='last_digit_scrty_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.prim_cust_count_delta>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ITLC_CUST_ACCT_RLTNP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='prim_cust'
WHERE A.MTH_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='prim_cust'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.DELI_COUNT_delta>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ITLC_DELI_FILE_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='deli count'
WHERE A.MTH_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='deli count'
;
QUIT;

/*CCC COUNT CHECK UPDATE*/

PROC SQL NOPRINT;
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.prd_cd_count_cycle_chng>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.CCC_REVL_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='CCC' AND B.ATTRIBUTE_NAME='prd_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='CCC' AND ATTRIBUTE_NAME='prd_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.DAY89DLQNT_COUNT>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK 
FROM SASCOLL.CCC_REVL_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='CCC' AND B.ATTRIBUTE_NAME='day89dlqnt'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='CCC' AND ATTRIBUTE_NAME='day89dlqnt'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.DAY119DLQNT_COUNT>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.CCC_REVL_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='CCC' AND B.ATTRIBUTE_NAME='day119dlqnt'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='CCC' AND ATTRIBUTE_NAME='day119dlqnt'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.nonvic_prd_cd_count_cycle_chng>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.CCC_REVL_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='CCC' AND B.ATTRIBUTE_NAME='nonvic_prd_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='CCC' AND ATTRIBUTE_NAME='nonvic_prd_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.ks_count_delta>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.CCC_BASEL_INSTR_FACT_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='CCC' AND B.ATTRIBUTE_NAME='ks_count'
WHERE A.MTH_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='CCC' AND ATTRIBUTE_NAME='ks_count'
;
quit;

/*ULOCC COUNT CHECK UPDATE */

PROC SQL NOPRINT;
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.Total_amount_cycle_chng>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='Total_amount'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='Total_amount'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.prod_code_cycle_chng>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='prod_code'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='prod_code'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.sub_prd_cd_cycle_chng>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='sub_prd_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='sub_prd_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.delq_prd_cd_cycle_chng>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='delq_prd_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='delq_prd_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.DELQ_DLQNT_DAY_COUNT>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='delq_dlqnt_day'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='delq_dlqnt_day'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.ind_heloc_cycle_chng>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='ind_heloc'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='ind_heloc'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.chrg_off_code_cycle_chng>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='chrg_off_code'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='chrg_off_code'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.block_recl_cycle_chng>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='block_recl'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='block_recl'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.proc_transit_cycle_chng>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ULOCC_PSNL_LOAN_DLY_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='proc_transit'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='proc_transit'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.recd_stat_cd_count>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ULOCC_PSNL_LOAN_DLY_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='recd_stat_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='recd_stat_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.comm_tp_count_delta>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ULOCC_BASEL_MORT_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='comm_tp'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='comm_tp'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.crnt_bal_count>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ULOCC_BASEL_MORT_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='crnt_bal'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='crnt_bal'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.pd_off_f_count>0 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ULOCC_BASEL_MORT_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='pd_off_f'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='pd_off_f'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN WEEKDAY("&SYSDATE"D) NE 3	THEN 'TESTED ON TUESDAYS'
	 WHEN WEEKDAY("&SYSDATE"D)=3 AND B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.txn_cd_set1_count>100000 THEN 'PASS' ELSE 'FAIL' 
	 END AS CHECK
FROM SASCOLL.ULOCC_SAV_DDA_TXN_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='txn_cd_set1'
WHERE A.POSTED_DT_TM_KEY=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='txn_cd_set1'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET COUNT_CHECK=(
SELECT 
CASE WHEN WEEKDAY("&SYSDATE"D) NE 3	THEN 'TESTED ON TUESDAYS'
	 WHEN WEEKDAY("&SYSDATE"D)=3 AND B.EFFECTIVE_DATE=B.EXPECTED_DATE AND A.txn_cd_set2_count>1000000 THEN 'PASS' ELSE 'FAIL' END AS CHECK
FROM SASCOLL.ULOCC_SAV_DDA_TXN_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='txn_cd_set2'
WHERE A.POSTED_DT_TM_KEY=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='txn_cd_set2'
;
QUIT;

/*INCLUDE THE LATEST UPDATES TO CHECK AND SET STATUS*/
/*CLEARING OUT THE STATUSES*/
PROC SQL NOPRINT;
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD SET STATUS=.;
QUIT;
/*TO PREVENT ERROR IN UPDATE STATEMENT - AS WE ARE READING AND UPDATING THE SAME DATASET*/
DATA COLLECTION_MODELS_THRESHOLD;
SET SASCOLL.COLLECTION_MODELS_THRESHOLD;
RUN;

/*ITLC STATUS UPDATE*/

PROC SQL NOPRINT;
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD
AND A.DAY_OVERDUE_COUNT>0) THEN 0
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.DAY_OVERDUE_COUNT=0) THEN 2	/*COUNTS ARE NEVER LESS THAN 0*/
ELSE 1 END AS STATUS
FROM SASCOLL.ITLC_SPL_DLY_ATTR_CHECK_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='day_overdue'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='day_overdue'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD
AND A.RECD_STAT_CD_COUNT>0) THEN 0
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD)
AND A.RECD_STAT_CD_COUNT=0) THEN 2	/*COUNTS ARE NEVER LESS THAN 0*/
ELSE 1 END AS STATUS
FROM SASCOLL.ITLC_SPL_DLY_ATTR_CHECK_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='recd_stat_code'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='recd_stat_code'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD
AND A.TOTAL_AMOUNT_COUNT>0) THEN 0
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD)
AND A.TOTAL_AMOUNT_COUNT=0) THEN 2	/*COUNTS ARE NEVER LESS THAN 0*/
ELSE 1 END AS STATUS
FROM SASCOLL.ITLC_SPL_DLY_ATTR_CHECK_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='bal_intr_amount'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='bal_intr_amount'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.COMMERCIAL_LOAN_CD_CYCLE_chng>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.COMMERCIAL_LOAN_CD_CYCLE_chng<=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ITLC_SPL_DLY_ATTR_CHECK_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='commercial_loan_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='commercial_loan_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.SCRTY_CD_CYCLE_chng > 0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.SCRTY_CD_CYCLE_chng <=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ITLC_SPL_DLY_ATTR_CHECK_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='scrty_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='scrty_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD
AND A.LAST_DIGIT_SCRTY_CD_CYCLE_chng > 0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.LAST_DIGIT_SCRTY_CD_CYCLE_chng <=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ITLC_SPL_DLY_ATTR_CHECK_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='last_digit_scrty_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='last_digit_scrty_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.prim_cust_count_delta>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.prim_cust_count_delta<=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ITLC_CUST_ACCT_RLTNP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='prim_cust'
WHERE A.MTH_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='prim_cust'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.DELI_COUNT_delta>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.DELI_COUNT_delta<=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ITLC_DELI_FILE_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ITLC' AND B.ATTRIBUTE_NAME='deli count'
WHERE A.MTH_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ITLC' AND ATTRIBUTE_NAME='deli count'
;
QUIT;



/*CCC STATUS UPDATE*/

PROC SQL NOPRINT;
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.prd_cd_count_cycle_chng>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.prd_cd_count_cycle_chng<=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.CCC_REVL_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='CCC' AND B.ATTRIBUTE_NAME='prd_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='CCC' AND ATTRIBUTE_NAME='prd_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.DAY89DLQNT_COUNT>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.DAY89DLQNT_COUNT=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.CCC_REVL_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='CCC' AND B.ATTRIBUTE_NAME='day89dlqnt'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='CCC' AND ATTRIBUTE_NAME='day89dlqnt'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.DAY119DLQNT_COUNT>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.DAY119DLQNT_COUNT=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.CCC_REVL_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='CCC' AND B.ATTRIBUTE_NAME='day119dlqnt'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='CCC' AND ATTRIBUTE_NAME='day119dlqnt'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.nonvic_prd_cd_count_cycle_chng>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.nonvic_prd_cd_count_cycle_chng<=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.CCC_REVL_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='CCC' AND B.ATTRIBUTE_NAME='nonvic_prd_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='CCC' AND ATTRIBUTE_NAME='nonvic_prd_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.ks_count>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.ks_count=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.CCC_BASEL_INSTR_FACT_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='CCC' AND B.ATTRIBUTE_NAME='ks_count'
WHERE A.MTH_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='CCC' AND ATTRIBUTE_NAME='ks_count'
;
quit;



/*ULOCC STATUS UPDATE*/

PROC SQL NOPRINT;
UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.Total_amount_cycle_chng>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.Total_amount_cycle_chng<=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='Total_amount'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='Total_amount'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.prod_code_cycle_chng>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.prod_code_cycle_chng<=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='prod_code'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='prod_code'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.sub_prd_cd_cycle_chng>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.sub_prd_cd_cycle_chng<=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='sub_prd_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='sub_prd_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.delq_prd_cd_cycle_chng>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.delq_prd_cd_cycle_chng<=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='delq_prd_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='delq_prd_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.DELQ_DLQNT_DAY_COUNT>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.DELQ_DLQNT_DAY_COUNT=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='delq_dlqnt_day'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='delq_dlqnt_day'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.ind_heloc_cycle_chng>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.ind_heloc_cycle_chng<=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='ind_heloc'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='ind_heloc'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.chrg_off_code_cycle_chng>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.chrg_off_code_cycle_chng<=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='chrg_off_code'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='chrg_off_code'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.block_recl_cycle_chng>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.block_recl_cycle_chng<=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_REV_CR_DLY_SNAP_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='block_recl'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='block_recl'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.proc_transit_cycle_chng>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.proc_transit_cycle_chng<=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_PSNL_LOAN_DLY_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='proc_transit'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='proc_transit'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.recd_stat_cd_count>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.recd_stat_cd_count=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_PSNL_LOAN_DLY_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='recd_stat_cd'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='recd_stat_cd'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.comm_tp_count_delta>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.comm_tp_count_delta<=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_BASEL_MORT_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='comm_tp'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='comm_tp'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.crnt_bal_count>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.crnt_bal_count=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_BASEL_MORT_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='crnt_bal'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='crnt_bal'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD 
AND A.pd_off_f_count>0) THEN 0 
WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
AND A.pd_off_f_count=0) THEN 2
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_BASEL_MORT_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='pd_off_f'
WHERE A.EFF_TM_iD=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='pd_off_f'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN WEEKDAY("&SYSDATE"D)=3 AND COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD) THEN 0 
	 WHEN WEEKDAY("&SYSDATE"D) NE 3 AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD) THEN 0
	 WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
		AND WEEKDAY("&SYSDATE"D)=3) THEN 2
	 WHEN WEEKDAY("&SYSDATE"D) NE 3 AND (B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) THEN 1
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_SAV_DDA_TXN_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='txn_cd_set1'
WHERE A.POSTED_DT_TM_KEY=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='txn_cd_set1'
;

UPDATE SASCOLL.COLLECTION_MODELS_THRESHOLD 
SET 
STATUS=(
SELECT 
CASE WHEN WEEKDAY("&SYSDATE"D)=3 AND COUNT_CHECK='PASS' AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD) THEN 0 
	 WHEN WEEKDAY("&SYSDATE"D) NE 3 AND (B.CHANGE BETWEEN B.CHANGE_UPPER_THRESHOLD AND B.CHANGE_LOWER_THRESHOLD) THEN 0 
	 WHEN COUNT_CHECK='FAIL' AND ((B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) 
		AND WEEKDAY("&SYSDATE"D)=3) THEN 2
	 WHEN WEEKDAY("&SYSDATE"D) NE 3 AND (B.CHANGE > B.CHANGE_UPPER_THRESHOLD OR B.CHANGE < B.CHANGE_LOWER_THRESHOLD) THEN 1
ELSE 1 END AS STATUS
FROM SASCOLL.ULOCC_SAV_DDA_TXN_AGGR A 
INNER JOIN COLLECTION_MODELS_THRESHOLD B ON 
B.PORTFOLIO='ULOCC' AND B.ATTRIBUTE_NAME='txn_cd_set2'
WHERE A.POSTED_DT_TM_KEY=B.TIME_ID)
WHERE PORTFOLIO='ULOCC' AND ATTRIBUTE_NAME='txn_cd_set2'
;
QUIT;


/*STAGE 5*/
/*MAINTAIN HISTORY OF STATUS*/
PROC TRANSPOSE DATA=SASCOLL.COLLECTION_MODELS_THRESHOLD OUT=THRESHOLD_TRANSPOSE (DROP=_name_ _label_ RENAME=('deli count'N = deli_count));
var Status;
ID ATTRIBUTE_NAME;
RUN;

PROC SQL NOPRINT;
DELETE FROM SASCOLL.COLLECTION_MODELS_THRESHOLD_HIST WHERE REPORT_DATE="&SYSDATE"D;
DELETE FROM DB2PROD.COLLECTION_MODELS_THRESHOLD_HIST WHERE REPORT_DATE="&SYSDATE"D;
INSERT INTO SASCOLL.COLLECTION_MODELS_THRESHOLD_HIST SELECT "&SYSDATE"D AS REPORT_DATE, * FROM THRESHOLD_TRANSPOSE;
INSERT INTO DB2PROD.COLLECTION_MODELS_THRESHOLD_HIST SELECT "&SYSDATE"D AS REPORT_DATE, * FROM THRESHOLD_TRANSPOSE;
QUIT;


/*STAGE 6*/
/*DASHBOARD*/
proc template;
 define style color;
 parent = Styles.pearl;
 style body from body /
 backgroundcolor=WHITE;
end;run;

PROC FORMAT;
VALUE COLOR
0='GREEN'
1='YELLOW'
2='RED';
RUN;
ODS LISTING CLOSE;
options orientation=landscape papersize=(14in 8.5in)
nonumber topmargin=.5in bottommargin=.25in
leftmargin=.05in rightmargin=.05in;
ods escapechar='^';
ODS PDF FILE="&coll_dir./flat_files/collection_models/collections_source_threshold_report.pdf" notoc;
TITLE1 "COLLECTION MODELS SOURCE DATA THRESHOLD REPORT";
FOOTNOTE;

PROC REPORT DATA=SASCOLL.COLLECTION_MODELS_THRESHOLD;
COLUMN PORTFOLIO TABLE_NAME ATTRIBUTE_NAME EFFECTIVE_DATE EXPECTED_DATE COUNT_CHECK CHANGE_UPPER_THRESHOLD CHANGE_LOWER_THRESHOLD CHANGE STATUS;
DEFINE PORTFOLIO / GROUP ORDER;
DEFINE STATUS / STYLE(COLUMN)={BACKGROUND=COLOR. FOREGROUND=COLOR.};
DEFINE CHANGE_UPPER_THRESHOLD /  STYLE(COLUMN)={BACKGROUND=GREY} FORMAT=PERCENTN10.2;
DEFINE CHANGE_LOWER_THRESHOLD /  STYLE(COLUMN)={BACKGROUND=GREY} FORMAT=PERCENTN10.2;
DEFINE CHANGE / FORMAT=PERCENTN10.2;
RUN;
ods _all_ close;

/*STAGE 7*/
/*EMAIL DASHBOARD ON YELLOW/RED*/
%email_declare;
%macro email;
filename mymail email 
to=("rodrigo.ferro@scotiabank.com")
CC=(&INTERNAL_LIST &INTERNAL_CC_LIST)
Subject="collection models source threshold report";

DATA _NULL_;
FILE mymail;
PUT "Hi" //;
PUT "Please find attached a copy of the source threshold report as of &sysdate.";
PUT " ";
PUT "Today's report has attributes that are flagged Yellow or Red and needs your attention.";
PUT '!EM_ATTACH! ("&coll_dir./flat_files/collection_models/collections_source_threshold_report.pdf")';
PUT " ";
PUT "regards,";
RUN;

%mend email;

%macro DISTRIBUTION;
PROC SQL NOPRINT;
SELECT MAX(STATUS) INTO :ERROR_CODE FROM SASCOLL.COLLECTION_MODELS_THRESHOLD;
QUIT;

  %if &ERROR_CODE > 0 %then %do;
  		%email;
     %end;
  %else %do;
  %end;
%mend DISTRIBUTION;

%DISTRIBUTION;

