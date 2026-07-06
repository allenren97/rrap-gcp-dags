Use crz_cust_scorecard;
drop view if exists v_cbs_model_scorecrd_vars;
CREATE VIEW v_cbs_model_scorecrd_vars AS 
SELECT cust_cid as cust_cid 
, seg_num as seg_num 
, sc_var as sc_var 
, sc_var_val as sc_var_val 
, eff_dt as eff_dt 
, date_type as date_type
FROM crz_cust_scorecard.cbs_model_scorecrd_var_seg3 seg3
UNION 
SELECT cust_cid as cust_cid 
, seg_num as seg_num 
, sc_var as sc_var 
, sc_var_val as sc_var_val 
, eff_dt as eff_dt 
, date_type as date_type
FROM crz_cust_scorecard.cbs_model_scorecrd_var_seg4 seg4
UNION 
SELECT cust_cid as cust_cid 
, seg_num as seg_num 
, sc_var as sc_var 
, sc_var_val as sc_var_val 
, eff_dt as eff_dt 
, date_type as date_type
FROM crz_cust_scorecard.cbs_model_scorecrd_var_seg5 seg5 
UNION 
SELECT cust_cid as cust_cid 
, seg_num as seg_num 
, sc_var as sc_var 
, sc_var_val as sc_var_val 
, eff_dt as eff_dt 
, date_type as date_type
FROM crz_cust_scorecard.cbs_model_scorecrd_var_seg6 seg6
UNION 
SELECT cust_cid as cust_cid 
, seg_num as seg_num 
, sc_var as sc_var 
, sc_var_val as sc_var_val 
, eff_dt as eff_dt 
, date_type as date_type
FROM crz_cust_scorecard.cbs_model_scorecrd_var_seg7 seg7
UNION 
SELECT cust_cid as cust_cid 
, seg_num as seg_num 
, sc_var as sc_var 
, sc_var_val as sc_var_val 
, eff_dt as eff_dt 
, date_type as date_type
FROM crz_cust_scorecard.cbs_model_scorecrd_var_seg8 seg8
UNION 
SELECT cust_cid as cust_cid 
, seg_num as seg_num 
, sc_var as sc_var 
, sc_var_val as sc_var_val 
, eff_dt as eff_dt 
, date_type as date_type
FROM crz_cust_scorecard.cbs_model_scorecrd_var_seg9 seg9 
;