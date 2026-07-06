use crz_cust_scorecard;
drop view if exists v_ux_ux300u1 ;
CREATE VIEW v_ux_ux300u1 AS select ux_ux300u1.uxstord_rec.agreement_no as agreement_no
,ux_ux300u1.uxstord_rec.standing_order_no as standing_order_no
,ux_ux300u1.uxstord_rec.so_order_type as so_order_type
,ux_ux300u1.uxstord_rec.so_blt as so_blt
,ux_ux300u1.uxstord_rec.so_start_dte as so_start_dte
,ux_ux300u1.uxstord_rec.so_end_dte as so_end_dte
,ux_ux300u1.uxstord_rec.so_end_dte_nullind as so_end_dte_nullind
,ux_ux300u1.uxstord_rec.so_last_chg_date as so_last_chg_date
,ux_ux300u1.uxstord_rec.so_last_chg_date_nullind as so_last_chg_date_nullind
,ux_ux300u1.uxstord_rec.so_status as so_status
,ux_ux300u1.uxstord_rec.special_inst as special_inst
,ux_ux300u1.uxstord_rec.orgntg_product_cde as orgntg_product_cde
,ux_ux300u1.uxstord_rec.orgntg_tr_no as orgntg_tr_no
,ux_ux300u1.uxstord_rec.orgntg_appl_key as orgntg_appl_key
,ux_ux300u1.uxstord_rec.appraised_val as appraised_val
,ux_ux300u1.uxstord_rec.appraised_date as appraised_date
,ux_ux300u1.uxstord_rec.appraised_date_nullind as appraised_date_nullind
,ux_ux300u1.uxstord_rec.shep_credit_limit as shep_credit_limit
,ux_ux300u1.uxstord_rec.shep_cred_lim_date as shep_cred_lim_date
,ux_ux300u1.uxstord_rec.rgstrd_amt as rgstrd_amt
,ux_ux300u1.uxstord_rec.rgstrn_date as rgstrn_date
,ux_ux300u1.uxstord_rec.rgstrn_date_nullind as rgstrn_date_nullind
,ux_ux300u1.uxstord_rec.rgstrn_nmbr as rgstrn_nmbr
,ux_ux300u1.uxstord_rec.prpty_desc_line1 as prpty_desc_line1
,ux_ux300u1.uxstord_rec.prpty_desc_line2 as prpty_desc_line2
,ux_ux300u1.uxstord_rec.prpty_desc_line3 as prpty_desc_line3
,ux_ux300u1.uxstord_rec.province_code as province_code
,ux_ux300u1.uxstord_rec.dischg_reasn_code as dischg_reasn_code
,ux_ux300u1.uxstord_rec.dischg_date as dischg_date
,ux_ux300u1.uxstord_rec.dischg_date_nullind as dischg_date_nullind
,ux_ux300u1.uxstord_rec.so_notice_date as so_notice_date
,ux_ux300u1.uxstord_rec.so_notice_date_nullind as so_notice_date_nullind
,ux_ux300u1.uxstord_rec.so_hrcert_no as so_hrcert_no
,ux_ux300u1.uxstord_rec.so_hrinsurer as so_hrinsurer
,ux_ux300u1.uxstord_rec.so_hr_start_dte as so_hr_start_dte
,ux_ux300u1.uxstord_rec.so_hr_start_dte_nullind as so_hr_start_dte_nullind
,ux_ux300u1.uxstord_rec.so_hr_revpriod as so_hr_revpriod
,ux_ux300u1.uxstord_rec.so_hr_end_dte as so_hr_end_dte
,ux_ux300u1.uxstord_rec.so_hr_end_dte_nullind as so_hr_end_dte_nullind
,ux_ux300u1.uxstord_rec.so_hr_premamt as so_hr_premamt
,ux_ux300u1.uxstord_rec.so_hr_applfee as so_hr_applfee
,ux_ux300u1.uxstord_rec.so_hr_authdate as so_hr_authdate
,ux_ux300u1.uxstord_rec.so_hr_authdate_nullind as so_hr_authdate_nullind
,ux_ux300u1.uxstord_rec.so_hr_cust1_cid as so_hr_cust1_cid
,ux_ux300u1.uxstord_rec.so_hr_cust2_cid as so_hr_cust2_cid
,ux_ux300u1.uxstord_rec.so_hr_mo_fund_id as so_hr_mo_fund_id
,ux_ux300u1.uxstord_rec.so_ali_ind as so_ali_ind
,ux_ux300u1.uxstord_rec.so_ali_date as so_ali_date
,ux_ux300u1.uxstord_rec.so_cti_ind as so_cti_ind
,ux_ux300u1.uxstord_rec.so_cti_date as so_cti_date
,ux_ux300u1.uxstord_rec.so_ali_pending_amt as so_ali_pending_amt
,ux_ux300u1.uxstord_rec.so_ali_product as so_ali_product
,ux_ux300u1.uxstord_rec.so_ali_acct_num as so_ali_acct_num
,ux_ux300u1.uxstord_rec.prev_so_status as prev_so_status
,ux_ux300u1.uxstord_rec.so_status_chg_dte as so_status_chg_dte
,ux_ux300u1.operationalfields.createdate as createdate
,ux_ux300u1.operationalfields.filename as filename
,ux_ux300u1.operationalfields.expirydate as expirydate
,ux_ux300u1.operationalfields.jobexecutionid as jobexecutionid
,ux_ux300u1.businesseffectivedate
from tsz.ux_ux300u1
;
