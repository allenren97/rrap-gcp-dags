use crz_cust_scorecard;
drop view if exists v_ux_ux300u2;
CREATE VIEW v_ux_ux300u2 AS select
ux_ux300u2.businesseffectivedate,
ux_ux300u2.uxactdtl_rec.agreement_no,
ux_ux300u2.uxactdtl_rec.standing_order_no,
ux_ux300u2.uxactdtl_rec.srce_dest_use_ind,
ux_ux300u2.uxactdtl_rec.sequence_no,
ux_ux300u2.uxactdtl_rec.product_cde,
ux_ux300u2.uxactdtl_rec.financial_instn,
ux_ux300u2.uxactdtl_rec.cab_transit,
ux_ux300u2.uxactdtl_rec.account_no,
ux_ux300u2.uxactdtl_rec.amount_1,
ux_ux300u2.uxactdtl_rec.amount_2,
ux_ux300u2.uxactdtl_rec.filler_0
from tsz.ux_ux300u2 ux_ux300u2
;