use crz_cust_scorecard;
alter table cbs_mdm_flags ADD COLUMNS (CUST_AGE INT COMMENT 'Customer''s Age');
alter table cbs_mdm_flags change time_on_books time_on_books double COMMENT 'Months since the customer has been on the accounting books';