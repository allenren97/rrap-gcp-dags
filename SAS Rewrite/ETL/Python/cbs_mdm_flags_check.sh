#!/bin/bash
#last_day_last_month=`date -d "$(date +%Y-%m-01) -1 day" +%Y-%m-%d`
current_date=$1
first_day_current_month=`date -d $current_date +'%Y-%m-01'`
last_day_last_month=`date -d "$first_day_current_month -1 day" +'%Y-%m-%d'`
echo $last_day_last_month
dtyyyymm=`date +%Y-%m`
record_partition=`beeline --showHeader=false --outputformat=tsv2 -u "jdbc:hive2://sdpsvrwm0124.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0128.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0162.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0123.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0127.scglobal.ad.scotiacapital.com:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "USE crz_cust_scorecard;SELECT count(*) from risk_cbs_audit_job_log where eff_dt='$last_day_last_month' AND target_tbl_nm='cbs_mdm_flags' AND has_data_f='Y';"`
if [ $record_partition -eq 0 ]
then
    ./cbs_mdm_flags_check_report.sh $current_date $last_day_last_month $record_partition
else
    file_name='check_mdm_data_'$dtyyyymm'.flag'
    echo "Data exists in <cbs_mdm_flags> table" > $file_name
    echo "Flag file created: <"$file_name">."
fi
