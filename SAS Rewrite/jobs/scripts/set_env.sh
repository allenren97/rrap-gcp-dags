#!/bin/bash



#beeline_cmd="jdbc:hive2://sddsvrwm369.scglobaluat.aduat.scotiacapital.com:2181,sddsvrwm367.scglobaluat.aduat.scotiacapital.com:2181,sddsvrwm368.scglobaluat.aduat.scotiacapital.com:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
#beeline_cmd="jdbc:hive2://sdusvrwm0125.scglobaluat.aduat.scotiacapital.com:10000/default;principal=hive/sdusvrwm0125.scglobaluat.aduat.scotiacapital.com@SCGLOBALUAT.ADUAT.SCOTIACAPITAL.COM;transportMode=binary"
beeline_cmd="jdbc:hive2://sdpsvrwm0124.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0128.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0162.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0123.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0127.scglobal.ad.scotiacapital.com:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"

#kinit -kt /app/dev_bbcx_cbs_appid/.keytab/dev_bbcx_cbs_appid.keytab dev_bbcx_cbs_appid@SCGLOBALUAT.ADUAT.SCOTIACAPITAL.COM
kinit -kt /app/bbcx_cbs_appid/.keytab/bbcx_cbs_appid.keytab bbcx_cbs_appid
#kinit -kt /home/cliu5/userkey/cliu5.keytab cliu5@SCGLOBAL.AD.SCOTIACAPITAL.COM

echo $beeline_cmd

