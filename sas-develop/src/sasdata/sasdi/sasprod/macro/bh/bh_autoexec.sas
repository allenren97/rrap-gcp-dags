/* Create metadata macro variables */
%let metaPort       = %nrquote(8561);
%let metaServer     = %nrquote(iw503);

/* Set metadata options */
options metaport       = &metaPort 
        metaserver     = "&metaServer";

options set=SAS_HADOOP_CONFIG_PATH="/sashome/hadoop/";
options set=SAS_HADOOP_JAR_PATH="/sashome/hadoop/lib";

libname prodrcrr hadoop server="sdpsvrwm0217.scglobal.ad.scotiacapital.com" 
schema=prod_rcrr1 DBMAX_TEXT=255 
authdomain=hadoop_bh
uri="jdbc:hive2://sdpsvrwm0217.scglobal.ad.scotiacapital.com:8443/prod_rcrr1;ssl=true;
sslTrustStore=/sashome/hadoop/gateway.jks;trustStorePassword=Test123?
hive.server2.transport.mode=http;hive.server2.thrift.http.path=gateway/default/hive"  LOGIN_TIMEOUT=0 ;

%let rrap_db=EDRTLRP1D;
%let env=prod;

libname NZRRAP netezza server=cs2iwntzp01 database=&rrap_db. authdomain=nz_auth;
libname EDBHT db2 datasrc=DM1P1D schema=RISKSAS authdomain=db2_auth;
/*libname EDBHT db2 datasrc=DM1P1D schema=EDBHT authdomain=db2_auth;*/



libname bh "/sasdata/sasdi/sas&env./data/bh";
