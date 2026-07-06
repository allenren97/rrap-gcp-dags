data cust_base;
	set cbs.cust_base;
	drop CUST_CID HIT_NOHIT_EDIT_REJCT_CD FICO_08_SCORE FICO_08_EXCLSN_CD ROW_NUM;
run;


proc export data=CUST_BASE dbms=csv outfile="/&owftp./cbs/cbs_customer_base_&yyyymmdd..csv" REPLACE; run;


proc sql noprint;
select count(1) into :recordcount
from cbs.CUST_BASE;
quit;
%let recordcount=&recordcount.;
%put recordcount=&recordcount.;
filename out "/&owftp./cbs/cbs_customer_base_&yyyymmdd..mrk";


data _null_; 
file out; 

put '{';
put '    "sourceAppl": {';
put '        "providingParty": "rrap",';
put '        "country": "can",';
put '        "region": "nam",';
put '        "appAcronym": "cbs",';
put '        "frequency": "mth",';
put '        "securityClassification": "int",';
put '        "fileTransfer": "pull",';
put "        %str(%"buste%"): %nrbquote("&yyyymmdd.")";
put '    },';
put '    "sourceFiles": [';
put '        {';
put '            "compressType": "",';
put '            "ingestMetadata": "",';
put '            "fileCompressed": "n",';
put '            "fileHeader": "y",';
put "            %str(%"recordCount%"): %nrbquote("&recordcount."),";
put '            "dataFileMD5": "",';
put '            "invalidRecordThreshold": 0,';
put '            "fileExtension": "csv",';
put "            %str(%"dataFileURI%"): %nrbquote("cbs_customer_base_&yyyymmdd..csv"),";
put '        }';
put '}';
run;

