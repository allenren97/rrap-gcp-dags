proc export data=cbs.CIS_DATA_POP dbms=csv outfile="/&owftp./cbs/cbs_cis_data_pop_&yyyymmdd..csv" REPLACE; run;

proc sql noprint;
select count(1) into :recordcount
from cbs.CIS_DATA_POP;
quit;
%let recordcount=&recordcount.;
%put recordcount=&recordcount.;
filename out "/&owftp./cbs/cbs_cis_data_pop_&yyyymmdd..mrk";


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
put "            %str(%"dataFileURI%"): %nrbquote("cbs_cis_data_pop_&yyyymmdd..csv"),";
put '        }';
put '}';
run;


