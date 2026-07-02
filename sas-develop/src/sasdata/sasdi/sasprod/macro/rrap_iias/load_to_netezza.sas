/****************************************************
| load to netezza                                   |
|                                                   |
| Build a table from the partitions partitions      |
| creates a row for each valeu  dates for processing|
|                                                   |
| Usage:                                            |
| %load_to_netezza(table_name=,parts=);             |
| table name = partitioned table names              |
| parts =  Total Number of Partitions               |      |
|***************************************************/

%macro load_to_netezza(table_name=,parts=);
%do pn=0 %to &parts-1;
	 
    %if &pn=0 %then %do;
		%delete_table(nzuser.&table_name.); 
			proc sql;  
       		connect using nzintmed as nzcon;
       			execute( create table &frg_db..&table_name. as (
             	select *
             	from &frg_db..&table_name._&pn. ) WITH DATA;
/*             distribute on hash (MORTGAGE_NO)*/
           ) by nzcon;
           disconnect from nzcon;
         quit;
      %end;
		%else %do;
		proc sql;  
       		connect using nzuser as nzcon;

       		 execute( insert into &frg_db..&table_name.
         	 select *
             from &frg_db..&table_name._&pn.
             ) by nzcon;
         disconnect from nzcon;
  quit;
      %end;
    %end;

  quit;
%mend load_to_netezza;