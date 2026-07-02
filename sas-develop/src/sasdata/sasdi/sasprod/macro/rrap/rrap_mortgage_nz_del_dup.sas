*===============================================================================================================;
*                                                                                       Declare macros                                                                                                          ;
*===============================================================================================================;

/****** NOTE:  MOVE MACROS TO MACRO LIBRARY    ******/
*Duplicates check;
*%include '/u/lkushnir/SAS Code/init_unix.sas';
*%include '/u/lkushnir/SAS Code/dupcheck.sas';

* Rename the tables in NZ;

%macro nz_rename(old_name,new_name);

        proc sql;
           /*     connect to nzuser as nzcon (database=&target_lib3 SCHEMA="&frg_usr" user="&iiasuser" password="&iiaspassword");
                execute (ALTER TABLE &target_lib3..&old_name. 
						 RENAME TO &target_lib3..&new_name.) by nzcon; */
	
                ALTER TABLE nzuser.&old_name  
						 RENAME TO nzuser.&new_name  ;
        quit;

%mend;

%macro delete_table(table_name);

   %if %sysfunc(exist(&table_name)) %then %do;
      proc sql;
      drop table &table_name.;
      quit;
   %end;

%mend delete_table;



%macro dupcheck(Dataset = &target_lib..&ds., ByVar =&acct_key. &time_key. , PrintVar = &acct_key. &time_key., Remove =Y );
       %if &remove eq Y %then %do;
           proc sort data=&dataset out=&dataset._nodups nodupkey;
               by &byvar;
           run; 
       %end;  
       %else %do;
           proc sort data=&dataset out=&dataset._dups;
               by &byvar;
           run;         
       %end;   
%mend dupcheck; 

