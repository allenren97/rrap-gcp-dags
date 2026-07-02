/**************************************************************
| Delete Table                                                |
|                                                             |
| Deletes a table only if the table exists.  This macro is    |
| helpful when deleting tables from a database.               | 
|                                                             |
| Usage:                                                      |
| %delete_table(nzuser.SCORECARD_VARS_CUST_PRODS_2015);       |
|*************************************************************/
%macro delete_table(table_name);

   %if %sysfunc(exist(&table_name)) %then %do;
      proc sql;
      drop table &table_name.;
      quit;
   %end;

%mend delete_table;
