/*****************************************************
| Build Partitions Table                             |
|                                                    |
| Builds an input table for Looping transformations. |
| The table contains a row for each partition value. |
|                                                    |
| Usage:                                             |
| %build_partitions_table(product=mor);              |
|****************************************************/

%macro build_partitions_table(product=);
      

   proc sql noprint;
    select value into:parts
    from control.parameters
     where name="&product._partitions";
    quit;

%put parts = &parts;

data &_output ;
   do i=0 to &parts-1; 
       pn=i;
       parts=&parts;
       output;
   end;
run;

%mend build_partitions_table;
