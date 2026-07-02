/********************************************************
| Get ULOCC Parameters                                   |
|                                                       |
| Get the value of a rundates by creating               |
| global macro variable "<parameter_name>_value".       |
|                                                       |
|*******************************************************/
%macro get_ulocc_parameters;

   %global ulocc_run_start_date ulocc_run_end_date ulocc_dly_run_start_date ulocc_dly_run_end_date;

   proc sql noprint;
   select value into :ulocc_run_start_date from control.ulocc_parameters where name='ulocc_start_date';
   select value into :ulocc_run_end_date from control.ulocc_parameters where name='ulocc_end_date';
   select value into :ulocc_dly_run_start_date from control.ulocc_parameters where name='ulocc_daily_start_date';
   select value into :ulocc_dly_run_end_date from control.ulocc_parameters where name='ulocc_daily_end_date';
   quit;

%mend get_ulocc_parameters;

