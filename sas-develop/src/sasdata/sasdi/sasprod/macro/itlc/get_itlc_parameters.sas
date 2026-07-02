/********************************************************
| Get ITLC Parameters                                   |
|                                                       |
| Get the value of a rundates by creating               |
| global macro variable "<parameter_name>_value".       |
|                                                       |
|*******************************************************/
%macro get_itlc_parameters;

   %global itlc_run_start_date itlc_run_end_date itlc_dly_run_start_date itlc_dly_run_end_date;

   proc sql noprint;
   select value into :itlc_run_start_date from control.itlc_parameters where name='itlc_start_date';
   select value into :itlc_run_end_date from control.itlc_parameters where name='itlc_end_date';
   select value into :itlc_dly_run_start_date from control.itlc_parameters where name='itlc_daily_start_date';
   select value into :itlc_dly_run_end_date from control.itlc_parameters where name='itlc_daily_end_date';
   quit;

%mend get_itlc_parameters;
