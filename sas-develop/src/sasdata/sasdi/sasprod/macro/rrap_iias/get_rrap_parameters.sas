/********************************************************
| Get RRAP Parameters                                   |
|                                                       |
| Get the value of a specific parameter by creating     |
| global macro variable "<parameter_name>_value".       |
|                                                       |
| Usage:                                                |
| %get_rrap_parameters(name=spl_model_start_period_dt); |
|*******************************************************/
%macro get_rrap_parameters(name=);

   %global &name._value;

   data _null_;
   set control.parameters;
   where name="&name";
   call symput("&name._value", value);
   run;

%mend get_rrap_parameters;
