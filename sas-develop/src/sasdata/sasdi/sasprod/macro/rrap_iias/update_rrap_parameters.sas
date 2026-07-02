/*****************************************************************************
| Update RRAP Parameters                                                     |
|                                                                            |
| Update the value of a specific parameter.  If the                          |
| parameter does not exist in the parameters table                           |
| it will added.                                                             |
|                                                                            |
| Usage:                                                                     |
| %update_rrap_parameters(name=spl_model_start_period_dt, value=28Feb2015);  |
|****************************************************************************/
%macro update_rrap_parameters(name=, value=);

   data new_parameter_value;
   format name $30. value $50.;
   name="&name";
   value="&value";
   run;

   proc sort data=control.parameters;
   by name;
   run;

   data control.parameters;
   update control.parameters new_parameter_value;
   by name;
   run;

   %get_rrap_parameters(name=&name);

%mend update_rrap_parameters;
