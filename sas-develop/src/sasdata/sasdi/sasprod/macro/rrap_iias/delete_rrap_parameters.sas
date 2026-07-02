/***********************************************************
| Delete RRAP Parameters                                   |
|                                                          |
| Delete a specific paramter from the parameters table.    |
|                                                          |
| Usage:                                                   |
| %delete_rrap_parameters(name=scorecard_vars_lb_period);  |
|**********************************************************/
%macro delete_rrap_parameters(name=);

   proc sql;
   delete * from control.parameters
   where name="&name";
   quit;

%mend delete_rrap_parameters;

