/****************************************************
| List RRAP Parameters                              |
|                                                   |
| Prints the contents of table control.parameters.  |
|                                                   |
| Usage:                                            |
| %list_rrap_parameters;                            |
|***************************************************/
%macro list_rrap_parameters();
   
   proc print data=control.parameters;
   run;

%mend list_rrap_parameters;
