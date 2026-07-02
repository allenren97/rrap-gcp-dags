
%macro get_target(product=);
   %global &product._target
           &product._lib;
   %let name=&product._target; 
   data _null_;
      set control.parameters;
      where name="&name";
      call symput("&product._target", value);
      if value = "frg_db" then lib_value='NZUSER';
      else if value ="rrap_db" then lib_value='NZRRAP'; 
      call symput("&product._lib",lib_value);
   run;

%mend get_target;