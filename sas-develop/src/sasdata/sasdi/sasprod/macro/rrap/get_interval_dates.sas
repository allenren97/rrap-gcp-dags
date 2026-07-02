/****************************************************
| Get interval dates                                |
|                                                   |
| Use the start and end dates to determine interval |
| dates for processing.  This macro assumes that    |
| macro variable start_period_dt exists.            |
|                                                   |
| Usage:                                            |
| %get_interval_dates;                              |
|***************************************************/

%macro get_interval_dates;
   options spool;
   %global begin_dt_0    begin_dt_6    begin_dt_12    begin_dt_24    begin_dt_36 
           begin_dt_0_nz begin_dt_6_nz begin_dt_12_nz begin_dt_24_nz begin_dt_36_nz
	   begin_dt_0_tm begin_dt_6_tm begin_dt_12_tm begin_dt_24_tm begin_dt_36_tm
           start_period_tm_key end_period_tm_key
           ; 
   data _null_;
	   format begin0 begin6 begin12 begin24 begin36 date9.;

       begin0 = intnx('month',"&start_period_dt."d,0,'e');
       call symput("begin_dt_0",put(begin0,date9.));
       call symput("begin_dt_0_nz",put(begin0,yymmdd10.));

       begin0 = intnx('month',"&start_period_dt."d,0,'e');
       call symput("begin_dt_0",put(begin0,date9.));
       call symput("begin_dt_0_nz",put(begin0,yymmdd10.));




       begin6 = intnx('month',"&start_period_dt."d,-6,'e');
       call symput("begin_dt_6",put(begin6,date9.));
       call symput("begin_dt_6_nz",put(begin6,yymmdd10.));
    
       begin12 = intnx('month',"&start_period_dt."d,-12,'e');
       call symput("begin_dt_12",put(begin12,date9.));
       call symput("begin_dt_12_nz",put(begin12,yymmdd10.));

       begin24 = intnx('month',"&start_period_dt."d,-24,'e');
       call symput("begin_dt_24",put(begin24,date9.));
	   call symput("begin_dt_24_nz",put(begin24,yymmdd10.));

       begin36 = intnx('month',"&start_period_dt."d,-36,'e');
       call symput("begin_dt_36",put(begin36,date9.));
       call symput("begin_dt_36_nz",put(begin36,yymmdd10.));
run;

data _null_;
       
	   set control.tm_id_lkp;
	   where sas_date in ("&begin_dt_0"d, "&begin_dt_6"d, "&begin_dt_12"d, "&begin_dt_24"d, "&begin_dt_36"d "&end_period_dt"d );
	   if sas_date= "&begin_dt_0"d then call symput('begin_dt_0_tm',(put(tm_key,5.)));
	   else if sas_date= "&begin_dt_6"d then call symput('begin_dt_6_tm',(put(tm_key,5.)));
	   else if sas_date= "&begin_dt_12"d then call symput('begin_dt_12_tm',(put(tm_key,5.)));
	   else if sas_date= "&begin_dt_24"d then call symput('begin_dt_24_tm',(put(tm_key,5.)));
	   else if sas_date= "&begin_dt_36"d then call symput('begin_dt_36_tm',(put(tm_key,5.)));
           if sas_date= "&end_period_dt"d then call symput('end_period_tm_key',(put(tm_key,5.)));
           if sas_date= "&&begin_dt_0"d then call symput('start_period_tm_key',(put(tm_key,5.)));

          
run;

   %put sas date vars;
   %put begin_dt_0 = &begin_dt_0;
   %put begin_dt_6 = &begin_dt_6;
   %put begin_dt_12 = &begin_dt_12;
   %put begin_dt_24 = &begin_dt_24;
   %put begin_dt_36 = &begin_dt_36;
   %put;
   %put netezza date vars;
   %put begin_dt_0_nz = &begin_dt_0_nz;
   %put begin_dt_6_nz = &begin_dt_6_nz;
   %put begin_dt_12_nz = &begin_dt_12_nz;
   %put begin_dt_24_nz = &begin_dt_24_nz;
   %put begin_dt_36_nz = &begin_dt_36_nz;
   %put;
   %put	tm_id vars;
   %put begin_dt_0_tm = &begin_dt_0_tm;
   %put begin_dt_6_tm = &begin_dt_6_tm;
   %put begin_dt_12_tm = &begin_dt_12_tm;
   %put begin_dt_24_tm = &begin_dt_24_tm;
   %put begin_dt_36_tm = &begin_dt_36_tm;
   %put start_period_tm_key = &start_period_tm_key;
   %put End_period_tm_key = &end_period_tm_key;
   
     	

%mend get_interval_dates;

