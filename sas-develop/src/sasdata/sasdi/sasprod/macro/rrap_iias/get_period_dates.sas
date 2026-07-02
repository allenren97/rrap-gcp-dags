/*******************************************************************
| Get Period Dates                                                 |
|                                                                  |
| This macro finds two product and process specific entries in the | 
| parameters control table and passes the values to macro vars     |
| start_period_dt and end_period_dt.  This macro assumes that      |
| parameters <product>_<process>_start_period_dt and               |
| <product>_<process>_end_period_dt exist in the parameters table. |
|                                                                  |
| The start_period_dt and end_period_dt macro variables are inputs |
| to many SPL jobs.  This macro allows these SPL jobs to obtain    |
| the date values from two parameters.  This logic can be used by  | 
| other products.                                                  |
|                                                                  |
| Usage:                                                           |
| %get_period_dates(product=SPL, process=scrd);                    |
|******************************************************************/

%macro get_period_dates(product=, process=);
   %global start_period_dt end_period_dt;  

   %let product=%lowcase(&product);

   %get_rrap_parameters(name=&product._&process._start_period_dt);
   %get_rrap_parameters(name=&product._&process._end_period_dt);

   /*
   %put Values of &product model start and end period dates:;
   %put &product._model_start_period_dt: &&&product._model_start_period_dt_value. ;
   %put &product._model_end_period_dt: &&&product._model_end_period_dt_value. ;
   */

   %let start_period_dt=&&&product._&process._start_period_dt_value.;
   %let end_period_dt=&&&product._&process._end_period_dt_value.;
  
   proc sql noprint;
	select tm_id into :start_period_tm_id
	from nzrrap.tm_dim
	where tm_lvl='Month' and tm_lvl_end_dt="&start_period_dt."d;

	select tm_id into :end_period_tm_id
	from nzrrap.tm_dim
	where tm_lvl='Month' and tm_lvl_end_dt="&end_period_dt."d;
quit;

%mend get_period_dates;
