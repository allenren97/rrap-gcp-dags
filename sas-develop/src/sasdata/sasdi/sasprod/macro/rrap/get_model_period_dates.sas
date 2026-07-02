/*******************************************************************
| Get Model Period                                                 |
|                                                                  |
| This macro finds two product specific entries in the parameters  |
| control table and passes the values to macro variables           |
| start_period_dt and end_period_dt.  This macro assumes that      |
| parameters <product>_model_start_period_dt and                   |
| <product>_model_end_period_dt exist in the parameters table.     |
|                                                                  |
| The start_period_dt and end_period_dt macro variables are inputs |
| in most SPL model jobs.  This allows these SPL model jobs to     |
| obtain the date values from two parameters.  This logic may be   | 
| used by other products.                                          |
|                                                                  |
| Usage:                                                           |
| %get_model_period_dates(product=spl);                            |
|******************************************************************/
%macro get_model_period_dates(product=);
   %global start_period_dt end_period_dt;  

   %get_rrap_parameters(name=&product._model_start_period_dt);
   %get_rrap_parameters(name=&product._model_end_period_dt);

   /*
   %put Values of &product model start and end period dates:;
   %put &product._model_start_period_dt: &&&product._model_start_period_dt_value. ;
   %put &product._model_end_period_dt: &&&product._model_end_period_dt_value. ;
   */

   %let start_period_dt=&&&product._model_start_period_dt_value.;
   %let end_period_dt=&&&product._model_end_period_dt_value.;

     
 
%mend get_model_period_dates;
