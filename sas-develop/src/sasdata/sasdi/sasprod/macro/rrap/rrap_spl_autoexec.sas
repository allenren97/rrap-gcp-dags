/* SPL Autoexec File - TO BE PLACED AT THE BEGINNING OF ALL JOBS */


%let rrap_dir=/sasdata/sasdi/sasdev;


LIBNAME control BASE "&rrap_dir/params/rrap";
LIBNAME intmed  BASE "&rrap_dir/data/rrap";

* Netezza and DB2 Servers and Databases;
* When making changes here, make sure to change the corresponding metadata library;

%let NZ_server = cs2iwntzp01;
/*%let RRAP_DB   = EDRTLRU10P;*/
/*%let FRG_DB    = FRG_USER_Q3_2015;*/
/*%let DB2_RRAP  = DM1U10D;*/
%let RRAP_DB   = EDRTLRS10P; 
%let FRG_DB    = EDRTLRS10P_FRG;
%let RRAP_DB2  = DM1S10D;
*%let RRAP_PRD_DB = EDRTLRP1D;



/* Used to define connections to Netezza and DB2 */
libname nzuser   netezza server = &NZ_server database = &FRG_DB     authdomain="NZ_Auth" bulkunload=yes ;
libname nzintmed netezza server = &NZ_server database = &FRG_DB     authdomain="NZ_Auth" bulkunload=yes ;
libname NZRRAP   netezza server = &NZ_server database = &RRAP_DB    authdomain="NZ_Auth" bulkunload=yes ;
LIBNAME DB2RRAP  DB2 	 DATABASE=&RRAP_DB2  SCHEMA=EDRRAPT;
*libname NZ netezza server = &NZ_server database = credit_risk authdomain="NZ_Auth" bulkunload=yes access=readonly;



options mautosource sasautos=('/sasdata/sasdi/sasdev/macro/rrap', sasautos);

* ---- END AUTOEXEC FILE ----;

*In order to hard code dates, run the code below in Enterprise Guide; 

/*************************************************************************

	%include '/sasdata/sasdi/sasdev/macro/rrap/rrap_spl_autoexec.sas';
	%HARD_CODE_DATES(RUN_START_DATE=30APR2015,RUN_END_DATE=30APR2015);
	%LIST_RRAP_PARAMETERS;

*************************************************************************/


/*LIBNAME intmed BASE "&rrapfrg_dir/data/intermediate";*/
/*LIBNAME results BASE "&rrapfrg_dir/data/results";*/
/*LIBNAME source BASE "&rrapfrg_dir/data/source";*/
/*LIBNAME target BASE "&rrapfrg_dir/data/target";*/

