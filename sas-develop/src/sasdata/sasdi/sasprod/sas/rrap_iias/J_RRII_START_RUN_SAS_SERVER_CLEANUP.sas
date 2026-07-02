/*RRMSS-1479: Datasets identified for deletion to cleanup SAS server space. To be deleted at the START of Month-End run*/
/*Author:	Nikhil Gaikwad*/
/*Date:		14-Jun-2022*/
/*Job name:	J_RRII_START_RUN_SAS_SERVER_CLEANUP.sas*/

%macro Mid_Run_Server_Cleanup(table_name);
proc sql;
	drop table &table_name;
QUIT;
%mend;
options Mprint;

%macro showdiskutil;
filename fpdf pipe 'df -k /sasdata/sasdi';
data _null_;	
   infile fpdf;
   length buffer $ 200;
   input buffer;
   put _infile_;
run;
%mend ;

%put "Disk Utilization Before Cleanup";
%showdiskutil;

/*%RRAP_MOR_TNG_AUTOEXEC*/
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);

/*Below Macro calls deletes the individual data sets from "/sasdata/sasdi/sasprod/data/rrap_iias/reporting" folder on the PROD server*/
%mid_run_server_cleanup(PRG_DATA.basel_acct_dim)
%mid_run_server_cleanup(PRG_DATA.bl_ins_fact_curr_ecl)
%mid_run_server_cleanup(PRG_DATA.bl_ins_fact_prev_ecl)
%mid_run_server_cleanup(PRG_DATA.bl_ins_fact_curr)
%mid_run_server_cleanup(PRG_DATA.basel_ifrs9_ecl_profile_fact)


/*RRMSS-1525/1586:	Include datasets from reporting folder*/
%mid_run_server_cleanup(PRG_DATA.basel_analytcl_bl_instrmnt_fact)

/*RRMSS-1525/1586:	Include datasets from triad folder*/
LIBNAME TRI_DATA "&rrap_dir./data/triad";

%mid_run_server_cleanup(TRI_DATA.basel_cust_acct_rltnp_snapshot)
%mid_run_server_cleanup(TRI_DATA.basel_analytcl_bl_instrmnt_fact)
%mid_run_server_cleanup(TRI_DATA.prepped_instrmnt_fact)
%mid_run_server_cleanup(TRI_DATA.pre_output)

%put "Disk Utilization After Cleanup";
%showdiskutil;
