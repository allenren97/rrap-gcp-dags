
***************************************************************************************************************************;

%let etls_jobname = J_RRAP_DT4_0205_DT4_SEGMENT_XREF_BKUP.sas;

***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  DT4_SEGMENT_XREF_BKUP
*  
*  Purpose: Backup DT4 Segmentation table prior to SCD2 load. Incase the SEGMENT_XREF fails, restore DT4_SEGMENT_XREF from 
*					this backup.
*  Frequency: Quarter End runs
*
*  Notes:  
*  		  
*
*	Change Log:
*	2021-12-04: Hadi Dimashkieh - Initial Development
*   
*  
*
***************************************************************************************************************************;



%rrap_dt4_autoexec();


********************PUT IN A SEPARATE JOB********************************;

proc sql;
	connect using nzrrap as nzcon;
	execute(drop table &RRAP_WRK..DT4_SEGMENT_XREF_BKUP if exists;) by nzcon;
	execute(commit;) by nzcon;
quit;

proc sql;
	connect using nzrrap as nzcon;
	execute(create table &RRAP_WRK..DT4_SEGMENT_XREF_BKUP as (select * from &RRAP_DB..DT4_SEGMENT_XREF) with data;) by nzcon;
	execute(commit;) by nzcon;
quit;

**************************************************************************;
