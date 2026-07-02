%rrap_mor_bns_autoexec
%let parts=8;
data parts ;
   do i=0 to &parts-1; 
       pn=i;
       parts=&parts;
       output;
   end;
run;


%macro loop;
	
	data _null_;
	set parts;
	where pn=&i-1;
	 call symput('pn',left(pn));
	 run;

                        proc sql;
                        connect USING NZUSER as nzcon ;
                        create table intmed.SCORECARD_VARS_MOR_09_3_&pn as 
                        select * from connection to nzcon (
                        select * from &target_lib3..SCORECARD_VARS_MOR_09_2 where mod(cast(mortgage_no as bigint), &parts) = &pn);
                        disconnect from nzcon;
                        quit;
                        
                        
                        /*****  Step 6 Coming out of netezza to SAS--Need to scale */
                        data intmed.SCORECARD_VARS_MOR_09_3_&pn;
                        set intmed.SCORECARD_VARS_MOR_09_3_&pn;
                        format cust_date date9.;
                        if file_yr_mth ne ' ' then do;
                           cust_date=intnx('month',mdy(substr(file_yr_mth,3,2),1,compress('20'||substr(file_yr_mth,1,2))),1)-1;
                           diff=abs(intck('month',time_key,cust_date));
                        end;
                        run;


                        proc sort data=intmed.SCORECARD_VARS_MOR_09_3_&pn;
                           by mortgage_no descending diff;
                        run;


						%delete_table(&target_lib..SCORECARD_VARS_MOR_09_3_&pn);
                        
                        *step 7;
                        data &target_lib..SCORECARD_VARS_MOR_09_3_&pn(drop=cust_date diff); /* ff changed to tmp3 instead of tmp2;*/ 
                        set intmed.SCORECARD_VARS_MOR_09_3_&pn;
                        by mortgage_no;
                        if last.mortgage_no then output;
                        run;
%mend loop;



%macro append_partitions(pn=);

%if %sysfunc(exist(&target_lib..SCORECARD_VARS_MOR_09_3)) %then %do;
  proc sql;  
    connect USING NZUSER as nzcon ;
    execute( insert into SCORECARD_VARS_MOR_09_3
             select *
             from SCORECARD_VARS_MOR_09_3_&pn
           ) by nzcon;
    disconnect from nzcon;
  quit;
 %end;
 %else %do;
  proc sql;  
    connect USING NZUSER as nzcon ;
    execute( create table SCORECARD_VARS_MOR_09_3 as
             select *
             from SCORECARD_VARS_MOR_09_3_&pn
           ) by nzcon;
    disconnect from nzcon;
  quit;
 %end;

%mend;


%macro gather_partitions;

%delete_table(nzuser.SCORECARD_VARS_MOR_09_3);

data _null_;
  *set years_to_process end=eof;
  set parts end=eof;
  call symput('part'||left(_n_), pn);
  if eof then call symput('rows', _n_);
run;

%do i=1 %to &rows;
	%loop
  %append_partitions(pn=&&part&i)
%end;
%mend;
%gather_partitions;