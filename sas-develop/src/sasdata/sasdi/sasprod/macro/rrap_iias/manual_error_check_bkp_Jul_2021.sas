
%macro manual_error_check;

%sysexec %str(echo ${program_name}_${NOW} >${SASPATH}/logs/${project_name}/${program_name}.txt);
%let prog_name=%sysget(program_name);


proc import datafile = "&rrap_dir/logs/rrap_iias/&prog_name..txt"
 out = name_now
 replace;
 getnames=NO;
 datarow=1;
run;

data _null_;
	set name_now;
	call symputx('prog_name_now',var1);
run;

%put &=prog_name_now;

%sysexec %str(rm ${SASPATH}/logs/${project_name}/${program_name}.txt);
%sysexec %str(grep -Ei "^ERROR:" ${SASPATH}/logs/${project_name}/${program_name}_${NOW}.sas.log | tr "\\t" "," >${SASPATH}/logs/${project_name}/MLS_${program_name}_${NOW}.csv);


data _null_;
  if _N_ > 2 or eof then do; 
     call symputx('FLAG',_n_-1); 
     stop; 
  end;
  infile "&rrap_dir/logs/rrap_iias/MLS_&prog_name_now..csv" end=eof;
  input;
run;

%put &=FLAG;

%if %eval(&FLAG)>0 %then %do;
  %put "Terminating the JOB Due to Manual ERROR Check";
	%abort abend 255;
%END;
%ELSE %DO;
  %sysexec %str(rm ${SASPATH}/logs/${project_name}/MLS_${program_name}_${NOW}.csv);
	%PUT "JOB COMPLETED SUCCESSFULLY";
%END;

%mend manual_error_check;