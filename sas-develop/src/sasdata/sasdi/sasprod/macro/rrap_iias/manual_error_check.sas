
%macro manual_error_check;

%let SAS_LOG=%sysget(SAS_LOG);
%put &=SAS_LOG;

%sysexec %str(grep -Ei "^ERROR:" ${SAS_LOG} | tr "\\t" "," >${SAS_LOG}.csv);


data _null_;
  if _N_ > 2 or eof then do; 
     call symputx('FLAG',_n_-1); 
     stop; 
  end;
  infile "&sas_log..csv" end=eof;
  input;
run;

%put &=FLAG;

%if %eval(&FLAG)>0 %then %do;
  %put "Terminating the JOB Due to Manual ERROR Check";
	%abort abend 255;
%END;
%ELSE %DO;
  %sysexec %str(rm ${SAS_LOG}.csv);
	%PUT "JOB COMPLETED SUCCESSFULLY";
%END;

%mend manual_error_check;