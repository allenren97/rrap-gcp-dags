***************************************************************************************************************************;
***************************************************************************************************************************;
***************************************************************************************************************************;
*  Job Name:J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_CC 
*                  
*  Target Database: EDRTLRPLL
*  Target Table:  BASEL_RCA_SCORE_DTL_SNAPSHOT_CC
*  
*  Purpose: MODEL CHANGE PARALLEL RUN
*
*  Frequency: MONTHLY
*
*  Notes: 
*  
*
* Change Log: INITIAL DEVELOPMENT - JANUARY 6, 2025
***************************************************************************************************************************;

/* Generate the process id for job  */ 
%put Process ID: &SYSJOBID;

/* General macro variables  */ 
%let jobID = %quote(A57SWEI7.BH0009HS);
%let etls_jobName = %nrquote(J_PLL_KS10_2301_BASEL_RCA_SCORE_SNAPSHOT_DTL_CC);
%let etls_userID = %nrquote(owprdsas);

/* Setup to capture return codes  */ 
%global job_rc trans_rc sqlrc syscc;
%let sysrc = 0;
%let job_rc = 0;
%let trans_rc = 0;
%let sqlrc = 0;
%let syscc = 0;
%global etls_stepStartTime; 
/* initialize syserr to 0 */
data _null_; run;

%macro rcSet(error); 
   %if (&error gt &trans_rc) %then 
      %let trans_rc = &error;
   %if (&error gt &job_rc) %then 
      %let job_rc = &error;
%mend rcSet; 

%macro rcSetDS(error); 
   if &error gt input(symget('trans_rc'),12.) then 
      call symput('trans_rc',trim(left(put(&error,12.))));
   if &error gt input(symget('job_rc'),12.) then 
      call symput('job_rc',trim(left(put(&error,12.))));
%mend rcSetDS; 

/* Create metadata macro variables */
%let IOMServer      = %nrquote(SASApp);
%let metaPort       = %nrquote(8561);
%let metaServer     = %nrquote(iw503);

/* Set metadata options */
options metaport       = &metaPort 
        metaserver     = "&metaServer"; 

/* Setup for capturing job status  */ 
%let etls_startTime = %sysfunc(datetime(),datetime.);
%let etls_recordsBefore = 0;
%let etls_recordsAfter = 0;
%let etls_lib = 0;
%let etls_table = 0;

%global etls_debug; 
%macro etls_setDebug; 
   %if %str(&etls_debug) ne 0 %then 
      OPTIONS MPRINT%str(;); 
%mend; 
%etls_setDebug; 

/*---- Start of Pre-Process Code  ----*/ 

DATA _NULL_;
	CALL SYMPUTX('PROCESSSTARTTIME',PUT(DATETIME(),DATETIME25.0));
RUN;

%PUT NOTE: ******* PROCESS START TIME: &PROCESSSTARTTIME. *******;
/*---- End of Pre-Process Code  ----*/ 

%rcSet(&syserr); 
%rcSet(&sqlrc); 

/*==========================================================================* 
 * Step:            AM_rrap_ks_autoexec                   A57SWEI7.BK000YRC * 
 * Transform:       AM_rrap_ks_autoexec                                     * 
 * Description:                                                             * 
 *==========================================================================*/ 

%let transformID = %quote(A57SWEI7.BK000YRC);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let _INPUT_count = 0; 
%let _OUTPUT_count = 0; 


%put WORK LOCATION: %sysfunc(getoption(work));
%include '&rrap_dir/macro/rrap_iias/rrap_pll_autoexec.sas';
%rrap_pll_autoexec(RRAPENV=REVOLVING_CREDIT);
%rcSet(&syserr); 
%rcSet(&sysrc); 
%rcSet(&sqlrc); 

%rcSet(&syscc); 



/**  Step end AM_rrap_ks_autoexec **/

/*==========================================================================* 
 * Step:            AM_sas_dataset_cleanup                A57SWEI7.BK000YRD * 
 * Transform:       AM_sas_dataset_cleanup                                  * 
 * Description:                                                             * 
 *==========================================================================*/ 

%let transformID = %quote(A57SWEI7.BK000YRD);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let _INPUT_count = 0; 
%let _OUTPUT_count = 0; 


/*CLEANUP MACRO*/
/*THIS MACRO DELETES ALL TEMPORARY SAS DATASETS LISTED BY THE USER AT THE SPECIFIED PATH ON AIX PLATFORM*/
/*LIBREF SHOULD BE THE LIBREF DECLARED FOR A UNIX PATH IN THE PROGRAM*/
/*DATASETS SHOULD BE THE LIST OF DATASETS SEPARATED BY A SINGLE SPACE*/
%MACRO SAS_DATASET_CLEANUP (LIBREF=, DATASETS=);

/*DELETE TEMPORARY DATASETS*/
PROC DATASETS LIBRARY=&LIBREF;
DELETE &DATASETS;
RUN;
QUIT;

%MEND;

%rcSet(&syserr); 
%rcSet(&sysrc); 
%rcSet(&sqlrc); 

%rcSet(&syscc); 



/**  Step end AM_sas_dataset_cleanup **/

/*==========================================================================* 
 * Step:            List of Time ID's                     A57SWEI7.BK000YRE * 
 * Transform:       User Written                                            * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Table:    TM_DIM - NZRRAP.TM_DIM                A57SWEI7.BE000412 * 
 * Target Table:    User Written - work.time_key          A57SWEI7.BN000NFZ * 
 *                                                                          * 
 * User Written:    SourceCode                                              * 
 *==========================================================================*/ 

%let transformID = %quote(A57SWEI7.BK000YRE);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let etls_recnt = 0;
%macro etls_recordCheck; 
   %let etls_recCheckExist = %eval(%sysfunc(exist(NZRRAP.TM_DIM, DATA)) or 
         %sysfunc(exist(NZRRAP.TM_DIM, VIEW))); 
   
   %if (&etls_recCheckExist) %then
   %do;
      proc sql noprint;
         select count(*) into :etls_recnt from NZRRAP.TM_DIM;
      quit;
   %end;
%mend etls_recordCheck;
%etls_recordCheck;

%let SYSLAST = %nrquote(NZRRAP.TM_DIM); 

%let _INPUT_count = 1;
%let _INPUT = NZRRAP.TM_DIM;

%let _INPUT1 = NZRRAP.TM_DIM;

%let _OUTPUT_count = 1;
%let _OUTPUT = work.time_key;
/* List of target columns to keep  */ 
%let _OUTPUT_keep = TM_ID TM_LVL_END_DT;

%let _OUTPUT1 = work.time_key;
/* List of target columns to keep  */ 
%let _OUTPUT1_keep = TM_ID TM_LVL_END_DT;

/*---- Start of User Written Code  ----*/ 


%get_model_period_dates(product=ks);
%put Start and End Dates for KS Models:;
%put start_period_dt: &start_period_dt ;
%put end_period_dt: &end_period_dt;



proc sql;
	create table time_key as
	select TM_ID, put(tm_lvl_end_dt,date9.) as tm_lvl_end_dt from 
	nzrrap.TM_DIM
	where tm_lvl='Month' and tm_lvl_end_dt between "&start_period_dt"d and "&end_period_dt"d
	order by 1;
quit;



%rcSet(&syserr); 
%rcSet(&sqlrc); 
%rcSet(&syscc); 



/**  Step end List of Time ID's **/

/*==========================================================================* 
 * Step:            Loop                                  A57SWEI7.BK000YRF * 
 * Transform:       Loop                                                    * 
 * Description:                                                             * 
 *                                                                          * 
 * Source Table:    User Written - work.time_key          A57SWEI7.BN000NFZ * 
 * Target Table:    Loop - work.WOLVMA                    A57SWEI7.BN000NG0 * 
 *==========================================================================*/ 

%let transformID = %quote(A57SWEI7.BK000YRF);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%let etls_recCheckExist = 0; 
%let etls_recnt = 0; 
%macro etls_recordCheck; 
   %let etls_recCheckExist = %eval(%sysfunc(exist(work.time_key, DATA)) or 
         %sysfunc(exist(work.time_key, VIEW))); 
   
   %if (&etls_recCheckExist) %then
   %do;
      %local etls_syntaxcheck; 
      %let etls_syntaxcheck = %sysfunc(getoption(syntaxcheck)); 
      /* Turn off syntaxcheck option to perform following steps  */ 
      options nosyntaxcheck;
      
      proc contents data = work.time_key out = work.etls_contents(keep = nobs) noprint; 
      run; 
      
      data _null_; 
         set work.etls_contents (obs = 1); 
         call symput("etls_recnt", left(put(nobs,32.))); 
      run;
      
      proc datasets lib = work nolist nowarn memtype = (data view);
         delete etls_contents;
      quit;
      
      /* Reset syntaxcheck option to previous setting  */ 
      options &etls_syntaxcheck; 
   %end;
%mend etls_recordCheck;
%etls_recordCheck;

%let SYSLAST = %nrquote(work.time_key); 

/* Capture summary statistics about this step for performance reporting  */ 
%put "DIS_SUMM";
%macro etls_getHandle(statusTable=, handleVariable=, row=); 
   %let etls_dsid = %sysfunc(open(&statusTable)); 
   %if (&etls_dsid = 0) %then 
      %put %sysfunc(sysmsg()); 
   %else 
   %do; 
      %let rc = %sysfunc(fetchobs(&etls_dsid, &row)); 
      %if (&rc ne 0) %then 
         %put %sysfunc(sysmsg()); 
      %else 
      %do; 
         %let etls_varnum = %sysfunc(varnum(&etls_dsid,&handleVariable)); 
         %if (&etls_varnum > 0) %then 
            %sysfunc(getvarc(&etls_dsid,&etls_varnum)); 
         %else 
            %put %sysfunc(sysmsg()); 
      %end; 
      %let rc = %sysfunc(close(&etls_dsid)); 
   %end; 
%mend etls_getHandle; 

%macro etls_freeHandle(statusTable=, statusVariable=, handleVariable=, 
   handleName=, statusSetting="Finished", 
   endTimeVariable=endTime, startTimeVariable=startTime, signoff=1, 
   returnCodeVariable=, returnCodeMacroVariable=, setMainJobRC=1, 
   statusUnknownReturnCode=., startTimeMacroVariable=, endTimeMacroVariable= ); 

   %if (&statusTable ne ) %then 
   %do; 
      %local etls_rcMacroVarExisted; 
      %let etls_rcMacroVarExisted = 0; 
      %if ("&returnCodeMacroVariable" ne "") %then 
      %do; 
         proc sql noprint; 
            select '1' into: etls_rcMacroVarExisted from dictionary.macros 
            where name=upcase("&returnCodeMacroVariable"); 
         quit; 

         %rcSet(&sqlrc); 
         %if (&etls_rcMacroVarExisted = 0) %then 
         %do; 
            %put WARNING%QUOTE(:) Return code from inner job not found.  Setting status to Unknown.;
            %let &returnCodeMacroVariable=&statusUnknownReturnCode; 
         %end; 
         %if (&setMainJobRC eq 1) %then 
            %rcSet(&&&returnCodeMacroVariable); 
      %end; 
      %else 
      %do; 
         %let returnCodeMacroVariable=etls_rcmacvar; 
         %let &returnCodeMacroVariable=&statusUnknownReturnCode; 
      %end; 

      %local etls_startTimeMacroVarExisted; 
      %let etls_startTimeMacroVarExisted = 0; 
      %if ("&startTimeMacroVariable" ne "") %then 
      %do; 
         proc sql noprint; 
            select '1' into: etls_startTimeMacroVarExisted from dictionary.macros 
            where name=upcase("&startTimeMacroVariable"); 
         quit; 

         %rcSet(&sqlrc); 
         %if (&etls_startTimeMacroVarExisted = 0) %then 
         %do; 
            %put WARNING%QUOTE(:) Start time from inner job not found.  No value will be set.;
            %let &startTimeMacroVariable=; 
         %end; 
      %end; 

      %local etls_endTimeMacroVarExisted; 
      %let etls_endTimeMacroVarExisted = 0; 
      %if ("&endTimeMacroVariable" ne "") %then 
      %do; 
         proc sql noprint; 
            select '1' into: etls_endTimeMacroVarExisted from dictionary.macros 
            where name=upcase("&endTimeMacroVariable"); 
         quit; 

         %rcSet(&sqlrc); 
         %if (&etls_endTimeMacroVarExisted = 0) %then 
         %do; 
            %put WARNING%QUOTE(:) End time from inner job not found.  Setting end time to current
      time.;
            %let &endTimeMacroVariable=%sysfunc(datetime()); 
         %end; 
      %end; 

      data &statusTable; 
         modify &statusTable(where=(&handleVariable = &handleName)); 
         %if ("&startTimeMacroVariable" ne "") %then 
         %do; 
            &startTimeVariable = input(symget("&startTimeMacroVariable"),32.);; 
         %end; 
         %if ("&endTimeVariable" ne "") %then 
         %do; 
            %if ("&endTimeMacroVariable" ne "") %then 
            %do; 
               &endTimeVariable = input(symget("&endTimeMacroVariable"),32.);; 
            %end; 
            %else 
               &endTimeVariable = datetime();; 
         %end; 
         %if ("&returnCodeVariable" ne "") %then 
            &returnCodeVariable = input(symget("&returnCodeMacroVariable"),32.);; 
         %if ("&statusVariable" ne "") %then 
         %do; 
            if (symget("etls_rcMacroVarExisted") eq "0") then 
               &statusVariable = "Unknown Status"; 
            else
               &statusVariable = &statusSetting;
         %end;
         call symput('handle',&handleVariable); 
         replace; 
         stop; 
      run; 

      %rcSet(&syserr); 

      %if (&signoff eq 1) %then 
         %etls_signoff(handleName=&handle); 
   %end; 
%mend etls_freeHandle; 

%macro etls_createHandle(statusTable=,
                         statusVariable=,
                         handleVariable=,
                         handlePrefix=rmt,
                         gridOptionSet=,
                         workloadMacroVariable=,
                         row=,
                         machineVariable=,
                         statusSetting="Running", 
                         startTimeVariable=startTime,
                         signon=1,
                         useGrid=1,
                         log=,
                         output=,
                         gridRC=,
                         cmacvar=etls_signonStatus,
                         additionalSignonOptions=,
                         signonRetries= ); 

   %local remoteSessionId; 
   %let remoteSessionId = &handlePrefix.&row; 
   %let &cmacvar = 1; 
   %local etls_machineId; 
   %if (&signon eq 1) %then 

      %etls_signon(handleName=&remoteSessionId,
                   useGrid=&useGrid,
                   machineIdMacroVariable=etls_machineId,
                   gridOptionSet=&etls_gridOptionSet,
                   workloadMacroVariable=&workloadMacroVariable,
                   log=&log,
                   output=&output,
                   cmacvar=&cmacvar,
                   gridRC=&gridRC,
                   additionalSignonOptions=&additionalSignonOptions,
					signonRetries=&signonRetries,
                   gridJobName=DIS_&etls_jobName._&row); 

   %else %let &cmacvar=0; 

   data &statusTable; 
      retain ptr &row; 
      modify &statusTable point = ptr; 
      &handleVariable = "&remoteSessionId"; 
      %if (&signon eq 1) %then 
         &machineVariable = "&etls_machineId";; 
      %if (&&&cmacvar ne 0) %then 
      %do; 
         &statusVariable = "Failed Signon"; 
      %end; 
      %else 
         &statusVariable = &statusSetting;; 
      %if (&startTimeVariable ne ) %then 
         &startTimeVariable = datetime();; 
      replace; 
      stop; 
   run; 

   %rcSet(&syserr); 
%mend etls_createHandle; 

%macro etls_getParameterNames(parameterTable=, parameterVariableMacro=, startingColumnNumber=1);
   %let &parameterVariableMacro = ;
   %let dsid = %sysfunc(open(&parameterTable));
   %if (&dsid gt 0) %then 
   %do; 
      %do i=&startingColumnNumber %to %sysfunc(attrn(&dsid,nvars)); 
         %let &parameterVariableMacro = &&&parameterVariableMacro %sysfunc(varname(&dsid,&i)); 
      %end; 
      %let dsid = %sysfunc(close(&dsid)); 
   %end; 
   %else 
      %put %sysfunc(sysmsg()); 
   %rcSet(&syserr); 
%mend etls_getParameterNames; 

%macro etls_getParameters(parameterTable=, row=, startingColumnNumber=1 , handleName=); 
   data _null_; 
      length vname $256 vtype $1 value $32767; 
      dsid = open("&parameterTable"); 
      if (dsid > 0) then 
      do; 
         do _i = 1 to &row; 
            fetchrc = fetch(dsid); 
         end; 
         do _i=&startingColumnNumber to attrn(dsid,'nvars'); 
            vname = varname(dsid,_i); 
            vtype = vartype(dsid,_i); 
            if (fetchrc = 0) then 
            do; 
               if (vtype = 'C') then 
               do; 
                  value = getvarc(dsid,_i); 
                  value = tranwrd(value,"%","%%"); 
                  value = tranwrd(value,"(","%("); 
                  value = tranwrd(value,")","%)"); 
                  value = tranwrd(value,'"','%"'); 
                  value = tranwrd(value,"'","%'"); 
               end; 
               else 
                  value = left(put(getvarn(dsid,_i),best32.)); 
            end; /* fetchrc = 0 */ 
            
            put ;
            if indexc(trimn(value),"+-*/<>=^~;, '()&%",'"')>0 then 
            do; 
               value='%nrstr('||trim(value)||')'; 
               put "ETLS_DIAG%QUOTE(:) Special characters encountered; References may require: %nrbquote(%)UNQUOTE(&" vname+(-1)').';
               put "NOTE: Special characters encountered; References may require: %nrbquote(%)UNQUOTE(&" vname+(-1)').';
            end; 
            %if %str(&handlename) ne %str() %then 
            %do; 
               value = '%syslput '||trim(vname)||'= '||trim(value)||" / remote = &handleName;"; 
            %end; 
            %else 
            %do; 
               value = '%let '||trim(vname)||'= '||trim(value)||';'; 
            %end; 
            
            put "NOTE: Setting macro variable " vname "with statement:" value ;
            call execute(value); 
         end; /* do i= */ 
         dsid = close(dsid); 
      end; /* dsid > 0 */ 
      else 
      do; 
         put "ERROR%QUOTE(:) Parameter table, &parameterTable., could not be"
              " opened.";
         abort; 
      end; 
      stop; 
   run; 
   
   %rcSet(&syserr); 
%mend etls_getParameters; 

%macro etls_loopW2F6P5H; 
   %local etls_filePrefix; 
   %let etls_filePrefix = ; 
   
   %macro etls_processToLoopW2F6P5R(parameterTable=, row=, handleName=rmt);
      %local etls_parmvars; 
      %etls_getParameterNames(parameterTable=&parameterTable, 
         parameterVariableMacro=etls_parmvars, 
         startingColumnNumber=1); 
      %local &etls_parmvars; 
      %etls_getParameters(parameterTable=&parameterTable, row=&row, 
         startingColumnNumber=1); 
      %let etls_previousFilePrefix = &etls_filePrefix;
      %local etls_filePrefix; 
      %let etls_filePrefix = &etls_previousFilePrefix.&handleName; 
      %macro etls_jobW2F6P61; 
      
         /*==========================================================================* 
          * Step:            RRAP_DM_INS_BASEL_RCA_SCORE_SNAPSHOT  A57SWEI7.BK000YRG * 
          *                  _DTL_CC                                                 * 
          * Transform:       User Written                                            * 
          * Description:                                                             * 
          *                                                                          * 
          * Source Table:    Loop - work.WOLVMA                    A57SWEI7.BN000NG0 * 
          *                                                                          * 
          * User Written:    SourceCode                                              * 
          *==========================================================================*/ 
         
         %let transformID = %quote(A57SWEI7.BK000YRG);
         %let trans_rc = 0;
         %let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 
         
         %let etls_recCheckExist = 0; 
         %let etls_recnt = 0; 
         %macro etls_recordCheck; 
            %let etls_recCheckExist = %eval(%sysfunc(exist(work.WOLVMA, DATA)) or 
                  %sysfunc(exist(work.WOLVMA, VIEW))); 
            
            %if (&etls_recCheckExist) %then
            %do;
               %local etls_syntaxcheck; 
               %let etls_syntaxcheck = %sysfunc(getoption(syntaxcheck)); 
               /* Turn off syntaxcheck option to perform following steps  */ 
               options nosyntaxcheck;
               
               proc contents data = work.WOLVMA out = work.etls_contents(keep = nobs) noprint; 
               run; 
               
               data _null_; 
                  set work.etls_contents (obs = 1); 
                  call symput("etls_recnt", left(put(nobs,32.))); 
               run;
               
               proc datasets lib = work nolist nowarn memtype = (data view);
                  delete etls_contents;
               quit;
               
               /* Reset syntaxcheck option to previous setting  */ 
               options &etls_syntaxcheck; 
            %end;
         %mend etls_recordCheck;
         %etls_recordCheck;
         
         %let SYSLAST = %nrquote(work.WOLVMA); 
         
         %let ETLS_SYSLAST = &SYSLAST;
         /*---- Start of Pre-Process Code  ----*/ 
         
         %let MODULE=KS;
         %let BATCH=2.3;
         
         PROC SQL noprint;
         select basel_model_id INTO :MODEL_ID_DEL SEPARATED BY ',' from NETCON.BASEL_MODEL_SCORECRD_HDR where BASEL_SCORECRD_NM like 'CC%';
         QUIT;
         %let MODEL_ID_DEL=&MODEL_ID_DEL.;
         
         %etl_job_start(TARGET_TABLE=NETCON.BASEL_RCA_SCORE_DTL_SNAPSHOT_CC,CONDITION=%nrbquote(BASEL_MODEL_ID IN (&MODEL_ID_DEL)) );
         /*---- End of Pre-Process Code  ----*/ 
         
         %rcSet(&syserr); 
         %rcSet(&sqlrc); 
         
         %let SYSLAST = &ETLS_SYSLAST;
         
         %let _INPUT_count = 1;
         %let _INPUT = work.WOLVMA;
         
         %let _INPUT1 = work.WOLVMA;
         
         %let _OUTPUT_count = 0;
         /*---- Start of User Written Code  ----*/ 
         
      
         
         %GLOBAL TMSTMP;
         %GLOBAL TIME_DIM_ID;
         
/*MACRO TO EXTRACT AND LOAD ACCOUNT IDS FOR EVERY CRITERIA LISTED IN CRITERIAS TABLE. USING A TYPICAL DO LOOP*/
%MACRO EXTRACT();

      %LET SESSIONTIME=%SYSFUNC(DATETIME(),BEST18.); 
      %LET i=1;

      /* HARD CODE TM_DIM_ID FOR TESTING PURPOSES*/
      %let TIME_DIM_ID=&MTH_TM_ID;
      %PUT MTH_TM_ID=&MTH_TM_ID;

      PROC SQL NOPRINT;
      CONNECT USING NETCON AS NZCON1;
      EXECUTE(DROP TABLE &net_db..BASEL_RCA_SCORE_DTL_SNAPSHOT_CC IF EXISTS)BY NZCON1;
      QUIT;

      %put &=syserr;
      %put &=sqlrc;

      %if &syserr>0 %then %do;
      %abort abend 255;
      %end;

      /*DEFINE AND CREATE TABLE STRUCTURE TO STORE ALL VALID CRITERIAS*/
      DATA prg_data.CRITERIAS_CC;
      LENGTH PRIM_ID 8 BASEL_MODEL_SCORECRD_DTL_ID 8 BASEL_MODEL_SCORECRD_HDR_ID 8 CRTRIA_DESC $255 BIN 8 BIN_CRTRIA_SQL_CD_STRG $2000;
      STOP;
      RUN;

      %put &=syserr;
      %put &=sqlrc;


      /*DEFINE AND CREATE TABLE STRUCTURE TO STORE RESULTING ACCOUNTS FOR EACH CRITERIA*/
      DATA prg_data.CRITERIAS_DTL_CC;
      LENGTH BASEL_MODEL_SCORECRD_DTL_ID 8 MTH_TM_ID 8 BASEL_ACCT_ID 8 BASEL_MODEL_ID 8 BASEL_MODEL_SCORECRD_HDR_ID 8 BIN 8 PT_CNT 8 INSRT_PROCESS_TMSTMP 8;
      FORMAT INSRT_PROCESS_TMSTMP DATETIME.;
      STOP;
      RUN;

      %put &=syserr;
      %put &=sqlrc;

      /*SELECT ALL VALID CRITERIAS AND LOAD THEM INTO THE TABLE CRITERIAS. THIS TABLE WILL BE SAVED AT AIX PATH prg_data DEFINED EARLIER*/
      PROC SQL NOPRINT;

      INSERT INTO prg_data.CRITERIAS_CC
      SELECT
      MONOTONIC() AS PRIM_ID, /* SIMPLE SEQUENCE*/
      DTL.BASEL_MODEL_SCORECRD_DTL_ID,
      DTL.BASEL_MODEL_SCORECRD_HDR_ID,
      'NODATA' AS CRTRIA_DESC,
      /*TRANWRD(DTL.CRTRIA_DESC,'0D'x,'') AS CRTRIA_DESC, *//*REPLACE CARRIAGE RETURN CHARACTERS WITH NULL*/
      DTL.BIN,
      DTL.BIN_CRTRIA_SQL_CD_STRG 
      FROM NETCON.BASEL_MODEL_SCORECRD_DTL DTL, NETCON.BASEL_MODEL_SCORECRD_HDR HDR 
      WHERE DTL.BASEL_MODEL_SCORECRD_HDR_ID=HDR.BASEL_MODEL_SCORECRD_HDR_ID
      AND HDR.SRC_SYS_CD='KS' AND (HDR.SCORECRD_END_DT IS NULL OR HDR.SCORECRD_END_DT='31DEC9999'd)
      AND DTL.BIN_CRTRIA_SQL_CD_STRG IS NOT NULL
      AND HDR.BASEL_MODEL_ID in (select basel_model_id from NETCON.BASEL_MODEL_SCORECRD_HDR where BASEL_SCORECRD_NM like 'CC%'); 
      /*AND DTL.BASEL_MODEL_SCORECRD_HDR_ID=4009;*/

      QUIT;

      %put &=syserr;
      %put &=sqlrc;


      PROC SQL NOPRINT;
      /*SETTING UP PASS THROUGH CONNECTION TO NETEZZA*/

      connect using NZRRAP as nzcon;
      /*PERFORM A COUNT OF VALID CRITERIAS FOR WHICH THE ITERATIVE LOOP WILL EXECUTE*/
      SELECT COUNT(*) INTO :CRITERIA_COUNT FROM prg_data.CRITERIAS_CC;

      %DO i=1 %TO &CRITERIA_COUNT;

      /*FOR EVERY VALUE OF i, SELECT THE SUBQUERY INTO A SAS VARIABLE*/
      SELECT BIN_CRTRIA_SQL_CD_STRG INTO :VAR1 FROM prg_data.CRITERIAS_CC WHERE PRIM_ID=&i;

      /*INSERT ROW BY ROW INTO THE PREDEFINED SAS TABLE THE ACCOUNTS FOR EVERY CRITERIA*/
      INSERT INTO prg_data.CRITERIAS_DTL_CC (BASEL_MODEL_SCORECRD_DTL_ID,MTH_TM_ID,BASEL_ACCT_ID,BASEL_MODEL_ID,BASEL_MODEL_SCORECRD_HDR_ID,BIN,PT_CNT,INSRT_PROCESS_TMSTMP)
      SELECT D.BASEL_MODEL_SCORECRD_DTL_ID, X.MTH_TM_ID, X.BASEL_ACCT_ID, C.BASEL_MODEL_ID, C.BASEL_MODEL_SCORECRD_HDR_ID, D.BIN, D.PT_CNT, &SESSIONTIME AS INSRT_PROCESS_TMSTMP
      FROM NETCON.BASEL_MODEL_SCORECRD_HDR C, NETCON.BASEL_MODEL_SCORECRD_DTL D, (SELECT * FROM CONNECTION TO NZCON (&VAR1)) X
      WHERE C.BASEL_MODEL_SCORECRD_HDR_ID=D.BASEL_MODEL_SCORECRD_HDR_ID
      AND D.BASEL_MODEL_SCORECRD_HDR_ID=(SELECT BASEL_MODEL_SCORECRD_HDR_ID FROM prg_data.CRITERIAS_CC WHERE PRIM_ID=&i) 
      AND D.BASEL_MODEL_SCORECRD_DTL_ID=(SELECT BASEL_MODEL_SCORECRD_DTL_ID FROM prg_data.CRITERIAS_CC WHERE PRIM_ID=&i)
      /*AND TRIM(D.CRTRIA_DESC)=(SELECT CRTRIA_DESC FROM prg_data.CRITERIAS_CC WHERE PRIM_ID=&i) */
      AND D.BIN=(SELECT BIN FROM prg_data.CRITERIAS_CC WHERE PRIM_ID=&i);

      /*END THE DO LOOP*/
      %END;

      /*QUIT THE PROC SQL PROCEDURE*/
      DISCONNECT FROM NZCON;
      QUIT;

      %put &=syserr;
      %put &=sqlrc;

/*END THE MACRO DEFINITION*/
%MEND;

/*EXECUTING THE MACRO*/
%EXTRACT();

PROC APPEND BASE=NETCON.BASEL_RCA_SCORE_DTL_SNAPSHOT_CC ( BULKLOAD=YES BL_METHOD=CLILOAD ) DATA=prg_data.CRITERIAS_DTL_CC FORCE; RUN;

%put &=syserr;
       %put &=sqlrc;

  /*    proc sql;
         select count(*) into: manul_error_check FROM NETCON.BASEL_RCA_SCORE_DTL_SNAPSHOT WHERE MTH_TM_ID=&TIME_DIM_ID
         and basel_model_id in (select basel_model_id from NETCON.BASEL_MODEL_SCORECRD_HDR where BASEL_SCORECRD_NM like 'CC%');
      quit;

      data _null_;
         if STRIP(&manul_error_check) = '0' then do;
            abort abend 255;
         end;
         else do;
            put "Job Completed successfully";
         end;
      run;

      %put &=syserr;
      %put &=sqlrc;*/






/*         Proc sql;*/
/*         */
/*         INSERT INTO NETCON.BASEL_RCA_SCORE_DTL_SNAPSHOT (*/
/*         		BASEL_MODEL_SCORECRD_DTL_ID*/
/*                , MTH_TM_ID*/
/*                , BASEL_ACCT_ID*/
/*                , BASEL_MODEL_ID*/
/*                , BASEL_MODEL_SCORECRD_HDR_ID*/
/*                , BIN*/
/*                , PT_CNT*/
/*                , INSRT_PROCESS_TMSTMP*/
/*         ) */
/*         select*/
/*         BASEL_MODEL_SCORECRD_DTL_ID,*/
/*         MTH_TM_ID,*/
/*         BASEL_ACCT_ID,*/
/*         BASEL_MODEL_ID, */
/*         BASEL_MODEL_SCORECRD_HDR_ID,*/
/*         BIN,*/
/*         PT_CNT,*/
/*         INSRT_PROCESS_TMSTMP*/
/*         from prg_data.CRITERIAS_DTL_CC*/
/*         ;*/
/*         quit;*/
         
         %SAS_DATASET_CLEANUP(LIBREF=PRG_DATA,DATASETS=CRITERIAS_CC CRITERIAS_DTL_CC);

         %put &=syserr;
         %put &=sqlrc;
         
         /*---- End of User Written Code  ----*/ 
         
         %rcSet(&syserr); 
         %rcSet(&sqlrc); 
         /*---- Start of Post-Process Code  ----*/ 
         
         /* %etl_job_end; */
         /*---- End of Post-Process Code  ----*/ 
         
         %rcSet(&syserr); 
         %rcSet(&sqlrc); 
         
         %rcSet(&syscc); 
         
         
         
         /**  Step end RRAP_DM_INS_BASEL_RCA_SCORE_SNAPSHOT_DTL_CC **/
         
      %mend etls_jobW2F6P61; 
      
      %etls_jobW2F6P61; 
      
      %let handleName = %etls_getHandle(statusTable=&etls_statusTable,
         handleVariable=etls_handleName, row=&&&etls_controlName);
         
      %let job_rc&handleName. = &job_rc; 
      
      %etls_freeHandle(statusTable=&etls_statusTable, statusVariable=etls_status, 
         handleVariable=etls_handleName, handleName="&handleName", 
         startTimeVariable=etls_startTime, endTimeVariable=etls_endTime, signoff=0, 
         returnCodeVariable=etls_jobRC, returnCodeMacroVariable=job_rc&handleName., setMainJobRc=1); 
         
   %mend etls_processToLoopW2F6P5R;
   
   %local etls_controlTable etls_statusTable etls_controlName 
      etls_processesRunning etls_maxProcesses etls_parameterTable 
      etls_additionalSignonOptions etls_signonRetries;
   %let etls_controlName = KS_; 
   %let etls_statusTable = work.WOLVMA; 
   %let etls_parameterTable = work.W2F6P5P; 
   %let &etls_controlName = 0;
   %let etls_controlTable = work.time_key;
   
   %put %str(NOTE: Creating status table...);
   data &etls_statusTable 
      (keep = etls_handleName etls_machineId etls_startTime etls_endTime etls_status 
              etls_jobRC TM_ID TM_LVL_END_DT
      );
      attrib etls_handleName length = $32
         label = 'Name of handle to remote session'; 
      attrib etls_machineId length = $32
         label = 'Name of machine executing the task'; 
      attrib etls_startTime length = 8
         format = nldatmap.
         label = 'Start time of task'; 
      attrib etls_endTime length = 8
         format = nldatmap.
         label = 'End time of task'; 
      attrib etls_status length = $32
         label = 'Current status of task'; 
      attrib etls_jobRC length = 8
         label = 'Return code of task'; 
      attrib TM_ID length = 8; 
      attrib TM_LVL_END_DT length = $9; 
      set &etls_controlTable;
   run;
   
   %rcSet(&syserr); 
   
   %put %str(NOTE: Creating parameter table...);
   proc sql; 
      create table &etls_parameterTable as 
         select TM_ID as mth_tm_id, 
                TM_LVL_END_DT as TM_LVL_END_DT 
         from &etls_controlTable; 
   quit; 
   
   %rcSet(&sqlrc); 
   
   /* Get the number of times to iterate from the number of rows in the source  */ 
   /*  table                                                                    */ 
   proc sql noprint; 
      select count(*) into :&etls_controlName._max from &etls_statusTable;
      %let &etls_controlName._max = &&&etls_controlName._max;
   quit;
   
   %rcSet(&sqlrc); 
   
   %let etls_maxProcesses = 1; 
   
   %if (&etls_maxProcesses > 0) %then 
   %do; 
      %do %until (&&&etls_controlName ge &&&etls_controlName._max); 
      
         %let etls_lastLoopPtr = &&&etls_controlName;
         
         %let etls_processesRunning = 0; 
         
         %do %while(&etls_processesRunning lt &&&etls_controlName._max 
            and &etls_processesRunning lt &etls_maxProcesses 
            and &&&etls_controlName lt &&&etls_controlName._max);
            
            %let &etls_controlName = %eval(&&&etls_controlName+1);
            
            %let job_rcLast=&job_rc; 
            
            %etls_createHandle(statusTable=&etls_statusTable,
                               statusVariable=etls_status,
                               handleVariable=etls_handleName,
                               handlePrefix=&etls_controlName,
                               statusSetting="Running",
                               row=&&&etls_controlName,
                               machineVariable=etls_machineId,
                               startTimeVariable=etls_startTime,
                               signon=0,
                               useGrid=0);
            
         %let etls_processesRunning = 1; 
         %etls_processToLoopW2F6P5R(parameterTable=&etls_parameterTable, row=&&&etls_controlName,
            handleName=%etls_getHandle(statusTable=&etls_statusTable,
            handleVariable=etls_handleName, row=&&&etls_controlName)); 
         
         /* Reset main Job_RC and Trans_RC to max return code of all iterations */ 
         %let job_rcThisIter=&job_rc; 
         %let job_rc=&job_rcLast; 
         %let trans_rc=&job_rcLast; 
         %rcSet(&job_rcThisIter); 
      %end; 
      
   %end; 
   
%end;

proc datasets lib = work nolist nowarn memtype = (data view);
   delete W2F6P5P;
quit;

%mend etls_loopW2F6P5H; 

%etls_loopW2F6P5H; 

%rcSet(&syscc); 



/**  Step end Loop **/

/*==========================================================================* 
 * Step:            Loop End                              A57SWEI7.BK000YRH * 
 * Transform:       Loop End                                                * 
 * Description:                                                             * 
 *==========================================================================*/ 

%let transformID = %quote(A57SWEI7.BK000YRH);
%let trans_rc = 0;
%let etls_stepStartTime = %sysfunc(datetime(), datetime20.); 

%rcSet(&syscc); 



/**  Step end Loop End **/

/*---- Start of Post-Process Code  ----*/ 

DATA _NULL_;
	CALL SYMPUTX('PROCESSENDTIME',PUT(DATETIME(),DATETIME25.0));
	CALL SYMPUTX('PROCESSRUNTIME',PUT((DATETIME()-"&PROCESSSTARTTIME"dt),TIME.));
RUN;

%PUT NOTE: ******* PROCESS END TIME: &PROCESSENDTIME.  *******;
%PUT NOTE: ******* PROCESS RUNTIME:  &PROCESSRUNTIME.  *******;
/*---- End of Post-Process Code  ----*/ 

%rcSet(&syserr); 
%rcSet(&sqlrc); 

%let etls_endTime = %sysfunc(datetime(),datetime.);