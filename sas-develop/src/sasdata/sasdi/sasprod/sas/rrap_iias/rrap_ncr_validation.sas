/*
01JUNE2016 - Hadi Dimashkieh - Fix issue with this code not running in batch
							   Remove Economic Capital from compare in flag 15
02/02/2022 - Ganesh Patro Decomission of NCR BE portion from below code.
*/

options mprint;
options validvarname=ANY;
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);
%report_validation;

%let st = %str(input 'Record Type'n  1 - 3 
      'PD Band Type'n  4 - 7 
       'Exposure Size Type'n  8 - 11
       'Geography Type'n  12 - 15
       'LTV Type'n 16 - 19
       'Exposure Class Type'n 20 - 23 
       'Securitization Type'n 24 - 27
       'Rates Type'n 28 - 31
       'Delinquency Bucket Type'n 32 - 35
@;);

        data RP000 ;
infile  &path2.;
&st.
if 'Record Type'n = '000' then do;
Input 'Institution ID'n $ 36  - 39
      'Reporting Date'n  40 - 47
       'Return Name'n $ 48 - 54
       'Return File Layout Version'n $ 55 - 60
       Filler $ 61 - 370
       'Row Counter'n 371 - 378
       'CR LF'n $ 379 - 380;

output RP000; 
end;
        run;

                data RP005 ;
infile  &path2.;
&st.
if 'Record Type'n = '005' then do;
Input 'PD Band Name'n $ 36 - 95
      'PD Band Percentage'n 96 - 101
       Filler $ 102 - 370
       'Row Counter'n 371 - 378
;

output RP005; 
end;
        run;

        %macro outstanding( RP = );
                     data RP&RP. ;
infile  &path2.;
&st.
if 'Record Type'n = "&RP." then do;
Input Outstanding 36 - 50
       Filler $ 51 - 370
       'Row Counter'n 371 - 378
;

output RP&RP.; 
end;
        run;
        %mend;
        %Outstanding( RP = 010 );
        %Outstanding( RP = 015 );
        %Outstanding( RP = 020 );
        %Outstanding( RP = 025 );
        %Outstanding( RP = 030 );
        %Outstanding( RP = 035 );

                          data RP040 ;
infile  &path2.;
&st.
if 'Record Type'n = "040" then do;
Input 'Accounts Over Limit'n 36 - 41
       Filler $ 42 - 370
       'Row Counter'n 371 - 378
;

output RP040; 
end;
        run;

                               data RP045 ;
infile  &path2.;
&st.
if 'Record Type'n = "045" then do;
Input 'Authorized'n  36 - 50
      'Fraud Losses'n 51 - 65   
      'Estimated IRB EAD'n 66 - 80
      'Number of Accounts Written Off'n 81 - 89
       Filler $  90 - 370
       'Row Counter'n 371 - 378
;

output RP045; 
end;
        run;

                                    data RP047 ;
infile  &path2.;
&st.
if 'Record Type'n = "047" then do;
Input 'Total Write Offs'n  36 - 50
      'IRB Expected Losses'n 51 - 65
       Filler $  66 - 370
       'Row Counter'n 371 - 378
;

output RP047; 
end;
        run;

                                         data RP050 ;
infile  &path2.;
&st.
if 'Record Type'n = "050" then do;
Input 'Recoveries'n  36 - 50
      'Economic Losses'n 51 - 65
      'IRB Capital'n 66 - 80
       Filler $  81 - 370
       'Row Counter'n 371 - 378
;

output RP050; 
end;
        run;

                                         data RP055 ;
infile  &path2.;
&st.
if 'Record Type'n = "055" then do;
Input 'Outstanding'n 36 - 50    
      'Number of Accounts'n 51 - 65
       Filler $  66 - 370
       'Row Counter'n 371 - 378
;

output RP055; 
end;
        run;

                                              data RP060 ;
infile  &path2.;
&st.
if 'Record Type'n = "060" then do;
Input 'Average Utilization Rate'n 36 - 41
       Filler $  42 - 370
       'Row Counter'n 371 - 378
;

output RP060; 
end;
        run;

                                              data RP065 ;
infile  &path2.;
&st.
if 'Record Type'n = "065" then do;
Input 
      'Authorized'n 36 - 50
      'Outstanding'n 51 - 65    
       Filler $  66 - 370
       'Row Counter'n 371 - 378
;

output RP065; 
end;
        run;

                                                   data RP070 ;
infile  &path2.;
&st.
if 'Record Type'n = "070" then do;
Input 
      'Number of Loans'n   36  - 50
      'Estimated Num of Defaulted Loans'n 51 - 65    
      'Realized Num of Defaulted Loans'n 66 - 80
     
       Filler $  81 - 370
       'Row Counter'n 371 - 378
;

output RP070; 
end;
        run;

data RP080 ;
infile  &path2.;
&st.
if 'Record Type'n = "080" then do;
Input 

'Gross Impaired Loans and Accepta'n 36 - 50
'Additions to Gross Impaired'n 51 - 65
'Gross Impaired Loans and AcceptR'n 66 - 80
'Total Allowances for Credit Loss'n 81 - 95
'Total Provisions for Credit Loss'n $ 96 - 110
'General Provisions for Credit Lo'n $ 111 - 125
'Specific Provisions for Credit L'n  $ 126 - 140

     
       Filler $  141 - 370
       'Row Counter'n 371 - 378
;

output RP080; 
end;
        run;

                                                             data RP090 ;
infile  &path2.;
&st.
if 'Record Type'n = "090" then do;
Input 

'Other Changes in Gross Impaired'n 36 - 50
'Other Changes in Allowances for'n 51 - 65
     
       Filler $  66 - 370
       'Row Counter'n 371 - 378
;

output RP090; 
end;
        run;

                data RP999 ;
infile  &path2.;
&st.;
if 'Record Type'n = '999' then do; 
Input  'Number of Body Records'n 36 - 44   
       'File Size in bytes'n    45 - 56
       'File Name'n  $  57 - 116
       'File Creation Date'n 117 - 124

        Filler $ 125 - 370 
       'Row Counter'n 371 - 378;
       /*'CR LF'n $ 379 - 380; */ 
output RP999; 
end;
        run;


proc contents data=RP000 varnum noprint; run;

data check000; set RP000;
if 
'PD Band Type'n ne 1499 or
'Exposure Size Type'n ne 1599 or
'Geography Type'n ne 399 or
'LTV Type'n ne 499 or
'Exposure Class Type'n ne 599 or
'Securitization Type'n ne 699 or
'Rates Type'n ne 799 or
'Delinquency Bucket Type'n ne 899 then Flag000_All_Fields = 'Y'; run;

proc sort data=check000 (keep=Flag000_All_Fields) nodupkey; by Flag000_All_Fields; run;

/****************************/
* Record Type Declaration 005;
/***************************/ 

* (A) Record Type 005, value check;

proc contents data=RP005 varnum noprint; run;

data check005; set RP005;

if 'PD Band Type'n  < 1401 or 'PD Band Type'n > 1425 then Flag005_PD_Band_Type = 'Y';
if 'Exposure Class Type'n not in ( 502, 503, 505, 506, 507, 510, 511, 512 ) then Flag005_Exposure_Class_Type = 'Y';
if 'Exposure Size Type'n ne 1599 or
'Geography Type'n ne 399 or     
'LTV Type'n  ne 499 or
'Securitization Type'n ne 699 or
'Rates Type'n   ne 799 or
'Delinquency Bucket Type'n  ne 899 then Flag005_Other_Fields = 'Y';
run;

data check005; set check005; keep Flag005_PD_Band_Type Flag005_Exposure_Class_Type Flag005_Other_Fields;
proc sort data=check005 nodupkey; by Flag005_PD_Band_Type Flag005_Exposure_Class_Type Flag005_Other_Fields; run;

* (B) Row count check;

%Macro rowct ( RP = , Dimension1 = , Dimension2 = , rt =  ); 
proc sql noprint;
     select count(*) into : totalrow
     from &RP.;
     select count(distinct &Dimension1. ) into : Dim1Row
     from &RP;
    select count(distinct &Dimension2. ) into : Dim2Row
     from &RP;
quit;
%put &totalrow &Dim1Row &Dim2Row;
data Check&rt.; if &totalrow ne &Dim1Row * &Dim2Row  then Flag&rt._row_ct = 'Y' ;  run;
%mend;
%rowct( RP = RP005, Dimension1 = %Str('PD Band Type'n), Dimension2 = %Str('Exposure Class Type'n), rt = 005b);

/*********************************/
* Record Type Securitization 010 
/********************************/

* 010;
* (A) Record Type 010 distinct value check;
data check010; set RP010;
if 'Securitization Type'n not in ( 600, 601 ) then Flag010_Securitization_Type = 'Y';
if 'Exposure Class Type'n not in ( 500, 501, 502, 503, 504, 505, 506, 507, 510, 511, 512 ) then Flag010_Exposure_Class_Type = 'Y';

if 
'PD Band Type'n ne 1499 or
'Exposure Size Type'n ne 1599 or
'Geography Type'n ne 399 or     
'LTV Type'n  ne 499 or
'Rates Type'n   ne 799 or
'Delinquency Bucket Type'n  ne 899 then Flag010_Other_Fields = 'Y';
run;

data check010; set check010; keep Flag010_Exposure_Class_Type Flag010_Securitization_Type Flag010_Other_Fields;
proc sort data=check010 nodupkey; by Flag010_Exposure_Class_Type Flag010_Securitization_Type Flag010_Other_Fields; run;

* (B) Row count check;
%rowct( RP = RP010, Dimension1 = %Str('Securitization Type'n), Dimension2 = %Str('Exposure Class Type'n), rt = 010b);

* (C) Aggregation Test;

* (i) Before & After Securitization, Overall;

proc sql noprint; create table Check010_Agg_1 as select sum(Outstanding) as Outstanding1 from RP010 where 
'Securitization Type'n = 600 and 'Exposure Class Type'n = 500;
          create table Check010_Agg_2 as select sum(Outstanding) as Outstanding2 from RP010 where 
'Securitization Type'n = 601 and 'Exposure Class Type'n = 500;
quit;
/***************************************/
/* 5.4.1 Aggregation by Summation */
/* 5% or %20000 tolerance level   */
/***************************************/

data Check010c; merge Check010_Agg_1 Check010_Agg_2;
if (abs(Outstanding1 / Outstanding2) > 1.05 or abs(Outstanding1 / Outstanding2) < 0.95) and 
abs(Outstanding1 - Outstanding2) > 20 then flag010c = 'Y'; 
run;
/*
proc means data=RP010 nway; var Outstanding; Classes Securitization_Type;
output out=out1 sum(Outstanding) = Outstanding; run; */

* (ii) within Sub-group Exposure_Class_Type;

proc sql noprint; create table temp as select sum(Outstanding) as Outstanding1,  'Exposure Class Type'n  from RP010
where 'Securitization Type'n = 600 group by 'Exposure Class Type'n  ;
          create table temp2 as select sum(Outstanding) as Outstanding2,  'Exposure Class Type'n from RP010
where 'Securitization Type'n = 601 group by 'Exposure Class Type'n  ;
quit;

proc sort data = temp; by 'Exposure Class Type'n ; run;
proc sort data = temp2; by 'Exposure Class Type'n ; run;

proc transpose data = temp out = temp (drop = _name_ ) prefix= _010_ ;
id 'Exposure Class Type'n;
run;

proc transpose data = temp2 out = temp2 (drop = _name_ ) prefix= _010b_;
id 'Exposure Class Type'n;
run;
data check010d; set temp;
_010_500_New = sum (_010_501 , _010_504 , _010_507);
_010_501_New = sum (_010_502 , _010_503 );
_010_504_New = sum (_010_505 , _010_506 );
_010_502_New = sum (_010_510 , _010_511 , _010_512);

if _010_500_New ne 0 or _010_500 ne 0 then do;
if (abs(_010_500_New / _010_500 ) > 1.05 or abs(_010_500_New / _010_500 ) < 0.95) and
(abs(_010_500_New - _010_500) > 20 or abs(_010_500 - _010_500_New ) > 20) then flag010d = 'Y'; 
end;

if _010_501_New ne 0 or _010_501 ne 0 then do;
if (abs(_010_501_New / _010_501 ) > 1.05 or abs(_010_501_New / _010_501 ) < 0.95) and 
(abs(_010_501_New - _010_501) > 20 or abs(_010_501 - _010_501_New ) > 20) then flag010d = 'Y';
end;

if _010_504_New ne 0 or _010_504 ne 0 then do;
if (abs(_010_504_New / _010_504 ) > 1.05 or abs(_010_504_New / _010_504 ) < 0.95) and 
(abs(_010_504_New - _010_504) > 20 or abs(_010_504 - _010_504_New ) > 20) then flag010d = 'Y';
end;

if _010_502_New ne 0 or _010_502 ne 0 then do;
if (abs(_010_502_New / _010_502 ) > 1.05 or abs(_010_502_New / _010_502 ) < 0.95) and 
(abs(_010_502_New - _010_502) > 20 or abs(_010_502 - _010_502_New ) > 20) then flag010d = 'Y';
end;

run;


data Check015; set RP015;               
if 'Geography Type'n < 300 or 'Geography Type'n > 319 then Flag015_Geography_Type = 'Y';
if 'Exposure Class Type'n not in ( 500, 501, 502, 503, 504, 505, 506, 507, 510, 511, 512 ) then Flag015_Exposure_Class_Type = 'Y'; 
if
'PD Band Type'n ne 1499 or
'Exposure Size Type'n ne 1599 or
'LTV Type'n     ne 499 or
'Securitization Type'n     ne 699 or
'Rates Type'n   ne 799 or
'Delinquency Bucket Type'n ne 899 then Flag015_Other_Fields = 'Y';
run;

data check015; set check015; keep Flag015_Exposure_Class_Type Flag015_Geography_Type Flag015_Other_Fields;
proc sort data=check015 nodupkey; by Flag015_Exposure_Class_Type Flag015_Geography_Type Flag015_Other_Fields; run;

* (B) Row count check;
%rowct( RP = RP015, Dimension1 = %Str('Geography Type'n), Dimension2 = %Str('Exposure Class Type'n), rt = 015b);

* (C) Aggregation Test 

* (i) Aggregate Test Overall;

proc sql noprint; create table Check015_Agg_1 as select sum(Outstanding) as Outstanding1 from RP015
where 'Exposure Class Type'n = 500 and 'Geography Type'n = 300 ;
          create table Check015_Agg_2 as select sum(Outstanding) as Outstanding2 from RP015
where 'Exposure Class Type'n in (501,504,507) and 'Geography Type'n in ( 301, 0315, 316, 317, 318, 319) ;
quit;

data Check015c; merge Check015_Agg_1 Check015_Agg_2;
if (abs(Outstanding1 / Outstanding2) > 1.05 or abs(Outstanding1 / Outstanding2) < 0.95) and 
abs(Outstanding1 - Outstanding2) > 20 then flag015c = 'Y'; 
run;
* (ii) Aggregation Test within SubGroup;

proc sql noprint; create table temp as select sum(Outstanding) as Outstanding,  'Geography Type'n  from RP015
where 'Exposure Class Type'n = 500 group by 'Geography Type'n ;
          create table temp2 as select sum(Outstanding) as Outstanding,  'Geography Type'n from RP015
where 'Exposure Class Type'n in ( 501, 504, 507) group by 'Geography Type'n ;
quit;

proc sort data = temp; by 'Geography Type'n ; run;
proc transpose data = temp out = temp (drop = _name_ ) prefix= _015_ ;
id 'Geography Type'n;
run;


data check015d_geography; set temp; 
_015_300_New = sum (_015_301, _015_315, _015_316 ,_015_317 ,_015_318, _015_319);
_015_301_New = sum (_015_302, _015_303, _015_304 ,_015_305 ,_015_306, _015_307,
_015_308, _015_309, _015_310 ,_015_311 ,_015_312, _015_313, _015_314); 

if (abs(_015_300_New / _015_300 ) > 1.05 or abs(_015_300_New / _015_300 ) < 0.95) and
(abs(_015_300_New - _015_300) > 20 or abs(_015_300 - _015_300_New ) > 20) then flag015d_geography = 'Y'; 

if (abs(_015_301_New / _015_301 ) > 1.05 or abs(_015_301_New / _015_301 ) < 0.95) and
(abs(_015_301_New - _015_301) > 20 or abs(_015_301 - _015_301_New ) > 20) then flag015d_geography = 'Y'; 
run;
proc sort data= check015d_geography (keep= flag015d_geography ) nodupkey ; by flag015d_geography; run;

proc sql noprint; create table temp2 as select sum(Outstanding) as Outstanding,  'Exposure Class Type'n   from RP015
where 'Geography Type'n = 300 group by 'Exposure Class Type'n  ;

proc transpose data = temp2 out = temp2 (drop = _name_ ) prefix= _015_ ;
id 'Exposure Class Type'n;
run;

data check015d; set temp2;
_015_500_New = sum (_015_501 , _015_504 , _015_507);
_015_501_New = sum (_015_502 , _015_503 );
_015_504_New = sum (_015_505 , _015_506 );
_015_502_New = sum (_015_510 , _015_511 , _015_512);

if _015_500_New ne 0 or _015_500 ne 0 then do;
if (abs(_015_500_New / _015_500 ) > 1.05 or abs(_015_500_New / _015_500 ) < 0.95) and 
(abs(_015_500_New - _015_500) > 20 or abs(_015_500 - _015_500_New ) > 20) then flag015d = 'Y'; 
end;

if _015_501_New ne 0 or _015_501 ne 0 then do;
if (abs(_015_501_New / _015_501 ) > 1.05 or abs(_015_501_New / _015_501 ) < 0.95) and
(abs(_015_501_New - _015_501) > 20 or abs(_015_501 - _015_501_New ) > 20) then flag015d = 'Y';
end;

if _015_504_New ne 0 or _015_504 ne 0 then do;
if (abs(_015_504_New / _015_504 ) > 1.05 or abs(_015_504_New / _015_504 ) < 0.95) and 
(abs(_015_504_New - _015_504) > 20 or abs(_015_504 - _015_504_New ) > 20) then flag015d = 'Y';
end;

if _015_502_New ne 0 or _015_502 ne 0 then do;
if (abs(_015_502_New / _015_502 ) > 1.05 or abs(_015_502_New / _015_502 ) < 0.95) and
(abs(_015_502_New - _015_502) > 20 or abs(_015_502 - _015_502_New ) > 20) then flag015d = 'Y';
end;
run;


/**********************************************************************************************/
/* IMPORTANT CHECK Outstanding balance between TABS 010 to 035, 55, 65 REPORT TYPE            */
/**********************************************************************************************/

/**************************************************************************************/
*Outstanding LTV 020;
/**************************************************************************************/

* (A) data vales check;

data Check020; set RP020; 
if 'LTV Type'n not in ( 400, 401, 402, 403, 404, 405 ) then Flag020_LTV_Type = 'Y';
if 'Exposure Class Type'n not in ( 502, 503, 510, 511, 512 ) then Flag020_Exposure_Class_Type = 'Y';
if
'PD Band Type'n ne 1499 or
'Exposure Size Type'n ne 1599 or
'Geography Type'n ne 399 or     
'Securitization Type'n ne 699 or
'Rates Type'n ne 799 or
'Delinquency Bucket Type'n ne 899 then  Flag020_Other_Fields = 'Y';
run;

data check020; set check020; keep Flag020_Exposure_Class_Type Flag020_LTV_Type Flag020_Other_Fields;
proc sort data=check020 nodupkey; by Flag020_Exposure_Class_Type Flag020_LTV_Type Flag020_Other_Fields; run;

* (B) Row count check;
%rowct( RP = RP020, Dimension1 = %Str('LTV Type'n), Dimension2 = %Str('Exposure Class Type'n), rt = 020b);

* (C) Data aggregates check;
* (i) Overall;

proc sql noprint; create table Check020_Agg_1 as select sum(Outstanding) as Outstanding1 from RP020
where 'LTV Type'n = 400 ;
          create table Check020_Agg_2 as select sum(Outstanding) as Outstanding2 from RP020
where 'LTV Type'n in ( 401, 402, 403, 404, 405) ;
quit;

data Check020c; merge Check020_Agg_1 Check020_Agg_2;
if abs(Outstanding1 / Outstanding2) > 1.05 then flag020c = 'Y';
if abs(Outstanding1 / Outstanding2) < 0.95 then flag020c = 'Y';
if abs(Outstanding1 - Outstanding2) > 20 then flag020c = 'Y'; 
run;

* (ii) within Sub-group Exposure_Class_Type to make sure it aligned with other Record Type;

proc sql noprint; create table temp2 as select sum(Outstanding) as Outstanding,  'Exposure Class Type'n   from RP020
where 'LTV Type'n = 400 group by 'Exposure Class Type'n  ; quit;

proc transpose data = temp2 out = temp2 (drop = _name_ ) prefix= _020_ ;
id 'Exposure Class Type'n;
run;

data check020d; set temp2;

_020_501_New = sum (_020_502 , _020_503 );
_020_502_New = sum (_020_510 , _020_511 , _020_512);

*if abs(_020_501_New / _020_501 ) > 1.05 or abs(_020_501_New / _020_501 ) < 0.95 then flag020d = 'Y';
*if abs(_020_501_New - _020_501) > 20 or abs(_020_501 - _020_501_New ) > 20 then flag020d = 'Y';
if _020_502_New ne 0 or _020_502 ne 0 then do;
if (abs(_020_502_New / _020_502 ) > 1.05 or abs(_020_502_New / _020_502 ) < 0.95) 
and (abs(_020_502_New - _020_502) > 20 or abs(_020_502 - _020_502_New ) > 20) then flag020d = 'Y';
end;

run;



/***************************************************/
*  Record Type Outstanding Rates (025) *;
/***************************************************/

* (A) data values check;

data Check025; set RP025;
if 'Rates Type'n not in ( 700, 701, 702 ) then Flag025_Rates_Type = 'Y';
if 'Exposure Class Type'n not in ( 502, 503, 507, 510, 511, 512  ) then Flag025_Exposure_Class_Type = 'Y';
if
'PD Band Type'n ne 1499    or
'Exposure Size Type'n ne 1599    or
'Geography Type'n    ne 399     or
'LTV Type'n ne 499       or
'Securitization Type'n     ne 699     or
'Delinquency Bucket Type'n ne 899 then Flag025_Other_Fields = 'Y'; run;

data check025; set check025; keep Flag025_Exposure_Class_Type Flag025_Rates_Type Flag025_Other_Fields;
proc sort data=check025 nodupkey; by Flag025_Exposure_Class_Type Flag025_Rates_Type Flag025_Other_Fields; run;

* (B) Row count check;
%rowct( RP = RP025, Dimension1 = %Str('Rates Type'n), Dimension2 = %Str('Exposure Class Type'n), rt = 025b);

* (C) Data aggregates check;

* (i) Overall;

proc sql noprint; create table Check025_Agg_1 as select sum(Outstanding) as Outstanding1 from RP025
where 'Rates Type'n = 700 ;
          create table Check025_Agg_2 as select sum(Outstanding) as Outstanding2 from RP025
where 'Rates Type'n in ( 701, 702) ;
quit;

data Check025c; merge Check025_Agg_1 Check025_Agg_2;
if (abs(Outstanding1 / Outstanding2) > 1.05 or abs(Outstanding1 / Outstanding2) < 0.95) and 
abs(Outstanding1 - Outstanding2) > 20 then flag025c = 'Y'; 
run;

* * (ii) within Sub-group;

proc sql noprint; create table temp2 as select sum(Outstanding) as Outstanding,  'Exposure Class Type'n   from RP025
where 'Rates Type'n = 700 group by 'Exposure Class Type'n  ; quit;

proc transpose data = temp2 out = temp2 (drop = _name_ ) prefix= _025_ ;
id 'Exposure Class Type'n;
run;
* ( 502, 503, 507, 510, 511, 512  );
data check025d; set temp2;

_025_501_New = sum (_025_502 , _025_503 );
_025_502_New = sum (_025_510 , _025_511 , _025_512);

*if abs(_025_501_New / _025_501 ) > 1.05 or abs(_025_501_New / _025_501 ) < 0.95 then flag025d = 'Y';
*if abs(_025_501_New - _025_501) > 20 or abs(_025_501 - _025_501_New ) > 20 then flag025d = 'Y';
if _025_502_New ne 0 or _025_502 ne 0 then do;
if (abs(_025_502_New / _025_502 ) > 1.05 or abs(_025_502_New / _025_502 ) < 0.95) and 
(abs(_025_502_New - _025_502) > 20 or abs(_025_502 - _025_502_New ) > 20) then flag025d = 'Y';
end;
run;

* Cross reference against Record Type 010, 020, 025

* 502 = 510 + 511 + 512
* 501 = 502 + 503
* 507;

/*************************************************************/
/* Outstanding; Exposure Class Size; Mortgage and QRRE (030) */
/*************************************************************/
* (A) data vales check;

data check030; set RP030; 

if 'Exposure Size Type'n not in ( 1500, 1501, 1502, 1503, 1504, 1505) then Flag030_Exposure_Size_Type = 'Y';
if 'Exposure Class Type'n not in ( 502, 505, 506, 507, 510, 511, 512 ) then Flag030_Exposure_Class_Type = 'Y';
if
'PD Band Type'n ne 1499    or
'Geography Type'n    ne 399 or
'LTV Type'n ne 499   or
'Securitization Type'n     ne 699 or
'Rates Type'n   ne 799 or
'Delinquency Bucket Type'n ne 899 then Flag030_Other_Fields = 'Y'; run;

data check030; set check030; keep Flag030_Exposure_Size_Type Flag030_Exposure_Class_Type Flag030_Other_Fields;
proc sort data=check030 nodupkey; by Flag030_Exposure_Size_Type Flag030_Exposure_Class_Type Flag030_Other_Fields; run;

* (B) Row count check;
%rowct( RP = RP030, Dimension1 = %Str('Exposure Size Type'n), Dimension2 = %Str('Exposure Class Type'n), rt = 030b);

* (C) Data aggregates check;

* (i) Overall no need since it $ bucket;

* * (ii) within Sub-group;

proc sql noprint; create table temp2 as select sum(Outstanding) as Outstanding,  'Exposure Class Type'n   from RP030
where 'Exposure Size Type'n in ( 1500, 1501, 1502, 1503, 1504, 1505) group by 'Exposure Class Type'n  ; quit;

proc transpose data = temp2 out = temp2 (drop = _name_ ) prefix= _030_ ;
id 'Exposure Class Type'n;
run;

* ( 502, 505, 506, 507, 510, 511, 512 );

data check030d; set temp2;
_030_504_New = sum (_030_505 , _030_506 );
_030_502_New = sum (_030_510 , _030_511 , _030_512);

if _030_502_New ne 0 or _030_502 ne 0 then do;
if (abs(_030_502_New / _030_502 ) > 1.05 or abs(_030_502_New / _030_502 ) < 0.95) and 
(abs(_030_502_New - _030_502) > 20 or abs(_030_502 - _030_502_New ) > 20) then flag030d = 'Y'; 
end; run;



/*************************************************************/
* Outstanding Exposure Class Size Mortgage and HELOC (035) *;
/*************************************************************/

* (A) data vales check;

data check035; set RP035; 

if 'Exposure Size Type'n not in ( 1506, 1507, 1508, 1509, 1510) then Flag035_Exposure_Size_Type = 'Y';
if 'Exposure Class Type'n not in ( 502, 503, 510, 511, 512 ) then Flag035_Exposure_Class_Type = 'Y';
if
'PD Band Type'n ne 1499    or
'Geography Type'n    ne 399 or
'LTV Type'n ne 499   or
'Securitization Type'n     ne 699 or
'Rates Type'n   ne 799 or
'Delinquency Bucket Type'n ne 899 then Flag035_Other_Fields = 'Y'; run;

data check035; set check035; keep Flag035_Exposure_Size_Type Flag035_Exposure_Class_Type Flag035_Other_Fields;
proc sort data=check035 nodupkey; by Flag035_Exposure_Size_Type Flag035_Exposure_Class_Type Flag035_Other_Fields; run;

* (B) Row count check;
%rowct( RP = RP035, Dimension1 = %Str('Exposure Size Type'n), Dimension2 = %Str('Exposure Class Type'n), rt = 035b);

* (C) Data aggregates check;

* (i) Overall no need since it $ bucket;; 

* * (ii) within Sub-group;

proc sql noprint; create table temp2 as select sum(Outstanding) as Outstanding,  'Exposure Class Type'n   from RP035
where 'Exposure Size Type'n in ( 1506, 1507, 1508, 1509, 1510) group by 'Exposure Class Type'n  ; quit;

proc transpose data = temp2 out = temp2 (drop = _name_ ) prefix= _035_ ;
id 'Exposure Class Type'n;
run;

data check035d; set temp2;

_035_501_New = sum (_035_502 , _035_503 );
_035_502_New = sum (_035_510 , _035_511 , _035_512);

*if abs(_035_501_New / _035_501 ) > 1.05 or abs(_035_501_New / _035_501 ) < 0.95 then flag035d = 'Y';
*if abs(_035_501_New - _035_501) > 20 or abs(_035_501 - _035_501_New ) > 20 then flag035d = 'Y';
if _035_502_New ne 0 or _035_502 ne 0 then do;
if (abs(_035_502_New / _035_502 ) > 1.05 or abs(_035_502_New / _035_502 ) < 0.95 ) and 
(abs(_035_502_New - _035_502) > 20 or abs(_035_502 - _035_502_New ) > 20) then flag035d = 'Y';
end;
run;



/***************************************************************/
/* Record type 040 */
/**************************************************************/


/* 5.6.2. Number of Accounts/Loans
Number of Accounts Written Off (ID #15) must not exceed 1000 times the Total Write Offs value (ID #16). 
(In other words, on average there should be at least $1.00 write offs for every accounts written off.) Otherwise, the data will be rejected.

Accounts Over Limit (ID #8) must not exceed 100%. Otherwise, the data will be rejected.

Estimated Number of Defaulted Loans (ID #4) must not exceed Number of Accounts (Loans) (ID #3). Otherwise, the data will be rejected.
Realized Number of Defaulted Loans (ID #5) must not exceed Number of Accounts (Loans) (ID #3). Otherwise, the data will be rejected.*/

* (A) data vales check;

data check040; set RP040; 

if 'Exposure Class Type'n not in ( 503, 505, 506, 507  ) then Flag040_Exposure_Class_Type = 'Y';

if
'PD Band Type'n ne 1499    or
'Exposure Size Type'n ne 1599 or
'Geography Type'n    ne 399 or
'LTV Type'n ne 499   or
'Securitization Type'n     ne 699 or
'Rates Type'n   ne 799 or
'Delinquency Bucket Type'n ne 899 then Flag040_Other_Fields = 'Y'; 
*if Accounts_Over_Limit > 100 then Flag040_Accounts_Over_Limit = 'Y';
run;

data check040; set check040; keep Flag040_Exposure_Class_Type Flag040_Other_Fields;
proc sort data=check040 nodupkey; by Flag040_Exposure_Class_Type Flag040_Other_Fields; run;

* (B) Row count check;
%Macro rowct1 ( RP = , Dimension1 = , rt =  ); 
proc sql noprint;
     select count(*) into : totalrow
     from &RP.;
     select count(distinct &Dimension1. ) into : Dim1Row
     from &RP;
quit;
%put &totalrow &Dim1Row;
data Check&rt.; if &totalrow ne &Dim1Row then Flag&rt._row_ct = 'Y' ;  run;
%mend;
%rowct1( RP = RP040, Dimension1 = %Str('Exposure Class Type'n), rt = 040b);

/************************************************************/
/* Fraud Losses and Number of Accounts Written Off (045)   
/************************************************************/

* (A) data vales check;

data check045; set RP045; 

if 'Exposure Class Type'n not in ( 502, 503, 505, 506, 507, 510, 511, 512 ) 
then Flag045_Exposure_Class_Type = 'Y';

if
'PD Band Type'n ne 1499    or
'Exposure Size Type'n ne 1599 or
'Geography Type'n    ne 399 or
'LTV Type'n ne 499   or
'Securitization Type'n     ne 699 or
'Rates Type'n   ne 799 or
'Delinquency Bucket Type'n ne 899 then Flag045_Other_Fields = 'Y'; 

/* ? Estimated IRB Exposure at Default (ID #13) must not exceed the Authorized value (ID #1) by more than 1%. 
Otherwise, the data will be rejected. */
*if (Estimated IRB EAD - Authorized) / Authorized > 0.01 then Flag045_Estimated_IRB_EAD = 'Y';
run;

data check045; set check045; keep Flag045_Exposure_Class_Type  Flag045_Other_Fields;
proc sort data=check045 nodupkey; by Flag045_Exposure_Class_Type  Flag045_Other_Fields; run;

* (B) Row count check;
%rowct1( RP = RP045, Dimension1 = %Str('Exposure Class Type'n), rt = 045b);

proc contents data=RP045 varnum noprint; run;
* (C) (D) overall and exposure class type together ;

* Authorized
Fraud Losses
Estimated IRB EAD
Number of Accounts Written Off;

proc sql noprint; create table temp2 as select sum(Authorized) as Authorized, sum('Fraud Losses'n) as 'Fraud Losses'n, 
sum('Estimated IRB EAD'n) as 'Estimated IRB EAD'n, sum('Number of Accounts Written Off'n) as 'Number of Accounts Written Off'n,
'Exposure Class Type'n   from RP045 group by 'Exposure Class Type'n  ; quit;

proc transpose data = temp2 out = temp2 (drop = _name_ ) prefix= _045_ ;
id 'Exposure Class Type'n;
run;

* ( 502, 503, 505, 506, 507, 510, 511, 512 ) ;
* ( 502, 503, 505, 506, 507, 510, 511, 512 ) ;

data check045d; set temp2;

_045_501_New = sum (_045_502 , _045_503 );
_045_504_New = sum (_045_505 , _045_506 );
_045_500_New = sum (_045_501_New , _045_504_New , _045_507);
_045_502_New = sum (_045_510 , _045_511 , _045_512);


if _045_502_New ne 0 or _045_502 ne 0 then do;
if (abs(_045_502_New / _045_502 ) > 1.05 or abs(_045_502_New / _045_502 ) < 0.95) and 
(abs(_045_502_New - _045_502) > 20 or abs(_045_502 - _045_502_New ) > 20) then flag045d = 'Y';
end;
run;

/************************************************************/ 
/* Write Offs and Expected Losses (047)
/************************************************************/ 

* (A) data vales check;

data check047; set RP047; 

if 'Exposure Class Type'n not in ( 500, 501, 502, 503, 505, 506, 507, 510, 511, 512 ) 
then Flag047_Exposure_Class_Type = 'Y';
if
'PD Band Type'n ne 1499    and
'Exposure Size Type'n ne 1599 and
'Geography Type'n    ne 399 and
'LTV Type'n ne 499   and
'Securitization Type'n     ne 699 and
'Rates Type'n   ne 799 and
'Delinquency Bucket Type'n ne 899 then Flag047_Other_Fields = 'Y'; run;

data check047; set check047; keep Flag047_Exposure_Class_Type Flag047_Other_Fields;
proc sort data=check047 nodupkey; by Flag047_Exposure_Class_Type Flag047_Other_Fields; run;

* (B) Row count check;

%rowct1( RP = RP047, Dimension1 = %Str('Exposure Class Type'n), rt = 047b);

* (C) overall & exposure class type;

proc sql noprint; create table temp2 as select sum('Total Write Offs'n) as 'Total Write Offs'n,
sum('IRB Expected Losses'n) as 'IRB Expected Losses'n, 'Exposure Class Type'n
from RP047 group by 'Exposure Class Type'n  ; quit;

proc transpose data = temp2 out = temp2 (drop = _name_ ) prefix= _047_ ;
id 'Exposure Class Type'n;
run;

* ( 500, 501, 502, 503, 505, 506, 507, 510, 511, 512 )  ;

data check047d; set temp2;

_047_501_New = sum (_047_502 , _047_503 );
_047_504_New = sum (_047_505 , _047_506 );
_047_500_New = sum (_047_501 , _047_504_New , _047_507);
_047_502_New = sum (_047_510 , _047_511 , _047_512);

*if abs(_047_500_New / _047_500 ) > 1.05 or abs(_047_500_New / _047_500 ) < 0.95 then flag047d = 'Y';
*if abs(_047_500_New - _047_500) > 20 or abs(_047_500 - _047_500_New ) > 20 then flag047d = 'Y'; 

if _047_501_New ne 0 or _047_501 ne 0 then do;
if (abs(_047_501_New / _047_501 ) > 1.05 or abs(_047_501_New / _047_501 ) < 0.95) and  
(abs(_047_501_New - _047_501) > 20 or abs(_047_501 - _047_501_New ) > 20) then flag047d = 'Y';
end;

*if _047_504_New ne 0 or _047_504 ne 0 then do;
*if abs(_047_504_New / _047_504 ) > 1.05 or abs(_047_504_New / _047_504 ) < 0.95 then flag047d = 'Y';
*if abs(_047_504_New - _047_504) > 20 or abs(_047_504 - _047_504_New ) > 20 then flag047d = 'Y';
*end;

if _047_502_New ne 0 or _047_502 ne 0 then do;
if (abs(_047_502_New / _047_502 ) > 1.05 or abs(_047_502_New / _047_502 ) < 0.95) and 
(abs(_047_502_New - _047_502) > 20 or abs(_047_502 - _047_502_New ) > 20) then flag047d = 'Y';
end;
run;

/************************************************************/ 
/* Recoveries, Economic Losses and IRB Capital (050) */
/************************************************************/

*%dload( x = %str('050'), f = RP050 );

* (A) data vales check;

data check050; set RP050; 

if 'Exposure Class Type'n not in ( 500, 501, 502, 503, 504, 505, 506, 507, 510, 511, 512 ) 
then Flag050_Exposure_Class_Type = 'Y';
if
'PD Band Type'n ne 1499    and
'Exposure Size Type'n ne 1599 and
'Geography Type'n    ne 399 and
'LTV Type'n ne 499   and
'Securitization Type'n     ne 699 and
'Rates Type'n   ne 799 and
'Delinquency Bucket Type'n ne 899 then Flag050_Other_Fields = 'Y'; run;

data check050; set check050; keep Flag050_Exposure_Class_Type Flag050_Other_Fields;
proc sort data=check050 nodupkey; by Flag050_Exposure_Class_Type Flag050_Other_Fields; run;

* (B) Row count check;
%rowct1( RP = RP050, Dimension1 = %Str('Exposure Class Type'n), rt = 050b);

proc sql noprint; create table temp2 as select sum('Recoveries'n) as 'Recoveries'n, sum('Economic Losses'n) as 'Economic Losses'n,
sum('IRB Capital'n) as 'IRB Capital'n,
'Exposure Class Type'n
from RP050 group by 'Exposure Class Type'n  ; quit;

proc transpose data = temp2 out = temp2 (drop = _name_ ) prefix= _050_ ;
id 'Exposure Class Type'n;
run;

* ( 500, 501, 502, 503, 504, 505, 506, 507, 510, 511, 512 );

data check050d; set temp2;

_050_501_New = sum (_050_502 , _050_503 );
_050_504_New = sum (_050_505 , _050_506 );
_050_500_New = sum (_050_501 , _050_504 , _050_507);
_050_502_New = sum (_050_510 , _050_511 , _050_512);

if _050_500_New ne 0 or _050_500 ne 0 then do;
if (abs(_050_500_New / _050_500 ) > 1.05 or abs(_050_500_New / _050_500 ) < 0.95) and 
(abs(_050_500_New - _050_500) > 20 or abs(_050_500 - _050_500_New ) > 20) then flag050d = 'Y'; 
end;

if _050_501_New ne 0 or _050_501 ne 0 then do;
if (abs(_050_501_New / _050_501 ) > 1.05 or abs(_050_501_New / _050_501 ) < 0.95) and 
(abs(_050_501_New - _050_501) > 20 or abs(_050_501 - _050_501_New ) > 20) then flag050d = 'Y';
end;

if _050_504_New ne 0 or _050_504 ne 0 then do;
if (abs(_050_504_New / _050_504 ) > 1.05 or abs(_050_504_New / _050_504 ) < 0.95) and 
(abs(_050_504_New - _050_504) > 20 or abs(_050_504 - _050_504_New ) > 20) then flag050d = 'Y';
end;

if _050_502_New ne 0 or _050_502 ne 0 then do;
if (abs(_050_502_New / _050_502 ) > 1.05 or abs(_050_502_New / _050_502 ) < 0.95) and  
(abs(_050_502_New - _050_502) > 20 or abs(_050_502 - _050_502_New ) > 20) then flag050d = 'Y';
end;
run;

/***************************************************************************/
/* DELQ BUCKET 055 */
/***************************************************************************/

* (A) data vales check;

data check055; set RP055; 

if 'Exposure Class Type'n not in ( 500, 501, 502, 503, 504, 505, 506, 507, 510, 511, 512 ) 
then Flag055_Exposure_Class_Type = 'Y';
if 'Geography Type'n not in ( 300, 301, 315, 316, 317, 318, 319) then Flag055_Geography_Type = 'Y';
if 'Delinquency Bucket Type'n not in ( 800, 801, 802, 803, 804, 805 )then Flag055_Delq_bucket_Type = 'Y';
if
'PD Band Type'n ne 1499    and
'Exposure Size Type'n ne 1599 and
'LTV Type'n ne 499   and
'Securitization Type'n     ne 699 and
'Rates Type'n   ne 799  then Flag055_Other_Fields = 'Y'; run;

data check055; set check055; keep Flag055_Exposure_Class_Type Flag055_Geography_Type Flag055_Delq_bucket_Type Flag055_Other_Fields;
proc sort data=check055 nodupkey; by Flag055_Exposure_Class_Type Flag055_Geography_Type Flag055_Delq_bucket_Type Flag055_Other_Fields; run;

* (B) row count check, three dimensions;
%Macro rowct ( RP = , Dimension1 = , Dimension2 = , Dimension3 = , rt =  ); 
proc sql noprint;
     select count(*) into : totalrow
     from &RP.;
     select count(distinct &Dimension1. ) into : Dim1Row
     from &RP;
    select count(distinct &Dimension2. ) into : Dim2Row
     from &RP;
     select count(distinct &Dimension3. ) into : Dim3Row
     from &RP;
quit;
%put &totalrow &Dim1Row &Dim2Row &Dim3Row;
data Check&rt.; if &totalrow ne &Dim1Row * &Dim2Row * &Dim3Row then Flag&rt._row_ct = 'Y' ;  run;
%mend;
%rowct( RP = RP055, Dimension1 = %Str('Geography Type'n), Dimension2 = %Str('Delinquency Bucket Type'n), 
Dimension3 = %str('Exposure Class Type'n),
rt = 055b);

* (C) Data aggregates check;
* (i) Overall;

proc sql noprint; create table Check055_Agg_1 as select sum(Outstanding) as Outstanding1 from RP055
where 'Delinquency Bucket Type'n = 800 and 'Geography Type'n = 300 and 'Exposure Class Type'n  = 500  ;
          create table Check055_Agg_2 as select sum(Outstanding) as Outstanding2 from RP055
where 'Delinquency Bucket Type'n in ( 801, 802, 803, 804, 805) and 'Geography Type'n = 300 and 'Exposure Class Type'n  = 500 ;
quit;

data Check055c; merge Check055_Agg_1 Check055_Agg_2;
if (abs(Outstanding1 / Outstanding2) > 1.05 or abs(Outstanding1 / Outstanding2) < 0.95 )
and abs(Outstanding1 - Outstanding2) > 20 then flag055c = 'Y'; 
run;

* (ii) within Sub-group;

* ( 500, 501, 502, 503, 504, 505, 506, 507, 510, 511, 512 ) ;

proc sql noprint; create table temp2 as select sum(Outstanding) as Outstanding,  'Exposure Class Type'n   from RP055
where 'Geography Type'n  =300 and 'Delinquency Bucket Type'n = 800 group by 'Exposure Class Type'n  ; quit;

proc transpose data = temp2 out = temp2 (drop = _name_ ) prefix= _055_ ;
id 'Exposure Class Type'n;
run;

* ( 500, 501, 502, 503, 504, 505, 506, 507, 510, 511, 512 ) ;

data check055d; set temp2;

_055_501_New = sum (_055_502 , _055_503 );
_055_504_New = sum (_055_505 , _055_506 );
_055_500_New = sum (_055_501 , _055_504 , _055_507);
_055_502_New = sum (_055_510 , _055_511 , _055_512);

if _055_500_New ne 0 or _055_500 ne 0 then do;
if (abs(_055_500_New / _055_500 ) > 1.05 or abs(_055_500_New / _055_500 ) < 0.95) and 
(abs(_055_500_New - _055_500) > 20 or abs(_055_500 - _055_500_New ) > 20) then flag055d = 'Y'; 
end;

if _055_501_New ne 0 or _055_501 ne 0 then do;
if (abs(_055_501_New / _055_501 ) > 1.05 or abs(_055_501_New / _055_501 ) < 0.95) and  
( abs(_055_501_New - _055_501) > 20 or abs(_055_501 - _055_501_New ) > 20) then flag055d = 'Y';
end;

if _055_504_New ne 0 or _055_504 ne 0 then do;
if (abs(_055_504_New / _055_504 ) > 1.05 or abs(_055_504_New / _055_504 ) < 0.95) and 
(abs(_055_504_New - _055_504) > 20 or abs(_055_504 - _055_504_New ) > 20) then flag055d = 'Y';
end;

if _055_502_New ne 0 or _055_502 ne 0 then do;
if (abs(_055_502_New / _055_502 ) > 1.05 or abs(_055_502_New / _055_502 ) < 0.95) 
and ( abs(_055_502_New - _055_502) > 20 or abs(_055_502 - _055_502_New ) > 20) then flag055d = 'Y';
end;
run;

/***************************************************************************/
/* Authorized and Outstanding by PD Band                               065 */
/***************************************************************************/

* (A) data vales check;

data check065; set RP065; 

if 'Exposure Class Type'n not in ( 502, 503, 505, 506, 507, 510, 511, 512 ) 
then Flag065_Exposure_Class_Type = 'Y';
if 'Geography Type'n not in ( 300, 301, 315, 316, 317, 318, 319) then Flag065_Geography_Type = 'Y';
if 'PD Band Type'n < 1400 or 'PD Band Type'n > 1426 then Flag065_PD_Band_Type = 'Y';
if

'Exposure Size Type'n ne 1599 and
'LTV Type'n ne 499   and
'Securitization Type'n     ne 699 and
'Rates Type'n   ne 799 and
'Delinquency Bucket Type'n ne 899 then Flag065_Other_Fields = 'Y'; run;
data check065; set check065; keep Flag065_Exposure_Class_Type Flag065_Geography_Type Flag065_PD_Band_Type Flag065_Other_Fields;
proc sort data=check065 nodupkey; by Flag065_Exposure_Class_Type Flag065_Geography_Type 
Flag065_PD_Band_Type Flag065_Other_Fields; run;

* (B) row count check, three dimensions;
%Macro rowct ( RP = , Dimension1 = , Dimension2 = , Dimension3 = , rt =  ); 
proc sql noprint;
     select count(*) into : totalrow
     from &RP.;
     select count(distinct &Dimension1. ) into : Dim1Row
     from &RP;
    select count(distinct &Dimension2. ) into : Dim2Row
     from &RP;
     select count(distinct &Dimension3. ) into : Dim3Row
     from &RP;
quit;
%put &totalrow &Dim1Row &Dim2Row &Dim3Row;
data Check&rt.; if &totalrow ne &Dim1Row * &Dim2Row * &Dim3Row then Flag&rt._row_ct = 'Y' ;  run;
%mend;
%rowct( RP = RP065, Dimension1 = %Str('Geography Type'n), Dimension2 = %Str('PD Band Type'n), 
Dimension3 = %str('Exposure Class Type'n),
rt = 065b);

* (C) Data aggregates check;
* (i) Overall;

*%dload( x = %str('065'), f = RP065 );

proc sql noprint; create table Check065_Agg_1 as select sum(Outstanding) as Outstanding1 from RP065
where 'PD Band Type'n = 1400 and 'Geography Type'n  =300  and 'Exposure Class Type'n  in (502, 503, 505, 506, 507);
          create table Check065_Agg_2 as select sum(Outstanding) as Outstanding2 from RP065
where 'PD Band Type'n ne 1400 and 'Geography Type'n  =300 and 'Exposure Class Type'n  in (502, 503, 505, 506, 507);
quit;

data Check065c; merge Check065_Agg_1 Check065_Agg_2;
if (abs(Outstanding1 / Outstanding2) > 1.05 or abs(Outstanding1 / Outstanding2) < 0.95) and  
abs(Outstanding1 - Outstanding2) > 20 then flag065c = 'Y'; 
run;

* (ii) within Sub-group;

*  ( 502, 503, 505, 506, 507, 510, 511, 512 );

proc sql noprint; create table temp2 as select sum(Outstanding) as Outstanding,  'Exposure Class Type'n   from RP065
where 'Geography Type'n  =300 and 'PD Band Type'n = 1400 group by 'Exposure Class Type'n  ; quit;

proc transpose data = temp2 out = temp2 (drop = _name_ ) prefix= _065_ ;
id 'Exposure Class Type'n;
run;

* ( 502, 503, 505, 506, 507, 510, 511, 512 );

data check065d; set temp2;

_065_501_New = sum (_065_502 , _065_503 );
_065_504_New = sum (_065_505 , _065_506 );
_065_500_New = sum (_065_501_New , _065_504_New , _065_507);
_065_502_New = sum (_065_510 , _065_511 , _065_512);



if _065_502_New ne 0 or _065_502 ne 0 then do;
if abs(_065_502_New / _065_502 ) > 1.05 or abs(_065_502_New / _065_502 ) < 0.95 then flag065d = 'Y';
if abs(_065_502_New - _065_502) > 20 or abs(_065_502 - _065_502_New ) > 20 then flag065d = 'Y';
end;
run;

* Record type 010, 015, 025, 030, 035, 055 and 065.;

/***************************************************************************/
/* Average Utilization Rate by PD Band                               060 */
/***************************************************************************/

* (A) data vales check;

data check060; set RP060; 

if 'Exposure Class Type'n not in ( 503, 505, 506) 
then Flag060_Exposure_Class_Type = 'Y';
if 'Geography Type'n not in ( 300, 301, 315, 316, 317, 318, 319) then Flag060_Geography_Type = 'Y';
if 'PD Band Type'n < 1400 or 'PD Band Type'n > 1426 then Flag060_PD_Band_Type = 'Y';
if

'Exposure Size Type'n ne 1599 and
'LTV Type'n ne 499   and
'Securitization Type'n     ne 699 and
'Rates Type'n   ne 799 and
'Delinquency Bucket Type'n ne 899 then Flag060_Other_Fields = 'Y'; run;
data check060; set check060; keep Flag060_Exposure_Class_Type Flag060_Geography_Type Flag060_PD_Band_Type Flag060_Other_Fields;
proc sort data=check060 nodupkey; by Flag060_Exposure_Class_Type Flag060_Geography_Type 
Flag060_PD_Band_Type Flag060_Other_Fields; run;

* (B) row count check, three dimensions;
%Macro rowct ( RP = , Dimension1 = , Dimension2 = , Dimension3 = , rt =  ); 
proc sql noprint;
     select count(*) into : totalrow
     from &RP.;
     select count(distinct &Dimension1. ) into : Dim1Row
     from &RP;
    select count(distinct &Dimension2. ) into : Dim2Row
     from &RP;
     select count(distinct &Dimension3. ) into : Dim3Row
     from &RP;
quit;
%put &totalrow &Dim1Row &Dim2Row &Dim3Row;
data Check&rt.; if &totalrow ne &Dim1Row * &Dim2Row * &Dim3Row then Flag&rt._row_ct = 'Y' ;  run;
%mend;
%rowct( RP = RP060, Dimension1 = %Str('Geography Type'n), Dimension2 = %Str('PD Band Type'n), 
Dimension3 = %str('Exposure Class Type'n),
rt = 060b);
/***************************************************************************/
/* Loan Count by PD Band                                               070 */
/***************************************************************************/

* (A) data vales check;

data check070; set RP070; 

if 'Exposure Class Type'n not in ( 502, 503, 505, 506, 507, 510, 511, 512) 
then Flag070_Exposure_Class_Type = 'Y';
if 'Geography Type'n not in ( 300, 301, 315, 316, 317, 318, 319) then Flag070_Geography_Type = 'Y';
if 'PD Band Type'n < 1400 or 'PD Band Type'n > 1426 then Flag070_PD_Band_Type = 'Y';
if

'Exposure Size Type'n ne 1599 and
'LTV Type'n ne 499   and
'Securitization Type'n     ne 699 and
'Rates Type'n   ne 799 and
'Delinquency Bucket Type'n ne 899 then Flag070_Other_Fields = 'Y'; run;
data check070; set check070; keep Flag070_Exposure_Class_Type Flag070_Geography_Type Flag070_PD_Band_Type Flag070_Other_Fields;
proc sort data=check070 nodupkey; 
by Flag070_Exposure_Class_Type Flag070_Geography_Type 
Flag070_PD_Band_Type Flag070_Other_Fields; run;

* Row count;

%rowct( RP = RP070, Dimension1 = %Str('Geography Type'n), Dimension2 = %Str('PD Band Type'n), 
Dimension3 = %str('Exposure Class Type'n),
rt = 070b);

* (C) ;

proc contents data=RP070 varnum noprint; run;

*%dload( x = %str('070'), f = RP070 );

proc sql noprint; create table Check070_Agg_1 as select sum('Number of Loans'n) as Out1 ,
sum('Estimated Num of Defaulted Loans'n) as out1b,
sum('Realized Num of Defaulted Loans'n) as out1c
from RP070
where 'PD Band Type'n = 1400 and 'Geography Type'n  =300  and 'Exposure Class Type'n  in (502, 503, 505, 506, 507) ;
          create table Check070_Agg_2 as select sum('Number of Loans'n) as Out2,
sum('Estimated Num of Defaulted Loans'n) as out2b,
sum('Realized Num of Defaulted Loans'n) as out2c
from RP070
where 'PD Band Type'n ne 1400 and 'Geography Type'n  =300  and 'Exposure Class Type'n  in (502, 503, 505, 506, 507);
quit;

data Check070c; merge Check070_Agg_1 Check070_Agg_2;
if (abs(Out1 / Out2) > 1.05 or abs(Out1 / Out2) < 0.95) and abs(Out1 - Out2) > 20 then flag070c = 'Y'; 
if (abs(Out1b / Out2b) > 1.05 or abs(Out1b / Out2b) < 0.95) and abs(Out1b - Out2b) > 20 then flag070c = 'Y'; 
if (abs(Out1c / Out2c) > 1.05 or abs(Out1c / Out2c) < 0.95) and abs(Out1c - Out2c) > 20 then flag070c = 'Y'; 
run;

/*********************************************************************************/
/* Impairments and Impairment Reversals 080 */
/**********************************************************************************/

* (A) data vales check;

data check080; set RP080; 

if 'Exposure Class Type'n not in ( 500, 501, 502, 503, 504, 505, 506, 507, 510, 511, 512) then Flag080_Exposure_Class_Type = 'Y';

if
'PD Band Type'n ne 1499    or
'Exposure Size Type'n ne 1599 or
'Geography Type'n    ne 399 or
'LTV Type'n ne 499   or
'Securitization Type'n     ne 699 or
'Rates Type'n   ne 799 or
'Delinquency Bucket Type'n ne 899 then Flag080_Other_Fields = 'Y'; 

run;

data check080; set check080; keep Flag080_Exposure_Class_Type Flag080_Other_Fields;
proc sort data=check080 nodupkey; by Flag080_Exposure_Class_Type Flag080_Other_Fields; run;

* (B) Row count check;
%Macro rowct1 ( RP = , Dimension1 = , rt =  ); 
proc sql noprint;
     select count(*) into : totalrow
     from &RP.;
     select count(distinct &Dimension1. ) into : Dim1Row
     from &RP;
quit;
%put &totalrow &Dim1Row;
data Check&rt.; if &totalrow ne &Dim1Row then Flag&rt._row_ct = 'Y' ;  run;
%mend;
%rowct1( RP = RP080, Dimension1 = %Str('Exposure Class Type'n), rt = 080b);

data temp; set RP080; 
total = 'Total Provisions for Credit Loss'n * 1;
*general = 'General Provisions for Credit Lo'n * 1;
*specific = 'Specific Provisions for Credit L'n * 1;

S = index('Total Provisions for Credit Loss'n, '-') ;
e = length('Total Provisions for Credit Loss'n);
l = s - e + 1;
  y = substr('Total Provisions for Credit Loss'n, s, l);
  if total ne . then total = total;
  if total = . then total = y*1;
drop s e l y; 
run;

data temp; set temp; 

general = 'General Provisions for Credit Lo'n * 1;
*specific = 'Specific Provisions for Credit L'n * 1;

S = index('General Provisions for Credit Lo'n, '-') ;
e = length('General Provisions for Credit Lo'n);
l = s - e + 1;
  y = substr('General Provisions for Credit Lo'n, s, l);
  if general ne . then general = general;
  if general = . then general = y*1;
drop s e l y; 
run;

data temp; set temp; 

* general = 'General Provisions for Credit Lo'n * 1;
specific = 'Specific Provisions for Credit L'n * 1;

S = index('Specific Provisions for Credit L'n, '-') ;
e = length('Specific Provisions for Credit L'n);
l = s - e + 1;
  y = substr('Specific Provisions for Credit L'n, s, l);
  if specific ne . then specific = specific;
  if specific = . then specific = y*1;
drop s e l y; 
run;
proc contents data= temp noprint; run;



proc sql noprint; create table Check080_Agg_1 as select sum('Additions to Gross Impaired'n) as Out1 ,
sum('Gross Impaired Loans and AcceptR'n) as out1b,
sum('Gross Impaired Loans and Accepta'n) as out1c,
sum('Total Allowances for Credit Loss'n) as out1d,
sum(total) as out1e,
sum(general) as out1f,
sum( specific) as out1g

from temp
where 'Exposure Class Type'n = 500 ;

create table Check080_Agg_2 as select sum('Additions to Gross Impaired'n) as Out2 ,
sum('Gross Impaired Loans and AcceptR'n) as out2b,
sum('Gross Impaired Loans and Accepta'n) as out2c,
sum('Total Allowances for Credit Loss'n) as out2d,
sum(total) as out2e,
sum(general) as out2f,
sum( specific) as out2g

from temp
where 'Exposure Class Type'n in ( 501, 504, 507 ) ;
         
quit;

data Check080c; merge Check080_Agg_1 Check080_Agg_2;
if (abs(Out1 / Out2) > 1.05 or
abs(Out1 / Out2) < 0.95) and  
abs(Out1 - Out2) > 20 then flag080a = 'Y'; 

if (abs(Out1 / Out2) > 1.05 or
abs(Out1 / Out2) < 0.95) and  
abs(Out1 - Out2) > 20 then flag080b = 'Y'; 

if (abs(Out1 / Out2) > 1.05 or
abs(Out1 / Out2) < 0.95) and  
abs(Out1 - Out2) > 20 then flag080c = 'Y'; 

if (abs(Out1 / Out2) > 1.05 or
abs(Out1 / Out2) < 0.95) and  
abs(Out1 - Out2) > 20 then flag080d = 'Y'; 

if (abs(Out1 / Out2) > 1.05 or
abs(Out1 / Out2) < 0.95) and  
abs(Out1 - Out2) > 20 then flag080e = 'Y'; 

if (abs(Out1 / Out2) > 1.05 or
abs(Out1 / Out2) < 0.95) and  
abs(Out1 - Out2) > 20 then flag080f = 'Y'; 

if (abs(Out1 / Out2) > 1.05 or
abs(Out1 / Out2) < 0.95) and  
abs(Out1 - Out2) > 20 then flag080g = 'Y'; 
run;

data Oustanding_between_tab; merge check010d check015d check020d check025d check030d check035d check055d check065d; 
keep _010_500_New
_010_501_New
_010_504_New
_010_502_New
flag010d
_015_500_New
_015_501_New
_015_504_New
_015_502_New
flag015d
_020_501_New
_020_502_New
flag020d
_025_501_New
_025_502_New
flag025d
_030_504_New
_030_502_New
flag030d
_035_501_New
_035_502_New
flag035d
_055_501_New
_055_504_New
_055_500_New
_055_502_New
flag055d
_065_501_New
_065_504_New
_065_500_New
_065_502_New
flag065d
;

run;

data Oustanding_between_tab; set Oustanding_between_tab;
if abs(_010_500_New - _015_500_New) > 20 and (abs(_010_500_New/ _015_500_New)>1.05 or abs(_010_500_New/ _015_500_New)<0.95 ) then flag_outstanding_tabs = 'Y';
if abs(_010_500_New - _055_500_New) > 20 and (abs(_010_500_New/_055_500_New)>1.05 or abs(_010_500_New/_055_500_New)<0.95 ) then flag_outstanding_tabs = 'Y';
if abs(_010_500_New - _065_500_New) > 20 and (abs(_010_500_New/_065_500_New)>1.05 or abs(_010_500_New/_065_500_New)<0.95 ) then flag_outstanding_tabs = 'Y';

if abs(_010_501_New - _015_501_New) > 20 and (abs(_010_500_New/_015_501_New)>1.05 or abs(_010_500_New/_015_501_New)<0.95 ) then flag_outstanding_tabs = 'Y';
if abs(_010_501_New - _020_501_New) > 20 and (abs(_010_500_New/_020_501_New)>1.05 or abs(_010_500_New/_020_501_New)<0.95 ) then flag_outstanding_tabs = 'Y';
if abs(_010_501_New - _025_501_New) > 20 and (abs(_010_500_New/_025_501_New)>1.05 or abs(_010_500_New/_025_501_New)<0.95 ) then flag_outstanding_tabs = 'Y';
if abs(_010_501_New - _035_501_New) > 20 and (abs(_010_500_New/_035_501_New)>1.05 or abs(_010_500_New/_035_501_New)<0.95 ) then flag_outstanding_tabs = 'Y';

if abs(_010_502_New - _030_502_New) > 20 then flag_outstanding_tabs = 'Y'; run;




data BD_final_results;
merge 
check000
check005
check005b
check010
check010b
check010c ( keep= flag010c)
check010d ( keep= flag010d)
check015
check015b
check015c ( keep= flag015c)
check015d ( keep= flag015d)
check015d_geography
check020
check020b
check020c ( keep= flag020c)
check020d ( keep= flag020d) 
check025
check025b
check025c ( keep= flag025c)
check025d ( keep= flag025d) 
check030
check030b
check030d ( keep= flag030d)
check035
check035b
check035d ( keep= flag035d) 
check040
check040b
check045
check045b
check045d ( keep= flag045d) 
check047
check047b
check047d ( keep= flag047d) 
check050
check050b
check050d ( keep= flag050d) 
check055
check055b
check055c ( keep= flag055c)
check055d ( keep= flag055d)
check065
check065b
check065c ( keep= flag065c)
check065d ( keep= flag065d) 
check060
check060b
check070
check070b
check070c ( keep= flag070c)
check080
check080b
check080c ( keep= flag080c);
run;


/*GATHERING AND EMAILING THE FINAL RESULTS*/
ods pdf file="&REPORT_PATH./NCR_report_validation.pdf" startpage=now;
options orientation=landscape date;

TITLE5 "NCR TEST RESULTS - COMPARING &monthyear WITH &prev_monthyear";
TITLE6 "Note: If you see 1 or Y in any of the columns please inform the data analysts";
TITLE7 "BUSINESS RULES VALIDATION";


TITLE1 "BD RESULTS";
ODS PDF STARTPAGE=NO;

proc sort data=WORK.BD_FINAL_RESULTS nodup;
by _all_;
run;

proc printto print="&REPORT_PATH./BD_FINAL_RESULTS.lst" new; run;
proc PRINT data=WORK.BD_FINAL_RESULTS;
	run;
quit;

FOOTNOTE1 "Generated by the SAS System (&_SASSERVERNAME, &SYSSCPL) on %TRIM(%QSYSFUNC(DATE(), NLDATE20.)) at %TRIM(%SYSFUNC(TIME(), TIMEAMPM12.))";
TITLE; FOOTNOTE;
ods pdf close;

%MACRO SENDEMAIL;
/*EMAIL */

FILENAME OUTMAIL EMAIL
SUBJECT= "[RRAP] NCR Report Validation - %sysfunc(putn("&MTH_END_DT."d, monname3.)) %sysfunc(year("&MTH_END_DT."d))";   * comparing &monthyear with &prev_monthyear - &SYSDATE9.";
/*FROM= "";*/

DATA _NULL_;
FILE OUTMAIL
TO= ("edwsupport@scotiabank.com" "GRT-Model-Support@scotiabank.com")
/*CC= ("min.zhou@scotiabank.com" "sreeraj.pulickal@scotiabank.com" "poornima.balakrishnan@scotiabank.com")*/
/*BCC= ("&BCC")*/
ATTACH= ("&REPORT_PATH./NCR_report_validation.pdf" CONTENT_TYPE="application/pdf")
;
PUT "Hi,";
PUT "The monthly NCR validation report is attached for your reference.";
PUT "If you find 1 or Y in any of the columns please inform the data analysts as the NCR reports need to be reviewed";
PUT "Please note this is a system generated email. Do not reply to this email as responses arent monitored";
PUT "regards";
RUN;
%MEND;
%sendemail;
