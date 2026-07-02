AT36_MAX12M=AT36_MAX12M;
PRINCIPAL_BALCHG3M=BALCHAG3M;
BR34_AVG3M=BR34_AVG3M;
PR11_MAX3M=PR11_MAX3M;
AT20=MTH_SINCE_OLDST_TRADE_OPND_CNT;
SUBV_FLAG=SUBV_FLAG;
PR21 = DERGTRY_PUB_RECD_CNT;



if AVG_of_Waverage > 0 and AVG_of_MSRP > 0 THEN DO;
WAVG_MSRP= AVG_of_Waverage/AVG_of_MSRP;  end;


if AT36_max12m >=3.5 then scr_AT36_max12m = 0;
else scr_AT36_max12m = 45;


if PRINCIPAL_BALchg3m <0 then scr_PRINCIPAL_BALchg3m = 0;
else scr_PRINCIPAL_BALchg3m = 123;

if MISSING(BR34_avg3m ) then scr_BR34_avg3m= 52;
else if BR34_avg3m  <     100 then scr_BR34_avg3m  = 0;
else scr_BR34_avg3m  = 52;

if PR21 >0  then scr_PR21 = 110;
else scr_PR21 = 0;


if NOT MISSING(PR11_max3m) AND 4166 <= PR11_max3m AND PR11_max3m < 8013 THEN DO;
  scr_PR11_max3m  = 103;
  end;
else if NOT MISSING(PR11_max3m) AND 8013 <= PR11_max3m THEN DO;
  scr_PR11_max3m  = 193;
  end;
else do;
  scr_PR11_max3m  = 0;
  end;

if NOT MISSING(Wavg_MSRP) AND Wavg_MSRP  <  0.12856246567996 THEN DO;
  scr_Wavg_MSRP  = 372;
  end;
else if NOT MISSING(Wavg_MSRP) AND 0.12856246567996 <= Wavg_MSRP AND Wavg_MSRP < 0.27043460513807 THEN DO;
  scr_Wavg_MSRP  = 191;
  end;
else if NOT MISSING(Wavg_MSRP ) AND 0.36760879409041 <= Wavg_MSRP AND Wavg_MSRP < 0.45559324295992 THEN DO;
  scr_Wavg_MSRP  = 68;
  end;
else if NOT MISSING(Wavg_MSRP ) AND 0.45559324295992 <= Wavg_MSRP  THEN DO;
  scr_Wavg_MSRP  =  0;
  end;
else do;
  scr_Wavg_MSRP  = 136;
  end;


if AT20 < 298.5 then scr_AT20 = 63;
else scr_AT20 = 0;

if SUBV_FLAG = '1' then scr_subv_flag = 0;
else scr_subv_flag = 42;

tot_score=scr_subv_flag + scr_Wavg_MSRP + scr_PR11_max3m + scr_PR21 +
scr_AT20 +  scr_AT36_max12m + scr_PRINCIPAL_BALchg3m + scr_BR34_avg3m
;

recovery_score=tot_score;
rcvry_score_desc="Recovery model";
model="ITLC_R01";
