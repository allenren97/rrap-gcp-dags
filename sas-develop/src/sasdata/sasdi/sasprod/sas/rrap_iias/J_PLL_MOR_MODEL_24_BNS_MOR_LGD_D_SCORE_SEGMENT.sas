%RRAP_MOR_BNS_AUTOEXEC

proc sql;
CONNECT USING PASTHRU AS IIASCON;
		create table with_scorecard_vars_lgdd as
		SELECT * FROM CONNECTION TO IIASCON
			(select a.mortgage_no,
				a.time_key,
				a.insurance,
				a.bulk_ind,
				a.total_bal,
				a.total_suspense,
				a.foreclose_ind,
				a.month_def,
				a.acct_lgd24_nocost,
				a.acct_lgd24_nocost_cap,
				a.acct_lgd24_cost,
				a.acct_lgd24_cost_cap15,
				b.ratio,
				b.urate,
				b.d2dbalmax12m,
				b.index_teranetV,
				b.ltv
			from &FRG_ORIG..ALL_LGD_D as a
				inner join
					&FRG_ORIG..SCORECARD_VARS_FINAL as b
					on (a.mortgage_no = b.mortgage_no and
					a.time_key = LAST_DAY(process_date)));
	DISCONNECT FROM IIASCON;
	quit;

data bns_lgd_d_scored_seg_accts;
		set with_scorecard_vars_lgdd;
		scorecard_points = 0;

		if upcase(foreclose_ind) in ('Y') then
			do;
				b_foreclose_ind = 1;
			end;
		else
			do;
				b_foreclose_ind =  2;
			end;

		if not missing(ratio) and 0.1478 <= ratio or urate>=0.1 then
			do;
				b_unemp_rate_ratio = 2;
			end;
		else
			do;
				b_unemp_rate_ratio =  1;
			end;

		if not missing(d2dbalmax12m) and d2dbalmax12m < 499.2 then
			do;
				b_d2dbalmax12m = 1;
			end;
		else
			do;
				b_d2dbalmax12m = 2;
			end;

		if not missing(month_def) and 6.5 <= month_def and month_def < 11.5 then
			do;
				b_month_def = 2;
			end;
		else if not missing(month_def) and 11.5 <= month_def and month_def < 15.5 then
			do;
				b_month_def = 3;
			end;
		else if not missing(month_def) and 15.5 <= month_def then
			do;
				b_month_def = 4;
			end;
		else
			do;
				b_month_def = 1;
			end;

		if not missing(index_teranetV) and 151065.35 <= index_teranetV and index_teranetV < 1000000 then
			do;
				b_index_teranetv = 2;
			end;
		else
			do;
				b_index_teranetv = 1;
			end;

		if not missing(ltv) and 0.6589 < ltv then
			do;
				b_ltv = 2;
			end;
		else
			do;
				b_ltv = 1;
			end;

		if b_foreclose_ind = 1 then
			score_foreclose_ind = 166;

		if b_foreclose_ind = 2 then
			score_foreclose_ind = 0;

		if b_unemp_rate_ratio = 1 then
			score_unemp_ratio = 0;

		if b_unemp_rate_ratio = 2 then
			score_unemp_ratio = 64;

		if b_d2dbalmax12m = 1 then
			score_d2dbalmax12m = 42;

		if b_d2dbalmax12m = 2 then
			score_d2dbalmax12m = 0;

		if b_month_def = 1 then
			score_month_def = 0;

		if b_month_def = 2 then
			score_month_def = 216;

		if b_month_def = 3 then
			score_month_def = 361;

		if b_month_def = 4 then
			score_month_def = 540;

		if b_index_teranetv = 1 then
			score_index_teranetv = 137;

		if b_index_teranetv = 2 then
			score_index_teranetv = 0;

		if b_ltv = 1 then
			score_ltv = 0;

		if b_ltv = 2 then
			score_ltv = 50;
		scorecard_points = score_foreclose_ind + score_unemp_ratio + score_d2dbalmax12m + score_month_def + score_index_teranetv + score_ltv;

		if month_def >= 24 then
			bns_lgd_d_segment = 14;
		else if total_bal <= 5000 and month_def < 24 then
			bns_lgd_d_segment = 13;
		else if (scorecard_points = . or scorecard_points < 247) and upcase(insurance) = 'UNINSURED' then
			do;
				bns_lgd_d_segment = 1;
			end;
		else if scorecard_points >= 247 and scorecard_points <= 496 and upcase(insurance) = 'UNINSURED' then
			do;
				bns_lgd_d_segment = 2;
			end;
		else if scorecard_points >= 497 and scorecard_points <= 647 and upcase(insurance) = 'UNINSURED' then
			do;
				bns_lgd_d_segment = 3;
			end;
		else if scorecard_points >= 648 and upcase(insurance) = 'UNINSURED' then
			do;
				bns_lgd_d_segment = 4;
			end;
		else if (scorecard_points = . or scorecard_points < 247) and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'N' then
			do;
				bns_lgd_d_segment = 5;
			end;
		else if scorecard_points >= 247 and scorecard_points <= 496 and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'N' then
			do;
				bns_lgd_d_segment = 6;
			end;
		else if scorecard_points >= 497 and scorecard_points <= 647 and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'N' then
			do;
				bns_lgd_d_segment = 7;
			end;
		else if scorecard_points >= 648 and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'N' then
			do;
				bns_lgd_d_segment = 8;
			end;
		else if (scorecard_points = . or scorecard_points < 247) and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'Y' then
			do;
				bns_lgd_d_segment = 9;
			end;
		else if scorecard_points >= 247 and scorecard_points <= 496 and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'Y' then
			do;
				bns_lgd_d_segment = 10;
			end;
		else if scorecard_points >= 497 and scorecard_points <= 647 and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'Y'  then
			do;
				bns_lgd_d_segment = 11;
			end;
		else if scorecard_points >= 648 and upcase(insurance) = 'INSURED' and upcase(bulk_ind) = 'Y' then
			do;
				bns_lgd_d_segment = 12;
			end;
	run;

%MACRO ANTQ_FETCH_DATES;

	PROC SQL NOPRINT;
		SELECT MAX(TIME_KEY) FORMAT 8. INTO :TGT_TIME_KEY FROM FRGPLL.BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ;
		SELECT MAX(TIME_KEY) FORMAT 8. INTO :SRC_TIME_KEY FROM BNS_LGD_D_SCORED_SEG_ACCTS;
		SELECT INTNX('MONTH',MAX(TIME_KEY),-24,'E') FORMAT YYMMDD10. INTO :TIME_KEY_24MTH FROM BNS_LGD_D_SCORED_SEG_ACCTS;
		SELECT INTNX('MONTH',MAX(TIME_KEY),-24,'E') INTO :DEL_TIME_KEY FROM BNS_LGD_D_SCORED_SEG_ACCTS;
	QUIT;

	DATA NZFORMATS;
		FORMAT NZSRC NZTGT YYMMDD10.;
		NZFORMAT="'"||"&TIME_KEY_24MTH"||"'";
		CALL SYMPUT('PRIOR24MTH',NZFORMAT);
		NZSRC=&SRC_TIME_KEY;
		NZTGT=&TGT_TIME_KEY;
		CALL SYMPUT('SRC_TIME_NZ',"'"||PUT(&SRC_TIME_KEY,YYMMDD10.)||"'");
		CALL SYMPUT('TGT_TIME_NZ',"'"||PUT(&TGT_TIME_KEY,YYMMDD10.)||"'");
	RUN;

	%PUT SRC_TIME_KEY=&SRC_TIME_KEY;
	%PUT TGT_TIME_KEY=&TGT_TIME_KEY;
	%PUT TIME_KEY_24MTH=&TIME_KEY_24MTH;
	%PUT DEL_TIME_KEY=&DEL_TIME_KEY;
	%PUT PRIOR24MTH=&PRIOR24MTH;
	%PUT SRC_TIME_NZ=&SRC_TIME_NZ;
	%PUT TGT_TIME_NZ=&TGT_TIME_NZ;
%MEND ANTQ_FETCH_DATES;

%MACRO CR69_HIST_MAINTAIN;
	%GLOBAL PRIOR24MTH;
	%GLOBAL SRC_TIME_NZ;
	%GLOBAL TGT_TIME_NZ;
	%GLOBAL TGT_TIME_KEY;
	%GLOBAL SRC_TIME_KEY;
	%GLOBAL TIME_KEY_24MTH;
	%GLOBAL DEL_TIME_KEY;

	%ANTQ_FETCH_DATES;
	%delete_table(FRGPLL.BNS_LGD_D_SCORED_SEG_ACCTS_TMP);

	DATA BNS_LGD_D_SCORED_SEG_ACCTS;
		SET BNS_LGD_D_SCORED_SEG_ACCTS;
		ACCT_LGD24_NOCOST_BKUP= ACCT_LGD24_NOCOST;
		ACCT_LGD24_NOCOST_CAP_BKUP= ACCT_LGD24_NOCOST_CAP;
		ACCT_LGD24_COST_BKUP= ACCT_LGD24_COST;
		ACCT_LGD24_COST_CAP15_BKUP= ACCT_LGD24_COST_CAP15;
	RUN;

	DATA FRGPLL.BNS_LGD_D_SCORED_SEG_ACCTS_TMP (bulkload=yes BL_METHOD=CLILOAD);
		SET BNS_LGD_D_SCORED_SEG_ACCTS;
		WHERE TIME_KEY>=&DEL_TIME_KEY;
	RUN;

	PROC SQL NOPRINT;
		CONNECT USING PASTHRU AS IIASCON;
		EXECUTE(DELETE FROM &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS WHERE TIME_KEY<&PRIOR24MTH) BY IIASCON;
		DISCONNECT FROM IIASCON;
	QUIT;

	PROC SQL NOPRINT;
		CONNECT USING PASTHRU AS IIASCON;
		EXECUTE(UPDATE &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS A
			SET ACCT_LGD24_NOCOST=B.ACCT_LGD24_NOCOST, ACCT_LGD24_NOCOST_CAP=B.ACCT_LGD24_NOCOST_CAP, 
				ACCT_LGD24_COST=B.ACCT_LGD24_COST, ACCT_LGD24_COST_CAP15=B.ACCT_LGD24_COST_CAP15
			FROM &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS_TMP B
				WHERE
					A.MORTGAGE_NO = B.MORTGAGE_NO AND 
					A.TIME_KEY = B.TIME_KEY AND
					A.TIME_KEY>=&PRIOR24MTH) BY IIASCON;
		DISCONNECT FROM IIASCON;
	QUIT;

	PROC SQL NOPRINT;
		CONNECT USING PASTHRU AS IIASCON;
		EXECUTE(DELETE FROM &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS WHERE TIME_KEY>=&SRC_TIME_NZ) BY IIASCON;
		DISCONNECT FROM IIASCON;
	QUIT;

	PROC SQL NOPRINT;
		CONNECT USING PASTHRU AS IIASCON;
		EXECUTE(INSERT INTO &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS SELECT * FROM &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS_TMP WHERE TIME_KEY>=&SRC_TIME_NZ) BY IIASCON;
		DISCONNECT FROM IIASCON;
	QUIT;

	%delete_table(FRGPLL.BNS_LGD_D_SCORED_SEG_ACCTS_TMP);

	PROC SQL NOPRINT;
		CONNECT USING PASTHRU AS IIASCON;
		EXECUTE(UPDATE &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ A
			SET ACCT_LGD24_NOCOST=B.ACCT_LGD24_NOCOST, ACCT_LGD24_NOCOST_CAP=B.ACCT_LGD24_NOCOST_CAP, 
				ACCT_LGD24_COST=B.ACCT_LGD24_COST, ACCT_LGD24_COST_CAP15=B.ACCT_LGD24_COST_CAP15
			FROM &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS B
				WHERE
					A.MORTGAGE_NO = B.MORTGAGE_NO AND 
					A.TIME_KEY = B.TIME_KEY AND
					A.TIME_KEY>=&PRIOR24MTH) BY IIASCON;
		DISCONNECT FROM IIASCON;
	QUIT;

	%IF (&SRC_TIME_KEY > &TGT_TIME_KEY) %THEN
		%DO;
			%put SIMPLE INSERT;

			PROC SQL NOPRINT;
				CONNECT USING PASTHRU AS IIASCON;
				EXECUTE(INSERT INTO &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ 
					SELECT * FROM &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS WHERE 
						TIME_KEY > &TGT_TIME_NZ) BY IIASCON;
			QUIT;

		%END;

	%IF (&SRC_TIME_KEY = &TGT_TIME_KEY) %THEN
		%DO;
			%put DELETE WILL HAPPEN;

			PROC SQL NOPRINT;
				CONNECT USING PASTHRU AS IIASCON;
				EXECUTE(DELETE FROM &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ 
					WHERE TIME_KEY = &SRC_TIME_NZ) BY IIASCON;
			QUIT;

			PROC SQL NOPRINT;
				CONNECT USING PASTHRU AS IIASCON;
				EXECUTE(INSERT INTO &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ 
					SELECT * FROM &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS WHERE 
						TIME_KEY >= &TGT_TIME_NZ) BY IIASCON;
			QUIT;

		%END;
%MEND;

%CR69_HIST_MAINTAIN;

PROC SQL;
	CONNECT USING PASTHRU AS IIASCON;
	CREATE TABLE MOR_LGDD_ANTQ_FNL_APPEND_TBL AS
	SELECT * FROM CONNECTION TO IIASCON
	(SELECT * FROM &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ
    WHERE TIME_KEY = &PRIOR24MTH);
	DISCONNECT FROM IIASCON;
QUIT;

DATA MOR_LGDD_ANTQ_FNL_APPEND_TBL;
	SET MOR_LGDD_ANTQ_FNL_APPEND_TBL;
	ACCT_LGD24_NOCOST_BKUP = ACCT_LGD24_NOCOST;
	ACCT_LGD24_NOCOST_CAP_BKUP = ACCT_LGD24_NOCOST_CAP;
	ACCT_LGD24_COST_BKUP = ACCT_LGD24_COST;
	ACCT_LGD24_COST_CAP15_BKUP = ACCT_LGD24_COST_CAP15;
RUN;

PROC SQL;
    UPDATE MOR_LGDD_ANTQ_FNL_APPEND_TBL AS T
    SET ACCT_LGD24_NOCOST = (SELECT LGD_NO_COST FROM INTMED.MOR_LGDD_REALIZED AS A
                             WHERE T.MORTGAGE_NO = A.MORTGAGE_NO AND T.TIME_KEY = A.PROCESS_DATE),
        ACCT_LGD24_NOCOST_CAP = (SELECT LGD_NO_COST_CAP FROM INTMED.MOR_LGDD_REALIZED AS B
                             WHERE T.MORTGAGE_NO = B.MORTGAGE_NO AND T.TIME_KEY = B.PROCESS_DATE),
        ACCT_LGD24_COST = (SELECT LGD_COST FROM INTMED.MOR_LGDD_REALIZED AS C
                             WHERE T.MORTGAGE_NO = C.MORTGAGE_NO AND T.TIME_KEY = C.PROCESS_DATE),
        ACCT_LGD24_COST_CAP15 = (SELECT LGD_COST_CAP FROM INTMED.MOR_LGDD_REALIZED AS D
                             WHERE T.MORTGAGE_NO = D.MORTGAGE_NO AND T.TIME_KEY = D.PROCESS_DATE)
    WHERE T.MORTGAGE_NO IN (SELECT MORTGAGE_NO FROM INTMED.MOR_LGDD_REALIZED) AND T.TIME_KEY IN (SELECT PROCESS_DATE FROM INTMED.MOR_LGDD_REALIZED);
QUIT;

PROC SQL;
    CONNECT USING PASTHRU AS IIASCON;
    EXECUTE(DELETE FROM &FRGPLL..BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ WHERE TIME_KEY = &PRIOR24MTH)BY IIASCON;
    DISCONNECT FROM IIASCON;
QUIT;

PROC APPEND BASE=FRGPLL.BNS_LGD_D_SCORED_SEG_ACCTS_ANTQ(BULKLOAD=YES BL_METHOD=CLILOAD)
            DATA=MOR_LGDD_ANTQ_FNL_APPEND_TBL;
RUN;