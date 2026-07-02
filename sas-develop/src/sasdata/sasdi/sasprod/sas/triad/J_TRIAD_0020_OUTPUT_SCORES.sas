x mv /owpftp/triad/outgoing/triad_edwout_scores_d_mnthly*.JUAHJJRW /owpftp/triad/outgoing/archive;

data TRIAD.PREPPED_INSTRMNT_FACT;
	merge TRIAD.BASEL_ANALYTCL_BL_INSTRMNT_FACT(in=a) TRIAD.BASEL_CUST_ACCT_RLTNP_SNAPSHOT(in=b keep= basel_acct_id PRIM_CUST_CID);
	by basel_acct_id;
	if a;
run;


data TRIAD.PRE_OUTPUT;
	set 
	TRIAD.PREPPED_INSTRMNT_FACT;
	ATTRIB ACCT_MNEMONIC_CD LENGTH=$3. FILLER LENGTH=$36.;
	where src_sys_cd in ('KS','SPL','MOR') and 	
			CONSM_PRD_TREATMNT_CD='A' and SML_BUS_F='N' and PIT_STAT_CD in ('CUR','DEF') and TRNST_EXCLSN_F='N';

	if src_sys_cd in ('KS','SPL') then do;
		ACCOUNT_ID=put(input(tranwrd(unq_acct_id,'CA0201',''),30.),z20.);
	end;
	else if src_sys_cd = 'MOR' then do;
		ACCOUNT_ID=put(input(tranwrd(unq_acct_id,'CA201',''),30.),z20.);
	end;
	CUST_ID=put(input(PRIM_CUST_CID,20.),z20.);

	if src_sys_cd='MOR' then do; 
		if STRIP(PD_MODEL_NM)='BNS MOR PD' then do;
			SCORE_PD_MG = PUT(PD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'MOR'; SEG_PD_MG = PUT(PD_BASEL_SEG_NUM,Z5.);
		end;
		if STRIP(LGD_MODEL_NM) in ('BNS MOR LGD-ND','BNS MOR LGD-D') then do;
			SCORE_LGD_MG = PUT(LGD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'MOR';
		end;
	end;

	else if src_sys_cd='SPL' then do;
		if substr(PD_MODEL_NM,1,3)='ITL' then do;
			SCORE_PD_ITL = PUT(PD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'ITL'; SEG_PD_ITL = PUT(PD_BASEL_SEG_NUM,Z5.);
		end;
		else if substr(PD_MODEL_NM,1,3)='DTL' then do;
			SCORE_PD_DTL = PUT(PD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'DTL'; SEG_PD_DTL = PUT(PD_BASEL_SEG_NUM,Z5.);
		end;

		if substr(LGD_MODEL_NM,1,3)='ITL' then do;
			SCORE_LGD_ITL = PUT(LGD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'ITL';
		end;
		else if substr(LGD_MODEL_NM,1,3)='DTL' then do;
			SCORE_LGD_DTL = PUT(LGD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'DTL';
		end;
	end;

	else if src_sys_cd='KS' then do;
		if substr(PD_MODEL_NM,1,5)='HELOC' then do;
			SCORE_PD_HELOC = PUT(PD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'HEL'; SEG_PD_HELOC = PUT(PD_BASEL_SEG_NUM,Z5.);
		end;
		else if substr(PD_MODEL_NM,1,2)='CC' then do;
			SCORE_PD_CC = PUT(PD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'CC'; SEG_PD_CC = PUT(PD_BASEL_SEG_NUM,Z5.);
		end;
		else if substr(PD_MODEL_NM,1,3)='LOC' and UPCASE(SCRTY_TP_DESC)='UNSECURED' then do;
			SCORE_PD_ULOC = PUT(PD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'LOC'; SEG_PD_ULOC = PUT(PD_BASEL_SEG_NUM,Z5.);
		end;
		else if substr(PD_MODEL_NM,1,2)='SL' then do;
			SCORE_PD_SL = PUT(PD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'ST'; SEG_PD_SL = PUT(PD_BASEL_SEG_NUM,Z5.);
		end;

		if substr(LGD_MODEL_NM,1,5)='HELOC' then do;
			SCORE_LGD_HELOC = PUT(LGD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'HEL';
		end;
		else if substr(LGD_MODEL_NM,1,2)='CC' then do;
			SCORE_LGD_CC = PUT(LGD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'CC';
		end;
		else if substr(LGD_MODEL_NM,1,3)='LOC' and UPCASE(SCRTY_TP_DESC)='UNSECURED' then do;
			SCORE_LGD_ULOC = PUT(LGD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'LOC';
		end;
		else if substr(LGD_MODEL_NM,1,2)='SL' then do;
			SCORE_LGD_SL = PUT(LGD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'ST';
		end;

		if substr(EAD_MODEL_NM,1,5)='HELOC' then do;
			SCORE_EAD_HELOC = PUT(EAD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'HEL';
		end;
		else if substr(EAD_MODEL_NM,1,2)='CC' then do;
			SCORE_EAD_CC = PUT(EAD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'CC';
		end;
		else if substr(EAD_MODEL_NM,1,3)='LOC' and UPCASE(SCRTY_TP_DESC)='UNSECURED' then do;
			SCORE_EAD_ULOC = PUT(EAD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'LOC';
		end;
		else if substr(EAD_MODEL_NM,1,2)='SL' then do;
			SCORE_EAD_SL = PUT(EAD_ACCT_SCORE,Z5.); ACCT_MNEMONIC_CD = 'ST';
		end;

	end;

	array scores {26}   CUST_ID SCORE_PD_ITL SCORE_PD_DTL SCORE_PD_MG SCORE_PD_HELOC SCORE_PD_CC  SCORE_PD_ULOC SCORE_PD_SL
						SCORE_LGD_ITL SCORE_LGD_DTL SCORE_LGD_MG SCORE_LGD_HELOC SCORE_LGD_CC SCORE_LGD_ULOC SCORE_LGD_SL
						SCORE_EAD_HELOC SCORE_EAD_CC SCORE_EAD_ULOC SCORE_EAD_SL
						SEG_PD_MG SEG_PD_ITL SEG_PD_DTL SEG_PD_HELOC SEG_PD_CC SEG_PD_ULOC SEG_PD_SL;
	do i = 1 to 26;
		scores{i} = tranwrd(scores{i},'.','');
	end;

	RECORD_TYPE='D';
	
	if missing(ACCT_MNEMONIC_CD) then delete;

	keep RECORD_TYPE src_sys_cd ACCOUNT_ID unq_acct_id CUST_ID PRIM_CUST_CID ACCT_MNEMONIC_CD SCRTY_TP_DESC
			PD_MODEL_NM PD_ACCT_SCORE SCORE_PD_ITL SCORE_PD_DTL SCORE_PD_MG SCORE_PD_HELOC SCORE_PD_CC  SCORE_PD_ULOC SCORE_PD_SL
			LGD_MODEL_NM LGD_ACCT_SCORE SCORE_LGD_ITL SCORE_LGD_DTL SCORE_LGD_MG SCORE_LGD_HELOC SCORE_LGD_CC SCORE_LGD_ULOC SCORE_LGD_SL
			EAD_MODEL_NM EAD_ACCT_SCORE SCORE_EAD_HELOC SCORE_EAD_CC SCORE_EAD_ULOC SCORE_EAD_SL 
			SEG_PD_MG SEG_PD_ITL SEG_PD_DTL SEG_PD_HELOC SEG_PD_CC SEG_PD_ULOC SEG_PD_SL FILLER
;
	
run;

/*proc sort data=triad.pre_output; by src_sys_cd; run;*/
/*proc surveyselect data=PRE_OUTPUT*/
/*        out=PRE_OUTPUT_sample*/
/*        Method= SRS*/
/*        Sampsize= 10000*/
/*		Seed= 13571;*/
/*		strata src_sys_cd ;*/
/*Run;*/

/*data PRE_OUTPUT;*/
/*set PRE_OUTPUT_sample;*/
/*run;*/

proc contents noprint data=triad.pre_output out=contents(keep=nobs); run;
data _null_;
	set contents(obs=1);
	call symputx('record_count',nobs);
run;
%put &record_count;

data _null_;
	call symputx('file_date',put("&mth_end_dt."d,yymmdd10.));
	call symputx('file_name_date',put("&mth_end_dt."d,yymmddn8.));

run;
%put &file_date. &file_name_date.;

/*triad_edwout_scores_d_mnthly_YYYYMMDD.del.JUAHJJRW*/
DATA HEADER;
	RECORD_TYPE='H';
	FILE_NAME="triad_edwout_scores_d_mnthly_&file_name_date..JUAHJJRW";
	FILE_DATE="&file_date.";
	CALL SYMPUTX('FILLER_H_LENGTH',TRIM(LEFT(PUT(150 - (LENGTH(FILE_NAME) + LENGTH(FILE_DATE)),3.))));
	CALL SYMPUTX('FILE_NAME_LENGTH',TRIM(LEFT(PUT(LENGTH(FILE_NAME),3.))));
RUN;

%put &FILLER_H_LENGTH. &FILE_NAME_LENGTH.;
DATA HEADER;
	SET HEADER;
	ATTRIB FILLER_H LENGTH=$&FILLER_H_LENGTH.;
RUN;

DATA TRAILER;
	ATTRIB RECORD_COUNT LENGTH=$10;
	RECORD_TYPE='T';
	RECORD_COUNT=PUT(&RECORD_COUNT.,z10.);
	CALL SYMPUTX('FILLER_T_LENGTH',TRIM(LEFT(PUT(150 - 1 - (LENGTH(RECORD_COUNT)),3.))));
RUN;

DATA TRAILER;
	SET TRAILER;
	ATTRIB FILLER_T LENGTH=$&FILLER_T_LENGTH.;
RUN;


data _null_;
	file "&owftp./triad/outgoing/triad_edwout_scores_d_mnthly_&file_name_date..JUAHJJRW";
	set header;
	put RECORD_TYPE $1. FILE_NAME $46. FILE_DATE $10. FILLER_H $143.;
run;
data _null_;
	file "&owftp./triad/outgoing/triad_edwout_scores_d_mnthly_&file_name_date..JUAHJJRW" mod;
	set TRIAD.PRE_OUTPUT;
put 
	RECORD_TYPE $1. CUST_ID $20. ACCOUNT_ID $20. ACCT_MNEMONIC_CD $3. 
	SCORE_EAD_CC $5. SCORE_EAD_HELOC $5. SCORE_EAD_LOC $5. SCORE_EAD_SL $5.
	SCORE_LGD_MG $5. SCORE_LGD_CC $5. SCORE_LGD_DTL $5. SCORE_LGD_HELOC $5. SCORE_LGD_ITL $5. SCORE_LGD_ULOC $5.
	SCORE_PD_MG $5. SCORE_PD_CC $5. SCORE_PD_DTL $5. SCORE_PD_HELOC $5. SCORE_PD_ITL $5. SCORE_PD_ULOC $5. SCORE_PD_SL $5.
	SEG_PD_MG $5. SEG_PD_CC $5. SEG_PD_DTL $5. SEG_PD_HELOC $5. SEG_PD_ITL $5. SEG_PD_ULOC $5. SEG_PD_SL $5.
	FILLER $36.
;
run;
data _null_;
	file "&owftp./triad/outgoing/triad_edwout_scores_d_mnthly_&file_name_date..JUAHJJRW" mod;
	SET TRAILER;
	PUT RECORD_TYPE $1. RECORD_COUNT $10. FILLER $189.;
run;


x gzip &owftp./triad/outgoing/triad_edwout_scores_d_mnthly_&file_name_date..JUAHJJRW;

x mv &owftp./triad/outgoing/triad_edwout_scores_d_mnthly_&file_name_date..JUAHJJRW.gz &owftp./triad/outgoing/triad_edwout_scores_d_mnthly_&file_name_date..JUAHJJRW;


/*
x mv &owftp./triad/outgoing/triad_edwout_scores_d_mnthly_&file_name_date..JUAHJJRW &owftp./triad/outgoing/triad_edwout_scores_d_mnthly_&file_name_date..JUAHJJRW.gz;

x gunzip &owftp./triad/outgoing/triad_edwout_scores_d_mnthly_&file_name_date..JUAHJJRW.gz;
