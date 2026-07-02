
options errorabend compress=yes;
/**************************************************************************** 

 *************************************************************************/ 
%put WORK LOCATION: %sysfunc(getoption(work));
%rrap_autoexec(RRAPENV=REVOLVING_CREDIT);




proc sql noprint;
	connect using nzrrap as nzcon;
	create table LOAD as select * from connection to nzcon(
	select * from &net_db..BASEL_ANALYTCL_BL_INSTRMNT_FACT where src_sys_cd = 'KS' and mth_tm_id = &mth_tm_id.);
quit;


******************** KS DB2 ;

PROC SQL NOPRINT;
	CONNECT USING DB2RRAP AS NZCON;
	EXECUTE(DELETE FROM &DBSCHEMA..BASEL_ANALYTCL_BL_INSTRMNT_FACT WHERE MTH_TM_ID=&MTH_TM_ID. and SRC_SYS_CD='KS') BY NZCON;
QUIT;

proc append base=&DB..BASEL_ANALYTCL_BL_INSTRMNT_FACT  (BULKLOAD=YES BL_METHOD=CLILOAD) data=LOAD force ; 
run;





/*m_DM_RRAP_data_sanity_BASEL_ANALYTCL_BL_INSTRMNT_FACT_KS*/
/*sum_PIT_STAT_CD=SUM(IIF(ISNULL(PIT_STAT_CD),1,0))*/
/*sum_PRD_ID=SUM(IIF(ISNULL(PRD_ID),1,0))*/
/*sum_PD_BAND=SUM(IIF(ISNULL(PD_BAND),1,0))*/
/*sum_ASST_CL_DESC=SUM(IIF(ISNULL(ASST_CL_DESC),1,0))*/
/*sum_PD_BASEL_SEG_NUM=SUM(IIF(PIT_STAT_CD='CUR' AND NOT ISNULL(ASST_CL_DESC) AND ISNULL(PD_BASEL_SEG_NUM),1,0))*/
/*sum_EAD_BASEL_SEG_NUM=SUM(IIF(PIT_STAT_CD='CUR' AND NOT ISNULL(ASST_CL_DESC) AND ISNULL(EAD_BASEL_SEG_NUM),1,0))*/
/*sum_LGD_BASEL_SEG_NUM=SUM(IIF((PIT_STAT_CD='CUR' OR PIT_STAT_CD='DEF') AND NOT ISNULL(ASST_CL_DESC) AND ISNULL(LGD_BASEL_SEG_NUM),1,0))*/
/*sum_NCR_EXPSR_CL_KEY_VAL=SUM(IIF((PIT_STAT_CD='CUR' OR PIT_STAT_CD='DEF') AND ISNULL(NCR_EXPSR_CL_KEY_VAL),1,0))*/
/*sum_NCR_RT_KEY_VAL=SUM(IIF(ISNULL(NCR_RT_KEY_VAL),1,0))*/
/*sum_NCR_DLQNT_BCKT_KEY_VAL=SUM(IIF(ISNULL(NCR_DLQNT_BCKT_KEY_VAL),1,0))*/
/*sum_NCR_EXPSR_SIZE_KEY_VAL=SUM(IIF(ISNULL(NCR_EXPSR_SIZE_KEY_VAL),1,0))*/
/*sum_NCR_GEO_KEY_VAL=SUM(IIF(ISNULL(NCR_GEO_KEY_VAL),1,0))*/
/*sum_NCR_LTV_KEY_VAL=SUM(IIF(ISNULL(NCR_LTV_KEY_VAL),1,0))*/
/*sum_NCR_PD_BAND_KEY_VAL=SUM(IIF(ISNULL(NCR_PD_BAND_KEY_VAL) OR NCR_PD_BAND_KEY_VAL='1498',1,0))*/
/*sum_LGD_FINAL_RPTG_RTO=SUM(IIF(NOT ISNULL(LGD_BASEL_SEG_NUM) AND ISNULL(LGD_FINAL_RPTG_RTO),1,0))*/
/***** SANITY CHECKS CODE UPDATED ON 14JAN2022 by VK *****/
proc sql;
	CONNECT USING DB2RRAP AS NZCON;
	create table load as
		select *              
			from connection to NZCON
				( SELECT CONSM_PRD_TREATMNT_CD,EAD_BASEL_SEG_ID,EAD_FINAL_RPTG_RTO,EAD_MODEL_NM,LGD_BASEL_SEG_ID,LGD_FINAL_RPTG_RTO,
					LGD_MODEL_NM,PD_BASEL_SEG_ID,PD_FINAL_RPTG_RTO,PD_MODEL_NM,PIT_STAT_CD,SML_BUS_F,SRC_SYS_CD,TRNST_EXCLSN_F,
					MTH_TM_ID,NCR_PD_BAND_KEY_VAL,NCR_EXPSR_SIZE_KEY_VAL,NCR_GEO_KEY_VAL,NCR_LTV_KEY_VAL,NCR_EXPSR_CL_KEY_VAL,
					NCR_RT_KEY_VAL,NCR_DLQNT_BCKT_KEY_VAL,AF_SECRTZTN_BAL_AMT,PD_BASEL_SEG_NUM,PD_MODEL_RTO,LGD_BASEL_SEG_NUM,
					LGD_MODEL_RTO,EAD_BASEL_SEG_NUM,EAD_MODEL_RTO,ASST_CL_DESC,PD_BAND,PRD_ID
				from &DBSCHEMA..BASEL_ANALYTCL_BL_INSTRMNT_FACT WHERE MTH_TM_ID=&MTH_TM_ID. and SRC_SYS_CD='KS'
					/*AND SML_BUS_F ='N'*/
					/*AND CONSM_PRD_TREATMNT_CD ='A'*/
					/**/
					/*AND ((PIT_STAT_CD IS NULL OR LTRIM(RTRIM(PIT_STAT_CD)) = '') OR */
					/*	 (TRNST_EXCLSN_F ='N' AND PIT_STAT_CD IN ('CUR','DEF'))*/
					/*)*/

				);
quit;

proc sql;
	create table sanity01 as 
		select 
			SUM
		(case 
			when PIT_STAT_CD eq '' then 1 
			else 0 
		end)
	as sum_PIT_STAT_CD,
		SUM
	(case 
		when PRD_ID eq ''  then 1 
		else 0 
	end)
as sum_PRD_ID,
	SUM
(case 
	when PD_BAND eq '' then 1 
	else 0 
end )
AS sum_PD_BAND,
SUM
(case 
when ASST_CL_DESC eq '' then 1 
else 0 
end )
AS sum_ASST_CL_DESC,
SUM
(case 
when PIT_STAT_CD='CUR' AND ASST_CL_DESC ne '' AND PD_BASEL_SEG_NUM eq . then 1 
else 0 
end )
AS sum_PD_BASEL_SEG_NUM,
SUM
(case 
when PIT_STAT_CD='CUR' AND ASST_CL_DESC ne ''  AND EAD_BASEL_SEG_NUM eq . then 1 
else 0 
end )
AS sum_EAD_BASEL_SEG_NUM,
SUM
(case 
when (PIT_STAT_CD='CUR' OR PIT_STAT_CD='DEF') AND ASST_CL_DESC ne '' AND LGD_BASEL_SEG_NUM eq . then 1 
else 0 
end )
AS sum_LGD_BASEL_SEG_NUM,
SUM
(case 
when (PIT_STAT_CD='CUR' OR PIT_STAT_CD='DEF') AND NCR_EXPSR_CL_KEY_VAL eq '' then 1 
else 0 
end )
AS sum_NCR_EXPSR_CL_KEY_VAL,
SUM
(case 
when NCR_RT_KEY_VAL eq '' then 1 
else 0 
end )
AS sum_NCR_RT_KEY_VAL,
SUM
(case 
when NCR_DLQNT_BCKT_KEY_VAL eq '' then 1 
else 0 
end )
AS sum_NCR_DLQNT_BCKT_KEY_VAL,
SUM
(case 
when NCR_EXPSR_SIZE_KEY_VAL eq '' then 1 
else 0 
end )
AS sum_NCR_EXPSR_SIZE_KEY_VAL,
SUM
(case 
when NCR_GEO_KEY_VAL eq '' then 1 
else 0 
end )
AS sum_NCR_GEO_KEY_VAL,
SUM
(case 
when NCR_LTV_KEY_VAL eq '' then 1 
else 0 
end )
AS sum_NCR_LTV_KEY_VAL,
SUM
(case 
when NCR_PD_BAND_KEY_VAL eq '' OR NCR_PD_BAND_KEY_VAL='1498' then 1 
else 0 
end )
AS sum_NCR_PD_BAND_KEY_VAL,
SUM
(case 
when LGD_BASEL_SEG_NUM ne . AND LGD_FINAL_RPTG_RTO eq . then 1 
else 0 
end )
AS sum_LGD_FINAL_RPTG_RTO
from Load
where SML_BUS_F ='N'
AND CONSM_PRD_TREATMNT_CD ='A'
AND ((PIT_STAT_CD IS NULL OR STRIP(PIT_STAT_CD) = '') OR 
(TRNST_EXCLSN_F ='N' AND PIT_STAT_CD IN ('CUR','DEF'))
				);
quit;

data sanity1;
	set sanity01;

	IF sum_PIT_STAT_CD>0 then
		do;
			CHECK0='PIT_STAT_CD';
		end;
	else
		do;
			CHECK0= '';
		end;

	if sum_PRD_ID>0 then
		do;
			CHECK1='PRD_ID';
		end;
	else
		do;
			CHECK1= '';
		end;

	if sum_PD_BAND>0 then
		do;
			CHECK2= 'PD_BAND';
		end;
	else
		do;
			CHECK2='';
		end;

	if sum_ASST_CL_DESC>0 then
		do;
			CHECK3_4= 'ASST_CL_DESC';
		end;
	else
		do;
			CHECK3_4='';
		end;

	IF sum_PD_BASEL_SEG_NUM>0 then
		do;
			CHECK5= 'PD_BASEL_SEG_NUM';
		end;
	else
		do;
			CHECK5= '';
		end;

	IF sum_EAD_BASEL_SEG_NUM>0 then
		do;
			CHECK6='EAD_BASEL_SEG_NUM';
		end;
	else
		do;
			CHECK6='';
		end;

	IF sum_LGD_BASEL_SEG_NUM>0 then
		do;
			CHECK7='LGD_BASEL_SEG_NUM';
		end;
	else
		do;
			CHECK7= '';
		end;

	IF sum_NCR_EXPSR_CL_KEY_VAL>0 then
		do;
			CHECK8= 'NCR_EXPSR_CL_KEY_VAL';
		end;
	else
		do;
			CHECK8= '';
		end;

	IF sum_NCR_RT_KEY_VAL>0 then
		do;
			CHECK9='NCR_RT_KEY_VAL';
		end;
	else
		do;
			CHECK9= '';
		end;

	IF sum_NCR_DLQNT_BCKT_KEY_VAL>0 then
		do;
			CHECK10= 'NCR_DLQNT_BCKT_KEY_VAL';
		end;
	else
		do;
			CHECK10= '';
		end;

	IF sum_NCR_EXPSR_SIZE_KEY_VAL>0 then
		do;
			CHECK11= 'NCR_EXPSR_SIZE_KEY_VAL';
		end;
	else
		do;
			CHECK11= '';
		end;

	IF sum_NCR_GEO_KEY_VAL>0 then
		do;
			CHECK12= 'NCR_GEO_KEY_VAL';
		end;
	else
		do;
			CHECK12='';
		end;

	IF sum_NCR_LTV_KEY_VAL>0 then
		do;
			CHECK13= 'NCR_LTV_KEY_VAL';
		end;
	else
		do;
			CHECK13= '';
		end;

	IF sum_NCR_PD_BAND_KEY_VAL>0 then
		do;
			CHECK14='NCR_PD_BAND_KEY_VAL';
		end;
	else
		do;
			CHECK14= '';
		end;

	IF sum_LGD_FINAL_RPTG_RTO>0 then
		do;
			CHECK15='LGD_FINAL_RPTG_RTO';
		end;
	else
		do;
			CHECK15='';
		end;

	IF  CHECK0=''  AND CHECK1='' AND CHECK2='' AND CHECK3_4='' AND CHECK5='' AND CHECK6='' AND CHECK7='' AND CHECK8='' AND CHECK9='' AND CHECK10='' AND CHECK11='' AND CHECK12='' AND CHECK13='' AND CHECK14='' AND CHECK15='' then
		do;
			CHECK_NULLS= 'PASSED';
		end;
	else
		do;
			CHECK_NULLS='Table: BASEL_ANALYTCL_BL_INSTRMNT_FACT, KS variables: '||  CHECK0 || ' '  || ' ' || CHECK1 || ' ' || CHECK2 || ' ' || CHECK3_4 || ' ' || CHECK5 || ' ' || CHECK6 || ' ' || CHECK7 || ' ' || CHECK8 || ' ' || CHECK9 || ' ' || CHECK10 || ' ' || CHECK11 || ' ' || CHECK12 || ' ' || CHECK13 || ' ' || CHECK14 || ' ' || CHECK15 || '  have (has) at least one missing value or their (its) default value is incorrect. Please resolve this before proceeding to next step';
			put CHECK_NULLS;
			abort cancel;
		end;
run;

/*m_DM_RRAP_data_sanity_AF_SECRTZTN_BAL_AMT*/
proc sql;
	create table Sanity02 as 
		SELECT COUNT(*) AS  ROW_CNT
			, SUM
		(CASE 
			WHEN AF_SECRTZTN_BAL_AMT IS NULL OR AF_SECRTZTN_BAL_AMT = 0 THEN 1 
			ELSE 0 
		END)
	AS null_zero_bal_amt_cnt
		, SUM(AF_SECRTZTN_BAL_AMT) AS sum_bal_amt
	FROM Load;
quit;

proc sql;
	SELECT  SUM(AF_ADJ_OS_BAL_AMT) format 25.10 AS sum_AF_ADJ_OS_BAL_AMT into :v_LKP_BAL_AMT 
		FROM DB2RRAP.BASEL_REVLVNG_CR_INSTRMNT_ADJ 
			WHERE MTH_TM_ID = &MTH_TM_ID AND AF_ADJ_OS_BAL_AMT > 0;
quit;

data Sanity2;
	set Sanity02;
	v_LKP_BAL_AMT=&v_LKP_BAL_AMT.;

	IF NULL_ZERO_BAL_AMT_CNT = ROW_CNT then
		do;
			v_ALL_NULL_ZERO_BAL_CHECK_MSG= 'The values of AF_SECRTZTN_BAL_AMT are all 0 (zero) and / or NULL.';
		end;
	else
		do;
			v_ALL_NULL_ZERO_BAL_CHECK_MSG='';
		end;

	IF v_ALL_NULL_ZERO_BAL_CHECK_MSG eq '' AND ROW_CNT gt 0 AND (v_LKP_BAL_AMT eq . OR round(SUM_BAL_AMT) ne round(v_LKP_BAL_AMT)) then
		do;
			v_BAL_UNMATCH_CHECK_MSG='The sum of AF_SECRTZTN_BAL_AMT = "' || SUM_BAL_AMT || '" does not match the sum of AF_ADJ_OS_BAL_AMT = "' || v_LKP_BAL_AMT || '" in table BASEL_REVLVNG_CR_INSTRMNT_ADJ.';
		end;
	else
		do;
			v_BAL_UNMATCH_CHECK_MSG='';
		end;

	IF v_ALL_NULL_ZERO_BAL_CHECK_MSG eq '' AND v_BAL_UNMATCH_CHECK_MSG eq '' then
		do;
			v_ABORT_MSG='';
		end;
	else
		do;
			v_ABORT_MSG='The following check(s) failed on table BASEL_ANALYTCL_BL_INSTRMNT_FACT. Please resolve this before proceeeding.' || v_ALL_NULL_ZERO_BAL_CHECK_MSG || v_BAL_UNMATCH_CHECK_MSG;
			put v_ABORT_MSG;
			abort cancel;
		end;

	o_FINAL_OUTPUT='MTH_TM_ID =' || &MTH_TM_ID || '; Table BASEL_ANALYTCL_BL_INSTRMNT_FACT: Row Count = ' || ROW_CNT || ', Null / Zero Balance Amt Count = ' || NULL_ZERO_BAL_AMT_CNT || ', Total Balance Amt (AF_SECRTZTN_BAL_AMT) = ' || SUM_BAL_AMT || '; Table BASEL_REVLVNG_CR_INSTRMNT_ADJ: Total Balance Amt (.AF_ADJ_OS_BAL_AMT)  = ' || v_LKP_BAL_AMT || '.';
run;

/*m_DM_RRAP_data_sanity_HIST_AVG_SEG_REALZ_VAL*/
proc sql;
	create table sanity03 as 
		SELECT  DISTINCT 

			a.PD_MODEL_NM, a.PD_BASEL_SEG_ID, a.PD_MODEL_RTO,
			a.LGD_MODEL_NM, a.LGD_BASEL_SEG_ID, a.LGD_MODEL_RTO,
			a.EAD_MODEL_NM, a.EAD_BASEL_SEG_ID, a.EAD_MODEL_RTO

		FROM Load a
			WHERE  a.CONSM_PRD_TREATMNT_CD ='A'
				AND a.PIT_STAT_CD IN ('CUR','DEF')
				AND a.SML_BUS_F ='N'
				AND a.TRNST_EXCLSN_F ='N'
				AND a.SRC_SYS_CD='KS';
quit;

proc sql;
	CONNECT USING NZRRAP AS NZCON;
	create table LGD as
		select *              
			from connection to NZCON
				( 
			SELECT distinct
				a.LGD_SEG_RTO AS LGD_SEG_RTO, 
				a.BASEL_SEG_ID AS BASEL_SEG_ID, 
				b.MODEL_NM AS MODEL_NM
			FROM  &net_db..LGD_SEG_MTH_REALZ_VAL a , &net_db..BASEL_MODEL b ,&net_db..BASEL_SEG c
				WHERE a.MTH_TM_ID=&MTH_TM_ID.  AND a.BASEL_MODEL_ID=b.BASEL_MODEL_ID AND a.BASEL_SEG_ID = c.BASEL_SEG_ID AND b.MODEL_END_DT='9999-12-31' AND c.SEG_END_DT IS NULL);
quit;

proc sql;
	CONNECT USING NZRRAP AS NZCON;
	create table EAD as 
		select *              
			from connection to NZCON
				( 
			SELECT distinct
				a.EAD_SEG_RTO AS EAD_SEG_RTO, 
				a.BASEL_SEG_ID AS BASEL_SEG_ID, 
				b.MODEL_NM AS MODEL_NM
			FROM &net_db..EAD_SEG_MTH_REALZ_VAL a , &net_db..BASEL_MODEL b ,&net_db..BASEL_SEG c
				WHERE a.MTH_TM_ID=&MTH_TM_ID. AND a.BASEL_MODEL_ID=b.BASEL_MODEL_ID AND a.BASEL_SEG_ID = c.BASEL_SEG_ID AND b.MODEL_END_DT='9999-12-31' AND c.SEG_END_DT IS NULL);
quit;

proc sql;
	CONNECT USING NZRRAP AS NZCON;
	create table PD as 
		select *              
			from connection to NZCON
				(
			SELECT distinct
				a.PD_SEG_RTO AS PD_SEG_RTO, 
				a.BASEL_SEG_ID AS BASEL_SEG_ID, 
				b.MODEL_NM AS MODEL_NM
			FROM &net_db..PD_SEG_MTH_REALZ_VAL a , &net_db..BASEL_MODEL b ,&net_db..BASEL_SEG c 
				WHERE a.MTH_TM_ID=&MTH_TM_ID. AND a.BASEL_MODEL_ID=b.BASEL_MODEL_ID AND a.BASEL_SEG_ID = c.BASEL_SEG_ID AND b.MODEL_END_DT='9999-12-31' AND c.SEG_END_DT IS NULL);
quit;

proc sql;
	create table sanity3 as 
		select a.*,lgd.LGD_SEG_RTO as v_LGD_LKP,ead.EAD_SEG_RTO as v_EAD_LKP,pd.PD_SEG_RTO as v_PD_LKP
			from 
				sanity03 a
			left join lgd on a.LGD_BASEL_SEG_ID=lgd.BASEL_SEG_ID and a.lgd_model_nm=lgd.MODEL_NM
			left join ead on a.eaD_BASEL_SEG_ID=ead.BASEL_SEG_ID and a.ead_model_nm=ead.MODEL_NM
			left join pd on a.pD_BASEL_SEG_ID=pd.BASEL_SEG_ID and a.pd_model_nm=pd.MODEL_NM
	;
quit;

data sanity3a;
	set sanity3;

	/*v_PD_LKP=:LKP.LKP_PD_SEG_MTH_REALZ_VAL(PD_BASEL_SEG_ID,PD_MODEL_NM) -- 2015-04-01: lookup now has only two inputs --:LKP.LKP_PD_SEG_MTH_REALZ_VAL(PD_BASEL_SEG_ID,PD_MODEL_NM,PD_MODEL_RTO)*/
	/*v_PD_FINAL_OUTPUT=IIF(NOT ISNULL(PD_MODEL_RTO) AND (ISNULL(v_PD_LKP) OR v_PD_LKP <> PD_MODEL_RTO),  ABORT('Table: BASEL_ANALYTCL_BL_INSTRMNT_FACT,  variables: '|| CHR(13)|| 'PD_BASEL_SEG_ID= ' || TO_CHAR(PD_BASEL_SEG_ID) || CHR(13)||'and PD_MODEL_NM= ' || PD_MODEL_NM || CHR(13)|| 'and PD_MODEL_RTO= ' || TO_CHAR(PD_MODEL_RTO) || CHR(13)|| 'Model RTO did not match the one in table: PD_SEG_MTH_REALZ_VAL, variable: ' || CHR(13)|| 'PD_MODEL_RTO= '|| TO_CHAR(v_PD_LKP) || CHR(13)|| ' '|| 'Please resolve this before proceeding to next step'), 'PD PASSED') --2015-04-01: print RTO value from lookup --IIF(ISNULL(v_PD_LKP) AND NOT ISNULL(PD_MODEL_RTO),  ABORT('Table: BASEL_ANALYTCL_BL_INSTRMNT_FACT,  variables: '|| CHR(13)|| 'PD_BASEL_SEG_ID= ' || PD_BASEL_SEG_ID|| CHR(13)||'and PD_MODEL_NM= ' || PD_MODEL_NM || CHR(13)|| 'and PD_MODEL_RTO= ' || PD_MODEL_RTO || CHR(13)|| 'did not match with table: PD_SEG_MTH_REALZ_VAL, variables: ' || CHR(13)|| 'PD_BASEL_SEG_ID= '||v_PD_LKP || CHR(13)|| ' '|| 'Please resolve it before proceeding to next step'), 'PD PASSED')*/
	IF PD_MODEL_RTO ne . AND (v_PD_LKP eq . OR round(v_PD_LKP,0.00000001) ne round(PD_MODEL_RTO,0.00000001)) then
		do;
			v_PD_FINAL_OUTPUT=  'Table: BASEL_ANALYTCL_BL_INSTRMNT_FACT,  variables: PD_BASEL_SEG_ID= ' || 
				PD_BASEL_SEG_ID ||' and PD_MODEL_NM= ' || PD_MODEL_NM ||'and PD_MODEL_RTO= ' || PD_MODEL_RTO ||
				'Model RTO did not match the one in table: PD_SEG_MTH_REALZ_VAL, variable: PD_MODEL_RTO= '|| v_PD_LKP ||
				' Please resolve this before proceeding to next step)';
			put v_PD_FINAL_OUTPUT;
			abort cancel;
		end;
	else
		do;
			v_PD_FINAL_OUTPUT='PD PASSED';
		end;

	/*v_LGD_FINAL_OUTPUT=IIF(NOT ISNULL(LGD_MODEL_RTO) AND (ISNULL(v_LGD_LKP) OR v_LGD_LKP <> LGD_MODEL_RTO),  ABORT('Table: BASEL_ANALYTCL_BL_INSTRMNT_FACT,  variables: '|| CHR(13)|| 'LGD_BASEL_SEG_ID= ' || TO_CHAR(LGD_BASEL_SEG_ID) || CHR(13)||'and LGD_MODEL_NM= ' || LGD_MODEL_NM || CHR(13)|| 'and LGD_MODEL_RTO= ' || TO_CHAR(LGD_MODEL_RTO) || CHR(13)|| 'Model RTO did not match the one in table: LGD_SEG_MTH_REALZ_VAL, variable: ' || CHR(13)|| 'LGD_MODEL_RTO= '|| TO_CHAR(v_LGD_LKP) || CHR(13)|| ' '|| 'Please resolve this before proceeding to next step'), 'LGD PASSED') --2015-04-01: print RTO value from lookup --IIF(ISNULL(v_LGD_LKP) AND NOT ISNULL(LGD_MODEL_RTO),  ABORT('Table: BASEL_ANALYTCL_BL_INSTRMNT_FACT,  variables:  LGD_BASEL_SEG_ID= ' || LGD_BASEL_SEG_ID|| 'and LGD_MODEL_NM= ' || LGD_MODEL_NM || 'and LGD_MODEL_RTO= ' || LGD_MODEL_RTO || 'did not match with table: LGD_SEG_MTH_REALZ_VAL, variables: LGD_BASEL_SEG_ID= '||v_LGD_LKP || ' '|| 'Please resolve it before proceeding to next step'), 'LGD PASSED')*/
	IF LGD_MODEL_RTO ne . AND (v_LGD_LKP eq . OR round(v_LGD_LKP,0.00000001) ne round(LGD_MODEL_RTO,0.00000001)) then
		do;
			v_LGD_FINAL_OUTPUT= 'Table: BASEL_ANALYTCL_BL_INSTRMNT_FACT,  variables: LGD_BASEL_SEG_ID= ' || LGD_BASEL_SEG_ID||' and LGD_MODEL_NM= ' || LGD_MODEL_NM || 'and LGD_MODEL_RTO= ' || LGD_MODEL_RTO || 'Model RTO did not match the one in table: LGD_SEG_MTH_REALZ_VAL, variable: ' ||'LGD_MODEL_RTO= '|| v_LGD_LKP ||  ' Please resolve this before proceeding to next step';
			put v_LGD_FINAL_OUTPUT;
			abort cancel;
		end;
	else
		do;
			v_LGD_FINAL_OUTPUT='LGD PASSED';
		end;

	IF EAD_MODEL_RTO ne . AND (v_EAD_LKP eq . OR round(v_EAD_LKP,0.00000001) ne round(EAD_MODEL_RTO,0.00000001)) then
		do;
			v_EAD_FINAL_OUTPUT=  'Table: BASEL_ANALYTCL_BL_INSTRMNT_FACT,  variables: EAD_BASEL_SEG_ID= ' || EAD_BASEL_SEG_ID || ' and EAD_MODEL_NM= ' || EAD_MODEL_NM || ' and EAD_MODEL_RTO= ' ||EAD_MODEL_RTO || 'Model RTO did not match the one in table: EAD_SEG_MTH_REALZ_VAL, variables: ' || 'EAD_MODEL_RTO= '|| v_EAD_LKP ||'Please resolve this before proceeding to next step';
			put v_EAD_FINAL_OUTPUT;
			abort cancel;
		end;
	else
		do;
			v_EAD_FINAL_OUTPUT='EAD PASSED';
		end;

	o_FINAL_OUTPUT=v_PD_FINAL_OUTPUT || ',' || v_LGD_FINAL_OUTPUT || ',' || v_EAD_FINAL_OUTPUT;
run;

/*m_DM_RRAP_data_sanity_RPTG_RTO_WITH_FINAL_FACTORS*/
proc sql;
	create table Sanity04pd as
		SELECT  DISTINCT 

			a.PD_MODEL_NM, a.PD_BASEL_SEG_ID, a.PD_FINAL_RPTG_RTO,. as  LGD_FINAL_RPTG_RTO,. as EAD_FINAL_RPTG_RTO

		FROM LOAD a
			WHERE a.CONSM_PRD_TREATMNT_CD ='A'
				AND a.PIT_STAT_CD IN ('CUR','DEF')
				AND a.SML_BUS_F ='N'
				AND a.TRNST_EXCLSN_F ='N'
				AND a.SRC_SYS_CD='KS';
	create table Sanity04ead as
		SELECT  DISTINCT 
			a.EAD_MODEL_NM, a.EAD_BASEL_SEG_ID, . as PD_FINAL_RPTG_RTO, . as LGD_FINAL_RPTG_RTO,a.EAD_FINAL_RPTG_RTO

		FROM LOAD a
			WHERE a.CONSM_PRD_TREATMNT_CD ='A'
				AND a.PIT_STAT_CD IN ('CUR','DEF')
				AND a.SML_BUS_F ='N'
				AND a.TRNST_EXCLSN_F ='N'
				AND a.SRC_SYS_CD='KS';
	create table Sanity04lgd as
		SELECT  DISTINCT 
			a.LGD_MODEL_NM, a.LGD_BASEL_SEG_ID, . as PD_FINAL_RPTG_RTO, a.LGD_FINAL_RPTG_RTO, . as EAD_FINAL_RPTG_RTO
		FROM LOAD a
			WHERE a.CONSM_PRD_TREATMNT_CD ='A'
				AND a.PIT_STAT_CD IN ('CUR','DEF')
				AND a.SML_BUS_F ='N'
				AND a.TRNST_EXCLSN_F ='N'
				AND a.SRC_SYS_CD='KS';
quit;

proc sql;
	CONNECT USING NZRRAP AS NZCON;
	create table RPTG as 
		select *              
			from connection to NZCON
				(
			SELECT 
				a.FINAL_RTO as FINAL_RTO, 
				a.BASEL_SEG_ID as BASEL_SEG_ID, 
				b.MODEL_NM as MODEL_NM

			FROM 
				&net_db..BASEL_SEG_RPTG_PARM a
				,&net_db..BASEL_MODEL b
				,&net_db..BASEL_SEG c
			WHERE  a.BASEL_MODEL_Id=b.BASEL_MODEL_ID AND a.BASEL_SEG_ID = c.BASEL_SEG_ID 			AND a.CRNT_F='Y' 			AND b.MODEL_END_DT='9999-12-31' 			AND c.SEG_END_DT IS NULL		);
quit;

proc sql;
	create table sanity04a as 
		select a.*,RPTG.FINAL_RTO as v_PD_lkp, . as v_EAD_lkp, . as v_LGD_lkp
			from Sanity04pd a left join RPTG on (a.PD_MODEL_NM=RPTG.MODEL_NM and a.PD_BASEL_SEG_ID=RPTG.BASEL_SEG_ID )
				union 
			select a.*,. as v_PD_lkp, RPTG.FINAL_RTO as v_EAD_lkp,. as v_LGD_lkp
				from 
					sanity04EAD  a left join  RPTG on (a.EAD_MODEL_NM=RPTG.MODEL_NM and a.EAD_BASEL_SEG_ID=RPTG.BASEL_SEG_ID )
					union 
				select a.*,. as v_PD_lkp,. as v_EAD_lkp, RPTG.FINAL_RTO as v_LGD_lkp
					from 
						sanity04LGD a left join RPTG on (a.LGD_MODEL_NM=RPTG.MODEL_NM and a.LGD_BASEL_SEG_ID=RPTG.BASEL_SEG_ID )
	;
quit;

data Sanity4;
	set sanity04a;

	/*v_PD_FINAL_OUTPUT=IIF(NOT ISNULL(PD_FINAL_RPTG_RTO) AND (ISNULL(v_PD_LKP) OR v_PD_LKP <> PD_FINAL_RPTG_RTO),  ABORT('Table BASEL_ANALYTCL_BL_INSTRMNT_FACT with variables' || CHR(13)|| 'PD_BASEL_SEG_ID= ' || TO_CHAR(PD_BASEL_SEG_ID) || ' and PD_MODEL_NM= ' || RTRIM(PD_MODEL_NM) || ' and PD_FINAL_RPTG_RTO= ' || TO_CHAR(PD_FINAL_RPTG_RTO) || CHR(13)|| 'Final RTO did not match the one in table BASEL_SEG_RPTG_PARM, variable' || CHR(13)|| 'FINAL_RTO=' || TO_CHAR(v_PD_LKP)  || CHR(13)|| 'Please resolve this before proceeding to next step') , 'PD PASSED') -- 2015-04-02: Change check condition --IIF(ISNULL(v_PD_LKP),  ABORT('Table BASEL_ANALYTCL_BL_INSTRMNT_FACT with variable PD_BASEL_SEG_ID= ' || PD_BASEL_SEG_ID|| 'and PD_MODEL_NM= ' || PD_MODEL_NM || 'and PD_FINAL_RPTG_RTO= ' || PD_FINAL_RPTG_RTO) ||' did not match with table BASEL_SEG_RPTG_PARM, variable FINAL_RTO=' ||v_PD_LKP||'Please resolve it before proceeding to next step' , 'PD PASSED')*/
	IF PD_FINAL_RPTG_RTO ne . AND (v_PD_LKP eq . OR round(v_PD_LKP,0.00000001) ne round(PD_FINAL_RPTG_RTO,0.00000001)) then
		do;
			v_PD_FINAL_OUTPUT= 'Table BASEL_ANALYTCL_BL_INSTRMNT_FACT with variables PD_BASEL_SEG_ID= ' || PD_BASEL_SEG_ID || ' and PD_MODEL_NM= ' || PD_MODEL_NM || ' and PD_FINAL_RPTG_RTO= ' || PD_FINAL_RPTG_RTO|| 'Final RTO did not match the one in table BASEL_SEG_RPTG_PARM, variable FINAL_RTO=' || v_PD_LKP|| 'Please resolve this before proceeding to next step';
			put v_PD_FINAL_OUTPUT;
			abort cancel;
		end;
	else
		do;
			v_PD_FINAL_OUTPUT= 'PD PASSED';
		end;

	/*v_LGD_FINAL_OUTPUT=IIF(NOT ISNULL(LGD_FINAL_RPTG_RTO) AND (ISNULL(v_LGD_LKP) OR v_LGD_LKP <> LGD_FINAL_RPTG_RTO),  ABORT('Table BASEL_ANALYTCL_BL_INSTRMNT_FACT with variables' || CHR(13)|| 'LGD_BASEL_SEG_ID= ' || TO_CHAR(LGD_BASEL_SEG_ID) || ' and LGD_MODEL_NM= ' || RTRIM(LGD_MODEL_NM) || ' and LGD_FINAL_RPTG_RTO= ' || TO_CHAR(LGD_FINAL_RPTG_RTO) || CHR(13)|| 'Final RTO did not match the one in table BASEL_SEG_RPTG_PARM, variable' || CHR(13)|| 'FINAL_RTO=' || TO_CHAR(v_LGD_LKP)  || CHR(13)|| 'Please resolve this before proceeding to next step') , 'LGD PASSED') -- 2015-04-02: Change check condition --IIF(ISNULL(v_LGD_LKP),  ABORT('Table BASEL_ANALYTCL_BL_INSTRMNT_FACT with variable LGD_BASEL_SEG_ID= ' || LGD_BASEL_SEG_ID|| 'and LGD_MODEL_NM= ' || LGD_MODEL_NM || 'and LGD_FINAL_RPTG_RTO= ' || LGD_FINAL_RPTG_RTO) ||' did not match with table BASEL_SEG_RPTG_PARM, variable FINAL_RTO=' ||v_LGD_LKP||'Please resolve it before proceeding to next step' , 'LGD PASSED')*/
	IF LGD_FINAL_RPTG_RTO ne . AND (v_LGD_LKP eq . OR round(v_LGD_LKP,0.00000001) ne round(LGD_FINAL_RPTG_RTO,0.00000001)) then
		do;
			v_LGD_FINAL_OUTPUT= 'Table BASEL_ANALYTCL_BL_INSTRMNT_FACT with variables LGD_BASEL_SEG_ID= ' || PD_BASEL_SEG_ID || ' and LGD_MODEL_NM= ' || PD_MODEL_NM || ' and LGD_FINAL_RPTG_RTO= ' || LGD_FINAL_RPTG_RTO|| 'Final RTO did not match the one in table BASEL_SEG_RPTG_PARM, variable FINAL_RTO=' || v_LGD_LKP|| 'Please resolve this before proceeding to next step';
			put v_LGD_FINAL_OUTPUT;
			abort cancel;
		end;
	else
		do;
			v_LGD_FINAL_OUTPUT='LGD PASSED';
		end;

	IF EAD_FINAL_RPTG_RTO ne .  AND (v_EAD_LKP eq . OR round(v_EAD_LKP,0.00000001) ne round(EAD_FINAL_RPTG_RTO,0.00000001)) then
		do;
			v_EAD_FINAL_OUTPUT= 'Table BASEL_ANALYTCL_BL_INSTRMNT_FACT with variables EAD_BASEL_SEG_ID= ' || PD_BASEL_SEG_ID || ' and EAD_MODEL_NM= ' || PD_MODEL_NM || ' and EAD_FINAL_RPTG_RTO= ' || EAD_FINAL_RPTG_RTO|| 'Final RTO did not match the one in table BASEL_SEG_RPTG_PARM, variable FINAL_RTO=' || v_EAD_LKP|| 'Please resolve this before proceeding to next step';
			put v_EAD_FINAL_OUTPUT;
			abort cancel;
		end;
	else
		do;
			v_EAD_FINAL_OUTPUT= 'EAD PASSED';
		end;

	o_FINAL_OUTPUT= v_PD_FINAL_OUTPUT || ',' || v_LGD_FINAL_OUTPUT || ',' || v_EAD_FINAL_OUTPUT;
run;

/************************** END ****************************/
