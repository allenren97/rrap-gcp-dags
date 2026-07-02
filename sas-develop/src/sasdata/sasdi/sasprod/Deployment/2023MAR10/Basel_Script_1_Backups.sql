/***************************************************************************
Deployment Date: MARCH 10  2023, 7:00 AM to 11:00 PM
CHANGE TICKET: CHG0648057
CHANGE: BACKUPS IN IIAS PROD
NOTE: RUN THIS BEFORE Basel_Script_2_DataPatch.xlsx
/****************************************************************************/

/************************** STEP 1 *****************************************
Change: Create table backups in IIAS PROD
****************************************************************************/

1. EDRTLRP1D.BASEL_REVLVNG_CR_BASE_DRVD_VARS
2. EDRTLRP1D.BASEL_REVLVNG_CR_ACCT_DRVD_VARS
3. EDRTLRP1D.REVLVNG_CR_OBSVTN_PT_DRVD_VAR
4. EDRTLRP1D.PD_SEG_ACCT_XREF
5. EDRTLRP1D.EAD_SEG_ACCT_XREF
6. EDRTLRP1D.LGD_SEG_ACCT_XREF
7. EDRTLRP1D.BASEL_MODEL_REL
8. EDRTLRP1D.BASEL_MODEL_SCORECRD_HDR
9. EDRTLRP1D.BASEL_MODEL_SCORECRD_DTL 	


/************************* STEP 2 ***********************************************
Change: Delete data in IIAS PROD - DO NOT RUN BEFORE STEP 1 COMPLETION
*********************************************************************************/

DELETE FROM EDRTLRP1D.BASEL_REVLVNG_CR_BASE_DRVD_VARS WHERE MTH_TM_ID >= 15796;
DELETE FROM EDRTLRP1D.BASEL_REVLVNG_CR_ACCT_DRVD_VARS WHERE MTH_TM_ID >= 17716;
DELETE FROM EDRTLRP1D.REVLVNG_CR_OBSVTN_PT_DRVD_VAR WHERE PROCESS_MTH_TM_ID >= 17716;
DELETE FROM EDRTLRP1D.PD_SEG_ACCT_XREF WHERE MTH_TM_ID >=19156 and basel_model_id in (8007,8011);
DELETE FROM EDRTLRP1D.EAD_SEG_ACCT_XREF WHERE MTH_TM_ID >=19156 and basel_model_id in (8004,8012,8008);
DELETE FROM EDRTLRP1D.LGD_SEG_ACCT_XREF WHERE MTH_TM_ID >= 18196 and basel_model_id in (8009,8010);
TRUNCATE TABLE EDRTLRP1D.BASEL_MODEL_REL;
TRUNCATE TABLE EDRTLRP1D.BASEL_MODEL_SCORECRD_HDR ;
TRUNCATE TABLE EDRTLRP1D.BASEL_MODEL_SCORECRD_DTL ;


/************************* STEP 3 ***********************************************
Change: Add columns in IIAS PROD - DO NOT RUN BEFORE STEP 2 COMPLETION
*********************************************************************************/

ALTER TABLE EDRTLRP1D.BASEL_REVLVNG_CR_BASE_DRVD_VARS 
ADD COLUMN PIT_STAT_VER_2_CD90  char(3)
ADD COLUMN PIT_STAT_VER_2_CD180 char(3);
/* reorg table after alter statement*/

/************************* STEP 4 ***********************************************
CHANGE: DB2 PROD DM1P1D - This step has dependency only on Basel_Script_8_DB2_Changes
which will be implemented on Mar 11.
As backup takes longer, this step has been placed here. 
**********************************************************************************/
Take backup of table: EDRRAPT.BASEL_ANALYTCL_BL_INSTRMNT_FACT

/*********************** END OF SCRIPT *****************************************/