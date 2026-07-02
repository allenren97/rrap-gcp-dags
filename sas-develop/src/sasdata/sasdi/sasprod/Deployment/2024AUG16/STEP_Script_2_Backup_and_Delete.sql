/***************************************************************************
Deployment Date: AUG 16, 2024
CHANGE TICKET: CHG0900152
JIRA: RRMSS-3055 (RRMSS-3000, 2996, 2998, 2999)
CHANGE: BACKUPS IN IIAS PROD
NOTE: RUN THIS BEFORE STEP_Script_3_DataPatch.xlsx
/****************************************************************************/


/************************** STEP 1 *****************************************
Change: Create table backups in IIAS PROD
****************************************************************************/

1. EDRTLRP1D.DT4_RPTG_DRVD_VARS
2. EDRTLRP1D.DT4_RT20_FINAL_RPTG_VARS
3. EDRTLRP1D.DT4_RT30_FINAL_RPTG_VARS
4. EDRTLRP1D.DT4_RT40_FINAL_RPTG_VARS
 


/************************* STEP 2 ***********************************************
Change: Delete data in IIAS PROD - DO NOT RUN BEFORE STEP 1 COMPLETION
*********************************************************************************/

DELETE FROM EDRTLRP1D.DT4_RPTG_DRVD_VARS WHERE SRC_SYS_CD != 'TNG-MOR' AND MTH_TM_ID BETWEEN 20036 AND 20316;
DELETE FROM EDRTLRP1D.DT4_RT20_FINAL_RPTG_VARS WHERE MODEL_NM NOT LIKE 'TNG-MOR%' AND PROCESS_MTH_TM_ID IN (20116,20236);
DELETE FROM EDRTLRP1D.DT4_RT30_FINAL_RPTG_VARS WHERE MODEL_NM NOT LIKE 'TNG-MOR%' AND PROCESS_MTH_TM_ID IN (20116,20236);
DELETE FROM EDRTLRP1D.DT4_RT40_FINAL_RPTG_VARS WHERE MODEL_NM NOT LIKE 'TNG-MOR%' AND PROCESS_MTH_TM_ID IN (20116,20236);