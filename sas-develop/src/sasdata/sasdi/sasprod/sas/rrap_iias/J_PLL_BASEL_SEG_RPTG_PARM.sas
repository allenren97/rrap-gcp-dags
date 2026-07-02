options errorabend;

***************************************************************************************************************************;
***************************************************************************************************************************;
*  Job Name : J_PLL_BASEL_SEG_RPTG_PARM.sas
*  Target Database: IIAS EDRTLRPLL
*  Target Table:  BASEL_SEG_RPTG_PARM  
*  
*  Purpose: Load the final IIAS Instrument Fact table to EDRTLRPLL
*
*  Frequency: Monthly
*
*  Notes:  Refresh BASEL_SEG_RPTG_PARM table before PRLL run to sync with latest changes in EDRTLRP1D's BASEL_SEG_RPTG_PARM table
*
*	Change Log: RRMSS-3540 - Kalind Patel - Job for parallel Run - Rework on Instrument fact - KS, MOR *
*	            RRMSS-3904 - Kalind Patel - [PARALLEL SOLUTION] Update BASEL_SEG_RPTG_PARM - Preserve 8007,8008 and 8031 models *
*
*  Generated on: Wednesday, February 27, 2025    EDT 
***************************************************************************************************************************;
%rrap_pll_ksmor_autoexec(RRAPENV=REVOLVING_CREDIT);

/*DELETE ALL BASEL_MODEL_IDs EXCEPT CC EAD, HELOC LGD-D, MOR LGD-D, LOC LGD-D*/
PROC SQL NOPRINT;
CONNECT USING NZUSER AS NZCON;
EXECUTE(
DELETE FROM &net_db..BASEL_SEG_RPTG_PARM bsrp WHERE bsrp.BASEL_MODEL_ID NOT IN (8023,8012,8002,8006,8007,8008,8031);
					) BY NZCON;
	DISCONNECT FROM NZCON;
QUIT;

/*INSERT BASEL_MODEL_IDs FROM EDRTLRP1D.BASEL_SEG_RPTG_PARM EXCEPT CC EAD, HELOC LGD-D, MOR LGD-D, LOC LGD-D*/

PROC SQL NOPRINT;
CONNECT USING NZUSER AS NZCON;
EXECUTE(
INSERT INTO &net_db..BASEL_SEG_RPTG_PARM SELECT * FROM &net_db_P1D..BASEL_SEG_RPTG_PARM bsrp  WHERE bsrp.BASEL_MODEL_ID NOT IN (8023,8012,8002,8006,8007,8008,8031) AND CRNT_F = 'Y'
					) BY NZCON;
	DISCONNECT FROM NZCON;
QUIT;
