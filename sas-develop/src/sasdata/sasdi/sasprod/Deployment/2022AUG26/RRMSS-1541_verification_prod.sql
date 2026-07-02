/* RRMSS-1541
VERIFICATION */

/* After UPDATE, below select should return 0 record */

select * from EDRTLRP1D.BASEL_REVLVNG_CR_MTH_SNAPSHOT where MTH_TM_ID =-19396;