
/*---------------------------------------------------------------------
 * NAME:       get_spl_product_id_fields
 *
 * PURPOSE:    Uses logic defined in the BSTM to populate product
 *             id related fields.  This logic is in a macro 
 *             since it is usd in both the Scorecard Vars and Get      
 *             Status processes.  Scorecard vars needs the information                                  
 *             to derive field subv_flag which is stored on Scorecard
 *             table BASEL_PSNL_LN_ACCT_SC_DRVD_VAR.  The macro is also 
 *             called in the Define Status job.  Product id related fields
 *             are used in the Define Status job and are also stored on
 *             its target table - BASEL_PSNL_LOAN_ACCT_DRVD_VARS_2.
 *              
 * USAGE:      %get_spl_product_id_fields(table=);
 *              
 * ASSUMPTIONS: The macro assumes that the following fields exist on the input             
 *              table with NULL values: prd, sub_product, prd_id, prd_tp, and
 *              product_id.  The macro also expects the following fields to be 
 *              populated: basel_acct_id, Num_Purpose_Code, num_scrty_cd, and
 *              step_pln_snapshot_id.  The macro also assumes the input table 
 *              is in the FRG database.
 *             
 * MODIFIED:
 *              24JUL2015 - JD -  Created
 *
 *
 * Copyright (c) 2015 by The Financial Risk Group, Cary, NC, USA.
 *---------------------------------------------------------------------*/


%macro get_spl_product_id_fields(table=);


/*-------------------------------------------------------------------------
* Product Id Steps
* Step 1: Set Product , Sub product, Product ID and Product Type to null.
* This is done before calling the macro
*--------------------------------------------------------------------------*/

/*-------------------------------------------------------
* Step 1-b: Update data types of Product related fields
*--------------------------------------------------------*/

proc sql noprint;
connect using nzuser as nzcon;
execute(
   alter table &FRG_DB..&table
  ALTER column prd set data type VARCHAR(60);
) by nzcon;
execute(
	alter table &FRG_DB..&table
   ALTER column sub_product set data type VARCHAR(60);
) by nzcon;
execute(
   alter table &FRG_DB..&table
   ALTER column prd_id set data type VARCHAR(60);
) by nzcon;
execute(
   alter table &FRG_DB..&table
   ALTER column prd_tp set data type VARCHAR(60);
) by nzcon;
execute(
   alter table &FRG_DB..&table
   ALTER column product_id set data type VARCHAR(60);
     ) by nzcon;
disconnect from nzcon;
 quit;


/*-------------------------------------------------------------------------------
* Step 2: 
* Look up on BASEL_PSNL_LN_SUBV_MST_SNAPSHT_NEW using  CRNT_BR_LOCTN_TRNST=CAB 
* AND  LOAN_NUM = LOAN_NO .
* If the loan and cab is present in the BASEL_PSNL_LN_SUBV_MST_SNAPSHT_NEW table THEN
* SET  <Product>  = 'Auto',
* <Sub_Product > = 'Rate Subvented', 
* <Product ID>='S10' 
* <Product Type> = 'Indirect'
*---------------------------------------------------------------------------------*/ 
proc sql noprint;
connect using nzuser as nzcon;
execute(
        update &FRG_DB..&table
        set prd='Auto',
            sub_product='Rate Subvented',
            prd_id='S10',
            prd_tp='INDIRECT'
        where basel_acct_id  in
	/* RRMSS-2757 Updated to new subvent table BASEL_PSNL_LN_SUBV_MST_SNAPSHT_NEW */
            (select distinct basel_acct_id from &RRAP_DB..BASEL_PSNL_LN_SUBV_MST_SNAPSHT_NEW);
          
       ) by nzcon;
 quit;



/*----------------------------------------------------------------------
* Step 3: If matching record not found in Step 2 THEN 
* Use purpose code and security code from BASEL_PSNL_LOAN_MTH_SNAPSHOT 
* to look up on PSNL_LOAN_SCRTY_CD_PRPS_CD_LKP and get product, 
* sub product, product id and Product type
*------------------------------------------------------------------------*/ 
proc sql noprint;
connect using nzuser as nzcon;
execute(
         update &FRG_DB..&table s
         set s.prd=p.prd,
             s.sub_product=p.sub_prd,
	     s.prd_id=p.prd_id,   
             s.prd_tp=p.tp
         from &RRAP_DB..PSNL_LOAN_SCRTY_CD_PRPS_CD_LKP p
         where s.prd_id is null and
               s.Num_Purpose_Code=p.prps_cd and 
	       s.num_scrty_cd=p.scrty_cd;
          
        ) by nzcon;
 quit;


/*-------------------------------------------------------------------------------
* Step 4: If matching record not found in Step 2 and Step 3 THEN
* Use security code from BASEL_PSNL_LOAN_MTH_SNAPSHOT to look up on 
* PSNL_LOAN_SCRTY_CD_LKP to get product, Sub product, Product id and Product type
*--------------------------------------------------------------------------------*/
proc sql noprint;
connect using nzuser as nzcon;
execute(
         update &FRG_DB..&table s
         set s.prd=p.prd,
             s.sub_product=p.sub_prd,
	     s.prd_id=p.prd_id,   
             s.prd_tp=p.tp
         from &RRAP_DB..PSNL_LOAN_SCRTY_CD_LKP p
         where s.prd_id is null and
	       s.num_scrty_cd=p.scrty_cd;
          
        ) by nzcon;
 quit;


/*------------------------------------------------
* Step 5: If product id is null then set to -1
*-------------------------------------------------*/
proc sql noprint;
connect using nzuser as nzcon;
execute(
         update &FRG_DB..&table
         set prd_id='-1'
         where prd_id is null;
	         
       ) by nzcon;
 quit;



/*-----------------------------------------------
* Step 6: If product id is null then set to S08
* 
* After all this logic is performed, you can have an update statement
* Where product type=Direct and if the basel_acct_id has 
* BASEL_PSNL_LOAN_MTH_SNAPSHOT.STEP_PLN_SNAPSHOT_ID <> -1 THEN
* SET PRODUCT ID TO S08
* Product= SPL under STEP
* Sub_Product  =SPL under STEP
*------------------------------------------------*/
proc sql noprint;
connect using nzuser as nzcon;
execute(
        update &FRG_DB..&table
        set prd='SPL under STEP',
            sub_product='SPL under STEP',
            prd_id='S08'
        
        where upper(prd_tp)='DIRECT' and step_pln_snapshot_id <> -1;
          
       ) by nzcon;
 quit;


%mend;


