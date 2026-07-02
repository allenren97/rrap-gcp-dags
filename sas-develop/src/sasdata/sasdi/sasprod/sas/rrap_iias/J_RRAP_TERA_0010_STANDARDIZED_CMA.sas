
***************************************************************************************************************************;
***************************************************************************************************************************;
*  Job Name : J_RRAP_TERA_0010_STANDARDIZED_CMA.sas
*  Target Database: IIAS EDRTLRP1D
*  Target Table:  STANDARDIZED_CMA  
*  
*  Purpose: Load STANDARDIZED_CMA.sas
*
*  Frequency: One time load. Going forward, edit database table directly.
*
*  Notes:  
*  		  
*
*	Change Log:
*
*   2022-11-04: Hadi Dimashkieh - Initial Development
*
***************************************************************************************************************************;
%rrap_dlgd_autoexec;


data standardized_cma;
    infile datalines4 DLM='|' missover dsd;
    INPUT STANDARDIZED_CMA : $CHAR50. SOURCE_CMA : $CHAR50.
         ;

format INSRT_PROCESS_TMSTMP UPDT_PROCESS_TMSTMP datetime25.; 
INSRT_PROCESS_TMSTMP = "&SYSDATE9.:&SYSTIME."dt; 
UPDT_PROCESS_TMSTMP	 = "&SYSDATE9.:&SYSTIME."dt;

DATALINES4;
N/A|N/A
11|11
6|6
Abbotsford - Mission|ABBOTSFORD_MISSION
Abbotsford - Mission|Abbotsford - Mission
Barrie|BARRIE
Barrie|Barrie
Belleville|BELLEVILLE
Belleville|Belleville
Brantford|BRANTFORD
Brantford|Brantford
Calgary|CALGARY
Calgary|Calgary
Edmonton|EDMONTON
Edmonton|Edmonton
Greater Sudbury / Grand Sudbury|GREATER_SUDBURY_GRAND_SUDBURY
Greater Sudbury / Grand Sudbury|Greater Sudbury / Grand Sudbury
Guelph|GUELPH
Guelph|Guelph
Halifax|HALIFAX
Halifax|Halifax
Hamilton|HAMILTON
Hamilton|Hamilton
Kelowna|KELOWNA
Kelowna|Kelowna
Kingston|KINGSTON
Kingston|Kingston
Kitchener - Cambridge - Waterloo|KITCHENER_CAMBRIDGE_WATERLOO
Kitchener - Cambridge - Waterloo|Kitchener - Cambridge - Waterloo
Kitchener - Cambridge - Waterloo|Kitchener
Lethbridge|LETHBRIDGE
Lethbridge|Lethbridge
London|LONDON
London|London
Moncton|MONCTON
Moncton|Moncton
Montreal|MONTREAL
Montreal|Montreal
Montreal|Montréal
Oshawa|OSHAWA
Oshawa|Oshawa
OTTAWA_GATINEAU|OTTAWA_GATINEAU
OTTAWA_GATINEAU|Ottawa - Gatineau
Peterborough|PETERBOROUGH
Peterborough|Peterborough
Quebec|QUEBEC
Quebec|Québec
Quebec|Quebec
Sherbrooke|SHERBROOKE
Sherbrooke|Sherbrooke
St. Catharines - Niagara|ST_CATHARINES_NIAGARA
St. Catharines - Niagara|St. Catharines - Niagara
St. Johns|ST_JOHNS
St. Johns|St. John's
Saint John|SAINT_JOHN
Saint John|Saint John
Thunder Bay|THUNDER_BAY
Thunder Bay|Thunder Bay
Toronto|TORONTO
Toronto|Toronto
Trois-Rivieres|TROIS_RIVIERES
Trois-Rivieres|Trois-Rivières
Vancouver|VANCOUVER
Vancouver|Vancouver
Victoria|VICTORIA
Victoria|Victoria
Windsor|WINDSOR
Windsor|Windsor
Winnipeg|WINNIPEG
Winnipeg|Winnipeg
;;;;
run;


proc sort data=standardized_cma nodupkey; by source_cma; run;

proc sql;
connect using nzrrap as nzcon;
execute(delete from &RRAP_DB..STANDARDIZED_CMA; commit;) by nzcon;
quit;


proc append base=nzrrap.STANDARDIZED_CMA(BULKLOAD=YES BL_METHOD=CLILOAD) data=standardized_cma force; run;






/*
DROP TABLE tngstd3d.standardized_cma IF EXISTS; COMMIT;

CREATE TABLE tngstd3d.standardized_cma (
		STANDARDIZED_CMA VARCHAR(50 OCTETS) NOT NULL,
		SOURCE_CMA VARCHAR(50 OCTETS) NOT NULL, 
		INSRT_PROCESS_TMSTMP TIMESTAMP,
		UPDT_PROCESS_TMSTMP TIMESTAMP
	)
	ORGANIZE BY COLUMN
	DATA CAPTURE NONE 
	DISTRIBUTE BY HASH (SOURCE_CMA); COMMIT;
