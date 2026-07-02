/*****************************************************************************
| Hard Code Dates                                                            |
|                                                                            |
|                                                                            |
|                                                                            |
| Usage:                                                                     |
| %HARD_CODE_DATES(RUN_START_DATE=30JUN2015,RUN_END_DATE=30JUN2015);         |
|****************************************************************************/


%MACRO HARD_CODE_DATES(RUN_START_DATE=,RUN_END_DATE=);
	PROC SQL;
		UPDATE CONTROL.PARAMETERS
			SET VALUE = "&RUN_START_DATE"
			WHERE NAME IN (
					'mor_model_start_period_dt','mor_scrd_start_period_dt', 
					'spl_model_start_period_dt','spl_scrd_start_period_dt'
							);
		UPDATE CONTROL.PARAMETERS
			SET VALUE = "&RUN_END_DATE"
			WHERE NAME IN (
					'mor_model_end_period_dt','mor_scrd_end_period_dt', 
					'spl_model_end_period_dt','spl_scrd_end_period_dt'
							);
	QUIT;

%MEND HARD_CODE_DATES;
