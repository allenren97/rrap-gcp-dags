UPDATE
      EDRTLRP1D.PD_BAND_DIM
SET
      CRNT_F = 'N',
      EFF_TO_YR_MTH = '202311',
      UPDT_PROCESS_TMSTMP = NOW()
WHERE
      NCR_EXPSR_CL_KEY_VAL = '0502'
      AND CRNT_F = 'Y'
      AND NCR_PD_BAND_KEY_VAL IN (1401, 1402)
      AND CMHC_F = 'Y';

INSERT INTO
      EDRTLRP1D.PD_BAND_DIM (
            NCR_PD_BAND_ID,
            NCR_PD_BAND_KEY_VAL,
            NCR_EXPSR_CL_KEY_VAL,
            PD_MIN_VAL,
            PD_MAX_VAL,
            PD_MIN_VAL_DESC,
            PD_MAX_VAL_DESC,
            PD_BAND_EXPSR_CL_DESC,
            NCR_PD_BAND_DESC,
            FRS_CD,
            PD_BAND,
            TRANSACTOR_F,
            CMHC_F,
            EFF_FROM_YR_MTH,
            EFF_TO_YR_MTH,
            CRNT_F,
            INSRT_PROCESS_TMSTMP,
            UPDT_PROCESS_TMSTMP
      )
VALUES
(
            17299,
            '1401',
            '0502',
            0.00050000,
            0.00050000,
            '0.0000%',
            '0.0500%',
            'Mortgage',
            'PD Band 1',
            '1',
            '1',
            NULL,
            'Y',
            '202312',
            '999912',
            'Y',
            NOW(),
            NOW()
      );

INSERT INTO
      EDRTLRP1D.PD_BAND_DIM (
            NCR_PD_BAND_ID,
            NCR_PD_BAND_KEY_VAL,
            NCR_EXPSR_CL_KEY_VAL,
            PD_MIN_VAL,
            PD_MAX_VAL,
            PD_MIN_VAL_DESC,
            PD_MAX_VAL_DESC,
            PD_BAND_EXPSR_CL_DESC,
            NCR_PD_BAND_DESC,
            FRS_CD,
            PD_BAND,
            TRANSACTOR_F,
            CMHC_F,
            EFF_FROM_YR_MTH,
            EFF_TO_YR_MTH,
            CRNT_F,
            INSRT_PROCESS_TMSTMP,
            UPDT_PROCESS_TMSTMP
      )
VALUES
(
            17302,
            '1402',
            '0502',
            0.00050001,
            0.00079631,
            '0.0500%',
            '0.0796%',
            'Mortgage',
            'PD Band 2',
            '2',
            '2',
            NULL,
            'Y',
            '202312',
            '999912',
            'Y',
            NOW(),
            NOW()
      );