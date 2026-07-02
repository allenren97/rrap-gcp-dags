describe table EDRTLRT.COLCTN_JU_EXTR;
describe table EDRTLRT.COLCTN_SCORING_DTL_MODEL_OUTPUT;

alter table EDRTLRT.COLCTN_JU_EXTR
        add column TR_SCORE1 INTEGER
        add column TR_SCORE2 INTEGER
        add column TR_SCORE3 INTEGER
        add column TR_DELTA1 INTEGER
        add column TR_DELTA2 INTEGER
        add column TR_BALANCE1 INTEGER
        add column TR_BALANCE2 INTEGER
;

alter table EDRTLRT.COLCTN_SCORING_DTL_MODEL_OUTPUT
        add column TR_SCORE1 INTEGER
        add column TR_SCORE2 INTEGER
        add column TR_SCORE3 INTEGER
        add column TR_DELTA1 INTEGER
        add column TR_DELTA2 INTEGER
        add column TR_BALANCE1 INTEGER
        add column TR_BALANCE2 INTEGER
;

reorg table EDRTLRT.COLCTN_JU_EXTR;
reorg table EDRTLRT.COLCTN_SCORING_DTL_MODEL_OUTPUT;

describe table EDRTLRT.COLCTN_JU_EXTR;
describe table EDRTLRT.COLCTN_SCORING_DTL_MODEL_OUTPUT;
