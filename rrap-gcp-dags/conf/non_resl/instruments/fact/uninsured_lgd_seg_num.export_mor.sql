{% set upstream_asset = [
        "ingestion.MORT_MTH_SNAPSHOT", 
        "instruments.CCAR_BASEL_PRD_TP_NM", 
        "instruments.LGD_MODEL_NM", 
        "instruments.LGD_BASEL_SEG_NUM", 
        "reference.BASEL_MODEL", 
        "reference.BASEL_SEG_RPTG_PARM",
        "reference.BASEL_SEG" 


] %}
with base as(
select 
a.basel_acct_id,
CCAR_BASEL_PRD_TP_NM,
lgd_model_nm,
lgd_basel_seg_num,
lgd_model_id,
model
FROM
{{upstream_asset[0]}}  a 
left join (select basel_acct_id, CCAR_BASEL_PRD_TP_NM from {{upstream_asset[1]}}  
where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}') b 
on a.basel_acct_id=b.basel_acct_id

left join (select basel_acct_id, lgd_model_nm,lgd_model_id from {{upstream_asset[2]}}  a 
where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}') c 
on a.basel_acct_id = c.basel_acct_id

left join (select basel_acct_id, lgd_basel_seg_num, model from {{upstream_asset[3]}}  
where obsn_dt='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
and stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}') d
on a.basel_acct_id=d.basel_acct_id

where mth_tm_id={{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}
), get_uninsured_seg as (
select basel_acct_id,CCAR_BASEL_PRD_TP_NM,lgd_model_id,lgd_basel_seg_num,model, 
case when (trim(CCAR_BASEL_PRD_TP_NM) like 'GENW%' OR trim(CCAR_BASEL_PRD_TP_NM) LIKE 'GUAR%')
and ((lgd_model_id) = 'mor_lgdnd') and lgd_basel_seg_num IN (5,9) THEN 1
WHEN (trim(CCAR_BASEL_PRD_TP_NM) like 'GENW%' OR trim(CCAR_BASEL_PRD_TP_NM) LIKE 'GUAR%')
and ((lgd_model_id) = 'mor_lgdnd') and lgd_basel_seg_num IN (6,10) THEN 2
WHEN (trim(CCAR_BASEL_PRD_TP_NM) like 'GENW%' OR trim(CCAR_BASEL_PRD_TP_NM) LIKE 'GUAR%')
and ((lgd_model_id) = 'mor_lgdnd') and lgd_basel_seg_num IN (7,11) THEN 3
WHEN (trim(CCAR_BASEL_PRD_TP_NM) like 'GENW%' OR trim(CCAR_BASEL_PRD_TP_NM) LIKE 'GUAR%')
and ((lgd_model_id) = 'mor_lgdnd') and lgd_basel_seg_num IN (8,12) THEN 4
when (trim(CCAR_BASEL_PRD_TP_NM) like 'GENW%' OR trim(CCAR_BASEL_PRD_TP_NM) LIKE 'GUAR%')
and ((lgd_model_id) = 'mor_lgdd') and lgd_basel_seg_num IN (5,9) THEN 1 
when (trim(CCAR_BASEL_PRD_TP_NM) like 'GENW%' OR trim(CCAR_BASEL_PRD_TP_NM) LIKE 'GUAR%')
and ((lgd_model_id) = 'mor_lgdd') and lgd_basel_seg_num IN (6,10) THEN 2
when (trim(CCAR_BASEL_PRD_TP_NM) like 'GENW%' OR trim(CCAR_BASEL_PRD_TP_NM) LIKE 'GUAR%')
and ((lgd_model_id) = 'mor_lgdd') and lgd_basel_seg_num IN (7,11) THEN 3
when (trim(CCAR_BASEL_PRD_TP_NM) like 'GENW%' OR trim(CCAR_BASEL_PRD_TP_NM) LIKE 'GUAR%')
and ((lgd_model_id) = 'mor_lgdd') and lgd_basel_seg_num IN (8,12) THEN 4
else lgd_basel_seg_num
end as UNINSURED_LGD_SEG_NUM
from base as a 
left join
(
select a.model_nm , c.seg_num as SEGMENT_NO, b.* 
from    {{upstream_asset[4]}} a, 
        {{upstream_asset[5]}} b, 
        {{upstream_asset[6]}}  c
where a.basel_model_id=b.basel_model_id 
and b.EFF_TO_DT >= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
and b.EFF_FROM_DT <= '{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' 
and trim(a.src_sys_cd)='MOR' 
and b.basel_seg_id=c.basel_seg_id
and b.stream='{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}'
order by 1,2
)as c
on (upper(a.LGD_MODEL_ID) = upper(c.BASEL_MODEL_ID) and
a.lgd_basel_seg_num = c.segment_no)
),final as (
select 
basel_acct_id,
CASE 
when (trim(CCAR_BASEL_PRD_TP_NM) like 'GENW%' OR trim(CCAR_BASEL_PRD_TP_NM) LIKE 'GUAR%') then UNINSURED_LGD_SEG_NUM
else NULL 
end as UNINSURED_LGD_SEG_NUM
from get_uninsured_seg
)
select 
basel_acct_id, 
'{{ task_instance.xcom_pull(task_ids="handle_month_context", key="stream") }}' as stream,
'{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}' as obsn_dt, 
UNINSURED_LGD_SEG_NUM
from final
