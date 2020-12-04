
/*
    Returns workflow node on error
*/ 

with running_flows
as (
    SELECT * FROM owf_mgr.WF_ITEMS_V WHERE END_DATE is null
),
activities_on_error
as (
    SELECT * 
    FROM owf_mgr.wf_item_activity_statuses_v join running_flows using(item_type, item_key)  
    WHERE 1=1
     --AND item_type = ...
    --AND item_key = ...
    AND activity_result_code = 'FAILURE' 
    AND source = 'R'
)    
select root_activity, item_type, item_key, activity_id, activity_label, activity_type_display_name  
from activities_on_error;

