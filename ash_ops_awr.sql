set pagesize 999
set lines 999

prompt  *****************
prompt Analysis of execution plan operations for the last N hours from a timepoint T for a specific sql_id from dba_hist_active_sess_history
prompt Enter a timepoint in 'DD-MM-YYYY HH24:MI:SS' format and how many hours before this you want to go (press enter fo all samples)
prompt  *****************
prompt

select SQL_PLAN_LINE_ID, SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type, round(ratio_to_report(count(*)) over() *100) PCNT, count(*) nosamples, cnttot nosamplestot
from (
	select SQL_PLAN_LINE_ID, A.SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type, count(*) over() cnttot
	from dba_hist_active_sess_history a  left join dba_objects b on(a.CURRENT_OBJ# = b.object_id)
	where        
		SAMPLE_TIME >= nvl(to_date('&timepoint', 'DD-MM-YYYY HH24:MI:SS'),SAMPLE_TIME)  - (nvl('&hours_from_timepoint',0)/24) 
		and sql_id = nvl('&sql_id',sql_id) 
		and SQL_CHILD_NUMBER = nvl('&SQL_CHILD_NUMBER',0)
)            
group by SQL_PLAN_LINE_ID, SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type, cnttot
order by count(*) desc
/