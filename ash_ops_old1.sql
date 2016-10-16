set pagesize 999
set lines 999

select SQL_PLAN_LINE_ID, SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type, round(ratio_to_report(count(*)) over() *100) PCNT, count(*) nosamples, cnttot nosamplestot
from (
	select SQL_PLAN_LINE_ID, A.SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type, count(*) over() cnttot
	from gv$active_session_history a left outer join dba_objects b on(a.CURRENT_OBJ# = b.object_id)
	where        
		SAMPLE_TIME > sysdate - (&minutes_from_now/(24*60))
		and sql_id = nvl('&sql_id',sql_id)
)            
group by SQL_PLAN_LINE_ID, SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type, cnttot
order by count(*) desc
/

--select SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type, round((count(*)/cnttot)*100) PCNT, count(*) nosamples, cnttot nosamplestot
--from (
--	select     A.SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type, count(*) over() cnttot
--	from gv$active_session_history a  join dba_objects b on(a.CURRENT_OBJ# = b.object_id)
--	where        
--		SAMPLE_TIME > sysdate - (&minutes_from_now/(24*60))
--		and sql_id = nvl('&sql_id',sql_id)
--)            
--group by SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type, cnttot
--order by count(*) desc
--/