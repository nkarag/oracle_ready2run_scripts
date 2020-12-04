------------------------------------------------------------------------------------------------
-- sqltune_rec_sqlprofs.sql
--
-- Description
--	Get recommendations for SQL Profiles for a specific SQL_ID or all.
--
-- Note
--	If the SQL Profile is accepted then the correpsonding columns from DBA_PROFILE will not be NULL
--
-- Parameters
--	1.	sql_id	The input sql id.
--
--	Author
--		Nikos Karagiannidis (C) 2014 - http://oradwstories.blogspot.gr/
------------------------------------------------------------------------------------------------

col object_id format 999
col obj_type format a8
col original_plan_hash_value format a25
col plan_hv_using_sql_prof format 999999999999999999999999
col schema_name format a20
col object_owner format a20
col sql_text format a130 trunc
col sql_id format a20
col prof_description format a100 trunc
col benefit_pct format 99D99
col command format a20
col rec_type for a12
col sql_prof_status for a20
col force_matching for a20


select  o.attr3 schema_name, o.attr1 sql_id, 
		t.created task_created, t.last_modified task_last_modified,
        r.type rec_type, 
		r.benefit/100 benefit_pct,
		o.attr2 original_plan_hash_value,
		xp.plan_hash_value plan_hv_using_sql_prof,
		t.owner task_owner, t.task_name, t.status task_status, t.last_execution execution_name, a.command,
		o.owner object_owner, o.object_id, o.type obj_type,         
        --f.*,        
        p.name sql_profile, p.status sql_prof_status, p.force_matching,  p.created sql_prof_created, p.last_modified sql_prof_last_modified,p.description prof_description,
		o.attr4 sql_text		
from DBA_ADVISOR_OBJECTS o
         join DBA_ADVISOR_TASKS t on (o.task_id = t.task_id and nvl(o.execution_name, t.last_execution) = t.LAST_EXECUTION )
             join DBA_ADVISOR_RECOMMENDATIONS r on (t.owner = r.owner and t.task_id = r.task_id and nvl(r.execution_name, t.last_execution) = t.LAST_EXECUTION )
                join DBA_ADVISOR_ACTIONS a on (a.owner = t.owner and a.task_id = t.task_id and nvl(a.execution_name, t.last_execution) = t.LAST_EXECUTION and r.REC_ID = a.REC_ID)
                    left outer join DBA_ADVISOR_SQLPLANS xp on (xp.task_id = t.task_id and nvl(xp.execution_name, t.last_execution) = t.LAST_EXECUTION and xp.object_id = o.object_id and xp.sql_id = o.attr1)                 
                    --join DBA_ADVISOR_FINDINGS f on (f.OWNER = t.owner and f.task_id = t.task_id and nvl(f.execution_name, t.last_execution) = t.LAST_EXECUTION and f.object_id = o.object_id and f.finding_id = r.finding_id)
                       left outer join DBA_SQL_PROFILES p on (p.TASK_ID = t.task_id and nvl(p.TASK_EXEC_NAME, t.last_execution) = t.LAST_EXECUTION and p.TASK_OBJ_ID = o.OBJECT_ID and p.TASK_REC_ID = r.rec_id)
where
    -- objects
    o.type in ('SQL', 'SQLSET')
    AND o.task_name <> 'SYS_AUTO_SQL_TUNING_TASK'
    AND o.task_name not like 'ADDM%'
    AND o.attr1 = nvl('&sql_id', o.attr1)
    -- tasks
    AND t.advisor_name = 'SQL Tuning Advisor'
    -- actions
    AND COMMAND_ID in (32,44) --ACCEPT SQL PROFILE, CREATE SQL PLAN BASELINE
    -- sqlplans
    AND xp.attribute = 'Using SQL profile'
    AND xp.id = 0 -- only the fisrt operation is enough in order to get the plan hash value
order by t.last_modified  desc	
/	