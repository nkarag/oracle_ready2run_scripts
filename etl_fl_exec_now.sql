REM
REM    Find all major flows executing NOW
REM

set linesize 999
col execution_name for a30
col execution_audit_status for a30

select execution_name, execution_audit_id, execution_audit_status, 
		created_on,
        updated_on,
        ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins
from owbsys.all_rt_audit_executions a
where A.PARENT_EXECUTION_AUDIT_ID is NULL
AND A.EXECUTION_AUDIT_STATUS = 'BUSY'
order by created_on
/