set pagesize 999
set lines 999
col task_id for 99999
col task_name for a30
col execution_name for a20
col object_id for 9999999
col sql_id for a17
col message for a100 word_wrapped
col parsing_schema format a15
col BENEFIT_TYPE for a15
col benefit for 9999
col sqlprof_name for a30
col created for a20
col last_modified a20
col rat_type for a20
col description a40 trunc
col FORCE_MATCHING for a15
col sql_text format a70 trunc
col signature for 99999999999999999999

alter session set NLS_DATE_FORMAT = 'dd-mm-yyyy HH24:mi:ss';

select a.task_id, a.task_name, a.execution_name, A.OBJECT_ID,  B.EXECUTION_END, a.attr1 sql_id, a.attr3 parsing_schema,
     C.REC_ID,  C.BENEFIT_TYPE, c.benefit, c.TYPE rec_type, 
     E.RATIONALE_ID, E.MESSAGE, e.type rat_type,
     D.NAME SQLPROF_NAME, d.status, to_char(D.CREATED, 'dd-mm-yyyy HH24:mi:ss') created, 
	 to_char(D.LAST_MODIFIED,'dd-mm-yyyy HH24:mi:ss') last_modified, 
	 D.DESCRIPTION, D.FORCE_MATCHING, D.SIGNATURE--, d.sql_text 
from DBA_ADVISOR_OBJECTS a left outer join dba_advisor_tasks b on(a.task_id = b.task_id and a.execution_name = b.last_execution)
     join DBA_ADVISOR_RECOMMENDATIONS c on(a.task_id = c.task_id and a.execution_name = C.EXECUTION_NAME )
     join dba_advisor_rationale e on (e.task_id = a.task_id and E.EXECUTION_NAME = A.EXECUTION_NAME and E.OBJECT_ID = A.OBJECT_ID and E.REC_ID = C.REC_ID)
     left outer join dba_sql_profiles d 
        on( a.task_id = d.task_id and a.execution_name = d.task_exec_name 
            and C.REC_ID = D.TASK_REC_ID 
            and A.OBJECT_ID = D.TASK_OBJ_ID )
where 
a.task_name = 'SYS_AUTO_SQL_TUNING_TASK' and a.type = 'SQL'
and a.attr1 = nvl('&sql_id',a.attr1) and a.attr3 = upper(nvl('&parsing_schema', a.attr3))
order by a.task_id, a.task_name, a.execution_name desc, A.OBJECT_ID, C.REC_ID, E.RATIONALE_ID
/