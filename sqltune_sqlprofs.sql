set pagesize 999
set lines 999
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
col last_modified format a20
col rat_type for a20
col description format a150 trunc
col FORCE_MATCHING for a15
col signature for 99999999999999999999
col sql_text format a70 trunc

alter session set NLS_DATE_FORMAT = 'dd-mm-yyyy HH24:mi:ss';

select A.NAME SQLPROF_NAME, b.attr1 sql_id, A.CATEGORY, A.STATUS, to_char(a.CREATED, 'dd-mm-yyyy HH24:mi:ss') created, 
	 to_char(a.LAST_MODIFIED,'dd-mm-yyyy HH24:mi:ss') last_modified,  A.DESCRIPTION, 
     b.attr3 parsing_schema, A.SQL_TEXT, A.SIGNATURE,
    A.TYPE, A.FORCE_MATCHING, A.TASK_ID, B.TASK_NAME, B.TYPE task_type, A.TASK_EXEC_NAME, A.TASK_OBJ_ID, A.TASK_REC_ID
from dba_sql_profiles a left outer join DBA_ADVISOR_OBJECTS b 
        on (a.task_id = b.task_id and  A.TASK_EXEC_NAME = nvl(B.EXECUTION_NAME,A.TASK_EXEC_NAME) 
                and A.TASK_OBJ_ID = B.OBJECT_ID)  
order by a.created desc                
/
