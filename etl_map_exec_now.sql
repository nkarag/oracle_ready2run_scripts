REM
REM    Find mappings executing NOW for a specific Major Flow    
REM

set linesize 999
col execution_name_short for a30
col type for a10
col execution_audit_status for a30
col root_execution_name for a30
col path a80 trunc

SELECT  execution_audit_id,
           SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
           DECODE (task_type,
                   'PLSQL', 'Mapping',
                   'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function')
              TYPE,
           execution_audit_status,
           created_on,
           updated_on,
           ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
           CONNECT_BY_ISLEAF "IsLeaf",
           LEVEL,
           SYS_CONNECT_BY_PATH (
              SUBSTR (execution_name, INSTR (execution_name, ':') + 1),
              '/')
              path,
           CONNECT_BY_ROOT execution_name root_execution_name,           
           CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
           CONNECT_BY_ROOT created_on root_created_on,
           CONNECT_BY_ROOT updated_on root_updated_on
FROM owbsys.all_rt_audit_executions a
WHERE   A.CREATED_ON > SYSDATE - (&days_back)
    AND a.execution_audit_status = 'BUSY'    --AND a.return_result = 'OK'
    AND CONNECT_BY_ISLEAF = 1
    AND task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
START WITH  a.PARENT_EXECUTION_AUDIT_ID IS NULL
            AND a.EXECUTION_NAME  = nvl(trim('&major_flow_name'), a.EXECUTION_NAME) 
            AND a.execution_audit_status <> 'COMPLETE'            --AND a.return_result = 'OK'
CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
/