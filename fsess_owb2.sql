/*
	Find sessions of username in ('PERIF','ETL_DW','OWF_MGR') (from GV$SESSION) and join to all_rt_audit_executions on gv$session.client_info 
	in order to connect sessions with OWB mappings								
*/
col sw_event  head EVENT for a40 truncate
col state format a10
col username format a13
col prog format a30 trunc
col sql_text format a130 trunc
col prev_sql_text format a130 trunc
col sid format 9999
col child for 99999
col avg_etime_secs for 999999,999.99
col wait_status for a21
col event format a30 trunc
col wait_class format a12
col blocking_instance format 9999999999999999
col blocking_session format 9999999999999999
col sql_id format a20
col prev_sql_id format a20
break on sql_id
compute COUNT LABEL TotalSessions OF distinct sid on sql_id

col owb_exec_audit_id format a10
col owb_execution_name_short format a30 trunc
col main_flow format a20 trunc
col	owb_type format a10 trunc
col owb_status format a10 trunc
col owb_result format a10 trunc

-- in this version we use v$session.client_info as the "driver" for getting the owb info and not the v$session username as in the previous version.
-- Only sessions that start from OWB are returned.E.g., if I log in as ETL_DW and run a query from TOAD, this will not be returned.
with owb_execs
as ( -- owb executions of interest
    select  execution_audit_id owb_audit_id,
            SUBSTR (execution_name, INSTR (execution_name, ':') + 1) owb_name,
            (select execution_name from owbsys.all_rt_audit_executions where execution_audit_id = t1.top_level_execution_audit_id) owb_flow,
            DECODE (task_type,'PLSQL', 'Mapping','PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', task_type) owb_type,
            execution_audit_status owb_status,
            return_result owb_result,
            created_on owb_created_on,
            updated_on owb_updated_on                         
    from owbsys.all_rt_audit_executions t1 
    where 1=1
        --AND EXECUTION_AUDIT_STATUS = 'BUSY' -- only running nodes
        AND SUBSTR (execution_name, INSTR (execution_name, ':') + 1) = nvl(upper('&&owb_node_name'), SUBSTR (execution_name, INSTR (execution_name, ':') + 1))
        AND created_on > sysdate - 1 -- for performance reasons!
),
owb_sessions
as( --  these are the DB sessions directly joined to all_rt_audit_executions 
    --  (note: these are not all relevant sessions, since there are also the generated ones from procedures)
    select t1.*, t2.owb_name --, t2.*
    from gv$session t1 join owb_execs t2 on (t1.client_info = t2.owb_audit_id)
    where 1=1
--          AND client_info in (select owb_audit_id from owb_execs)
        AND client_info IS NOT NULL 
        AND regexp_like (client_info,'^\d+$') -- only numeric client_info value, so that you avoid an ORA-01722:invalid number
    order by t2.owb_flow    
),
owb_sid_all
as ( -- get all relevant sessions, even the generated ones from a PL/SQL  procedure call
    select inst_id, sid, serial#  from owb_sessions
    union
    select inst_id, sid, serial# from gv$session -- get generated sessions (e.g. sessions invoked by a call to DBMS_MVIEW.refresh)
    where PLSQL_ENTRY_OBJECT_ID in (select plsql_entry_object_id from owb_sessions)
    union   --  also in the case of an MV refresh OWB node, 
            --  get also all sessions of the same user that have a PLSQL_ENTRY_OBJECT_ID = DBMS_SNAPSHOT.REFRESH
    select inst_id, sid, serial# 
    from gv$session
    where
        1 = case when (select count(*) from owb_sessions where owb_name like '%REFRESH%MV%') > 0 THEN 1 ELSE 0 END
        AND  PLSQL_ENTRY_OBJECT_ID in (select object_id from dba_procedures where object_name = 'DBMS_SNAPSHOT' and procedure_name = 'REFRESH')
),
owb_sessions_all
as(
    select *
    from gv$session
    where (inst_id, sid, serial#) in (select inst_id, sid, serial# from owb_sid_all)
),
final
as (
    -- outer join to get owb_execs columns
    -- outer join to get sql_text
    -- outer join to get object from dba_objects
    select t2.*, t1.*, t3.owner obj_owner, t3.object_name obj_name, t3.object_type obj_type, T4.PLAN_HASH_VALUE, t4.executions, t4.sql_text,T5.PLAN_HASH_VALUE prev_plan_hash_value, t5.executions prev_executions, t5.sql_text prev_sql_text
    from owb_sessions_all t1 
            left outer join owb_execs t2 on (t1.client_info = t2.owb_audit_id)
                left outer join dba_objects t3 on  (t1.ROW_WAIT_OBJ# = t3.object_id) 
                    left outer join gv$sql t4 on (t1.sql_id = T4.SQL_ID AND t1.sql_child_number = T4.CHILD_NUMBER AND t1.inst_id = t4.inst_id)
                        left outer join gv$sql t5 on (t1.prev_sql_id = T5.SQL_ID AND t1.prev_child_number = T5.CHILD_NUMBER AND t1.inst_id = t5.inst_id)
)     
select  --*** OWB stuff
        owb_flow,
        username,
        owb_name,
        owb_type,
        owb_audit_id,       
        owb_created_on,
        owb_updated_on,
        owb_status,
        owb_result,
        --*** session identifiers 
        inst_id,
        sid,
        serial#,
        logon_time,
        status,
        program,
        --*** running sql
        sql_id,
        sql_child_number,
        sql_exec_start,
        plan_hash_value,
        sql_text,
        (select owner||'.'||object_name||'.'||procedure_name from dba_procedures where object_id = f.plsql_entry_object_id and subprogram_id = f.PLSQL_ENTRY_SUBPROGRAM_ID)
            entry_plsql_proc,
        --*** wait info
        CASE WHEN state != 'WAITING' THEN 'WORKING'
             ELSE 'WAITING'
        END 
            wait_state,  
        CASE WHEN state != 'WAITING' THEN 'On CPU / runqueue'
             ELSE WAIT_CLASS
        END                          
           WAIT_CLASS,
        CASE WHEN state != 'WAITING' THEN 'On CPU / runqueue'
        ELSE event
        END 
            event,
        CASE WHEN state != 'WAITING'  THEN  '(last wait) '  
        else ''
        END || round(WAIT_TIME_MICRO/1e6,1)
            secs_in_wait,
--            WAIT_TIME_MICRO/1e6,
--            TIME_SINCE_LAST_WAIT_MICRO/1e6,
        --*** object waiting for
        obj_owner,
        obj_name,
        obj_type,            
        --*** blocking session info
        blocking_instance,
        blocking_session,
        (select username from gv$session where inst_id = f.blocking_instance and sid = f.blocking_session) 
            blocker,
        (select 'exec kill_session('||sid||', '||serial#||', '||inst_id||')' from gv$session where inst_id = f.blocking_instance and sid = f.blocking_session)
            kill_blocker_stmnt,                                                         
        --*** previous sql
        prev_sql_id,
        prev_child_number,
        prev_exec_start,
        prev_plan_hash_value,
        prev_sql_text,        
        --*** OS identifiers
        osuser,
        process osprocess,
        machine,
        port,
        terminal        
from final f
order by owb_flow, username, owb_name;

-- 2283, 225, 1

--alter session set NLS_DATE_FORMAT = 'dd-mm-yyyy HH24:mi:ss'
--/

  SELECT username, sid, serial#, a.INST_ID,
         -- include info from OWB executions
            a.client_info owb_exec_audit_id, 
            (select SUBSTR (execution_name, INSTR (execution_name, ':') + 1) from owbsys.all_rt_audit_executions where execution_audit_id = a.client_info) owb_execution_name_short,
            (select execution_name from owbsys.all_rt_audit_executions where execution_audit_id = (select top_level_execution_audit_id from owbsys.all_rt_audit_executions  where execution_audit_id = a.client_info)) main_flow,            
            (select DECODE (task_type,'PLSQL', 'Mapping','PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function') TYPE from owbsys.all_rt_audit_executions where execution_audit_id = a.client_info) owb_type, 
            (select execution_audit_status from owbsys.all_rt_audit_executions where execution_audit_id = a.client_info) owb_status, 
            (select return_result from owbsys.all_rt_audit_executions where execution_audit_id = a.client_info) owb_result, 
            (select created_on from owbsys.all_rt_audit_executions where execution_audit_id = a.client_info) owb_created_on, 
            (select updated_on from owbsys.all_rt_audit_executions where execution_audit_id = a.client_info)owb_updated_on,
            /*(select SYS_CONNECT_BY_PATH (SUBSTR (execution_name, INSTR (execution_name, ':') + 1),'/')
                from owbsys.all_rt_audit_executions 
                where execution_audit_id = a.client_info
                START WITH  PARENT_EXECUTION_AUDIT_ID IS NULL
                CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
            ) owb_path,*/  
         a.status, WAIT_CLASS, --EVENT,
		 CASE WHEN state != 'WAITING' THEN 'On CPU / runqueue'
					ELSE event
					END AS sw_event, 
		--EVENT, 
		SECONDS_IN_WAIT, decode(WAIT_TIME,0,'Currently Waiting', WAIT_TIME) wait_status,  --WAIT_TIME_MICRO/10e6 WAIT_TIME_SECS,     
		CASE WHEN state != 'WAITING' THEN 'WORKING'
			 ELSE 'WAITING'
		END AS state, 		 
		 SECONDS_IN_WAIT, DECODE (WAIT_TIME, 0, 'Currently Waiting', WAIT_TIME) wait_status, --WAIT_TIME_MICRO/10e6 WAIT_TIME_SECS,
         a.logon_time,program prog, machine,    --address, hash_value,
         a.sql_id, a.sql_child_number child, a.sql_hash_value,
         b.executions execs, (b.elapsed_time / DECODE (NVL (b.executions, 0), 0, 1, b.executions))/ 1000000 avg_etime_secs,
         b.sql_text,
         a.prev_sql_id,
         a.prev_child_number prev_child,
         a.prev_hash_value,
         b2.executions execs, (  b2.elapsed_time/ DECODE (NVL (b2.executions, 0), 0, 1, b2.executions)) / 1000000  avg_etime_secs,
         b2.sql_text prev_sql_text,
         blocking_instance,
         blocking_session,
         c.owner,
         c.object_name,
         c.object_type
    FROM gv$session a,
         gv$sql b,
         gv$sql b2,
         dba_objects c --owbsys.all_rt_audit_executions d         
   WHERE     username in ('PERIF', 'ETL_DW','OWF_MGR', 'MIS')          
         AND (    a.sql_id = b.sql_id(+)
              AND a.sql_child_number = b.child_number(+)
              AND a.inst_id = b.inst_id(+))
         AND (    a.prev_sql_id = b2.sql_id(+)
              AND a.prev_child_number = b2.child_number(+)
              AND a.inst_id = b2.inst_id(+))
         AND a.ROW_WAIT_OBJ# = c.OBJECT_ID(+) 
-- and sql_text not like 'select username, sid, serial#, a.INST_ID, a.status, program prog, machine, address, hash_value, b.sql_id, child_number child,%' -- don't show this query
ORDER BY username, sql_id, sql_child_number
/