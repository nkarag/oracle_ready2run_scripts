set pagesize 999
set lines 999

col username format a25
col inst_id format 9999999
col sid format 9999
col LOGON_TIME for a20
col prog format a30 trunc
col sql_text format a130 trunc
col prev_sql_text format a80 trunc
col event for a40
col wait_class format a20
col secs_in_wait for a20
col wait_state for a21
col sql_id format a20
col prev_sql_id format a20
col plan_hash_value format 99999999999999999
col prev_plan_hash_value format 99999999999999999
col sql_exec_start for a20
col PREV_EXEC_START for a20
col SQL_CHILD_NUMBER for 9999999999999
col PREV_CHILD_NUMBER for 9999999999999 
col ENTRY_PLSQL_PROC for a50
col blocking_instance format 9999999999999999
col blocking_session format 9999999999999999
col blocker	for a30
col KILL_BLOCKER_STMNT for a50
col osuser for a10
col osprocess for 99999999
col port for 999999
col terminal for a10
col machine for a20
col obj_owner for a20
col obj_name for a40
col obj_type for a10

col sw_event head EVENT for a40 truncate

col state format a10

col child for 99999
col avg_etime_secs for 999999,999.99
col wait_status for a21

break on sql_id
compute COUNT LABEL TotalSessions OF distinct sid on sql_id

col type for a10


alter session set NLS_DATE_FORMAT = 'dd-mm-yyyy HH24:mi:ss'
/


select        --*** session identifiers 
		username,
        a.inst_id,
        sid,
        serial#,
        logon_time,
        a.status, 	-- ACTIVE / INACTIVE:  If it’s ‘ACTIVE’ then that session is in the process of consuming database resources (running a SQL statement etc)
		round(a.LAST_CALL_ET/60,2) mins_act_inact, 	/*
													If the session STATUS is currently ACTIVE, then the value represents the elapsed time (in seconds) since the session has become active.
													If the session STATUS is currently INACTIVE, then the value represents the elapsed time (in seconds) since the session has become inactive.													*/
        program prog,
        --*** running sql
        a.sql_id,
        sql_child_number,
        sql_exec_start,
        b.plan_hash_value plan_hash_value,
        b.sql_text sql_text,
        (select owner||'.'||object_name||'.'||procedure_name from dba_procedures where object_id = a.plsql_entry_object_id and subprogram_id = a.PLSQL_ENTRY_SUBPROGRAM_ID)
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
       --*** object waiting for
        c.owner obj_owner,
        c.object_name obj_name,
        c.object_type obj_type,            
        --*** blocking session info
        blocking_instance,
        blocking_session,
        (select username from gv$session where inst_id = a.blocking_instance and sid = a.blocking_session) 
            blocker,
        (select 'exec kill_session('||sid||', '||serial#||', '||inst_id||')' from gv$session where inst_id = a.blocking_instance and sid = a.blocking_session)
            kill_blocker_stmnt,                                                         
        --*** previous sql
        prev_sql_id,
        prev_child_number,
        prev_exec_start,
        b2.plan_hash_value prev_plan_hash_value,
        b2.sql_text prev_sql_text,        
        --*** OS identifiers
        osuser,
        process osprocess,
        machine,
        port,
        terminal        
from gv$session a, gv$sql b, gv$sql b2, dba_objects c
where 
username = nvl(upper('&username'), username)
and sid = nvl('&sid', sid)
and 
(
a.sql_id  = b.sql_id (+)
and a.sql_child_number = b.child_number (+)
and a.inst_id = b.inst_id (+)
)
and
(
a.prev_sql_id  = b2.sql_id (+)
and a.prev_child_number = b2.child_number (+)
and a.inst_id = b2.inst_id (+)        
)          
and a.ROW_WAIT_OBJ# = c.OBJECT_ID (+)
-- and sql_text not like 'select username, sid, serial#, a.INST_ID, a.status, program prog, machine, address, hash_value, b.sql_id, child_number child,%' -- don't show this query
order by sql_id, sql_child_number
/

/*
select username, sid, serial#, a.INST_ID, a.status, 
WAIT_CLASS, EVENT, SECONDS_IN_WAIT, decode(WAIT_TIME,0,'Currently Waiting', WAIT_TIME) wait_status, --WAIT_TIME_MICRO/10e6 WAIT_TIME_SECS, 	
a.logon_time, program prog, machine, 
--address, hash_value, 
b.sql_id, child_number child, plan_hash_value, executions execs, 
(elapsed_time/decode(nvl(executions,0),0,1,executions))/1000000 avg_etime_secs, 
sql_text,
blocking_instance, blocking_session, c.owner, c.object_name, c.object_type
from gv$session a, gv$sql b, dba_objects c
where 
username = nvl(upper('&username'), username)
and sid = nvl('&sid', sid)
and a.sql_id  = b.sql_id (+)
and a.sql_child_number = b.child_number (+)
and a.inst_id = b.inst_id (+)
and a.ROW_WAIT_OBJ# = c.OBJECT_ID (+)
-- and sql_text not like 'select username, sid, serial#, a.INST_ID, a.status, program prog, machine, address, hash_value, b.sql_id, child_number child,%' -- don't show this query
order by sql_id, sql_child_number
*/