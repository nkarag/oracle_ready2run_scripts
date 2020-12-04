set pagesize 999
set lines 999
col sw_event head EVENT for a40 truncate
col event for a40
col wait_state for a21
col secs_in_wait for 999G999D999
col state format a10
col username format a30
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
col osuser for a10
col process for 99999999
col port for 999999
col terminal for a10
col type for a10


alter session set NLS_DATE_FORMAT = 'dd-mm-yyyy HH24:mi:ss'
/


select username, sid, serial#, a.INST_ID, a.status, 
/*
WAIT_CLASS, 
CASE WHEN state != 'WAITING' THEN 'On CPU / runqueue'
			ELSE event
			END AS sw_event, 
--EVENT, 
	SECONDS_IN_WAIT, decode(WAIT_TIME,0,'Currently Waiting', WAIT_TIME) wait_status,  --WAIT_TIME_MICRO/10e6 WAIT_TIME_SECS,     
    CASE WHEN state != 'WAITING' THEN 'WORKING'
         ELSE 'WAITING'
    END AS state, 
*/
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
a.logon_time, program prog, machine, 
--address, hash_value, 
a.sql_id, a.sql_child_number child, a.sql_hash_value, b.executions execs, 
(b.elapsed_time/decode(nvl(b.executions,0),0,1,b.executions))/1000000 avg_etime_secs, 
b.sql_text, 
a.prev_sql_id, a.prev_child_number prev_child, a.prev_hash_value, b2.executions execs, 
(b2.elapsed_time/decode(nvl(b2.executions,0),0,1,b2.executions))/1000000 avg_etime_secs, 
b2.sql_text prev_sql_text,
blocking_instance, blocking_session, c.owner, c.object_name, c.object_type,
osuser, process, port, terminal, type
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