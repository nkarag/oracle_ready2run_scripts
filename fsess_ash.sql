set pagesize 999
set lines 999
col sample_time format a25
col username format a13
col program  format a10 trunc
col modules  format a10 trunc
col session_id format 9999
col session_serial# format 99999
col WAIT_CLASS format a10

break on sql_id
compute COUNT LABEL TotalSessions OF distinct sid on sql_id

accept dateval prompt "enter value for a point in time ('DD-MM-YYYY HH24:MI:SS'): "

select sample_time, c.username, SESSION_ID, SESSION_SERIAL#,INST_ID,PROGRAM,module, IS_AWR_SAMPLE, a.sql_id, IS_SQLID_CURRENT, 
    SESSION_STATE, decode(WAIT_TIME, 0, '0 - i.e., waiting', '>0 - i.e., on cpu'), WAIT_CLASS, EVENT, P1TEXT, P2TEXT, P3TEXT,  TIME_WAITED/100 time_waited_secs,
    BLOCKING_SESSION, BLOCKING_SESSION_SERIAL#, BLOCKING_INST_ID  
from gv$active_session_history a, 
 dba_users c
where 
a.user_id = c.user_id 
and c.username = nvl(upper('&username'), username)
and a.session_id = nvl('&sid', a.session_id)
and nvl(to_timestamp('&dateval','DD-MM-YYYY HH24:MI:SS'),sample_time) between a.sample_time - 2.5/(24*60) and a.sample_time + 2.5/(24*60)
order by sample_time
/
