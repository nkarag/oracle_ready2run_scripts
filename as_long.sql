set pagesize 999
set lines 999
col username format a13
col prog format a30 trunc
col sql_text format a41 trunc
col sid format 9999
col child for 99999
col avg_etime for 999,999.99
col logon_time format a21
col event format a30 trunc
col p1text format a30 trunc
col p2text format a30 trunc
col p3text format a30 trunc
col wait_class format a10

alter session set NLS_DATE_FORMAT = 'dd-mm-yyyy HH24:mi:ss'
/

select  round((sysdate -SQL_EXEC_START)*24,2) hours_running, username, sid, SERIAL#,  a.INST_ID, a.LOGON_TIME, status, state, WAIT_CLASS, EVENT, WAIT_TIME_MICRO/1000000 WAIT_TIME_SECS, P1TEXT, P2TEXT, P3TEXT, program prog, address, hash_value, b.sql_id, child_number child, plan_hash_value, executions execs, 
(elapsed_time/decode(nvl(executions,0),0,1,executions))/1000000 avg_etime_secs, sql_text
from gv$session a, gv$sql b
where 
status = 'ACTIVE' and
(sysdate -SQL_EXEC_START)*24 > &hours
and username is not null
and a.sql_id = b.sql_id
and a.sql_child_number = b.child_number
and sql_text not like 'select username, sid, substr(program,1,19) prog, address, hash_value, b.sql_id, child_number child,%' -- don't show this query
order by (sysdate -SQL_EXEC_START)*24 desc, sql_id, sql_child_number
/
