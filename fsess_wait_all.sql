set pagesize 999
set lines 999
col event format a30 trunc
col p1text format a30 trunc
col p2text format a30 trunc
col p3text format a30 trunc
col sql_text format a50 trunc
col kill_session_sql format a50 trunc

alter session set NLS_DATE_FORMAT = 'dd-mm-yyyy HH24:mi:ss'
/

prompt Find where all session are waiting right now - from gv$session_wait.(I have excluded idle class events)

SELECT 
a.SID, b.serial#, b.username, b.status, p.spid, b.logon_time, a.WAIT_CLASS, a.event, l.NAME latch_name, a.SECONDS_IN_WAIT, 
b.sql_id, b.osuser,  b.module, b.action, b.program, 
a.p1,a.p1raw,  a.p2, a.p3,   --, b.row_wait_obj#, b.row_wait_file#, b.row_wait_block#, b.row_wait_row#,
q.sql_text, 
'alter system kill session ' || '''' || a.SID || ', '|| b.serial# || '''' || ' immediate;' kill_session_sql 
FROM gv$session_wait a, gv$session b, gv$latchname l, gv$process p, gv$sql q 
WHERE 
b.inst_id = nvl('&inst_id', b.inst_id)
and  b.sid = nvl('&sid', b.sid)
and b.serial# = nvl('&serial', b.serial#)
AND a.SID = b.SID  and A.INST_ID = B.INST_ID
AND b.username IS NOT NULL 
AND b.TYPE <> 'BACKGROUND' 
AND a.event NOT IN (SELECT NAME FROM v$event_name WHERE wait_class = 'Idle') 
AND (l.latch#(+) = a.p2) and L.INST_ID(+) = A.INST_ID
AND b.paddr = p.addr and B.INST_ID = P.INST_ID
AND B.SQL_ID = Q.SQL_ID AND B.SQL_CHILD_NUMBER = Q.CHILD_NUMBER AND B.INST_ID = Q.INST_ID
--AND a.sid = 559 
--AND module IN ('JDBC Thin Client') 
--AND p.spid = 13317
--AND b.sql_hash_value = '4119097924'
--AND event like 'library cache pin%' 
--AND b.osuser = 'oracle' 
--AND b.username = 'APPS' 
ORDER BY a.SECONDS_IN_WAIT DESC
/
