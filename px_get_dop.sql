set pagesize 999
set lines 999
col username format a13
col prog format a30 trunc
col sql_text format a100 trunc
col sid format 9999
col child for 99999
col avg_etime for 999,999.99
break on sql_id
compute COUNT LABEL TotalSessions OF distinct sid on sql_id

alter session set NLS_DATE_FORMAT = 'dd-mm-yyyy HH24:mi:ss'
/

select inst_id, sid, serial#, t2.username, t2.sql_id, t2.sql_child_number, t1.DEGREE, t1.REQ_DEGREE, 
        t2.status, t2.logon_time, t2.program prog, t2.machine  
from gv$PX_SESSION t1 join gv$session t2 using (inst_id, sid,serial#)
where
username = nvl(upper('&username'), username)
and sid = nvl('&sid', sid)
order by t2.sql_id
/