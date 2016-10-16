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

select t2.inst_id, t2.sid, t2.serial#, t2.username, t2.sql_id, t2.sql_child_number, t1.DEGREE, t1.REQ_DEGREE, 
        t2.status, t2.logon_time, t2.program prog, t2.machine, sql_text
from gv$PX_SESSION t1 join gv$session t2 on (t1.inst_id = t2.inst_id and t1.sid = t2.sid  and t1.serial# = t2.serial#)
		left outer join gv$sql t3 on (t2.inst_id = t3.inst_id and t2.sql_id = t3.sql_id and t2.sql_child_number = t3.child_number)
where
username = nvl(upper('&username'), username)
and t2.sid = nvl('&sid', t2.sid)
order by t2.sql_id, t2.sql_child_number
/