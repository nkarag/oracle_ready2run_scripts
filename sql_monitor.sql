col sql_text for a60 wrap
set verify off
set pagesize 999
set lines 999
col username format a13
--col prog format a22
col sid format 9999
--col child_number format 99999 heading CHILD
--col ocategory format a10
--col avg_etime format 9,999,999.99
--col avg_pio format 9,999,999.99
--col avg_lio format 999,999,999
--col etime format 9,999,999.99

select sid, SESSION_SERIAL#, USERNAME, sql_id, sql_exec_id, SQL_EXEC_START, STATUS, 
PX_MAXDOP, PX_SERVERS_REQUESTED, PX_SERVERS_ALLOCATED, PX_IS_CROSS_INSTANCE, sql_text 
from gv$sql_monitor s
where sql_id = '&sql_id'
order by 1, 2, 3,4,5,6
/
--upper(sql_text) like upper(nvl('&sql_text',sql_text))
--and sql_text not like '%from v$sql_monitor s where upper(sql_text) like upper(nvl(%'
--and sql_id like nvl('&sql_id',sql_id)
--order by 1, 2, 3
--/
