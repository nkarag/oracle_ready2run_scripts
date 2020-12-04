col sql_trace format a10
col sql_trace_waits format a16
col sql_trace_binds format a16

select sql_trace, sql_trace_waits, sql_trace_binds
from gv$session
where username = USER and (sid) = (select sid from v$mystat where rownum = 1)
/