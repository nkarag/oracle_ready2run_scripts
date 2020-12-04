
select round(sofar/totalwork,2)*100 pct_completed, a.*
from gv$session_longops a
where
	a.sql_id = nvl('&sql_id', a.sql_id)
and	username = nvl(upper('&username'), username)
and sid = nvl('&sid', sid) 
and a.inst_id = nvl('&inst_id', a.inst_id)
/