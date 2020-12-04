select inst_id, SID, sql_id, PX_MAXDOP, PX_SERVERS_REQUESTED, PX_SERVERS_ALLOCATED
from gv$sql_monitor
where
sql_id = nvl('&sql_id',sql_id)
and px_maxdop is not null
order by px_maxdop desc, sql_id
/
