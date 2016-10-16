select  SID, PX_MAXDOP, PX_SERVERS_REQUESTED, PX_SERVERS_ALLOCATED
from gv$sql_monitor
where
sql_id = '&sql_id'
and px_maxdop is not null
/
