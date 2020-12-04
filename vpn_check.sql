select owner, trigger_name, status
from dba_triggers
where owner = 'DWADMIN'
and trigger_name = 'CHECK_VPN_ACCESS'
/