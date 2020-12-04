column SQL_FEATURE format a20
column ISDEFAULT format a20

select inst_id, sid, id, NAME, SQL_FEATURE, ISDEFAULT, VALUE
from gv$ses_optimizer_env
where sid = '&sid' and inst_id = '&inst_id'
order by name
/