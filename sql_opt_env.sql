column isdefault format a10

select NAME, ISDEFAULT, VALUE
from gv$sql_optimizer_env
where sql_id = '&sql_id'
order by name
/