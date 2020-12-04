alter session set nls_date_format='dd-mm-yyyy hh24:mi:ss'
/

spool 20181002_Target_Nodes.csv replace

select
'FLOW_NAME'||'|'||
'NODE_LEVEL'||'|'||
'ROOT_NODE'||'|'||
'NODE_NAME'||'|'||
'TYPE'||'|'||
'P80_DURATION_MINS'||'|'||
'NODE_PATH'
from dual
union all
select
FLOW_NAME||'|'||
NODE_LEVEL||'|'||
ROOT_NODE||'|'||
NODE_NAME||'|'||
TYPE||'|'||
P80_DURATION_MINS||'|'||
NODE_PATH
from monitor_dw.owb_etlpa2_tmp5;

spool off

