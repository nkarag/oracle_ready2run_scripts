alter session set nls_date_format='dd-mm-yyyy hh24:mi:ss'
/

spool 20160609_mviews_for_tuning.csv replace

select
'FLOW_NAME'||'|'||
'NODE_PATH'||'|'||
'SQL_ID'||'|'||
'MVIEW_NAME'||'|'||
'DUR_SECS'
from dual
union all
select
FLOW_NAME||'|'||
NODE_PATH||'|'||
SQL_ID||'|'||
MVIEW_NAME||'|'||
DUR_SECS
from monitor_dw.owb_pa_mviews_tmp1;

spool off

