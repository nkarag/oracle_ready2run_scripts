set pagesize 999
set lines 300
col username format a13
col prog format a10 trunc
col sql_text format a100 trunc
col session_id format 9999
col avg_etime for 999,999.99
break on sql_id
compute COUNT LABEL TotalSessions OF distinct SESSION_ID on sql_id


accept dateval prompt "enter value for a point in time ('DD-MM-YYYY HH24:MI:SS'): "

select sample_time ,SESSION_ID ,SESSION_SERIAL# ,c.username, a.sql_id, executions_total execs_total, 
    (elapsed_time_total/decode(nvl(executions_total,0),0,1,executions_total))/1000000 avg_etime 
    ,sql_text    
from dba_hist_active_sess_history a, dba_hist_sqlstat b, dba_users c, dba_hist_sqltext e
where 
a.user_id = c.user_id 
and username = nvl(upper('&username'), username)
and a.SQL_ID = b.SQL_ID 
and A.SQL_PLAN_HASH_VALUE = B.PLAN_HASH_VALUE
and a.INSTANCE_NUMBER = b.INSTANCE_NUMBER 
and b.sql_id = e.sql_id
and e.sql_text not like 'select a.SNAP_ID ,BEGIN_INTERVAL_TIME ,sample_time ,SESSION_ID ,SESSION_SERIAL# ,c.username, a.sql_id, executions_total execs_total,%' -- don't show this query
and nvl(to_timestamp('&dateval','DD-MM-YYYY HH24:MI:SS'),sample_time) between a.sample_time - 10/(24*60*60) and a.sample_time + 10/(24*60*60)
order by sample_time
/

