set pagesize 999
set lines 999
col sql_text format a70 trunc
col child format 99999
col execs format 9,999
col avg_etime format 99,999.99
col "OFFLOADED_%" format a11
col avg_px format 999
col offload for a7
-- using dba_hist_sqlstat
select snap_id, BEGIN_INTERVAL_TIME, END_INTERVAL_TIME, PARSING_SCHEMA_NAME, sql_id, plan_hash_value, avg_lio, avg_etime as avg_etime_secs, command_type_desc, sql_text
from (
    select snap_id, BEGIN_INTERVAL_TIME, END_INTERVAL_TIME, PARSING_SCHEMA_NAME, 
            sql_id, PLAN_HASH_VALUE,
            BUFFER_GETS_TOTAL/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL) avg_lio,
            ((ELAPSED_TIME_TOTAL/1000000)/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)/
    decode(PX_SERVERS_EXECS_TOTAL,0,1,PX_SERVERS_EXECS_TOTAL)/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)) avg_etime,
            sql_text, aa.name command_type_desc           
    from DBA_HIST_SQLSTAT a  join
         DBA_HIST_SNAPSHOT b using (SNAP_ID) join
         DBA_HIST_SQLTEXT c using (SQL_ID) join
         audit_actions aa on (COMMAND_TYPE = aa.ACTION)    
)  
where
upper(sql_text) like upper(nvl('&sql_text',sql_text))
and sql_id = nvl('&sql_id',sql_id)
order by 1 desc,3 desc; 