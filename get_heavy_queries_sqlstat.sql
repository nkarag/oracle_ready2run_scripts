-----------------------------------------------------------------
--  Find a heavy query in dba_hist_sqlstats. Heavy is based on logical I/Os and/or Elapsed time
--  Elapsed time is not accurate. It is the average elapsed time from all executions which is then divided by
--  the average number of parallel slaves from all executions
--  nkarag
-----------------------------------------------------------------
set pagesize 999
set lines 999
col sql_text format a70 trunc
col child format 99999
col execs format 9,999
col avg_etime format 99,999.99
col "OFFLOADED_%" format a11
col avg_px format 999
col offload for a7
col begin_interval_time a20
col end_interval_time a20
col snap_id 9999999
col instance_number 9

-- using dba_hist_sqlstat
select SNAP_ID, INSTANCE_NUMBER, BEGIN_INTERVAL_TIME, END_INTERVAL_TIME, PARSING_SCHEMA_NAME, 
		sql_id, avg_lio, avg_etime_secs, command_type_desc, sql_text
from (
    select  SNAP_ID, INSTANCE_NUMBER, BEGIN_INTERVAL_TIME, END_INTERVAL_TIME, PARSING_SCHEMA_NAME, 
            sql_id, avg_lio, avg_etime as avg_etime_secs, command_type_desc, sql_text,
            row_number() over(partition by sql_id order by avg_lio desc, avg_etime desc ) r
    from (
        select SNAP_ID, B.INSTANCE_NUMBER, B.BEGIN_INTERVAL_TIME, B.END_INTERVAL_TIME, PARSING_SCHEMA_NAME, 
                sql_id, 
                BUFFER_GETS_TOTAL/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL) avg_lio,
                (ELAPSED_TIME_TOTAL/1000000)/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)/
        decode(PX_SERVERS_EXECS_TOTAL,0,1,PX_SERVERS_EXECS_TOTAL/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)) avg_etime,
                sql_text, aa.name command_type_desc           
        from DBA_HIST_SQLSTAT a join
             DBA_HIST_SNAPSHOT b using (SNAP_ID) join
             DBA_HIST_SQLTEXT c using (SQL_ID) join
             audit_actions aa on (COMMAND_TYPE = aa.ACTION)
        where
            PARSING_SCHEMA_NAME like upper(nvl('&parsing_schema', parsing_schema_name))
            --in     ( select SCHEMA_NAME from MONITOR_DW.MONDW_INDEX_USAGE_TRG_SCHEMAS where INCLUDE_IND = 1)
    )  
    where
        avg_lio > nvl('&min_avg_lio','500000')
        and avg_etime > nvl('&min_etime','0')
        --and  sql_text like nvl('&sql_text',sql_text) 
        order by avg_lio desc, avg_etime    
)
where
    r = 1
    and rownum <= nvl('&topn', rownum)
/	


-- using dba_hist_sqlstat
--select PARSING_SCHEMA_NAME, sql_id, avg_lio, avg_etime as avg_etime_secs, command_type_desc, sql_text
--from (
--    select PARSING_SCHEMA_NAME, 
--            sql_id, 
--            BUFFER_GETS_TOTAL/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL) avg_lio,
--            (ELAPSED_TIME_TOTAL/1000000)/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)/
--    decode(PX_SERVERS_EXECS_TOTAL,0,1,PX_SERVERS_EXECS_TOTAL/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)) avg_etime,
--            sql_text, aa.name command_type_desc           
--    from DBA_HIST_SQLSTAT a join
--         DBA_HIST_SNAPSHOT b using (SNAP_ID) join
--         DBA_HIST_SQLTEXT c using (SQL_ID) join
--         audit_actions aa on (COMMAND_TYPE = aa.ACTION)
--    where
--    PARSING_SCHEMA_NAME like upper(nvl('&parsing_schema', parsing_schema_name))
--    --in     ( select SCHEMA_NAME from MONITOR_DW.MONDW_INDEX_USAGE_TRG_SCHEMAS where INCLUDE_IND = 1)
--)  
--where
--avg_lio > nvl('&min_avg_lio','500000')
--and avg_etime > nvl('&min_etime','0')
----and  upper(sql_text) like upper(nvl('&sql_text',sql_text)) --(sql_text like '&&sql_text' or sql_text like upper('&&sql_text') or sql_text like lower('&&sql_text'))
--order by avg_lio desc; 