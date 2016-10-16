
------------------- Queries -------------------------------------
-- using dba_hist_sqlstat
select PARSING_SCHEMA_NAME, sql_id, avg_lio, avg_etime as avg_etime_secs, command_type_desc, sql_text
from (
    select PARSING_SCHEMA_NAME, 
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
    PARSING_SCHEMA_NAME in     
    ( select SCHEMA_NAME from MONITOR_DW.MONDW_INDEX_USAGE_TRG_SCHEMAS where INCLUDE_IND = 1)
)  
where
avg_lio > nvl('&min_avg_lio','500000')
and (sql_text like '&sql_text' or sql_text like upper('&sql_text') or sql_text like lower('&sql_text'))
order by avg_lio desc  


-- using my custom sql_history table (longer history retained)
select PARSING_SCHEMA_NAME, sql_id, avg_lio, avg_etime as avg_etime_secs, command_type_desc, sql_text
from (
    select *           
    from monitor_dw.sql_history
    where
    PARSING_SCHEMA_NAME in     
    ( select SCHEMA_NAME from MONITOR_DW.MONDW_INDEX_USAGE_TRG_SCHEMAS where INCLUDE_IND = 1)
)  
where
avg_lio > nvl('&min_avg_lio','500000')
and (sql_text like '&sql_text' or sql_text like upper('&sql_text') or sql_text like lower('&sql_text'))
order by avg_lio desc  



/*
select PARSING_SCHEMA_NAME, sql_id, avg_lio, avg_etime as avg_etime_secs, sql_text
from (
    select PARSING_SCHEMA_NAME, 
            sql_id, 
            BUFFER_GETS_TOTAL/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL) avg_lio,
            (ELAPSED_TIME_TOTAL/1000000)/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)/
    decode(PX_SERVERS_EXECS_TOTAL,0,1,PX_SERVERS_EXECS_TOTAL/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)) avg_etime
    from DBA_HIST_SQLSTAT join
         DBA_HIST_SNAPSHOT using (SNAP_ID)
    where
    PARSING_SCHEMA_NAME in     
    ( select SCHEMA_NAME from MONITOR_DW.MONDW_INDEX_USAGE_TRG_SCHEMAS where INCLUDE_IND = 1)
)  join
DBA_HIST_SQLTEXT using (SQL_ID)
where
avg_lio > nvl('&min_avg_lio','500000')
and upper(sql_text) like upper(nvl('&sql_text',sql_text))
--and sql_id = '26c42696qutd8'
*/

------------------- Views ---------------------------------------------------------
create or replace view monitor_dw.all_sql
as
select PARSING_SCHEMA_NAME, sql_id, avg_lio, avg_etime as avg_etime_secs, MODULE, command_type_desc, sql_text
from (
    select *           
    from monitor_dw.sql_history
    where
    PARSING_SCHEMA_NAME in     
    ( select SCHEMA_NAME from MONITOR_DW.MONDW_INDEX_USAGE_TRG_SCHEMAS where INCLUDE_IND = 1)
);  


--drop view monitor_dw.all_heavy_sql_lio

create or replace view monitor_dw.all_heavy_sql_by_lio
as
select PARSING_SCHEMA_NAME, sql_id, avg_lio, avg_etime as avg_etime_secs, MODULE,command_type_desc, sql_text
from (
    select *           
    from monitor_dw.sql_history
    where
    PARSING_SCHEMA_NAME in     
    ( select SCHEMA_NAME from MONITOR_DW.MONDW_INDEX_USAGE_TRG_SCHEMAS where INCLUDE_IND = 1)
)
where
avg_lio > 500000;  


create or replace view monitor_dw.all_heavy_sql_by_etime
as
select PARSING_SCHEMA_NAME, sql_id, avg_lio, avg_etime as avg_etime_secs, module, command_type_desc, sql_text
from (
    select *           
    from monitor_dw.sql_history
    where
    PARSING_SCHEMA_NAME in     
    ( select SCHEMA_NAME from MONITOR_DW.MONDW_INDEX_USAGE_TRG_SCHEMAS where INCLUDE_IND = 1)
)
where
avg_etime > 60*60*(15/60)  


------------------- sql_history table and maintenance ---------------------------

drop table monitor_dw.sql_history

create table monitor_dw.sql_history
parallel 32
compress
as
select
    BUFFER_GETS_TOTAL/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL) avg_lio,
    (ELAPSED_TIME_TOTAL/1000000)/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)/
        decode(PX_SERVERS_EXECS_TOTAL,0,1,PX_SERVERS_EXECS_TOTAL/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)) avg_etime,
   SNAP_ID,
   DBID,
   INSTANCE_NUMBER,
   SQL_ID,
   PLAN_HASH_VALUE,
   OPTIMIZER_COST,
   OPTIMIZER_MODE,
   OPTIMIZER_ENV_HASH_VALUE,
   SHARABLE_MEM,
   LOADED_VERSIONS,
   VERSION_COUNT,
   MODULE,
   ACTION,
   SQL_PROFILE,
   FORCE_MATCHING_SIGNATURE,
   PARSING_SCHEMA_ID,
   PARSING_SCHEMA_NAME,
   FETCHES_TOTAL,
   FETCHES_DELTA,
   END_OF_FETCH_COUNT_TOTAL,
   END_OF_FETCH_COUNT_DELTA,
   SORTS_TOTAL,
   SORTS_DELTA,
   EXECUTIONS_TOTAL,
   EXECUTIONS_DELTA,
   PX_SERVERS_EXECS_TOTAL,
   PX_SERVERS_EXECS_DELTA,
   LOADS_TOTAL,
   LOADS_DELTA,
   INVALIDATIONS_TOTAL,
   INVALIDATIONS_DELTA,
   PARSE_CALLS_TOTAL,
   PARSE_CALLS_DELTA,
   DISK_READS_TOTAL,
   DISK_READS_DELTA,
   BUFFER_GETS_TOTAL,
   BUFFER_GETS_DELTA,
   ROWS_PROCESSED_TOTAL,
   ROWS_PROCESSED_DELTA,
   CPU_TIME_TOTAL,
   CPU_TIME_DELTA,
   ELAPSED_TIME_TOTAL,
   ELAPSED_TIME_DELTA,
   IOWAIT_TOTAL,
   IOWAIT_DELTA,
   CLWAIT_TOTAL,
   CLWAIT_DELTA,
   APWAIT_TOTAL,
   APWAIT_DELTA,
   CCWAIT_TOTAL,
   CCWAIT_DELTA,
   DIRECT_WRITES_TOTAL,
   DIRECT_WRITES_DELTA,
   PLSEXEC_TIME_TOTAL,
   PLSEXEC_TIME_DELTA,
   JAVEXEC_TIME_TOTAL,
   JAVEXEC_TIME_DELTA,
   BIND_DATA,
   STARTUP_TIME,
   BEGIN_INTERVAL_TIME,
   END_INTERVAL_TIME,
   FLUSH_ELAPSED,
   SNAP_LEVEL,
   ERROR_COUNT,
   SQL_TEXT,
   COMMAND_TYPE,
   COMMAND_TYPE_DESC        
from (               
    select    
   SNAP_ID,
   DBID,
   INSTANCE_NUMBER,
   SQL_ID,
   PLAN_HASH_VALUE,
   OPTIMIZER_COST,
   OPTIMIZER_MODE,
   OPTIMIZER_ENV_HASH_VALUE,
   SHARABLE_MEM,
   LOADED_VERSIONS,
   VERSION_COUNT,
   MODULE,
   a.ACTION,
   SQL_PROFILE,
   FORCE_MATCHING_SIGNATURE,
   PARSING_SCHEMA_ID,
   PARSING_SCHEMA_NAME,
   FETCHES_TOTAL,
   FETCHES_DELTA,
   END_OF_FETCH_COUNT_TOTAL,
   END_OF_FETCH_COUNT_DELTA,
   SORTS_TOTAL,
   SORTS_DELTA,
   EXECUTIONS_TOTAL,
   EXECUTIONS_DELTA,
   PX_SERVERS_EXECS_TOTAL,
   PX_SERVERS_EXECS_DELTA,
   LOADS_TOTAL,
   LOADS_DELTA,
   INVALIDATIONS_TOTAL,
   INVALIDATIONS_DELTA,
   PARSE_CALLS_TOTAL,
   PARSE_CALLS_DELTA,
   DISK_READS_TOTAL,
   DISK_READS_DELTA,
   BUFFER_GETS_TOTAL,
   BUFFER_GETS_DELTA,
   ROWS_PROCESSED_TOTAL,
   ROWS_PROCESSED_DELTA,
   CPU_TIME_TOTAL,
   CPU_TIME_DELTA,
   ELAPSED_TIME_TOTAL,
   ELAPSED_TIME_DELTA,
   IOWAIT_TOTAL,
   IOWAIT_DELTA,
   CLWAIT_TOTAL,
   CLWAIT_DELTA,
   APWAIT_TOTAL,
   APWAIT_DELTA,
   CCWAIT_TOTAL,
   CCWAIT_DELTA,
   DIRECT_WRITES_TOTAL,
   DIRECT_WRITES_DELTA,
   PLSEXEC_TIME_TOTAL,
   PLSEXEC_TIME_DELTA,
   JAVEXEC_TIME_TOTAL,
   JAVEXEC_TIME_DELTA,
   BIND_DATA,
   STARTUP_TIME,
   BEGIN_INTERVAL_TIME,
   END_INTERVAL_TIME,
   FLUSH_ELAPSED,
   SNAP_LEVEL,
   ERROR_COUNT,
   SQL_TEXT,
   COMMAND_TYPE,
   aa.NAME as COMMAND_TYPE_DESC           
    from DBA_HIST_SQLSTAT a join
         DBA_HIST_SNAPSHOT b using (SNAP_ID, DBID, INSTANCE_NUMBER) join
         DBA_HIST_SQLTEXT c using (SQL_ID, DBID) join
         audit_actions aa on (COMMAND_TYPE = aa.ACTION)
) t


select count(*) from monitor_dw.sql_history
-- 62314


exec dbms_stats.gather_table_stats('MONITOR_DW', 'SQL_HISTORY')
 

alter session enable parallel dml;

merge  into monitor_dw.sql_history trg
using(
    select
        BUFFER_GETS_TOTAL/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL) avg_lio,
        (ELAPSED_TIME_TOTAL/1000000)/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)/
            decode(PX_SERVERS_EXECS_TOTAL,0,1,PX_SERVERS_EXECS_TOTAL/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)) avg_etime,
       SNAP_ID,
       DBID,
       INSTANCE_NUMBER,
       SQL_ID,
       PLAN_HASH_VALUE,
       OPTIMIZER_COST,
       OPTIMIZER_MODE,
       OPTIMIZER_ENV_HASH_VALUE,
       SHARABLE_MEM,
       LOADED_VERSIONS,
       VERSION_COUNT,
       MODULE,
       ACTION,
       SQL_PROFILE,
       FORCE_MATCHING_SIGNATURE,
       PARSING_SCHEMA_ID,
       PARSING_SCHEMA_NAME,
       FETCHES_TOTAL,
       FETCHES_DELTA,
       END_OF_FETCH_COUNT_TOTAL,
       END_OF_FETCH_COUNT_DELTA,
       SORTS_TOTAL,
       SORTS_DELTA,
       EXECUTIONS_TOTAL,
       EXECUTIONS_DELTA,
       PX_SERVERS_EXECS_TOTAL,
       PX_SERVERS_EXECS_DELTA,
       LOADS_TOTAL,
       LOADS_DELTA,
       INVALIDATIONS_TOTAL,
       INVALIDATIONS_DELTA,
       PARSE_CALLS_TOTAL,
       PARSE_CALLS_DELTA,
       DISK_READS_TOTAL,
       DISK_READS_DELTA,
       BUFFER_GETS_TOTAL,
       BUFFER_GETS_DELTA,
       ROWS_PROCESSED_TOTAL,
       ROWS_PROCESSED_DELTA,
       CPU_TIME_TOTAL,
       CPU_TIME_DELTA,
       ELAPSED_TIME_TOTAL,
       ELAPSED_TIME_DELTA,
       IOWAIT_TOTAL,
       IOWAIT_DELTA,
       CLWAIT_TOTAL,
       CLWAIT_DELTA,
       APWAIT_TOTAL,
       APWAIT_DELTA,
       CCWAIT_TOTAL,
       CCWAIT_DELTA,
       DIRECT_WRITES_TOTAL,
       DIRECT_WRITES_DELTA,
       PLSEXEC_TIME_TOTAL,
       PLSEXEC_TIME_DELTA,
       JAVEXEC_TIME_TOTAL,
       JAVEXEC_TIME_DELTA,
       BIND_DATA,
       STARTUP_TIME,
       BEGIN_INTERVAL_TIME,
       END_INTERVAL_TIME,
       FLUSH_ELAPSED,
       SNAP_LEVEL,
       ERROR_COUNT,
       SQL_TEXT,
       COMMAND_TYPE,
       COMMAND_TYPE_DESC        
    from (               
        select    
       SNAP_ID,
       DBID,
       INSTANCE_NUMBER,
       SQL_ID,
       PLAN_HASH_VALUE,
       OPTIMIZER_COST,
       OPTIMIZER_MODE,
       OPTIMIZER_ENV_HASH_VALUE,
       SHARABLE_MEM,
       LOADED_VERSIONS,
       VERSION_COUNT,
       MODULE,
       a.ACTION,
       SQL_PROFILE,
       FORCE_MATCHING_SIGNATURE,
       PARSING_SCHEMA_ID,
       PARSING_SCHEMA_NAME,
       FETCHES_TOTAL,
       FETCHES_DELTA,
       END_OF_FETCH_COUNT_TOTAL,
       END_OF_FETCH_COUNT_DELTA,
       SORTS_TOTAL,
       SORTS_DELTA,
       EXECUTIONS_TOTAL,
       EXECUTIONS_DELTA,
       PX_SERVERS_EXECS_TOTAL,
       PX_SERVERS_EXECS_DELTA,
       LOADS_TOTAL,
       LOADS_DELTA,
       INVALIDATIONS_TOTAL,
       INVALIDATIONS_DELTA,
       PARSE_CALLS_TOTAL,
       PARSE_CALLS_DELTA,
       DISK_READS_TOTAL,
       DISK_READS_DELTA,
       BUFFER_GETS_TOTAL,
       BUFFER_GETS_DELTA,
       ROWS_PROCESSED_TOTAL,
       ROWS_PROCESSED_DELTA,
       CPU_TIME_TOTAL,
       CPU_TIME_DELTA,
       ELAPSED_TIME_TOTAL,
       ELAPSED_TIME_DELTA,
       IOWAIT_TOTAL,
       IOWAIT_DELTA,
       CLWAIT_TOTAL,
       CLWAIT_DELTA,
       APWAIT_TOTAL,
       APWAIT_DELTA,
       CCWAIT_TOTAL,
       CCWAIT_DELTA,
       DIRECT_WRITES_TOTAL,
       DIRECT_WRITES_DELTA,
       PLSEXEC_TIME_TOTAL,
       PLSEXEC_TIME_DELTA,
       JAVEXEC_TIME_TOTAL,
       JAVEXEC_TIME_DELTA,
       BIND_DATA,
       STARTUP_TIME,
       BEGIN_INTERVAL_TIME,
       END_INTERVAL_TIME,
       FLUSH_ELAPSED,
       SNAP_LEVEL,
       ERROR_COUNT,
       SQL_TEXT,
       COMMAND_TYPE,
       aa.NAME as COMMAND_TYPE_DESC           
        from DBA_HIST_SQLSTAT a join
             DBA_HIST_SNAPSHOT b using (SNAP_ID, DBID, INSTANCE_NUMBER) join
             DBA_HIST_SQLTEXT c using (SQL_ID, DBID) join
             audit_actions aa on (COMMAND_TYPE = aa.ACTION)
    ) t
) src
on (src.SNAP_ID = trg.SNAP_ID and src.SQL_ID = trg.SQL_ID)
when not matched then
insert /*+ append */( 
   avg_lio,
   avg_etime,
   SNAP_ID,
   DBID,
   INSTANCE_NUMBER,
   SQL_ID,
   PLAN_HASH_VALUE,
   OPTIMIZER_COST,
   OPTIMIZER_MODE,
   OPTIMIZER_ENV_HASH_VALUE,
   SHARABLE_MEM,
   LOADED_VERSIONS,
   VERSION_COUNT,
   MODULE,
   ACTION,
   SQL_PROFILE,
   FORCE_MATCHING_SIGNATURE,
   PARSING_SCHEMA_ID,
   PARSING_SCHEMA_NAME,
   FETCHES_TOTAL,
   FETCHES_DELTA,
   END_OF_FETCH_COUNT_TOTAL,
   END_OF_FETCH_COUNT_DELTA,
   SORTS_TOTAL,
   SORTS_DELTA,
   EXECUTIONS_TOTAL,
   EXECUTIONS_DELTA,
   PX_SERVERS_EXECS_TOTAL,
   PX_SERVERS_EXECS_DELTA,
   LOADS_TOTAL,
   LOADS_DELTA,
   INVALIDATIONS_TOTAL,
   INVALIDATIONS_DELTA,
   PARSE_CALLS_TOTAL,
   PARSE_CALLS_DELTA,
   DISK_READS_TOTAL,
   DISK_READS_DELTA,
   BUFFER_GETS_TOTAL,
   BUFFER_GETS_DELTA,
   ROWS_PROCESSED_TOTAL,
   ROWS_PROCESSED_DELTA,
   CPU_TIME_TOTAL,
   CPU_TIME_DELTA,
   ELAPSED_TIME_TOTAL,
   ELAPSED_TIME_DELTA,
   IOWAIT_TOTAL,
   IOWAIT_DELTA,
   CLWAIT_TOTAL,
   CLWAIT_DELTA,
   APWAIT_TOTAL,
   APWAIT_DELTA,
   CCWAIT_TOTAL,
   CCWAIT_DELTA,
   DIRECT_WRITES_TOTAL,
   DIRECT_WRITES_DELTA,
   PLSEXEC_TIME_TOTAL,
   PLSEXEC_TIME_DELTA,
   JAVEXEC_TIME_TOTAL,
   JAVEXEC_TIME_DELTA,
   BIND_DATA,
   STARTUP_TIME,
   BEGIN_INTERVAL_TIME,
   END_INTERVAL_TIME,
   FLUSH_ELAPSED,
   SNAP_LEVEL,
   ERROR_COUNT,
   SQL_TEXT,
   COMMAND_TYPE,
   COMMAND_TYPE_DESC        
  )
 VALUES (
   src.avg_lio,
   src.avg_etime,
   src.SNAP_ID,
   src.DBID,
   src.INSTANCE_NUMBER,
   src.SQL_ID,
   src.PLAN_HASH_VALUE,
   src.OPTIMIZER_COST,
   src.OPTIMIZER_MODE,
   src.OPTIMIZER_ENV_HASH_VALUE,
   src.SHARABLE_MEM,
   src.LOADED_VERSIONS,
   src.VERSION_COUNT,
   src.MODULE,
   src.ACTION,
   src.SQL_PROFILE,
   src.FORCE_MATCHING_SIGNATURE,
   src.PARSING_SCHEMA_ID,
   src.PARSING_SCHEMA_NAME,
   src.FETCHES_TOTAL,
   src.FETCHES_DELTA,
   src.END_OF_FETCH_COUNT_TOTAL,
   src.END_OF_FETCH_COUNT_DELTA,
   src.SORTS_TOTAL,
   src.SORTS_DELTA,
   src.EXECUTIONS_TOTAL,
   src.EXECUTIONS_DELTA,
   src.PX_SERVERS_EXECS_TOTAL,
   src.PX_SERVERS_EXECS_DELTA,
   src.LOADS_TOTAL,
   src.LOADS_DELTA,
   src.INVALIDATIONS_TOTAL,
   src.INVALIDATIONS_DELTA,
   src.PARSE_CALLS_TOTAL,
   src.PARSE_CALLS_DELTA,
   src.DISK_READS_TOTAL,
   src.DISK_READS_DELTA,
   src.BUFFER_GETS_TOTAL,
   src.BUFFER_GETS_DELTA,
   src.ROWS_PROCESSED_TOTAL,
   src.ROWS_PROCESSED_DELTA,
   src.CPU_TIME_TOTAL,
   src.CPU_TIME_DELTA,
   src.ELAPSED_TIME_TOTAL,
   src.ELAPSED_TIME_DELTA,
   src.IOWAIT_TOTAL,
   src.IOWAIT_DELTA,
   src.CLWAIT_TOTAL,
   src.CLWAIT_DELTA,
   src.APWAIT_TOTAL,
   src.APWAIT_DELTA,
   src.CCWAIT_TOTAL,
   src.CCWAIT_DELTA,
   src.DIRECT_WRITES_TOTAL,
   src.DIRECT_WRITES_DELTA,
   src.PLSEXEC_TIME_TOTAL,
   src.PLSEXEC_TIME_DELTA,
   src.JAVEXEC_TIME_TOTAL,
   src.JAVEXEC_TIME_DELTA,
   src.BIND_DATA,
   src.STARTUP_TIME,
   src.BEGIN_INTERVAL_TIME,
   src.END_INTERVAL_TIME,
   src.FLUSH_ELAPSED,
   src.SNAP_LEVEL,
   src.ERROR_COUNT,
   src.SQL_TEXT,
   src.COMMAND_TYPE,
   src.COMMAND_TYPE_DESC        
  );    
  
  
create or replace procedure monitor_dw.merge_sql_history
IS
begin
  execute immediate 'alter session enable parallel dml'; 

merge  into monitor_dw.sql_history trg
using(
    select
        BUFFER_GETS_TOTAL/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL) avg_lio,
        (ELAPSED_TIME_TOTAL/1000000)/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)/
            decode(PX_SERVERS_EXECS_TOTAL,0,1,PX_SERVERS_EXECS_TOTAL/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)) avg_etime,
       SNAP_ID,
       DBID,
       INSTANCE_NUMBER,
       SQL_ID,
       PLAN_HASH_VALUE,
       OPTIMIZER_COST,
       OPTIMIZER_MODE,
       OPTIMIZER_ENV_HASH_VALUE,
       SHARABLE_MEM,
       LOADED_VERSIONS,
       VERSION_COUNT,
       MODULE,
       ACTION,
       SQL_PROFILE,
       FORCE_MATCHING_SIGNATURE,
       PARSING_SCHEMA_ID,
       PARSING_SCHEMA_NAME,
       FETCHES_TOTAL,
       FETCHES_DELTA,
       END_OF_FETCH_COUNT_TOTAL,
       END_OF_FETCH_COUNT_DELTA,
       SORTS_TOTAL,
       SORTS_DELTA,
       EXECUTIONS_TOTAL,
       EXECUTIONS_DELTA,
       PX_SERVERS_EXECS_TOTAL,
       PX_SERVERS_EXECS_DELTA,
       LOADS_TOTAL,
       LOADS_DELTA,
       INVALIDATIONS_TOTAL,
       INVALIDATIONS_DELTA,
       PARSE_CALLS_TOTAL,
       PARSE_CALLS_DELTA,
       DISK_READS_TOTAL,
       DISK_READS_DELTA,
       BUFFER_GETS_TOTAL,
       BUFFER_GETS_DELTA,
       ROWS_PROCESSED_TOTAL,
       ROWS_PROCESSED_DELTA,
       CPU_TIME_TOTAL,
       CPU_TIME_DELTA,
       ELAPSED_TIME_TOTAL,
       ELAPSED_TIME_DELTA,
       IOWAIT_TOTAL,
       IOWAIT_DELTA,
       CLWAIT_TOTAL,
       CLWAIT_DELTA,
       APWAIT_TOTAL,
       APWAIT_DELTA,
       CCWAIT_TOTAL,
       CCWAIT_DELTA,
       DIRECT_WRITES_TOTAL,
       DIRECT_WRITES_DELTA,
       PLSEXEC_TIME_TOTAL,
       PLSEXEC_TIME_DELTA,
       JAVEXEC_TIME_TOTAL,
       JAVEXEC_TIME_DELTA,
       BIND_DATA,
       STARTUP_TIME,
       BEGIN_INTERVAL_TIME,
       END_INTERVAL_TIME,
       FLUSH_ELAPSED,
       SNAP_LEVEL,
       ERROR_COUNT,
       SQL_TEXT,
       COMMAND_TYPE,
       COMMAND_TYPE_DESC        
    from (               
        select    
       SNAP_ID,
       DBID,
       INSTANCE_NUMBER,
       SQL_ID,
       PLAN_HASH_VALUE,
       OPTIMIZER_COST,
       OPTIMIZER_MODE,
       OPTIMIZER_ENV_HASH_VALUE,
       SHARABLE_MEM,
       LOADED_VERSIONS,
       VERSION_COUNT,
       MODULE,
       a.ACTION,
       SQL_PROFILE,
       FORCE_MATCHING_SIGNATURE,
       PARSING_SCHEMA_ID,
       PARSING_SCHEMA_NAME,
       FETCHES_TOTAL,
       FETCHES_DELTA,
       END_OF_FETCH_COUNT_TOTAL,
       END_OF_FETCH_COUNT_DELTA,
       SORTS_TOTAL,
       SORTS_DELTA,
       EXECUTIONS_TOTAL,
       EXECUTIONS_DELTA,
       PX_SERVERS_EXECS_TOTAL,
       PX_SERVERS_EXECS_DELTA,
       LOADS_TOTAL,
       LOADS_DELTA,
       INVALIDATIONS_TOTAL,
       INVALIDATIONS_DELTA,
       PARSE_CALLS_TOTAL,
       PARSE_CALLS_DELTA,
       DISK_READS_TOTAL,
       DISK_READS_DELTA,
       BUFFER_GETS_TOTAL,
       BUFFER_GETS_DELTA,
       ROWS_PROCESSED_TOTAL,
       ROWS_PROCESSED_DELTA,
       CPU_TIME_TOTAL,
       CPU_TIME_DELTA,
       ELAPSED_TIME_TOTAL,
       ELAPSED_TIME_DELTA,
       IOWAIT_TOTAL,
       IOWAIT_DELTA,
       CLWAIT_TOTAL,
       CLWAIT_DELTA,
       APWAIT_TOTAL,
       APWAIT_DELTA,
       CCWAIT_TOTAL,
       CCWAIT_DELTA,
       DIRECT_WRITES_TOTAL,
       DIRECT_WRITES_DELTA,
       PLSEXEC_TIME_TOTAL,
       PLSEXEC_TIME_DELTA,
       JAVEXEC_TIME_TOTAL,
       JAVEXEC_TIME_DELTA,
       BIND_DATA,
       STARTUP_TIME,
       BEGIN_INTERVAL_TIME,
       END_INTERVAL_TIME,
       FLUSH_ELAPSED,
       SNAP_LEVEL,
       ERROR_COUNT,
       SQL_TEXT,
       COMMAND_TYPE,
       aa.NAME as COMMAND_TYPE_DESC           
        from DBA_HIST_SQLSTAT a join
             DBA_HIST_SNAPSHOT b using (SNAP_ID, DBID, INSTANCE_NUMBER) join
             DBA_HIST_SQLTEXT c using (SQL_ID, DBID) join
             audit_actions aa on (COMMAND_TYPE = aa.ACTION)
    ) t
) src
on (src.SNAP_ID = trg.SNAP_ID and src.SQL_ID = trg.SQL_ID)
when not matched then
insert /*+ append */( 
   avg_lio,
   avg_etime,
   SNAP_ID,
   DBID,
   INSTANCE_NUMBER,
   SQL_ID,
   PLAN_HASH_VALUE,
   OPTIMIZER_COST,
   OPTIMIZER_MODE,
   OPTIMIZER_ENV_HASH_VALUE,
   SHARABLE_MEM,
   LOADED_VERSIONS,
   VERSION_COUNT,
   MODULE,
   ACTION,
   SQL_PROFILE,
   FORCE_MATCHING_SIGNATURE,
   PARSING_SCHEMA_ID,
   PARSING_SCHEMA_NAME,
   FETCHES_TOTAL,
   FETCHES_DELTA,
   END_OF_FETCH_COUNT_TOTAL,
   END_OF_FETCH_COUNT_DELTA,
   SORTS_TOTAL,
   SORTS_DELTA,
   EXECUTIONS_TOTAL,
   EXECUTIONS_DELTA,
   PX_SERVERS_EXECS_TOTAL,
   PX_SERVERS_EXECS_DELTA,
   LOADS_TOTAL,
   LOADS_DELTA,
   INVALIDATIONS_TOTAL,
   INVALIDATIONS_DELTA,
   PARSE_CALLS_TOTAL,
   PARSE_CALLS_DELTA,
   DISK_READS_TOTAL,
   DISK_READS_DELTA,
   BUFFER_GETS_TOTAL,
   BUFFER_GETS_DELTA,
   ROWS_PROCESSED_TOTAL,
   ROWS_PROCESSED_DELTA,
   CPU_TIME_TOTAL,
   CPU_TIME_DELTA,
   ELAPSED_TIME_TOTAL,
   ELAPSED_TIME_DELTA,
   IOWAIT_TOTAL,
   IOWAIT_DELTA,
   CLWAIT_TOTAL,
   CLWAIT_DELTA,
   APWAIT_TOTAL,
   APWAIT_DELTA,
   CCWAIT_TOTAL,
   CCWAIT_DELTA,
   DIRECT_WRITES_TOTAL,
   DIRECT_WRITES_DELTA,
   PLSEXEC_TIME_TOTAL,
   PLSEXEC_TIME_DELTA,
   JAVEXEC_TIME_TOTAL,
   JAVEXEC_TIME_DELTA,
   BIND_DATA,
   STARTUP_TIME,
   BEGIN_INTERVAL_TIME,
   END_INTERVAL_TIME,
   FLUSH_ELAPSED,
   SNAP_LEVEL,
   ERROR_COUNT,
   SQL_TEXT,
   COMMAND_TYPE,
   COMMAND_TYPE_DESC        
  )
 VALUES (
   src.avg_lio,
   src.avg_etime,
   src.SNAP_ID,
   src.DBID,
   src.INSTANCE_NUMBER,
   src.SQL_ID,
   src.PLAN_HASH_VALUE,
   src.OPTIMIZER_COST,
   src.OPTIMIZER_MODE,
   src.OPTIMIZER_ENV_HASH_VALUE,
   src.SHARABLE_MEM,
   src.LOADED_VERSIONS,
   src.VERSION_COUNT,
   src.MODULE,
   src.ACTION,
   src.SQL_PROFILE,
   src.FORCE_MATCHING_SIGNATURE,
   src.PARSING_SCHEMA_ID,
   src.PARSING_SCHEMA_NAME,
   src.FETCHES_TOTAL,
   src.FETCHES_DELTA,
   src.END_OF_FETCH_COUNT_TOTAL,
   src.END_OF_FETCH_COUNT_DELTA,
   src.SORTS_TOTAL,
   src.SORTS_DELTA,
   src.EXECUTIONS_TOTAL,
   src.EXECUTIONS_DELTA,
   src.PX_SERVERS_EXECS_TOTAL,
   src.PX_SERVERS_EXECS_DELTA,
   src.LOADS_TOTAL,
   src.LOADS_DELTA,
   src.INVALIDATIONS_TOTAL,
   src.INVALIDATIONS_DELTA,
   src.PARSE_CALLS_TOTAL,
   src.PARSE_CALLS_DELTA,
   src.DISK_READS_TOTAL,
   src.DISK_READS_DELTA,
   src.BUFFER_GETS_TOTAL,
   src.BUFFER_GETS_DELTA,
   src.ROWS_PROCESSED_TOTAL,
   src.ROWS_PROCESSED_DELTA,
   src.CPU_TIME_TOTAL,
   src.CPU_TIME_DELTA,
   src.ELAPSED_TIME_TOTAL,
   src.ELAPSED_TIME_DELTA,
   src.IOWAIT_TOTAL,
   src.IOWAIT_DELTA,
   src.CLWAIT_TOTAL,
   src.CLWAIT_DELTA,
   src.APWAIT_TOTAL,
   src.APWAIT_DELTA,
   src.CCWAIT_TOTAL,
   src.CCWAIT_DELTA,
   src.DIRECT_WRITES_TOTAL,
   src.DIRECT_WRITES_DELTA,
   src.PLSEXEC_TIME_TOTAL,
   src.PLSEXEC_TIME_DELTA,
   src.JAVEXEC_TIME_TOTAL,
   src.JAVEXEC_TIME_DELTA,
   src.BIND_DATA,
   src.STARTUP_TIME,
   src.BEGIN_INTERVAL_TIME,
   src.END_INTERVAL_TIME,
   src.FLUSH_ELAPSED,
   src.SNAP_LEVEL,
   src.ERROR_COUNT,
   src.SQL_TEXT,
   src.COMMAND_TYPE,
   src.COMMAND_TYPE_DESC        
  );    

commit;

dbms_stats.gather_table_stats('MONITOR_DW', 'SQL_HISTORY');
end;
  
  
-- JOB DDL
BEGIN
sys.dbms_scheduler.create_job( 
job_name => '"MONITOR_DW"."GET_SQL_HIST"',
job_type => 'PLSQL_BLOCK',
job_action => 'begin
monitor_dw.merge_sql_history;
end;',
repeat_interval => 'FREQ=WEEKLY;BYDAY=MON;BYHOUR=20;BYMINUTE=0;BYSECOND=0',
start_date => systimestamp at time zone 'Europe/Istanbul',
job_class => 'DEFAULT_JOB_CLASS',
auto_drop => FALSE,
enabled => TRUE);
END;

BEGIN
sys.dbms_scheduler.disable( '"MONITOR_DW"."GET_SQL_HIST"' ); 
sys.dbms_scheduler.set_attribute( name => '"MONITOR_DW"."GET_SQL_HIST"', attribute => 'repeat_interval', value => 'FREQ=DAILY;BYHOUR=20;BYMINUTE=0;BYSECOND=0'); 
sys.dbms_scheduler.enable( '"MONITOR_DW"."GET_SQL_HIST"' ); 
END;




 