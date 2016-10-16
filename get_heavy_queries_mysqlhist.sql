
-----------------------------------------------------------------
--  Find a heavy query in monitor_dw.sql_history. Heavy is based on logical I/Os and/or Elapsed time
--  Elapsed time is not accurate. It is the average elapsed time from all executions which is then divided by
--  the average number of parallel slaves from all executions
--  nkarag
-----------------------------------------------------------------

set pagesize 999
set lines 190
col sql_text format a70 trunc
col child format 99999
col execs format 9,999
col avg_etime format 99,999.99
col "OFFLOADED_%" format a11
col avg_px format 999
col offload for a7

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
and avg_etime > nvl('&min_etime','0')
and (sql_text like '&&sql_text' or sql_text like upper('&&sql_text') or sql_text like lower('&&sql_text'))
order by avg_lio desc;