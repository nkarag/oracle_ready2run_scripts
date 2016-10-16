-----------------------------------------------------------------
--  Find a heavy query in v$sql (shared pool). Heavy is based on logical I/Os and/or Elapsed time
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

select * from (
select sql_id, child_number child, plan_hash_value plan_hash, executions execs, 
(elapsed_time/1000000)/decode(nvl(executions,0),0,1,executions)/
decode(px_servers_executions,0,1,px_servers_executions/decode(nvl(executions,0),0,1,executions)) avg_etime, 
px_servers_executions/decode(nvl(executions,0),0,1,executions) avg_px,
decode(IO_CELL_OFFLOAD_ELIGIBLE_BYTES,0,'No','Yes') Offload,
decode(IO_CELL_OFFLOAD_ELIGIBLE_BYTES,0,0,100*(IO_CELL_OFFLOAD_ELIGIBLE_BYTES-IO_INTERCONNECT_BYTES)
/decode(IO_CELL_OFFLOAD_ELIGIBLE_BYTES,0,1,IO_CELL_OFFLOAD_ELIGIBLE_BYTES)) "IO_SAVED_%",
buffer_gets/decode(nvl(executions,0),0,1,executions) avg_lio,  -- buffer_gets lio
sql_text
from gv$sql s
where upper(sql_text) like upper(nvl('&sql_text',sql_text))
and sql_text not like 'BEGIN :sql_text := %'
and sql_text not like '%IO_CELL_OFFLOAD_ELIGIBLE_BYTES%'
)
where 
avg_lio > nvl('&min_avg_lio','500000')
and avg_etime > nvl('&min_etime','0');