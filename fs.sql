--              This script can be used to locate statements in the shared pool and 
--              determine (among other) whether they have been executed via Smart Scans.
--
--              It is based on the observation that the IO_CELL_OFFLOAD_ELIGIBLE_BYTES
--              column in V$SQL is only greater than 0 when a statement is executed
--              using a Smart Scan. The IO_SAVED_% column attempts to show the ratio of
--              of data received from the storage cells to the actual amount of data
--              that would have had to be retrieved on non-Exadata storage. Note that 
--              as of 11.2.0.2, there are issues calculating this value with some queries.
--
--              Note that the AVG_ETIME will not be acurate for parallel queries. The 
--              ELAPSED_TIME column contains the sum of all parallel slaves. So the 
--              script divides the value by the number of PX slaves used which gives an 
--              approximation. 
--
--              Note also that if parallel slaves are spread across multiple nodes on
--              a RAC database the PX_SERVERS_EXECUTIONS column will not be set.
--
--	(C) Nikos Karagiannidis - http://oradwstories.blogspot.com


col sql_text for a70 trunc
set verify off
set pagesize 999
set lines 999
col username format a13
col prog format a22
col sid format 999
col child_number format 99999 heading CHILD
col ocategory format a10
col avg_etime_secs format 9,999,999.99
col avg_pio format 9,999,999.99
col avg_lio format 999,999,999
col etime format 9,999,999.99
col is_shareable format a12
col is_bind_sensitive format a17
col is_bind_aware format a14
col offload for a7

alter session set NLS_DATE_FORMAT = 'dd-mm-yyyy HH24:mi:ss'
/

select  PARSING_SCHEMA_NAME, inst_id, sql_id, child_number, plan_hash_value, executions execs, 
		OPTIMIZER_ENV_HASH_VALUE,
(elapsed_time/1000000)/decode(nvl(executions,0),0,1,executions) avg_etime_secs, 
buffer_gets/decode(nvl(executions,0),0,1,executions) avg_lio,
last_active_time, 
SQL_PROFILE,
 is_shareable, is_bind_sensitive, is_bind_aware,
sql_text,
decode(IO_CELL_OFFLOAD_ELIGIBLE_BYTES,0,'No','Yes') Offload,
decode(IO_CELL_OFFLOAD_ELIGIBLE_BYTES,0,0,100*(IO_CELL_OFFLOAD_ELIGIBLE_BYTES-IO_INTERCONNECT_BYTES)
/decode(IO_CELL_OFFLOAD_ELIGIBLE_BYTES,0,1,IO_CELL_OFFLOAD_ELIGIBLE_BYTES)) "IO_SAVED_%"
from gv$sql s
where upper(sql_text) like upper(nvl('&sql_text',sql_text))
and sql_text not like '%from gv$sql where upper(sql_text) like nvl(%'
and sql_id like nvl(trim('&sql_id'),sql_id)
order by 1, 2, 3, last_active_time desc
/
