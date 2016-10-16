------------------------------------------------------------------------------------------------------------------
--	fs_awr.sql
--				Find executions history of an SQL_ID in the AWR repository
--
---             The offloading info is based on the observation that the IO_CELL_OFFLOAD_ELIGIBLE_BYTES
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
--				author: (C) Nikos Karagiannidis - http://oradwstories.blogspot.com
------------------------------------------------------------------------------------------------------------------

set pagesize 999
set lines 999
col sql_text format a70 trunc
col child format 99999
col sql_id for a15
col plan_hash_value for 9999999999999
col executions_total format 9999999999999999
col avg_etime format 999,999,999,999.99
col avg_lio format 999,999,999,999.99
col NOCHILD_CURSORS for 999999999999999
col "OFFLOADED_%" format a11
col avg_px format 999999
col offload for a7
col BEGIN_INTERVAL_TIME format a30
col END_INTERVAL_TIME format a30
-- using dba_hist_sqlstat

select    a.INSTANCE_NUMBER, snap_id, BEGIN_INTERVAL_TIME, END_INTERVAL_TIME, 
        PARSING_SCHEMA_NAME, 
        sql_id, PLAN_HASH_VALUE,
		aa.name command_type_desc,
        SQL_PROFILE,        		
        executions_total,
        OPTIMIZER_COST,
        (ELAPSED_TIME_TOTAL/1e6)/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL)/
			decode(PX_SERVERS_EXECS_TOTAL,0,1,PX_SERVERS_EXECS_TOTAL)/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL) avg_etime,
        decode(PX_SERVERS_EXECS_TOTAL,0,1,PX_SERVERS_EXECS_TOTAL)/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL) avg_px,
        BUFFER_GETS_TOTAL/decode(nvl(EXECUTIONS_TOTAL,0),0,1,EXECUTIONS_TOTAL) avg_lio,            
        VERSION_COUNT nochild_cursors,
        decode(IO_OFFLOAD_ELIG_BYTES_TOTAL,0,'No','Yes') Offload,
        decode(IO_OFFLOAD_ELIG_BYTES_TOTAL,0,0,100*(IO_OFFLOAD_ELIG_BYTES_TOTAL-IO_INTERCONNECT_BYTES_TOTAL))
        /decode(IO_OFFLOAD_ELIG_BYTES_TOTAL,0,1,IO_OFFLOAD_ELIG_BYTES_TOTAL) "IO_SAVED_%",
		c.sql_text
from DBA_HIST_SQLSTAT a  left outer join
     DBA_HIST_SNAPSHOT b using (SNAP_ID) left outer join
     DBA_HIST_SQLTEXT c using (SQL_ID) left outer join
     audit_actions aa on (COMMAND_TYPE = aa.ACTION)      
where
    upper(dbms_lob.substr(sql_text, 4000, 1)) like upper(nvl('&sql_text',upper(dbms_lob.substr(sql_text, 4000, 1))))  --use dbms_lob.substr in order not to get an "ORA-22835: Buffer too small for CLOB to CHAR or BLOB to RAW conversion"
    and sql_id = nvl(trim('&sql_id'),sql_id)
	and b.begin_interval_time > sysdate - &days_back
order by 2 desc,3 desc;  

undef days_back