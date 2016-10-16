---------------------------------------------------------------------------------------------
--				Find the available execution plans for a specific SQL_ID
--
--              Note that the AVG_ET_SECS (average elpased time) will not be accurate for parallel queries. 
--				The ELAPSED_TIME column contains the sum of all parallel slaves. So the 
--              script divides the value by the number of PX slaves used which gives an 
--              approximation. 
--
--              Note also that if parallel slaves are spread across multiple nodes on
--              a RAC database the PX_SERVERS_EXECUTIONS column will not be set.
--
--				author: Nikos Karagiannidis (C) 2013 - http://oradwstories.blogspot.com
---------------------------------------------------------------------------------------------

prompt
prompt ********************************************************
prompt Find the available execution plans for a specific SQL_ID
prompt ********************************************************
prompt

set linesize 999
col avg_et_secs justify right format 9999999.99 
col cost justify right format 9999999999 
col timestamp justify center format a25 
col parsing_schema_name justify center format a30
col inst_id format 999999999 
col executions_total format 99999999999999999
col executions format 99999999999999

alter session set nls_date_format='dd-mm-yyyy hh24:mi:ss';

select	'gv$sqlarea_plan_hash' source, INST_ID, 
		SQL_ID, PLAN_HASH_VALUE, 
		executions, 
		round(elapsed_time/decode(nvl(executions,0),0,1,executions)/1e6/
			decode(px_servers_executions,0,1,px_servers_executions)/decode(nvl(executions,0),0,1,executions),2)	avg_et_secs, 
		px_servers_executions/decode(nvl(executions,0),0,1,executions) avg_px,		
		optimizer_cost cost, 
		LAST_LOAD_TIME timestamp, 
		parsing_schema_name --FIRST_LOAD_TIME, LAST_LOAD_TIME, LAST_ACTIVE_TIME, SQL_PROFILE
from gv$sqlarea_plan_hash
where sql_id = nvl(trim('&&sql_id'),sql_id)
UNION
SELECT 	'dba_hist_sql_plan' source, null INST_ID, 
		t1.sql_id sql_id, t1.plan_hash_value plan_hash_value, 
		t2.executions_total, 
		t2.avg_et_secs avg_et_secs, 
		t2.avg_px, 
		t1.cost cost, 
		t1.timestamp timestamp, 
		NULL parsing_schema_name 
FROM dba_hist_sql_plan t1,
	(
		SELECT	sql_id, plan_hash_value, 
				max(executions_total) executions_total, --round(SUM(elapsed_time_total)/decode(SUM(executions_total),0,1,SUM(executions_total))/1e6,2) avg_et_secs 
				round(SUM(elapsed_time_total)/decode(SUM(executions_total),0,1,SUM(executions_total))/1e6/		
					decode(SUM(px_servers_execs_total),0,1,SUM(px_servers_execs_total))/decode(SUM(executions_total),0,1,SUM(executions_total)),2)	avg_et_secs, 
				SUM(px_servers_execs_total)/decode(SUM(executions_total),0,1,SUM(executions_total)) avg_px		
		FROM dba_hist_sqlstat
		WHERE 
			executions_total > 0
		GROUP BY	sql_id, plan_hash_value
	) t2
WHERE 
	t1.sql_id = nvl(TRIM('&sql_id.'), t1.sql_id)
	AND t1.depth = 0
	AND t1.sql_id = t2.sql_id(+)
	AND t1.plan_hash_value = t2.plan_hash_value(+)
order by avg_et_secs, cost --timestamp desc 
/
undef sql_id

