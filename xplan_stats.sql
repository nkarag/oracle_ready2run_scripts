-- ----------------------------------------------------------------------------------------------
-- xplan_stats.sql
--
--	Return execution plan statistics by directly quering GV$SQL_PLAN_STATISTICS_ALL  
--	If STATISTICS_LEVEL is set to all (e.g. at the session level) - alter session set statistics_level = all;
--	or the hint /*+ gather_plan_statistics */ is used, then it returns also operation-level runtime statistics
--	(equivalent to dbms_xplan.display_cursor with an 'ALLSTATS LAST' format parameter)
--	Also note that the query must be completed in order to get these operation-level runtime statistics.
--	If not, then you just get the plan as it appears in v$sql_plan with no runtime statistics.
--
-- PARAMETERS
--   1. SQL_ID (required)
--
--	Notes:
--	makes the comparison between estimate and actual easier because includes starts*cardinality to get the e-rows_x_starts 
--	which can compare directly to a-rows, ie output_rows. See also: http://datavirtualizer.com/power-of-display_cursor/
--
-- (C) 2015 Nikos Karagiannidis - http://oradwstories.blogspot.com    
-- ----------------------------------------------------------------------------------------------

COLUMN id FORMAT 99999
COLUMN operation FORMAT a50
COLUMN options FORMAT a11
COLUMN actual_time FORMAT 99D999 
COLUMN object_name FORMAT a30 HEADING "Object|Name"
COLUMN last_starts FORMAT 999G999G999 
COLUMN a_rows FORMAT 999G999G999 
COLUMN e_rows_x_starts FORMAT 999G999G999 
COLUMN lio FORMAT 999G999G999 
COLUMN bgets FORMAT 999G999G999 
COLUMN pread FORMAT 999G999G999 
COLUMN pwrites FORMAT 999G999G999 
COLUMN LAST_MEMORY_USED FORMAT 999G999G999 
COLUMN last_memory_used_mbs FORMAT 999G999G999
COLUMN LAST_TEMPSEG_SIZE FORMAT 999G999G999 
COLUMN last_tempseg_size_mbs FORMAT 999G999G999
COLUMN LAST_EXECUTION FORMAT a20
column cost format 999G999G999
 
select 		id 
		, operation
		,object_name
        ,actual_time
        ,last_starts
        ,a_rows
		,e_rows_x_starts
		, COST
		, lio
		--,  bgets
		,  pread
		,  pwrites
		, LAST_DEGREE		-- Degree of parallelism used, during the last execution of the cursor
		, LAST_MEMORY_USED/1024 last_memory_used_mbs  -- LAST_MEMORY_USED: Memory size (in KB) used by this work area during the last execution of the curso
		, LAST_TEMPSEG_SIZE/1024/1024 last_tempseg_size_mbs  -- LAST_TEMPSEG_SIZE: Temporary segment size (in bytes) created in the last instantiation of this work area. This column is null if the last instantiation of this work area did not spill to disk.
		, LAST_EXECUTION	-- indicates whether this work area ran using OPTIMAL, ONE PASS, 
							-- or under ONE PASS memory requirement (MULTI-PASS), during the last execution of the cursor
from (
SELECT 	rank() over(order by inst_id) r -- use it as a filter to avoid multiple occurences due to many instances
		,id
        --,LPAD (' ', DEPTH) || operation operation
        --,options
		,LPAD(' ',depth)||OPERATION||'_'||OPTIONS  operation
		--,LPAD(' ',depth)||OPERATION||'_'||OPTIONS||' '||OBJECT_NAME  operation
		,object_name
        ,last_elapsed_time / 1e6 actual_time
        ,last_starts
        ,last_output_rows a_rows
		,last_starts * cardinality e_rows_x_starts
		, COST
		,last_cu_buffer_gets + last_cr_buffer_gets lio
		--, LAST_CR_BUFFER_GETS bgets
		, LAST_DISK_READS     pread
		, LAST_DISK_WRITES    pwrites
		, last_degree
		, LAST_MEMORY_USED
		, LAST_TEMPSEG_SIZE
		, LAST_EXECUTION	-- indicates whether this work area ran using OPTIMAL, ONE PASS, 
							-- or under ONE PASS memory requirement (MULTI-PASS), during the last execution of the cursor	
		, child_number
FROM gv$sql_plan_statistics_all
WHERE sql_id = '&sql_id' and child_number = '&child_number'
)
where r=1
ORDER BY  child_number, id
/

/*
SELECT
	id,
      LPAD(' ',depth)||P.OPERATION||'_'||P.OPTIONS||' '||P.OBJECT_NAME  operation
    , last_starts * cardinality e_rows_x_starts
    , last_output_rows  a_rows
    , LAST_CR_BUFFER_GETS bgets
    , LAST_DISK_READS     pread
    , LAST_DISK_WRITES    pwrites
    , round(LAST_ELAPSED_TIME/1e6)   elapsed_secs
    , LAST_MEMORY_USED
    , LAST_TEMPSEG_SIZE
    , LAST_EXECUTION
  FROM
       V$SQL_PLAN_statistics_all P
  WHERE
        sql_id='&sql_id'
  order by child_number,id
/
*/