-- ----------------------------------------------------------------------------------------------
--  ash_ops.sql
--
--   Returns from Active Session History, for each operation of an execution plan for a specific sql id,
--  the DB time consumed as well as the Wall-Clock time consumed, for the last N minutes.
--
--  PARAMETERS
--
--  1.  how_to_sort        (optional)       Sort operations by DB time or Wall-Clock time (Default)
--  2.  minutes_from_now    (required)      Time interval for which you wish to examine ASH samples
--  3.  SQL_ID              (optional)      If not specified it will return results for ALL sql id in ASH!
--  4.  child_number        (optional)      Default 0
--
-- (C) 2015 Nikos Karagiannidis - http://oradwstories.blogspot.com    
-- ----------------------------------------------------------------------------------------------

set pagesize 999
set lines 999

def	how_to_sort = "W"
accept how_to_sort  prompt "Do you want operations sorted by DBtime desc (D) or Wall Clock time desc (W) - default is W:"

col sort_by_dbtime noprint new_value _SORT_BY_DBTIME 
col sort_by_wctime noprint new_value _SORT_BY_WCTIME 

set timing off

select decode(upper(nvl('&&how_to_sort','W')),'D','','--') sort_by_dbtime,
	decode(upper(nvl('&&how_to_sort','W')),'W','','--') sort_by_wctime
from dual;

set timing on

select   SQL_EXEC_START, SQL_PLAN_LINE_ID, SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type,
         count(*) "DB Time (secs)",
         count(distinct sample_time) "WC Time (secs)",
         db_time_secs "DB Time Total (secs)",
         wc_time_secs "WC Time Total (secs)",
         round(count(*)/db_time_secs *100) "DB Time (%)",
         round(count(distinct sample_time)/wc_time_secs *100) "WC Time (%)"   
from (
    select  SQL_EXEC_START, sample_time, SQL_PLAN_LINE_ID, A.SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type, --count(*) over() cnttot,
            count(*) over(partition by SQL_EXEC_START) db_time_secs, 
            count(distinct sample_time) over(partition by SQL_EXEC_START) wc_time_secs
    from gv$active_session_history a left outer join dba_objects b on(a.CURRENT_OBJ# = b.object_id)
    where        
        SAMPLE_TIME > sysdate - (&minutes_from_now/(24*60))
        and sql_id = nvl('&sql_id',sql_id) and SQL_CHILD_NUMBER = nvl('&SQL_CHILD_NUMBER',0)
        and sql_exec_id is not null
)            
group by SQL_EXEC_START, db_time_secs, wc_time_secs, SQL_PLAN_LINE_ID, SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type --, cnttot
order by SQL_EXEC_START desc, 
	&_SORT_BY_DBTIME "DB Time (%)" desc
	&_SORT_BY_WCTIME  "WC Time (%)" desc
/

--select SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type, round((count(*)/cnttot)*100) PCNT, count(*) nosamples, cnttot nosamplestot
--from (
--	select     A.SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type, count(*) over() cnttot
--	from gv$active_session_history a  join dba_objects b on(a.CURRENT_OBJ# = b.object_id)
--	where        
--		SAMPLE_TIME > sysdate - (&minutes_from_now/(24*60))
--		and sql_id = nvl('&sql_id',sql_id)
--)            
--group by SQL_PLAN_OPERATION, SQL_PLAN_OPTIONS, owner, object_name, object_type, cnttot
--order by count(*) desc
