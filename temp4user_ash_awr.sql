------------------------------------------------------------------------------------------------
-- temp4user_ash_awr
--
-- Description
--	Find temp space consumption from DBA_HIST_ACTIVE_SESS_HISTORY in the history of interest
--  For a specific user
--
-- Notes
--	For parallel queries, we sum up the temp allocation of all sessions of the parallel query at a specific sample time (of course
--	not all parallel slave session of a query will appear in ASH at a specific sample time - only the active ones), therefore the number is
--	somewhat approximate. Then we retrieve the maximum temp allocation (or summed temp allocation) for a query out of all these distinct sample times.
-- 	In the case of a parallel query, the column "max_num_of_sessions" will have a value greater than 1.
--
-- 	Parameters
-- 		days_back   (optional)  Num of days back from sysdate of required history ASH history (from when ash sampling started)			
--		username	(optional)	Specify a username to filter ash samples for this user's sessions
-- 		monthly_only      (optional) 1 if we want to filter out all execution and keep the ones created on the 1st day of the month
-- 		Mondays_only      (optional) 1 if we want to filter out all execution and keep the ones created on Mondays	
--		
--	Author
--		Nikos Karagiannidis (C) 2017 - http://oradwstories.blogspot.gr/
-----------------------------------------------------------------------------------------------
col SQL_ID format a16 justify center
col r format 999
col username format a30
col sql_opname format a15
col sql_text format a130 trunc
col object_type format a11
col OBJECT_OWNER format a14
col procedure_name format a30
col object_name format a30
col total_temp_gbs format 999G999G999
col temp_space_allocated_GBs format 999G999G999


select time_window, sum(temp_space_allocated_GBs) tot_gbs_temp
from (
    -- group by sql_id and sample_time truncated to hh24 level
    select  h_ash.sql_id, h_ash.SQL_OPNAME, h_ash.sql_exec_start, trunc(sample_time, 'hh24') time_window,
                        max(h_ash.num_of_sessions) max_num_of_sessions,
                        round(max(H_ASH.TEMP_SPACE_ALLOCATED_SUM)/1024/1024/1024) temp_space_allocated_GBs
    from (
                        -- group by sample_time in order to sum up the temp allocation for all parallel slaves (in case of parallel queries)
                        select  user_id, 
                                PLSQL_ENTRY_OBJECT_ID, 
                                PLSQL_ENTRY_SUBPROGRAM_ID, 
                                sql_id, 
                                SQL_OPNAME, 
                                sql_exec_start,
                                sample_time,
                                count(distinct session_id) num_of_sessions,
                                sum(TEMP_SPACE_ALLOCATED) TEMP_SPACE_ALLOCATED_SUM  -- TEMP_SPACE_ALLOCATED: Amount of TEMP memory (in bytes) consumed by this session at the time this sample was taken
                        from dba_hist_active_sess_history 
                        where
                            extract (HOUR from sample_time) between 10 and 14 
                        -- basic filters
                        AND	SESSION_TYPE = 'FOREGROUND'
                        AND TEMP_SPACE_ALLOCATED is not null
                        and user_id = nvl((select user_id from dba_users where username = upper('&username')), user_id)
                        and IN_SQL_EXECUTION = 'Y'
                        and IN_PLSQL_EXECUTION = 'N'
                        and IS_SQLID_CURRENT = 'Y'
                        --and PLSQL_ENTRY_OBJECT_ID is not null
                        --and PLSQL_ENTRY_SUBPROGRAM_ID is not null
                        and SQL_ID is not null
                        --and SQL_OPCODE in (2, 7, 6, 189, 1) -- in ('INSERT', 'DELETE', 'UPDATE', 'UPSERT', 'CREATE TABLE')
                        and sql_exec_start > sysdate - &days_back  
                        AND to_char(sql_exec_start, 'DD') = case when nvl('&&monthly_only',0) = 1 then '01' else to_char(sql_exec_start, 'DD') end    
                        AND trim(to_char(sql_exec_start, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(sql_exec_start, 'DAY')) end    
                        group by user_id, PLSQL_ENTRY_OBJECT_ID, PLSQL_ENTRY_SUBPROGRAM_ID, sql_id, SQL_OPNAME, sql_exec_start, sample_time
                ) h_ash
                group by h_ash.sql_id, h_ash.SQL_OPNAME, h_ash.sql_exec_start, trunc(sample_time, 'hh24')
                order by trunc(sample_time, 'hh24'), temp_space_allocated_GBs desc
)                					
group by time_window
order by time_window					
					
					
select sysdate, extract (HOUR FROM systimestamp)
from dual