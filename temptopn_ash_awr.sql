------------------------------------------------------------------------------------------------
-- temptopn_ash_awr
--
-- Description
--	Find the top N temp space cosnumers from DBA_HIST_ACTIVE_SESS_HISTORY in the history of interest
--  You can filter also by a specific sql_id and/or username
--
-- Notes
--	For parallel queries, we sum up the temp allocation of all sessions of the parallel query at a specific sample time (of course
--	not all parallel slave session of a query will appear in ASH at a specific sample time - only the active ones), therefore the number is
--	somewhat approximate. Then we retrieve the maximum temp allocation (or summed temp allocation) for a query out of all these distinct sample times.
-- 	In the case of a parallel query, the column "max_num_of_sessions" will have a value greater than 1.
--
-- 	Parameters
-- 		days_back   (optional)  Num of days back from sysdate of required history ASH history (from when ash sampling started)	
-- 		sql_id		(optional)	Specify an sql_id to find the performance metrics for a specific sql id		
--		username	(optional)	Specify a username to filter ash samples for this user's sessions
-- 		monthly_only      (optional) 1 if we want to filter out all execution and keep the ones created on the 1st day of the month
-- 		Mondays_only      (optional) 1 if we want to filter out all execution and keep the ones created on Mondays
-- 		n			Specify "N" from top N	
--		
--	Author
--		Nikos Karagiannidis (C) 2014 - http://oradwstories.blogspot.gr/
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

--with temp_tot_gbs as (
--SELECT round(SUM (C.bytes) / 1024 / 1024/ 1024) gb_total FROM v$tablespace B, v$tempfile C WHERE B.ts#= C.ts# GROUP BY B.name, C.block_size
--)
select  row_number() over(order by topn.temp_space_allocated_GBs desc) r,
        topn.temp_space_allocated_GBs,
		(SELECT round(SUM (C.bytes) / 1024 / 1024/ 1024) gb_total FROM v$tablespace B, v$tempfile C WHERE B.ts#= C.ts# GROUP BY B.name, C.block_size) total_temp_gbs,
		--temp_tot_gbs.gb_total total_temp_gbs, 
		--round(topn.temp_space_allocated_GBs / temp_tot_gbs.gb_total) * 100 pcnt_used_from_total,
        round(topn.temp_space_allocated_GBs / (SELECT SUM (C.bytes) / 1024 / 1024/ 1024 gb_total FROM v$tablespace B, v$tempfile C WHERE B.ts#= C.ts# GROUP BY B.name, C.block_size) * 100) pcnt_used_from_total,
		topn.max_num_of_sessions,
        topn.sql_exec_start,
        topn.sql_id,
        topn.SQL_OPNAME, 
        u.username,
        p.owner object_owner,        
        p.object_name,
        p.object_type,
        p.procedure_name,       
        -- convert CLOB to VARCHAR2 because in some cases I got an ORA-22275: invalid LOB locator specified
		case when s.sql_id is not null then dbms_lob.substr( s.sql_text, 130, 1 ) --s.SQL_TEXT
			else null end sql_text
from (
    select *
    from (		
			select  h_ash.user_id, h_ash.PLSQL_ENTRY_OBJECT_ID, h_ash.PLSQL_ENTRY_SUBPROGRAM_ID, h_ash.sql_id, h_ash.SQL_OPNAME, h_ash.sql_exec_start,
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
						sql_id = nvl('&sql_id', sql_id)
					-- basic filters
					AND SESSION_TYPE = 'FOREGROUND'
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
			group by h_ash.user_id, h_ash.PLSQL_ENTRY_OBJECT_ID, h_ash.PLSQL_ENTRY_SUBPROGRAM_ID, h_ash.sql_id, h_ash.SQL_OPNAME, h_ash.sql_exec_start
			order by temp_space_allocated_GBs desc
    )
    where
        rownum <= '&n'
) topn
    left outer join dba_users u using(user_id)
        left outer join dba_procedures p on (topn.PLSQL_ENTRY_OBJECT_ID = p.OBJECT_ID AND topn.PLSQL_ENTRY_SUBPROGRAM_ID = p.SUBPROGRAM_ID)
            left outer join DBA_HIST_SQLTEXT s on (topn.sql_id = s.sql_id) --, temp_tot_gbs           
/			