-----------------------------------------------------------------------------------------------------------------------
-- Find TOP N OWB nodes with the most DB time (cumulative) in the history of interest (from DBA_HIST_ACTIVE_SESS_HISTORY)
--
-- This query tries to join PL/SQL entry procedure from DBA_HIST_ACTIVE_SESS_HISTORY to DBA_PROCEDURES and from then to owbsys.all_rt_audit_executions
-- in order to obtain the OWB mapping/peocedure name (leaf node) and the name of the main flow.
-- 
-- There is also an optional input parameter (&sql_id) that if entered, the query returns the above metrics for a specific sql_id.
-- 
-- 	Parameters:
-- 		days_back   (optional)  Num of days back from sysdate of required history ASH history (from when ash sampling started)	
-- 		sql_id		(optional)	Specify an sql_id to find the performance metrics for a specific sql id		
-- 		monthly_only      (optional) 1 if we want to filter out all execution and keep the ones created on the 1st day of the month
-- 		Mondays_only      (optional) 1 if we want to filter out all execution and keep the ones created on Mondays
-- 		n			Specify "N" from top N	
-----------------------------------------------------------------------------------------------------------------------

col username format a8
col owb_object_type format a20
col sql_opname format a15
col sql_text format a130 trunc
col object_type format a11
col procedure_name format a30
col object_name format a30
col owb_node_name format a30
col main_flow format a30
col DBtime_secs format 999,999,999,999,999
col sql_id format a15
col r format 999

undef days_back

select /*+ parallel(16) */  
		row_number() over(order by DBtime_secs desc ) r,
		fin.DBtime_secs,
		fin.sql_id,
        fin.SQL_OPNAME,  
		fin.username,
        (   select execution_name 
            from owbsys.all_rt_audit_executions 
            where execution_audit_id =  (
                                            select max(TOP_LEVEL_EXECUTION_AUDIT_ID) 
                                            from owbsys.all_rt_audit_executions  
                                            where -- for the case where OWF appends a _XX suffix to OWB node names
                                                REGEXP_REPLACE(execution_name, '_\d\d$', '') like '%:'|| fin.owb_node_name 
                                                --execution_name like '%:'|| fin.owb_node_name
                                                --SUBSTR (execution_name, INSTR (execution_name, ':') + 1) =  fin.owb_node_name                                                
                                                AND created_on > sysdate - &&days_back
                                        )
        ) main_flow,            
        fin.owb_node_name,
        fin.owb_object_type,
        fin.object_owner,        
        fin.object_name,
        fin.object_type,
        fin.procedure_name,      
        fin.sql_text
from (
    select 
        decode(topn.USER_ID, 3839, 'ETL_DW', 3980, 'PERIF', (select username from dba_users where user_id = topn.user_id)) username,
 		case    when p.object_type = 'PACKAGE' AND p.procedure_name = 'MAIN' THEN p.object_name  
					when p.object_type = 'PACKAGE' AND p.procedure_name <> 'MAIN' THEN p.procedure_name
					when p.object_type = 'PROCEDURE' THEN p.object_name
					when p.object_type = 'FUNCTION' THEN p.object_name
					else p.object_name
        end owb_node_name,        
		case 	when p.object_type = 'PACKAGE' AND p.procedure_name = 'MAIN' THEN 'Mapping'  
				when p.object_type = 'PACKAGE' AND p.procedure_name <> 'MAIN' THEN 'Procedure'
				when p.object_type = 'PROCEDURE' THEN 'Procedure'
				when p.object_type = 'FUNCTION' THEN 'Function'
				else p.object_type
        end owb_object_type,
        p.owner object_owner,
        p.object_name,
        p.object_type,
        p.procedure_name,
        topn.sql_id,
        topn.SQL_OPNAME,
        topn.DBtime_secs, 
        -- convert CLOB to VARCHAR2 because in some cases I got an ORA-22275: invalid LOB locator specified
        case when s.sql_id is not null then dbms_lob.substr( s.sql_text, 130, 1 ) --s.SQL_TEXT
                    else null end sql_text       
    from
    (
        select *
        from (
            select h_ash.user_id, h_ash.PLSQL_ENTRY_OBJECT_ID, h_ash.PLSQL_ENTRY_SUBPROGRAM_ID, h_ash.sql_id, h_ash.SQL_OPNAME, sum(10) DBtime_secs
            from dba_hist_active_sess_history h_ash
            where 
				h_ash.sql_id = nvl('&sql_id', h_ash.sql_id)			
                -- basic filters
                and h_ash.user_id in (3839, 3980) -- ETL_DW, PERIF
                and h_ash.IN_SQL_EXECUTION = 'Y'
                and h_ash.IN_PLSQL_EXECUTION = 'N'
                and h_ash.IS_SQLID_CURRENT = 'Y'
                --and h_ash.PLSQL_ENTRY_OBJECT_ID is not null
                --and h_ash.PLSQL_ENTRY_SUBPROGRAM_ID is not null
                and h_ash.SQL_ID is not null
                --and SQL_OPCODE in (2, 7, 6, 189, 1) -- in ('INSERT', 'DELETE', 'UPDATE', 'UPSERT', 'CREATE TABLE')
                and h_ash.sample_time > sysdate - &days_back  
                AND to_char(h_ash.sql_exec_start, 'DD') = case when nvl('&montlh_only',0) = 1 then '01' else to_char(h_ash.sql_exec_start, 'DD') end    
                AND trim(to_char(h_ash.sql_exec_start, 'DAY')) = case when nvl('&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(h_ash.sql_exec_start, 'DAY')) end
            GROUP BY h_ash.user_id, h_ash.PLSQL_ENTRY_OBJECT_ID, h_ash.PLSQL_ENTRY_SUBPROGRAM_ID, h_ash.sql_id, h_ash.SQL_OPNAME
            order by DBtime_secs desc            
        ) where rownum <= '&n' 
    ) topn
        left outer join dba_procedures p on (topn.PLSQL_ENTRY_OBJECT_ID = p.OBJECT_ID AND topn.PLSQL_ENTRY_SUBPROGRAM_ID = p.SUBPROGRAM_ID)
            left outer join DBA_HIST_SQLTEXT s on (topn.sql_id = s.sql_id)
) fin
order by DBtime_secs desc
/