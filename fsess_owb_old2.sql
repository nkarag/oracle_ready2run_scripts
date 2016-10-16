-----------------------------------------------------------------------------------------------
--	fsess_owb.sql
--
--	Find sessions invoked by Oracle Warehouse Builder (OWB) and show GV$SESSION info combined
--	with owbsys.all_rt_audit_executions info.
--
--	Note:
--		1.	We use GV$SESSION.CLIENT_INFO to join to owbsys.all_rt_audit_executions. 
-- 			In this version we use v$session.client_info as the "driver" for getting the owb info and not the v$session username for 
--			selected users (e.g., ETL_DW etc.), as in the previous version. This means that only sessions that start from OWB are returned. 
--			So, for example if I log in as ETL_DW and run a query from TOAD, this will not be returned.
--
--		2.	For cases where an OWB procedure invokes separate DB sessions, such in the case of MView refreshes, this script tries
--			to return also these sessions by using the PLSQL_ENTRY_OBJECT_ID column of V$SESSION. So if there is a session who has
--			a plsql entry that equals an OWB procedure, this will be returned even if it has a nul value in the client_info column.
--			Moreover, if the OWB procedure includes the pattern "like '%REFRESH%MV%'", then the srcipt returns all sessions of the same user
--			whose plsql entry procedure is DBMS_SNAPSHOT.REFRESH. These are usually DBMS_SCHEDULER session generated automatically by
--			DBMS_SNAPSHOT.REFRESH and we cannot connect them exactly to a specific OWB procedure. This is just the best that we could do 
--			in order to combine the OWB procedure with "refresh sessions" generated automatically by Oracle.
--	
--	Parameters
--		owb_node_name	(optional)	Provide the name of the OWB mapping or procedure for which you want to see the DB sessions
--									There must be a corresponding row in owbsys.all_rt_audit_executions for a running mapping/procedure
--									in order to return results. If left null, then all running mappings/procedures for which there is
--									a session in GV$SESSION with the client_info filled, will be returned
-----------------------------------------------------------------------------------------------

col owb_flow for a20
col blocker_main_flow for a20
col owb_name for a30
col blocker_owb_node for a30
col owb_audit_id for 99999999999999
col	owb_type format a15 trunc
col owb_status format a10 trunc
col owb_result format a10 trunc
col owb_created_on for a20
col owb_updated_on for a20

col username format a25
col inst_id format 9999999
col sid format 9999
col prog format a30 trunc
col logon_time for a20
col status format a15

col sql_text format a120 trunc
col prev_sql_text format a80 trunc
col event for a40
col wait_class format a20
col secs_in_wait for a20
col wait_state for a21
col sql_id format a20
col prev_sql_id format a20
col plan_hash_value format 99999999999999999
col prev_plan_hash_value format 99999999999999999
col sql_exec_start for a20
col PREV_EXEC_START for a20
col SQL_CHILD_NUMBER for 9999999999999
col PREV_CHILD_NUMBER for 9999999999999 
col entry_plsql_proc for a60 trunc
col blocking_instance format 9999999999999
col blocking_session format 99999999999999
col blocker	for a30
col KILL_BLOCKER_STMNT for a50
col osuser for a20
col osprocess for 999999999999
col port for 99999999
col terminal for a10
col machine for a50
col obj_owner for a20
col obj_name for a40
col obj_type for a10

col sw_event head EVENT for a40 truncate

col state format a10

col child for 99999
col avg_etime_secs for 999999,999.99
col wait_status for a21

break on sql_id
compute COUNT LABEL TotalSessions OF distinct sid on sql_id

col type for a10

alter session set NLS_DATE_FORMAT = 'dd-mm-yyyy HH24:mi:ss'
/

with owb_execs
as ( -- owb executions of interest
    select  execution_audit_id owb_audit_id,
            SUBSTR (execution_name, INSTR (execution_name, ':') + 1) owb_name,
            (select execution_name from owbsys.all_rt_audit_executions where execution_audit_id = t1.top_level_execution_audit_id) owb_flow,
            DECODE (task_type,'PLSQL', 'Mapping','PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', task_type) owb_type,
            execution_audit_status owb_status,
            return_result owb_result,
            created_on owb_created_on,
            updated_on owb_updated_on                         
    from owbsys.all_rt_audit_executions t1 
    where 1=1
        --AND EXECUTION_AUDIT_STATUS = 'BUSY' -- only running nodes
        AND SUBSTR (execution_name, INSTR (execution_name, ':') + 1) = nvl(upper(trim('&&owb_node_name')), SUBSTR (execution_name, INSTR (execution_name, ':') + 1))
        AND created_on > sysdate - 1 -- for performance reasons!
),
owb_sessions
as( --  these are the DB sessions directly joined to all_rt_audit_executions 
    --  (note: these are not all relevant sessions, since there are also the generated ones from procedures)
    select t1.*, t2.owb_name --, t2.*
    from gv$session t1 join owb_execs t2 on (t1.client_info = t2.owb_audit_id)
    where 1=1
--          AND client_info in (select owb_audit_id from owb_execs)
        AND client_info IS NOT NULL 
        AND regexp_like (client_info,'^\d+$') -- only numeric client_info value, so that you avoid an ORA-01722:invalid number
    order by t2.owb_flow    
),
owb_sid_all
as ( -- get all relevant sessions, even the generated ones from a PL/SQL  procedure call
    select inst_id, sid, serial#  from owb_sessions
    union
    select inst_id, sid, serial# from gv$session -- get generated sessions (e.g. sessions invoked by a call to DBMS_MVIEW.refresh)
    where PLSQL_ENTRY_OBJECT_ID in (select plsql_entry_object_id from owb_sessions)
    union   --  also in the case of an MV refresh OWB node, 
            --  get also all sessions of the same user that have a PLSQL_ENTRY_OBJECT_ID = DBMS_SNAPSHOT.REFRESH
			--  	and have started (SQL_EXEC_START) after the parent session (i.e., the owb procedure that triggers the refresh)
    select inst_id, sid, serial# 
    from gv$session
    where
        1 = case when (select count(*) from owb_sessions where owb_name like '%REFRESH%') > 0 THEN 1 ELSE 0 END
        AND	PLSQL_ENTRY_OBJECT_ID in (select object_id from dba_procedures where object_name = 'DBMS_SNAPSHOT' and procedure_name like 'REFRESH%')
		AND	username in (select username from owb_sessions where owb_name like '%REFRESH%')
		AND SQL_EXEC_START > (select min(SQL_EXEC_START) from owb_sessions where owb_name like '%REFRESH%')
),
owb_sessions_all
as(
    select *
    from gv$session
    where (inst_id, sid, serial#) in (select inst_id, sid, serial# from owb_sid_all)
),
final
as (
    select t2.*, t1.*, t3.owner obj_owner, t3.object_name obj_name, t3.object_type obj_type, T4.PLAN_HASH_VALUE, t4.executions, t4.sql_text,T5.PLAN_HASH_VALUE prev_plan_hash_value, t5.executions prev_executions, t5.sql_text prev_sql_text
    from owb_sessions_all t1 
            left outer join owb_execs t2 on (t1.client_info = t2.owb_audit_id)
                left outer join dba_objects t3 on  (t1.ROW_WAIT_OBJ# = t3.object_id) 
                    left outer join gv$sql t4 on (t1.sql_id = T4.SQL_ID AND t1.sql_child_number = T4.CHILD_NUMBER AND t1.inst_id = t4.inst_id)
                        left outer join gv$sql t5 on (t1.prev_sql_id = T5.SQL_ID AND t1.prev_child_number = T5.CHILD_NUMBER AND t1.inst_id = t5.inst_id)
)     
select  --*** OWB stuff
        owb_flow,
        username,
        owb_name,
        owb_type,
        owb_audit_id,       
        owb_created_on,
        owb_updated_on,
        owb_status,
        owb_result,
        --*** session identifiers 
        inst_id,
        sid,
        serial#,
        logon_time,
        status,
        program prog,
        --*** running sql
        sql_id,
        sql_child_number,
        sql_exec_start,
        plan_hash_value,
        sql_text,
        (select owner||'.'||object_name||'.'||procedure_name from dba_procedures where object_id = f.plsql_entry_object_id and subprogram_id = f.PLSQL_ENTRY_SUBPROGRAM_ID)
            entry_plsql_proc,
        --*** wait info
        CASE WHEN state != 'WAITING' THEN 'WORKING'
             ELSE 'WAITING'
        END 
            wait_state,  
        CASE WHEN state != 'WAITING' THEN 'On CPU / runqueue'
             ELSE WAIT_CLASS
        END                          
           WAIT_CLASS,
        CASE WHEN state != 'WAITING' THEN 'On CPU / runqueue'
        ELSE event
        END 
            event,
        CASE WHEN state != 'WAITING'  THEN  '(last wait) '  
        else ''
        END || round(WAIT_TIME_MICRO/1e6,1)
            secs_in_wait,
--            WAIT_TIME_MICRO/1e6,
--            TIME_SINCE_LAST_WAIT_MICRO/1e6,
        --*** object waiting for
        obj_owner,
        obj_name,
        obj_type,            
        --*** blocking session info
        blocking_instance,
        blocking_session,
        (select username from gv$session where inst_id = f.blocking_instance and sid = f.blocking_session) 
            blocker,
        (select SUBSTR (execution_name, INSTR (execution_name, ':') + 1) from owbsys.all_rt_audit_executions where execution_audit_id = (select client_info from gv$session where inst_id = f.blocking_instance and sid = f.blocking_session and regexp_like (client_info,'^\d+$')))
			blocker_owb_node,
        (select execution_name from owbsys.all_rt_audit_executions where execution_audit_id = (select top_level_execution_audit_id from owbsys.all_rt_audit_executions  where execution_audit_id = (select client_info from gv$session where inst_id = f.blocking_instance and sid = f.blocking_session and regexp_like (client_info,'^\d+$')))) 
			blocker_main_flow,            			
        (select 'exec kill_session('||sid||', '||serial#||', '||inst_id||')' from gv$session where inst_id = f.blocking_instance and sid = f.blocking_session)
            kill_blocker_stmnt,                                                         
        --*** OS identifiers
        osuser,
        process osprocess,
        machine,
        port,
        terminal,        			
        --*** previous sql
        prev_sql_id,
        prev_child_number,
        prev_exec_start,
        prev_plan_hash_value,
        prev_sql_text        
from final f
order by owb_flow, username, owb_name
/

undef owb_node_name