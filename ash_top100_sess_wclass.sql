set pagesize 999
set lines 999
col username format a13
col program  format a10 trunc
col modules  format a10 trunc
col session_id format 9999
col session_serial# format 99999
col inst_id format 9 justify right
col slq_id format justify right
col CPU format 99999 justify right
col WAIT_CLASS format a10 justify center
col WAITING format 999999999 JUSTIFY RIGHT
col TOTAL format 999999 justify right

select *
from (
select
 username, PROGRAM,module, SESSION_ID, SESSION_SERIAL#, INST_ID, sql_id,
    sum(decode(ash.session_state,'ON CPU',1,0)) CPU,
	WAIT_CLASS,
	sum(decode(ash.session_state,'WAITING',1,0)) WAITING,
    sum(1) TOTAL --sum(decode(session_state,'ON CPU',1,1))	TOTAL
from (
select sample_time , c.username, SESSION_ID, SESSION_SERIAL#, INST_ID, PROGRAM,module, IS_AWR_SAMPLE, a.sql_id, IS_SQLID_CURRENT, 
    SESSION_STATE, WAIT_TIME, WAIT_CLASS, EVENT, P1TEXT, P2TEXT, P3TEXT,  TIME_WAITED/100 time_waited_secs,
    BLOCKING_SESSION, BLOCKING_SESSION_SERIAL#, BLOCKING_INST_ID  
from gv$active_session_history a, 
 dba_users c
where 
a.user_id = c.user_id 
) ash
where  
	SAMPLE_TIME > sysdate - (&minutes_from_now/(24*60))
group by username, PROGRAM,module, SESSION_ID, SESSION_SERIAL#, INST_ID, sql_id, WAIT_CLASS
order by sum(1) desc -- sum(decode(session_state,'ON CPU',1,1)) desc
)
where rownum < 101
/