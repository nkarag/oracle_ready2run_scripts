set pagesize 999
set lines 999
col event format a30 trunc
col p1text format a30 trunc
col p2text format a30 trunc
col p3text format a30 trunc
col p1 format 9999999999999999999
col p2 format 9999999999999999999
col p3 format 9999999999999999999

prompt Find where a session is waiting for (from GV$SESSION) this moment. Input you give the sid.

select 	sid, serial#, a.INST_ID, a.username,
		aa.name command_type,
		status, state, 
		WAIT_CLASS, EVENT, 
		--WAIT_TIME_MICRO/10e6 WAIT_TIME_SECS, 
		SECONDS_IN_WAIT, P1, P1TEXT, P2, P2TEXT, P3, P3TEXT, 
		 TIME_REMAINING_MICRO/1000000 TIME_REMAINING_SECS, TIME_SINCE_LAST_WAIT_MICRO
BLOCKING_INSTANCE, BLOCKING_SESSION, PDML_STATUS, PDDL_STATUS
from gv$session a join audit_actions aa on (a.COMMAND = aa.ACTION)
where 
inst_id = nvl('&inst_id', inst_id)
and  sid = nvl('&sid', sid)
and serial# = nvl('&serial', serial#)
--and a.wait_class <> 'Idle'
order by SECONDS_IN_WAIT desc
/
