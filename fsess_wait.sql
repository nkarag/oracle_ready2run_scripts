prompt Find all wait events waited by this session (from GV$SESSION_EVENT). Ordered DESC by TIME_WAITED

select inst_id, sid, EVENT, WAIT_CLASS, TIME_WAITED/100 TIME_WAITED_SECS, TOTAL_WAITS, AVERAGE_WAIT/100 AVERAGE_WAIT_SECS, MAX_WAIT/100 MAX_WAIT_SECS 
from gv$session_event 
where 
inst_id = nvl('&inst_id', inst_id)
and  sid = nvl('&sid', sid)
order by inst_id, sid, TIME_WAITED desc
/