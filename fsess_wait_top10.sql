select *
from (
select EVENT, WAIT_CLASS, TIME_WAITED/100 TIME_WAITED_SECS, TOTAL_WAITS, AVERAGE_WAIT/100 AVERAGE_WAIT_SECS, MAX_WAIT/100 MAX_WAIT_SECS 
from gv$session_event 
where 
inst_id = nvl('&inst_id', inst_id)
and  sid = nvl('&sid', sid)
order by TIME_WAITED desc
)
where rownum < 11
/