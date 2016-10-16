-- Lib Cache Lock : blockers and waiters 
-- from Kylie H

-- Output
--  WAITER WLOCKP1          WEVENT              BLOCKER BEVENT
-- ------- ---------------- ----------------- --------- -----------------
--     129 00000003B76AB620 library cache pin 135,15534 PL/SQL lock timer

column wevent format a20
column bevent format a20
column blocker format a10
column waiter format 99999


select
       waiter.sid   waiter,
       waiter.event wevent,
       to_char(blocker_event.sid)||','||to_char(blocker_session.serial#) blocker,
       substr(decode(blocker_event.wait_time,
                     0, blocker_event.event,
                    'ON CPU'),1,30) bevent,
       --blocker_event.event  bevent,
       blocker_session.SQL_HASH_VALUE sql_hash,
       sql.sql_text
from
       x$kglpn p,
       gv$session      blocker_session,
       gv$session_wait waiter,
       gv$session_wait blocker_event,
       gv$sqltext sql
where
       blocker_session.SQL_HASH_VALUE  =sql.HASH_VALUE (+)
   and (sql.PIECE=0 or sql.piece is null)
   and p.kglpnuse=blocker_session.saddr
   and p.kglpnhdl=waiter.p1raw
   and (waiter.event = 'library cache pin' or
        waiter.event = 'library cache lock' or
        waiter.event = 'library cache load lock')
   and blocker_event.sid=blocker_session.sid
   and waiter.sid != blocker_event.sid
order by
      waiter.p1raw,waiter.sid
/



