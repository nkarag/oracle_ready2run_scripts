column chain_id format 9999
column num_waiters format 999
column blocker_osid format 9999
column osid format 9999

SELECT chain_id, num_waiters, in_wait_secs, osid, sid, sess_serial#, blocker_osid, blocker_sid, blocker_sess_serial#, substr(wait_event_text,1,30) 
FROM v$wait_chains;