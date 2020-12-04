----------------------------------------------------------------------------------------
--
-- File name:   rman_sess.sql
--
-- Purpose:     Monitor RMAN sessions and relevant OS processes
-
-- Author:      Nikos Karagiannidis (oradwstories.blogspot.gr)
--
-- Usage:       @rman_sess
--
-- Description: Monitor RMAN sessions and relevant OS processes
--
-----------------------------------------------------------------------------------------

SELECT b.sid, b.serial#, a.spid, b.client_info
FROM gv$process a, gv$session b
WHERE 
	a.addr = b.paddr
	AND   b.client_info LIKE '%rman%'
	AND a.inst_id = b.inst_id
/	