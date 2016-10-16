/*****************************************************************************
fosproc.sql
			Find OS process for a specific session 
	Notes:
			You might need it in the case you want to kill a process from the OS:
				kill -9 spid (in Unix), 
				orakill $ORACLE_SID spid (in Windows - where orakill.exe is in $ORACLE_HOME/bin)
******************************************************************************/
SELECT p.spid, s.sid, s.serial#, s.inst_id, s.username, s.osuser, s.machine, p.terminal, s.program
FROM gv$session s, gv$process p
WHERE s.paddr = p.addr
	AND s.INST_ID = p.INST_ID
	AND s.inst_id = nvl('&inst_id', s.inst_id)
	AND s.username = nvl(upper('&username'), s.username)
	and s.sid = nvl('&sid', sid)
/