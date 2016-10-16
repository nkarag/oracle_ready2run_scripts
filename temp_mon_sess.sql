-------------------------------------------------------
-- Monitor Temp segment usage per session.(if not specified find the most TEMP consuming sessions - write now!)
-------------------------------------------------------

col sid_serial for a15
col MB_USED for 999,999,999,999
col tablespace for a4
col USED_FOR for a10
col username for a30

SELECT S.sid || ',  ' || S.serial# sid_serial, t.inst_id, SUM (T.blocks) * TBS.block_size / 1024 / 1024 mb_used,
	T.SQL_ID, S.username, S.osuser, P.spid, S.module,
P.program,  T.segtype USED_FOR, T.tablespace,
COUNT(*) statements
FROM gv$tempseg_usage T, gv$session S, dba_tablespaces TBS, gv$process P
WHERE T.session_addr = S.saddr and T.SESSION_NUM = S.SERIAL# and t.inst_id = s.inst_id
AND S.paddr = P.addr and s.inst_id = p.inst_id
AND T.tablespace = TBS.tablespace_name
GROUP BY S.sid, S.serial#, t.inst_id, T.SQL_ID, S.username, S.osuser, P.spid, S.module, T.segtype,
P.program, TBS.block_size, T.tablespace
ORDER BY MB_USED desc, sid_serial
/



--SELECT S.sid || ',' || S.serial# sid_serial, S.username, S.osuser, P.spid, S.module,
--P.program, SUM (T.blocks) * TBS.block_size / 1024 / 1024 mb_used, T.tablespace,
--COUNT(*) statements
--FROM v$tempseg_usage T, v$session S, dba_tablespaces TBS, v$process P
--WHERE T.session_addr = S.saddr
--AND S.paddr = P.addr
--AND T.tablespace = TBS.tablespace_name
--GROUP BY S.sid, S.serial#, S.username, S.osuser, P.spid, S.module,
--P.program, TBS.block_size, T.tablespace
--ORDER BY MB_USED desc
--/
