---------------------------------------------------------------------
-- Monitor Temp segment usage per sql_id. (if not specified, find the most TEMP consuming sql_ids - write now!)	
---------------------------------------------------------------------

col sql_id for a15
col MB_USED for 999,999,999,999
col tablespace for a4
col USED_FOR for a10
col username for a30

SELECT T.SQL_ID, 
	SUM (T.blocks) * TBS.block_size / 1024 / 1024 mb_used,
	count(distinct S.sid || ',' || S.serial# ) num_of_sessions, T.inst_id,
	--S.sid || ',' || S.serial# sid_serial, 
	S.username, S.osuser, --P.spid, 
	S.module,--P.program,  
	T.segtype USED_FOR, T.tablespace,
COUNT(*) statements
FROM gv$tempseg_usage T, gv$session S, dba_tablespaces TBS, gv$process P
WHERE T.session_addr = S.saddr and T.SESSION_NUM = S.SERIAL# and t.inst_id = s.inst_id
AND S.paddr = P.addr and s.inst_id = p.inst_id
AND T.tablespace = TBS.tablespace_name
AND NVL(T.SQL_ID, 1) = NVL(trim('&sqlid'),NVL(T.SQL_ID, 1))
GROUP BY T.SQL_ID, --S.sid, S.serial#, 
		S.username, S.osuser, --P.spid, 
		S.module, T.segtype,
		--P.program, 
		TBS.block_size, T.tablespace, T.inst_id
ORDER BY MB_USED desc , T.SQL_ID 
/

--select *
--FROM gv$tempseg_usage T
--/