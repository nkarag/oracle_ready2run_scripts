-- Find the transactions of a specific session
set pagesize 999
set lines 999
col xname format a30
col START_SCN format 99999999999999999999999999


select username, sid, SERIAL#, a.inst_id, sql_id, xid, START_TIME, START_SCN, NAME xname, b.STATUS xstatus, c.tablespace_name undotbspace, 
c.segment_id undo_sgid, c.segment_name undo_sgname, 
USED_UBLK used_undo_blks, LOG_IO, PHY_IO, CR_GET, CR_CHANGE
from gv$session a join gv$transaction b on (a.taddr = b.addr and a.saddr = b.ses_addr and a.inst_id = b.inst_id)
	join DBA_ROLLBACK_SEGS c on (b.xidusn = c.segment_id)
where 
username = nvl(upper('&&username'), username)
AND a.sql_id = NVL ('&&sql_id', sql_id)
and sid = nvl('&&sid', sid)
and a.inst_id = nvl('&&inst_id', a.inst_id)
order by sql_id
/

undef username
undef sql_id
undef sid
undef inst_id