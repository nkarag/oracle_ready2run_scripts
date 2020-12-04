/***************************************************************************
--	rollbcack_time.sql
--	Estimate how much time is remaining for a session to rollback
--
ΒΗΜΑΤΑ

1.       Για το sid που  κάνει rollback (π.χ. 1645) τρέχουμε το παρακάτω select:

select username, sid, SERIAL#, a.inst_id, xid, START_TIME, START_SCN, NAME xname, b.STATUS xstatus, c.tablespace_name undotbspace,
c.segment_id undo_sgid, c.segment_name undo_sgname,
USED_UBLK used_undo_blks, LOG_IO, PHY_IO, CR_GET, CR_CHANGE
from gv$session a join gv$transaction b on (a.taddr = b.addr and a.saddr = b.ses_addr and a.inst_id = b.inst_id)
    join DBA_ROLLBACK_SEGS c on (b.xidusn = c.segment_id)
where
username = nvl(upper('&username'), username)
and sid = nvl('&sid', sid)

 

2.       Σημειώνουμε την τιμή της κολώνας «used_undo_blks», έστω U1

 

3.       Ξανατρέχουμε το παραπάνω query μετά από 1 λεπτό και σημειώνουμε πάλι την τιμή της κολώνας «used_undo_blks», έστω U2

 

4.       Για να βρούμε πόσα λεπτά απομένουν για να κάνουμε rollback, κάνουμε την πράξη:
U2 / (U1-U2)

Το παραπάνω δεν το λέω εγώ αλλά ο “σοφός Tom” https://asktom.oracle.com/pls/asktom/f?p=100:11:::::P11_QUESTION_ID:7143624535091
****************************************************************************/

col used_undo_blks_now new_value undo_blks_now
col used_undo_blks_later new_value undo_blks_later

SELECT username,
       sid,
       SERIAL#,
       a.inst_id,
       xid,
       START_TIME,
       START_SCN,
       NAME xname,
       b.STATUS xstatus,
       c.tablespace_name undotbspace,
       c.segment_id undo_sgid,
       c.segment_name undo_sgname,
       USED_UBLK used_undo_blks_now,
       LOG_IO,
       PHY_IO,
       CR_GET,
       CR_CHANGE
  FROM gv$session a
       JOIN
       gv$transaction b
          ON (    a.taddr = b.addr
              AND a.saddr = b.ses_addr
              AND a.inst_id = b.inst_id)
       JOIN DBA_ROLLBACK_SEGS c ON (b.xidusn = c.segment_id)
 WHERE     username = NVL (UPPER ('&&username'), username)
       AND sid = NVL ('&&sid', sid)
/

prompt Please wait while rollback time is calculated ...

-- sleep for a minute
exec dbms_lock.sleep(60)

SELECT username,
       sid,
       SERIAL#,
       a.inst_id,
       xid,
       START_TIME,
       START_SCN,
       NAME xname,
       b.STATUS xstatus,
       c.tablespace_name undotbspace,
       c.segment_id undo_sgid,
       c.segment_name undo_sgname,
       USED_UBLK used_undo_blks_later,
       LOG_IO,
       PHY_IO,
       CR_GET,
       CR_CHANGE
  FROM gv$session a
       JOIN
       gv$transaction b
          ON (    a.taddr = b.addr
              AND a.saddr = b.ses_addr
              AND a.inst_id = b.inst_id)
       JOIN DBA_ROLLBACK_SEGS c ON (b.xidusn = c.segment_id)
 WHERE     username = NVL (UPPER ('&&username'), username)
       AND sid = NVL ('&&sid', sid)
/

select round(&&undo_blks_later/ (&&undo_blks_now - &&undo_blks_later)) "Estimated Minutes for Rollback" 
from dual
/

undef username
undef sid
undef undo_blks_now
undef undo_blks_later