/***************************************************************************
--	rollback_t_sqlid.sql
--	Estimate how much time is remaining for all sessions of a sql_id to rollback
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

--col used_undo_blks_now new_value undo_blks_now

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
       AND a.sql_id = NVL ('&&sql_id', sql_id);
/

prompt Please wait while rollback time is calculated ...

----------------------------------------------
-- main
----------------------------------------------
set serveroutput on
set verify off

declare
	l_undo_blks_now		number;
	l_undo_blks_later	number;
	l_sleep_time_secs	number	:=	1;
	l_est_time			number;
begin
	-- get undo blocks used now
	SELECT sum(USED_UBLK)  into  l_undo_blks_now
	FROM gv$session a
		   JOIN
		   gv$transaction b
			  ON (    a.taddr = b.addr
				  AND a.saddr = b.ses_addr
				  AND a.inst_id = b.inst_id)
		   JOIN DBA_ROLLBACK_SEGS c ON (b.xidusn = c.segment_id)
	 WHERE     username = NVL (UPPER ('&&username'), username)
		   AND a.sql_id = NVL ('&&sql_id', sql_id);
		   
	if (l_undo_blks_now is null) then
		dbms_output.put_line(chr(10)||chr(10)||'Sorry, the corresponding transaction could not be found!');
		return;
	end if;
		
	-- sleep for some time
	dbms_lock.sleep(l_sleep_time_secs);	
	
	-- get undo blocks used, again ...
	SELECT sum(USED_UBLK)  into  l_undo_blks_later
	FROM gv$session a
		   JOIN
		   gv$transaction b
			  ON (    a.taddr = b.addr
				  AND a.saddr = b.ses_addr
				  AND a.inst_id = b.inst_id)
		   JOIN DBA_ROLLBACK_SEGS c ON (b.xidusn = c.segment_id)
	 WHERE     username = NVL (UPPER ('&&username'), username)
		   AND a.sql_id = NVL ('&&sql_id', sql_id);

	if (l_undo_blks_later < l_undo_blks_now) then
		l_est_time :=	round(	(l_undo_blks_later * l_sleep_time_secs/60) / (l_undo_blks_now - l_undo_blks_later)	); 
		dbms_output.put_line(chr(10)||chr(10)||chr(10)||'Estimated Minutes for Rollback is: '||l_est_time||' (mins)');
	else
		dbms_output.put_line(chr(10)||chr(10)||chr(10)||'No rollback takes place!');
	end if;
end;
/

undef username
undef sql_id
--undef undo_blks_now

set serveroutput off
set verify on
