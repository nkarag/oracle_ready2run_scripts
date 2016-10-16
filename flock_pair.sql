set pagesize 9999
set lines 999
col ltype format a5
col ltype_desc format a15
col who format a8
col id1_tag format a50 trunc
col id2_tag format a50 trunc
col description format a70 trunc
col is_user_enqueue format a15
col username format a15
col mode_held_desc format a15
col mode_requested format 9
col mode_requested_desc format a19

SELECT DECODE(a.request,0,'Holder: ','Waiter: ') Who, 
		a.INST_ID,   a.sid sess, b.ORACLE_USERNAME username, 
		a.id1, a.id2, 
		a.type ltype, d.name ltype_desc, --d.id1_tag, d.id2_tag, d.is_user is_user_enqueue, d.description,
		a.lmode mode_held, 	decode(a.lmode,0,'none',1,'null (NULL)',2,'row-S (SS)',3, 'row-X (SX)',4,'share (S)',5,'S/Row-X (SSX)',6,'exclusive (X)') mode_held_desc, 
		a.request mode_requested, decode(a.request,0,'none',1,'null (NULL)',2,'row-S (SS)',3, 'row-X (SX)',4,'share (S)',5,'S/Row-X (SSX)',6,'exclusive (X)') mode_requested_desc, 
		round(a.ctime/60) time_mins,	
		c.owner obj_owner, c.object_name, c.object_type, 
		b.OS_USER_NAME, b.PROCESS os_process_id
   FROM GV$LOCK a join GV$LOCKED_OBJECT b on (a.sid = b.session_id and a.inst_id = b.inst_id) 
			join dba_objects c on (b.object_id = c.object_id)
			join v$lock_type d on (a.type = d.type)
 WHERE (a.id1, a.id2, a.type) IN (SELECT id1, id2, type FROM GV$LOCK WHERE request > 0)
   ORDER BY a.id1, a.request
/

set lines 180

--SELECT DECODE(request,0,'Holder: ','Waiter: ') Type, 
--       INST_ID,   sid sess, id1, id2, lmode, request, type
--   FROM GV$LOCK
-- WHERE (id1, id2, type) IN (SELECT id1, id2, type FROM GV$LOCK WHERE request > 0)
--   ORDER BY id1, request;
