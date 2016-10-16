set pagesize 999
set lines 999

select 
    decode(grouping(t2.username), 1, 'TOTAL',t2.username) username, 
    decode(grouping(t2.sql_id), 1, 'TOTAL', t2.sql_id) sql_id,
    decode(grouping(t2.sql_child_number),1,'TOTAL',t2.sql_child_number) sql_child_number, 
	decode(grouping(t1.inst_id),1,'TOTAL',t1.inst_id) inst_id, 
    count(server_name) num_px_procs
from gv$PX_PROCESS t1 join gv$session t2 on (t1.inst_id = t2.inst_id and t1.sid = t2.sid  and t1.serial# = t2.serial#)
        left outer join gv$sql t3 on (t2.inst_id = t3.inst_id and t2.sql_id = t3.sql_id and t2.sql_child_number = t3.child_number)
where
username = nvl(upper('&username'), username)
and t2.sql_id like nvl('&sql_id',t2.sql_id)
group by rollup(t2.username, (t2.sql_id, t2.sql_child_number), t1.inst_id)
order by grouping_id(t2.username) desc, t2.username, grouping_id(t2.sql_id, t2.sql_child_number) desc, t2.sql_id, t2.sql_child_number, grouping_id(t1.inst_id) desc, t1.inst_id
/
--having group_id() = 0
--order by  2, 3, 4, 1, grouping_id(t1.inst_id,t2.username, t2.sql_id, t2.sql_child_number)



--select inst_id, count(server_name)
--from gv$PX_PROCESS
--where (inst_id, sid,serial#) in (
--    select inst_id, sid, serial#
--    from gv$session
--    where 
--    username = nvl(upper('&username'), username)
--    and sid = nvl('&sid', sid)
--)
--group by rollup (inst_id)
--order by 1
--/