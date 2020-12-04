----------------------------------------------------------------------------------------
--
-- File name:   lib_cache_locks_objs.sql
--
-- Purpose:     Shows which objects and by what user are locked in the library cache.
--				(Oracle Reference Doc:)V$ACCESS displays information about locks that are currently imposed on library cache objects. 
--				The locks are imposed to ensure that they are not aged out of the library cache while they are required for SQL execution.
--             
-- Author:      Nikos Karagiannidis (http://oradwstories.blogspot.gr/)
---------------------------------------------------------------------------------------

col username for a30
col inst_id for 9
col sid for 99999
col owner for a30
col object for a50
col obj_type for a20

select b.username, a.inst_id, a.sid, a.owner, a.object, a.type obj_type
FROM GV$ACCESS a, GV$SESSION b
where 
    a.inst_id = b.inst_id
    and a.sid = b.sid
    and b.username = nvl(upper('&&username'),b.username)
    and a.owner = nvl(upper('&&owner'),a.owner)
    and a.object = nvl(upper('&&object'),a.object)
order by b.username, a.owner, a.object
/	

undef username
undef owner
undef object