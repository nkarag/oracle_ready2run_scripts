select sid, serial#, inst_id
from gv$session
where username = USER and (sid) = (select sid from v$mystat where rownum = 1)
/