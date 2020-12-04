col sid format 9999999
select sid, serial#
from v$session
where username = USER and (sid) = (select sid from v$mystat where rownum = 1)
/