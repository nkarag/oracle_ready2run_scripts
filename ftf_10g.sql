set linesize 999
column trace_file_name format a100

SELECT s.sid,
       s.server,
       lower(
         CASE  
           WHEN s.server IN ('DEDICATED','SHARED') THEN (SELECT value FROM v$parameter WHERE name = 'user_dump_dest')||'/'||
             i.instance_name || '_' || 
             nvl(pp.server_name, nvl(ss.name, 'ora')) || '_' || 
             p.spid
           ELSE
             NULL
         END 
       ) ||
       CASE
         WHEN p.traceid IS NOT NULL THEN
           '_' || p.traceid
         ELSE
           ''
       END ||
       '.trc' AS trace_file_name
FROM v$instance i, 
     v$session s, 
     v$process p, 
     v$px_process pp, 
     v$shared_server ss
WHERE s.paddr = p.addr
AND s.sid = pp.sid (+)
AND s.paddr = ss.paddr(+)
AND s.type = 'USER'
and s.sid = (select sys_context('USERENV', 'SID') from dual)
ORDER BY s.sid;