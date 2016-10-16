set linesize 999
column trace_file_name format a100

SELECT s.inst_id, s.sid, s.serial#,
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
FROM gv$instance i, 
     gv$session s, 
     gv$process p, 
     gv$px_process pp, 
     gv$shared_server ss
WHERE 
s.sid = nvl('&sid',(select sys_context('USERENV', 'SID') from dual))
AND s.paddr = p.addr and s.inst_id = p.inst_id
AND s.sid = pp.sid (+) and s.inst_id = pp.inst_id (+)
AND s.paddr = ss.paddr(+) and s.inst_id = ss.inst_id (+)
AND s.type = 'USER'
ORDER BY trace_file_name
/