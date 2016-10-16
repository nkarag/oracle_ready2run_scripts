column resource_consumer_group format a30
column username format a30

SELECT decode(grouping(s.resource_consumer_group),1,'total_groups',s.resource_consumer_group) resource_consumer_group, decode(grouping(s.username),1,'total_sessions',s.username) username,  count(*) 
FROM v$session s, v$process p 
WHERE ( (s.username IS NOT NULL) 
AND (NVL (s.osuser, 'x') <> 'SYSTEM') 
AND (s.TYPE <> 'BACKGROUND') ) 
AND (p.addr(+) = s.paddr) 
AND s.username not in ('SYS','DBSNMP') 
GROUP BY rollup(s.resource_consumer_group, s.username)  
ORDER BY grouping_id(s.resource_consumer_group, s.username), resource_consumer_group nulls first, username; 