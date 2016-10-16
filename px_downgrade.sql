--If you want to know how many operations were downgraded on a running instance, and 
--by how much, you can execute the following query.
column value format 9999999,99
SELECT name, value
FROM v$sysstat
WHERE name like 'Parallel operations%'; 