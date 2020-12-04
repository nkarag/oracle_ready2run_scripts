
prompt Check if incremental statistics gathering is used for a specific table ...

SELECT o.name, c.name, decode(bitand(h.spare2, 8), 8, 'yes', 'no') incremental--, o.*, h.*, c.*
FROM   sys.hist_head$ h, sys.obj$ o, sys.col$ c
WHERE  h.obj# = o.obj#
AND    o.obj# = c.obj#
AND    h.intcol# = c.intcol#
AND    o.name = upper('&table_name') AND  o.owner# = (select user_id from dba_users where username = upper('&table_owner'))
AND    o.subname is null
/