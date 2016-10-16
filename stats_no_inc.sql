prompt find partitioned tables  where incremental statistics is not used ...

select distinct t2.owner, t2.table_name
from (
SELECT username owner, o.name tab_name, c.name col_name, decode(bitand(h.spare2, 8), 8, 'yes', 'no') incremental--, o.*, h.*, c.*
FROM   sys.hist_head$ h, sys.obj$ o, sys.col$ c,
(    select user_id, username from dba_users    )  u
WHERE  h.obj# = o.obj#
AND    o.obj# = c.obj#
AND    h.intcol# = c.intcol# AND
--AND    o.name = 'USAGE_FCT' AND 
o.owner# = u.user_id 
AND o.subname is null
) t1 join dba_part_tables t2 on (t1.tab_name = t2.table_name and t1.owner = t2.owner)
where t1.incremental = 'no'
order by 1,2 desc
/

--select distinct t2.owner, t2.table_name
--from (
--SELECT username owner, o.name tab_name, c.name col_name, decode(bitand(h.spare2, 8), 8, 'yes', 'no') incremental--, o.*, h.*, c.*
--FROM   sys.hist_head$ h, sys.obj$ o, sys.col$ c,
--(
--    select user_id, username from dba_users where username in (
--        select P.SCHEMA_NAME from MONITOR_DW.MONDW_INDEX_USAGE_TRG_SCHEMAS p where P.INCLUDE_IND =1
  --      union
    --    select 'ETL_DW' from dual
      --  union
        --select 'PERIF' from dual
        --union
        --select 'PRESTAGE_DW' from dual    
        --union
        --select 'STAGE_DW' from dual    
        --union
        --select 'SHADOW_DW' from dual    
        --union
        --select 'STAGE_DW' from dual    
        --union
        --select 'STAGE_PERIF' from dual    
        --union
        --select 'PRESTAGE_PERIF' from dual    
        --union
        --select 'SHADOW_PERIF' from dual                
        --)
    --)  u
--WHERE  h.obj# = o.obj#
--AND    o.obj# = c.obj#
--AND    h.intcol# = c.intcol# AND
----AND    o.name = 'USAGE_FCT' AND 
--o.owner# = u.user_id 
--AND o.subname is null
--) t1 join dba_part_tables t2 on (t1.tab_name = t2.table_name and t1.owner = t2.owner)
--where t1.incremental = 'no'
--order by 1,2 desc
--/