-----------------------------------------------------------------------

-----------------------------------------------------------------------
select name, round(value/1024/1024) as mb from v$statname 
natural join v$mystat where name in
(
'physical read total bytes',
'physical write total bytes',
'cell physical IO interconnect bytes',
'cell physical IO interconnect bytes returned by smart scan',
'cell physical IO bytes saved by storage index',
'cell flash cache read hits',
'cell physical IO bytes eligible for predicate offload',
'cell index scans',
'cell scans',
'table scans (direct read)',
'table scans (rowid ranges)'
)
or name like 'cell phy%';

