set linesize 999
set pagesize 999

col avail_MBs format 999,999,999.99
col used_MBs format 999,999,999.99
col free_MBs format 999,999,999.99
--break on tablespace_name
--compute SUM LABEL TotalAvailableMBs OF avail_MBs --ON tablespace_name

select *
from (
SELECT dts.tablespace_name,NVL(ddf.bytes / 1024 / 1024, 0) avail_MBs,
    NVL(ddf.bytes - NVL(dfs.bytes, 0), 0)/1024/1024 used_MBs,
    NVL(dfs.bytes / 1024 / 1024, 0) free_MBs,
    TO_CHAR(NVL((ddf.bytes - NVL(dfs.bytes, 0)) / ddf.bytes * 100, 0), '990.00') "Used %",
    dts.contents,dts.extent_management,dts.status FROM sys.dba_tablespaces dts,
(select tablespace_name, sum(bytes) bytes
    from dba_data_files group by tablespace_name) ddf,
(select tablespace_name, sum(bytes) bytes
    from dba_free_space group by tablespace_name) dfs
WHERE dts.tablespace_name = ddf.tablespace_name(+)
    AND dts.tablespace_name = dfs.tablespace_name(+)
    AND NOT (dts.extent_management like 'LOCAL'
    AND dts.contents like 'TEMPORARY')
UNION ALL
SELECT dts.tablespace_name,NVL(dtf.bytes / 1024 / 1024, 0) avail,
    NVL(t.bytes, 0)/1024/1024 used,NVL(dtf.bytes - NVL(t.bytes, 0), 0)/1024/1024 free,
    TO_CHAR(NVL(t.bytes / dtf.bytes * 100, 0), '990.00') "Used %",dts.contents,
    dts.extent_management,dts.status
FROM sys.dba_tablespaces dts,
(select tablespace_name, sum(bytes) bytes
from dba_temp_files group by tablespace_name) dtf,
(select tablespace_name, sum(bytes_used) bytes
from v$temp_space_header group by tablespace_name) t
    WHERE dts.tablespace_name = dtf.tablespace_name(+)
    AND dts.tablespace_name = t.tablespace_name(+)
	AND dts.extent_management like 'LOCAL'
	AND dts.contents like 'TEMPORARY'
)
where tablespace_name = nvl(upper('&tblspace_name'), tablespace_name)
order by free_MBs desc
/ 