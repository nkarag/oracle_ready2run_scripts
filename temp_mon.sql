column MB_TOTAL format 999,999,999
column MB_TOTAL_LOCAL format 999,999,999
column mb_used format 999,999,999
column mb_free_from_local format 999,999,999

SELECT A.inst_id,  A.tablespace_name tablespace,  
    D.mb_total MB_TOTAL,
    A.total_blocks * D.block_size/1024/1024 MB_TOTAL_LOCAL,
    (A.used_blocks * D.block_size) / 1024 / 1024 mb_used,
    ((A.total_blocks * D.block_size)-  (A.used_blocks * D.block_size)) / 1024 / 1024 mb_free_from_local,
    round(((A.used_blocks * D.block_size) / 1024 / 1024)/D.mb_total * 100,2) PCNT_USED_FROM_TOTAL,
    round((A.used_blocks * D.block_size)/(A.total_blocks * D.block_size) * 100,2) PCNT_USED_FROM_LOCAL
FROM gv$sort_segment A,
(
SELECT B.name, C.block_size, SUM (C.bytes) / 1024 / 1024 mb_total
FROM v$tablespace B, v$tempfile C
WHERE B.ts#= C.ts#
GROUP BY B.name, C.block_size
) D
WHERE A.tablespace_name = D.name
order by 1
/


--SELECT A.tablespace_name tablespace, round((SUM (A.used_blocks * D.block_size) / 1024 / 1024)/D.mb_total,2) PCNT_USED,
--D.mb_total,
--SUM (A.used_blocks * D.block_size) / 1024 / 1024 mb_used,
--D.mb_total- SUM (A.used_blocks * D.block_size) / 1024 / 1024 mb_free
--FROM v$sort_segment A,
--(
--SELECT B.name, C.block_size, SUM (C.bytes) / 1024 / 1024 mb_total
---FROM v$tablespace B, v$tempfile C
--WHERE B.ts#= C.ts#
--GROUP BY B.name, C.block_size
--) D
--WHERE A.tablespace_name = D.name
--GROUP by A.tablespace_name, D.mb_total
--/