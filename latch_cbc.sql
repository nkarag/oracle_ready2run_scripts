col object_name for a35
col cnt for 99999
SELECT
cnt, object_name, object_type,file#, dbablk, obj, tch, hladdr
FROM (
select count(*) cnt, rfile, block from (
SELECT /*+ ORDERED USE_NL(l.x$ksuprlat) */
--l.laddr, u.laddr, u.laddrx, u.laddrr,
dbms_utility.data_block_address_file(to_number(object,'XXXXXXXX')) rfile,
dbms_utility.data_block_address_block(to_number(object,'XXXXXXXX')) block
FROM
(SELECT /*+ NO_MERGE */ 1 FROM DUAL CONNECT BY LEVEL <= 100000) s,
(SELECT ksuprlnm LNAME, ksuprsid sid, ksuprlat laddr,
  TO_CHAR(ksulawhy,'XXXXXXXXXXXXXXXX') object
FROM x$ksuprlat) l,
(select indx, kslednam from x$ksled ) e,
(SELECT
indx
, ksusesqh sqlhash
   , ksuseopc
   , ksusep1r laddr
FROM x$ksuse) u
WHERE LOWER(l.Lname) LIKE LOWER('%cache buffers chains%')
AND u.laddr=l.laddr
AND u.ksuseopc=e.indx
AND e.kslednam like '%cache buffers chains%'
)
group by rfile, block
) objs,
x$bh bh,
dba_objects o
WHERE
bh.file#=objs.rfile
and bh.dbablk=objs.block
and o.object_id=bh.obj
order by cnt
/