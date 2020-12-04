col is_top_plan for a12
col CPU_MANAGED for a12
col INSTANCE_CAGING for a20

set linesize 1000

select * from GV$RSRC_PLAN
order by is_top_plan desc, inst_id
/