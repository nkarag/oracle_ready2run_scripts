set pagesize 999
set lines 999
col BEGIN_INTERVAL_TIME format a25
col END_INTERVAL_TIME format a25

accept dateval prompt "enter value for a point in time ('DD-MM-YYYY HH24:MI:SS'): "
select 
	SNAP_ID
,	DBID
,	INSTANCE_NUMBER
,	BEGIN_INTERVAL_TIME
,	END_INTERVAL_TIME
from dba_hist_snapshot
where to_date('&dateval','DD-MM-YYYY HH24:MI:SS') between BEGIN_INTERVAL_TIME and END_INTERVAL_TIME;