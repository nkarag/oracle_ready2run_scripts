REM --------------------------------------
REM It must be called after mystat so as to compute the difference in a statistic value
REM --------------------------------------

col name for a60
col value for 99999999999999

select name, value, value - &&prev_value   diff
from v$mystat s, v$statname n
where n.statistic# = s.statistic#
and name like nvl('&stat_name',name)
/

undef stat_name
undef prev_value
