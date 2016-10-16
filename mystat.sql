col name for a60
col value for 99999999999999

REM  define a variable to hold the value in order to calculate the diff in mystat2 script
col value new_value prev_value
col name new_value stat_name

select name, value
from v$mystat s, v$statname n
where n.statistic# = s.statistic#
and name like nvl('&statistic_name',name)
/
