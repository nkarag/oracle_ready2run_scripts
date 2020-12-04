select result, round(ratio_to_report(count(result)) over() * 100) ||'%' happy_life_KPI
from (
select  decode(time_spent, 'FAMILY', 'enjoy', 'FRIENDS', 'enjoy', 'ORACLE', 'enjoy', 'deadly dull') result
from mylife.days
start with yesterday is null and status = 'BORN'
connect by prior day = yesterday and status <> 'DEAD'
)
group by result
order by 2 desc
/


select round(count(decode(result,'enjoy',1,null))/count(*) *100) ||'%' happy_life_KPI
from (
select  decode(time_spent, 'FAMILY', 'enjoy', 'FRIENDS', 'enjoy', 'ORACLE', 'enjoy', 'deadly dull') result
from mylife.days
start with yesterday is null and status = 'BORN'
connect by prior day = yesterday and status <> 'DEAD'
)
/