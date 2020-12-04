select  decode(time_spent, 'FAMILY', 'enjoy', 'FRIENDS', 'enjoy', 'ORACLE', 'enjoy', 'deadly dull') result
from mylife.days
start with yesterday is null and status = 'BORN'
connect by prior day = yesterday and status <> 'DEAD'
/