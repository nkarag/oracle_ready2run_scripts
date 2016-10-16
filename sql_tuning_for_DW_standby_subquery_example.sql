select *
from tsmall
where
    id in (select /*+ NO_UNNEST */ id from tlarge)

with q as (
select id from tlarge
)
select tsmall.*
from tsmall join q on (tsmall.id = q.id);

--- rewrite OR condition to avoid FILTER operation
select *
from tsmall
where
    id between 10 and 100
    or
    id not in (select /*+ UNNEST */ id from tlarge)

    select *
    from tsmall
    where
        id between 10 and 100
UNION ALL    
    select *
    from tsmall
    where
        id not in (select id from tlarge)
    


---- rewrite NOT IN as ANTI-JOIN --------
    
select *
from tlarge
where
    id between 10 and 100
    and
    id not in (select id from tsmall)
    
select *
from tlarge left outer join tsmall on (tlarge.id = tsmall.id)
where
    tlarge.id between 10 and 100
    and tsmall.id IS NULL
            