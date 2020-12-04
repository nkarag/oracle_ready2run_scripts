---------------------------------------------------
-- for details see:
-- http://oradwstories.blogspot.com/2015/06/recursive-subquery-factoring-and-how-to.html
---------------------------------------------------

-- this is the 2nd version, where we dont use a counter for checking for the recursion end.

-- we assume ';' as the list delimiter. If not, use replace() to change it.
with q1
  as(
    select 1 grp, '1afgf;(2b  );3++&  ;4;5;6' s from dual
    union
	-- replace ',' to ';'
    select 2 grp, replace('asd, rrrr, dddd' ,
                        ',',';') s
    from dual
  ),
  q2(grp, element, list)
  as(
    select grp, regexp_substr(s,'^([^;]*);?',1,1,'i',1) first_elmnt, substr(s,regexp_instr(s,'^([^;]*);?',1,1,1)) rest_lst from q1 
    union all
    select grp, regexp_substr(q2.list,'^([^;]*);?',1,1,'i',1) first_elmnt, substr(q2.list,regexp_instr(q2.list,'^([^;]*);?',1,1,1)) rest_lst
    from q2
    where      
        regexp_like(q2.list,'^([^;]*);?')
  )
  SEARCH DEPTH FIRST BY element SET order_col 
select *
from q2
order by grp, order_col;


-- this is the 1st version
-- we assume ';' as the list delimiter. If not use replace() to change it.
with q1
  as(
    select 1 grp, '1afgf;(2b  );3++&  ;4;5;6' s from dual
    union
	-- replace ',' to ';'
    select 2 grp, replace('asd, rrrr, dddd' ,
                        ',',';') s
    from dual
  ),
  q2(grp, element, list, cnt)
  as(
    select grp, s first_elmnt, s lst, regexp_count(s,';')+1 cnt from q1 
    union all
    select grp, regexp_substr(q2.list,'^([^;]*);?',1,1,'i',1) first_elmnt, substr(q2.list,regexp_instr(q2.list,'^([^;]*);?',1,1,1)) rest_lst, q2.cnt-1
    from q2
    where
      q2.cnt > 0 
      --regexp_like(q2.list,'^([^;]*);?')
  )
select *
from q2
where 
  q2.element <> nvl(q2.list, 'x')
order by grp, cnt desc;