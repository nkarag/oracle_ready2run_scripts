/******************************
-- how to produce 10 rows
******************************/

-- 1. with recursive subquery factoring (RSF)
with data(p)
as (
    select 1 from dual
    union all
    select p+1 from data
    where
        p < 10
)
select *
from data

-- with connect by
select level
from dual
connect by level <= 10

/******************************
-- how to run a hierarchical query
******************************/

    -- 1.. with CONNECT BY
 select level,
         lpad('*', 2*level, '*')||last_name
      from hr.employees
    start with manager_id is null
    connect by prior employee_id = manager_id
    order siblings by last_name
        
    
    -- 2. with RSF
 with emp_data(last_name,employee_id,manager_id,l)
    as
     (select last_name, employee_id, manager_id, 1 lvl from hr.employees where manager_id is null
      union all
      select emp.last_name, emp.employee_id, emp.manager_id, ed.l+1
        from hr.employees emp, emp_data ed
       where emp.manager_id = ed.employee_id
     )
    SEARCH DEPTH FIRST BY last_name SET order_by
   select l,
         lpad('*' ,2*l, '*')||last_name nm
     from emp_data
    order by order_by    
    