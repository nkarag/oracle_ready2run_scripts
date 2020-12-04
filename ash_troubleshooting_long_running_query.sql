-- ******************************************
-- Goal--> use ASH for troubleshooting a long running query
-- *****************************************

-- Step 1: Find the sql_id of the long_running query

    -- run @fsess and give as input the username and copy the sql_id and child_number
    
    -- alternatively you can run @ash_top100_sql  and pick the sql_id
    
-- Step 2: Analyze to top wait events of this query for a period of time (e.g., for the last 10 minutes)
    -- run @ash_events  

   select event, wait_class, P1TEXT, P2TEXT, P3TEXT, round((count(*)/cnttot)*100) PCNT, count(*) nosamples, cnttot nosamplestot
    from (
    select
    event, wait_class, P1TEXT, P2TEXT, P3TEXT,  count(*) over() cnttot
    from gv$active_session_history a  
    where  
    SAMPLE_TIME > sysdate - (&minutes_from_now/(24*60))
    and session_state= 'WAITING' and WAIT_TIME = 0
    and sql_id = nvl('&sql_id',sql_id)
    and SQL_CHILD_NUMBER = nvl('&SQL_CHILD_NUMBER',0)
    )t
    group by event, wait_class, P1TEXT, P2TEXT, P3TEXT, cnttot
    order by count(*) desc
/    


-- Step 4: try to minimize the wait

    -- find the operation that takes the most db time
    select SQL_PLAN_OPERATION, owner, object_name, object_type, round((count(*)/cnttot)*100) PCNT, count(*) nosamples, cnttot nosamplestot
    from (
        select     A.SQL_PLAN_OPERATION, owner, object_name, object_type, count(*) over() cnttot
        from gv$active_session_history a  join dba_objects b on(a.CURRENT_OBJ# = b.object_id)
        where        
            SAMPLE_TIME > sysdate - (&minutes_from_now/(24*60))
            and sql_id = nvl('&sql_id',sql_id)
    )            
    group by SQL_PLAN_OPERATION, owner, object_name, object_type, cnttot
    order by count(*) desc