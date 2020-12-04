-- ----------------------------------------------------------------------------------------------
--    owb_pa_mviews.sql  
--
--    DESCRIPTION 
--    This script outputs the Mviews of a specific flow that are the most time consuming during refresh. 
--
--    Input Parameters:
--        flow_name    Give the name of the main flow for which the performance analysis must run. 
--        node_name   Name of the Subflow node which is the parent/ancestor of the mview refreshes
--       username     ETL_DW or PERIF. Name of the database user under which the flow runs  
--
--     NOTE:
--      The script reads data from table monitor_dw.owb_etlpa2_tmp4. This is a temporary table
--      created by script owb_pa.sql and contains all tree executions of specific level 1 subflows
--      of the last M (e.g. 15) days. We need this table in order to find the start and end dates
--      of the parent subflows (see input parameter "node_name") and then search in DBA_HIST_ACTIVE_SESS_HISTORY
--      for INSERT statements that refresh MVIEWS.
--
-- (C) 2015 Nikos Karagiannidis - http://oradwstories.blogspot.com    
-- ----------------------------------------------------------------------------------------------

declare 
    l_table_exists    number;
begin
    select count(*) into l_table_exists
    from dba_tables 
    where owner = 'MONITOR_DW'
        and table_name = upper('owb_pa_mviews_tmp1');

    if(l_table_exists > 0) then
        execute immediate 'drop table MONITOR_DW.owb_pa_mviews_tmp1';
    end if;
end;
/

--drop table monitor_dw.owb_pa_mviews_tmp1;

create table monitor_dw.owb_pa_mviews_tmp1
as
with q1
as(
    select *
    from monitor_dw.owb_etlpa2_tmp4
    where
        flow_name = '&&flow_name'
        and trim(node_name) = trim('&&node_name')
        and type = 'ProcessFlow'
    order by top_level_execution_audit_id        
),
ash
as(
    select /* materialize no_merge */ *
    from dba_hist_active_sess_history t join dba_hist_sqltext using(sql_id)
    where
        T.USER_ID = (select user_id from dba_users where username = upper('&&username'))
),
fin 
as(
    select /*+ materialize no_merge */ *
    from q1 
            join ash on(ash.sample_time between q1.created_on and q1.updated_on)
    where
        ash.sql_text like 'INSERT /*+ BYPASS_RECURSIVE_CHECK APPEND  */%'        
)
select  flow_name, node_path, sql_id,  to_char(regexp_substr(sql_text, 'INSERT /\*\+ BYPASS_RECURSIVE_CHECK APPEND  \*/ INTO ("\S+"\."\S+")',1,1,'i',1)) mview_name,
        count(distinct sample_time)*10 dur_secs
from fin
group by flow_name, node_path, sql_id,  to_char(regexp_substr(sql_text, 'INSERT /\*\+ BYPASS_RECURSIVE_CHECK APPEND  \*/ INTO ("\S+"\."\S+")',1,1,'i',1))
order by count(distinct sample_time) desc;

--select   to_char(regexp_substr('INSERT /*+ BYPASS_RECURSIVE_CHECK APPEND  */ INTO "KPI_DW"."GNV_RATEPLAN_COUNT_MV" SELECT /*+ PARALLEL(8) */ * FROM KPI_DW.GNV_RATEPLAN_COUNT_V'
--            ,'INSERT /\*\+ BYPASS_RECURSIVE_CHECK APPEND  \*/ INTO ("\S+"\."\S+")',1,1,'i',1)) from dual

exec dbms_stats.gather_table_stats('MONITOR_DW', upper('owb_pa_mviews_tmp1'))

-- uncomment to see output
/*
select * from monitor_dw.owb_pa_mviews_tmp1;
*/

-- spool result to csv
col d new_value fname1
select to_char(sysdate, 'yyyymmdd')||'_mviews_for_tuning.csv' d from dual;
@spool2csv monitor_dw owb_pa_mviews_tmp1 &fname1 |
       

undef flow_name
undef node_name
undef username
undef fname1