-- ----------------------------------------------------------------------------------------------
--	owb_pa.sql  (ETL Performance Analysis script)
--
--	DESCRIPTION 
--    This script outputs the OWB mappings/procedures of specific flows that are in need of tuning.
--    In order to do this the script follows a top-down approach (from the high-level flow to the 
--    individual mapping/procedure) based on a set of established time-thresholds (i.e., the maximum
--    acceptable execution time for each flow)
--
--	Input Parameters:
--		flow_name	Give the name of the main flow for which the performance analysis must run. 
--					Press <Enter> to analyse all critical flows.
--
--	Version Info
--		v1:	Initial Release
--
-- (C) 2015 Nikos Karagiannidis - http://oradwstories.blogspot.com    
-- ----------------------------------------------------------------------------------------------

-----------------------------------------------------------
--
-- *** STEP 1: Find Specific Flow Executions for Further Drill-to-Detail Analysis.
--
--             Compare Flows Execution of the last M days to Thresholds.
--             Find those flows whose p80 time is outside the [low, high] interval.
--
-----------------------------------------------------------

declare 
	l_table_exists	number;
begin
	select count(*) into l_table_exists
	from dba_tables 
	where owner = 'MONITOR_DW'
		and table_name = upper('owb_etlpa2_tmp1');

	if(l_table_exists > 0) then
		execute immediate 'drop table MONITOR_DW.owb_etlpa2_tmp1';
	end if;
end;
/

--drop table MONITOR_DW.owb_etlpa2_tmp1;

create table monitor_dw.owb_etlpa2_tmp1
as
with fl_execs
as(
  select  t2.flow_name,
          t1.created_on,
          t1.dur_hrs_clean,
          t1.p80_dur_hrs_clean,
          t2.HRS_LOW_BOUND,
          t2.HRS_HIGH_BOUND,
          t1.execution_audit_id,
          t1.top_level_execution_audit_id
  from  monitor_dw.v_dwp_flows_exec_times t1
        join monitor_dw.dwp_etl_flows_thresholds t2 on(t1.execution_name = t2.flow_name)
  where 1=1
    AND t1.execution_name = nvl('&&flow_name', t1.execution_name)
    AND t1.execution_audit_status = 'COMPLETE'
    AND t1.return_result = 'OK'
    --AND NOT(t1.p80_dur_hrs_clean between t2.hrs_low_bound AND t2.hrs_high_bound)
    AND t1.p80_dur_hrs_clean > t2.hrs_high_bound
  --order by t2.flow_name, t1.dur_hrs_clean desc
)
select  rank() over(order by (p80_dur_hrs_clean - HRS_HIGH_BOUND)/HRS_HIGH_BOUND  desc) ranking,
        flow_name,
        round((p80_dur_hrs_clean - HRS_HIGH_BOUND)/HRS_HIGH_BOUND * 100) pct_deviation,
        created_on,
        dur_hrs_clean,
        p80_dur_hrs_clean,
        HRS_LOW_BOUND,
        HRS_HIGH_BOUND,
        execution_audit_id,
        top_level_execution_audit_id
from fl_execs --fl_execs_rank
where 1=1
  --AND r = 1
order by ranking, created_on desc;

exec dbms_stats.gather_table_stats('MONITOR_DW', upper('owb_etlpa_tmp1'))

select * from monitor_dw.owb_etlpa2_tmp1;

-- spool result to csv
col d new_value fname1
select to_char(sysdate, 'yyyymmdd')||'_FlowsDeviations.csv' d from dual;
@spool2csv monitor_dw owb_etlpa2_tmp1 &fname1 |

-----------------------------------------------------------
--
-- *** STEP 2: Find Critical Path for all the  flow executions
--             in the period of investigation eg 15 days (not just one execution per flow)
--
--             Select the nodes per flow with a p80-duration (and not the duration of a single execution) above a certain threshold 
--
-----------------------------------------------------------

declare 
	l_table_exists	number;
begin
	select count(*) into l_table_exists
	from dba_tables 
	where owner = 'MONITOR_DW'
		and table_name = upper('owb_etlpa2_tmp2');

	if(l_table_exists > 0) then
		execute immediate 'drop table MONITOR_DW.owb_etlpa2_tmp2';
	end if;
end;
/

-- drop table monitor_dw.owb_etlpa2_tmp2;


create table monitor_dw.owb_etlpa2_tmp2
parallel 32
as 
with q1 
as ( 
  -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
  select /*+  qb_name(subq_factoring)  materialize no_merge dynamic_sampling (4)  */  --   parallel(32) full(a)   
	  execution_audit_id, 
	  parent_execution_audit_id,
	  TOP_LEVEL_EXECUTION_AUDIT_ID,
	  execution_name,
	  (select execution_name from owbsys.all_rt_audit_executions where execution_audit_id = a.TOP_LEVEL_EXECUTION_AUDIT_ID) flow_name,
	  task_type,
	  created_on,
	  updated_on,            
	  row_number() over(partition by parent_execution_audit_id order by (updated_on - created_on) desc) lpf_ind, -- longest per family indicator
	  PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((updated_on - created_on) * 24 * 60, 1)  ASC) OVER (partition by execution_name) p80_duration_mins,
	  round(avg((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) avg_duration_mins,
	  round(stddev((updated_on - created_on) * 24 * 60) OVER (partition by execution_name) ,1) stddev_duration_mins,
	  round(min((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) min_duration_mins,
	  round(max((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) max_duration_mins,
	  count(distinct TOP_LEVEL_EXECUTION_AUDIT_ID||execution_name) OVER (partition by execution_name) numof_executions        
  from  owbsys.all_rt_audit_executions a
  where
	  TOP_LEVEL_EXECUTION_AUDIT_ID in (select top_level_execution_audit_id from monitor_dw.owb_etlpa2_tmp1)
),
t1 
as (
	select /*+ dynamic_sampling (4)  qb_name(owb_hierarch_qry) no_merge materialize  */  
	  CONNECT_BY_ROOT a.execution_name root_execution_name,
	  a.lpf_ind,
	  a.created_on,
	  a.updated_on,        
	  prev.created_on prev_created_on,
	  prev.updated_on prev_updated_on,                
	  level flow_level,
	  lpad(' ', 2*(level - 1))||REGEXP_REPLACE(REGEXP_REPLACE(a.execution_name, '_\d\d$', ''),'\w+:' , '' ) execution_name_short,    
	  DECODE (a.task_type,'PLSQL', 'Mapping','PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', a.task_type)      TYPE,
	  lpad(' ', 2*(level - 1))||REGEXP_REPLACE(REGEXP_REPLACE(prev.execution_name, '_\d\d$', ''),'\w+:' , '' ) prev_execution_name_short,    
	  DECODE (prev.task_type,'PLSQL', 'Mapping','PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', prev.task_type)      PREV_TYPE,                
	  lpad(' ', 2*(level - 1),'   ')||ROUND ( (a.updated_on - a.created_on) * 24 * 60, 1) duration_mins,
	  ROUND ( (prev.updated_on - prev.created_on) * 24 * 60, 1) prev_duration_mins,
	  lpad(' ', 2*(level - 1),'   ')||a.p80_duration_mins p80_duration_mins,
	  lpad(' ', 2*(level - 1),'   ')||a.avg_duration_mins avg_duration_mins,
	  lpad(' ', 2*(level - 1),'   ')||a.stddev_duration_mins stddev_duration_mins,
	  lpad(' ', 2*(level - 1),'   ')||a.min_duration_mins min_duration_mins,
	  lpad(' ', 2*(level - 1),'   ')||a.max_duration_mins max_duration_mins,
	  lpad(' ', 2*(level - 1),'   ')||a.numof_executions numof_executions,
	  --PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((updated_on - created_on) * 24 * 60, 1)  ASC) OVER (partition by execution_name) p80_duration_mins,            
	  a.TOP_LEVEL_EXECUTION_AUDIT_ID,
	  CONNECT_BY_ROOT a.execution_audit_id root_execution_audit_id,        
	  a.execution_audit_id, 
	  a.execution_name,
	  prev.execution_audit_id prev_execution_audit_id,
	 ROUND((CONNECT_BY_ROOT a.updated_on - a.created_on) * 24 * 60, 1) mins_until_end_of_root,
	 --b.critical_ind,
	 ROUND ( (CONNECT_BY_ROOT a.updated_on - CONNECT_BY_ROOT a.created_on) * 24, 1) root_duration_hrs,
	 CONNECT_BY_ROOT a.created_on root_created_on,
	 CONNECT_BY_ROOT a.updated_on root_updated_on,
	 CONNECT_BY_ISLEAF "IsLeaf",
	 SYS_CONNECT_BY_PATH (SUBSTR (a.execution_name, INSTR (a.execution_name, ':') + 1),'/') path         
	from q1 a 
		  left outer join q1 prev
		  on (
				  a.TOP_LEVEL_EXECUTION_AUDIT_ID = prev.TOP_LEVEL_EXECUTION_AUDIT_ID -- same root execution
				  AND a.parent_execution_audit_id = prev.parent_execution_audit_id --same family
				  AND a.created_on between prev.updated_on and (prev.updated_on + 2/24/60/60 )-- sequential execution
				  --AND trunc(a.created_on, 'MI') = trunc(prev.updated_on, 'MI') -- sequential execution
				  AND a.execution_audit_id <> prev.execution_audit_id -- avoid node selfjoin 
			 )                  
	WHERE 1=1 
		--and level = 2     
	   --AND a.task_type <> 'AND'
	   --AND a.task_type NOT IN ('AND') AND prev.task_type NOT IN ('AND')  
	  --task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
	 --AND CONNECT_BY_ISLEAF = 1   
	 --AND CONNECT_BY_ROOT execution_name = b.flow_name
	  --SUBSTR (execution_name, INSTR (execution_name, ':') + 1) NOT LIKE '%CHECK%' -- exclude "check" nodes
	START WITH  a.execution_name = a.flow_name 
	CONNECT BY  PRIOR a.execution_audit_id = a.parent_execution_audit_id
	ORDER SIBLINGS BY  a.TOP_LEVEL_EXECUTION_AUDIT_ID desc, a.created_on asc, a.p80_DURATION_MINS DESC -- a.p80_DURATION_MINS DESC
),
t2
as (
	select /*+ qb_name(T2_block) parallel(32) materialize no_merge */ count(*) over(partition by t1.execution_audit_id ) cnt,  -- find nodes with more than one "previous nodes"
		  t1.*
	from t1
	where 
	  -- filter on level 2 to get the cpath under the root node
	  t1.flow_level = 2     
),
t3
as (
	select /*+ qb_name(seq_path_qry) no_merge inline */ CONNECT_BY_ISLEAF "IsLeaf2",
		 LEVEL seq_length, SYS_CONNECT_BY_PATH(execution_name_short||' ['||trim(to_char(duration_mins))||', p80-'||trim(to_char(p80_duration_mins))||', TYPE:'||type||']', ' -->') SEQ_PATH,
		 nkarag.EVAL(SYS_CONNECT_BY_PATH(NVL(TO_CHAR(duration_mins),'NULL'),'+')) PATH_DURATION_MINS,               
		 t2.duration_mins + prior t2.duration_mins,
		 T2.*
	from t2
	where      
	  -- exclude AND nodes as "previous nodes" in cases where 2 or more "previous nodes" exist
	  not regexp_like( t2.prev_execution_name_short,  case when t2.cnt > 1 then '^\s*AND(\d|_)+.*' ELSE 'xoxoxo' end)
	  --regexp_like (t2.prev_execution_name_short,  case when t2.cnt > 1 then '^AND\d+{0}?' ELSE '.*' end)
	  -- exclude OR nodes as "previous nodes" in cases where 2 or more "previous nodes" exist
	  AND not regexp_like( t2.prev_execution_name_short,  case when t2.cnt > 1 then '^\s*OR(\d|_)+.*' ELSE 'xoxoxo' end)                         
	start with t2.prev_created_on is null
	connect by NOCYCLE  t2.prev_execution_audit_id = prior t2.execution_audit_id             
),
t4
as (
	select /*+ qb_name(T4_block) no_merge materialize */  root_execution_name, TOP_LEVEL_EXECUTION_AUDIT_ID,
		  root_created_on, 
		  root_updated_on, 
		--  flow_level, 
		  SEQ_PATH, seq_length, 
		  max(seq_length) over(partition by root_created_on) maxlength,
		  PATH_DURATION_MINS,
		  max(PATH_DURATION_MINS) over(partition by root_created_on) max_path_dur_mins
	from t3
	order by root_created_on desc, seq_length desc			   
),
t5
as (
	select  /*+ qb_name(main) leading(@subq_factoring)  no_merge materialize     */
			  row_number() over(partition by  root_created_on order by  path_duration_mins desc, seq_length desc) r,    
			  root_execution_name, TOP_LEVEL_EXECUTION_AUDIT_ID,
			  root_created_on, 
			  root_updated_on, 
		   --   flow_level, 
			  PATH_DURATION_MINS,   
			  seq_length,         
			  SEQ_PATH CRITICAL_PATH     
	from t4
	where 1=1
	  AND max_path_dur_mins = path_duration_mins
	  --AND seq_length = maxlength -- relax this restriction because it is not true always and thus it prunes flow executions (missing days from the result)            
),
t6
as(
	select /*+ materialize */ *
	from t5
	where
	  r = 1    
	--order by root_created_on desc, path_duration_mins desc, seq_length desc
)
select *
from t6
order by root_execution_name, top_level_execution_audit_id desc;


exec dbms_stats.gather_table_stats('MONITOR_DW', upper('owb_etlpa2_tmp2'))

-- uncomment to see output
/*
select * from monitor_dw.owb_etlpa2_tmp2
order by root_execution_name, top_level_execution_audit_id desc;
*/

-- create a view in order to sort the result appropriately 
create or replace view monitor_dw.v_owb_etlpa2_tmp2
as
select * from monitor_dw.owb_etlpa2_tmp2
order by root_execution_name, top_level_execution_audit_id desc;

-- spool result to csv
col d new_value fname2
select to_char(sysdate, 'yyyymmdd')||'_CriticalPaths.csv' d from dual;
@spool2csv monitor_dw v_owb_etlpa2_tmp2 &fname2 |

declare 
	l_table_exists	number;
begin
	select count(*) into l_table_exists
	from dba_tables 
	where owner = 'MONITOR_DW'
		and table_name = upper('owb_etlpa2_tmp3');

	if(l_table_exists > 0) then
		execute immediate 'drop table MONITOR_DW.owb_etlpa2_tmp3';
	end if;
end;
/

--drop table monitor_dw.owb_etlpa2_tmp3;

create table monitor_dw.owb_etlpa2_tmp3
as
-- "De-listagg" the critical path so as to select each node separately based on the duration and type.
with q1
  as(
    select root_execution_name, TOP_LEVEL_EXECUTION_AUDIT_ID, root_created_on, root_updated_on, path_duration_mins, seq_length,
           replace(critical_path, '-->',';') cp 
    from monitor_dw.owb_etlpa2_tmp2
  ),
  q2(grp, element, list, cnt, root_created_on, root_updated_on, path_duration_mins, seq_length)
  as(
    -- return the 1st element
    select root_execution_name ||' - '|| TOP_LEVEL_EXECUTION_AUDIT_ID grp, cp first_elmnt, cp lst, regexp_count(cp,';')+1 cnt, root_created_on, root_updated_on, path_duration_mins, seq_length from q1 
    union all
    -- return the next element
    select grp, regexp_substr(q2.list,'^([^;]*);?',1,1,'i',1) first_elmnt, substr(q2.list,regexp_instr(q2.list,'^([^;]*);?',1,1,1)) rest_lst, q2.cnt-1, root_created_on, root_updated_on, path_duration_mins, seq_length
    from q2
    where
      q2.cnt > 0 
      --regexp_like(q2.list,'^([^;]*);?')
  ),
q3 
as(  
select q2.*
from q2
where 
  q2.element <> nvl(q2.list, 'x')
order by grp, cnt desc
)
select  regexp_substr(grp, '(\S+)\s-',1,1,'i',1) flow_name,           
        row_number() over(partition by grp order by cnt desc) order_seq,
        regexp_substr(element, '(\S+)\s\[',1,1,'i',1) node, 
        regexp_substr(element, '\S+,\sTYPE:(\w+)\]',1,1,'i',1) type,
        to_number(regexp_substr(element, '\S+\s\[(\d?+\.?\d?)',1,1,'i',1)) duration_mins,         
        to_number(regexp_substr(element, '\S+\s\[(\d?+\.?\d?),\sp80-(\d?+\.?\d?)',1,1,'i',2)) p80_duration_mins,
        root_created_on, root_updated_on, 
        regexp_substr(grp, '(\S+)\s-\s(\S+)',1,1,'i',2) top_level_execution_audit_id,
        path_duration_mins, seq_length, list rest_of_flow
from q3
order by grp, duration_mins desc;

exec dbms_stats.gather_table_stats('MONITOR_DW', upper('owb_etlpa2_tmp3'))

-- uncomment to see output
/*
select * from monitor_dw.owb_etlpa2_tmp3;
*/

-- spool result to csv
col d new_value fname3
select to_char(sysdate, 'yyyymmdd')||'_CriticalPaths_detail.csv' d from dual;
@spool2csv monitor_dw owb_etlpa2_tmp3 &fname3 |

-----------------------------------------------------------
--
-- *** STEP 3: Detailed Analysis per node ( for long running nodes)
--
--  For each Node in the CP (previous step) that exceeds a certain time threshold
--  initiate analysis. I.e., find the leaf nodes (mappings procedures)
--  that consume most of the time.The selecion of nodes is based on P80_duration.
--
--  Essentially the output of this step is the list of mappings that must be tuned
--  because ****constantly**** (ie based on p80) show bad perfomance
-----------------------------------------------------------

declare 
	l_table_exists	number;
begin
	select count(*) into l_table_exists
	from dba_tables 
	where owner = 'MONITOR_DW'
		and table_name = upper('owb_etlpa2_tmp4');

	if(l_table_exists > 0) then
		execute immediate 'drop table MONITOR_DW.owb_etlpa2_tmp4';
	end if;
end;
/

--drop table monitor_dw.owb_etlpa2_tmp4;

create table monitor_dw.owb_etlpa2_tmp4
as
with src
as (
--    select *
--    from (
        select flow_name,  order_seq, node, type , TOP_LEVEL_EXECUTION_AUDIT_ID
        from monitor_dw.owb_etlpa2_tmp3
        where type in ('Procedure', 'Mapping', 'ProcessFlow', 'Function', 'Shell')
            and p80_duration_mins > 15 -- 15 minutes threshold
         --  and node = 'LEVEL1_DIM_1EXTRACTION' --and top_level_execution_Audit_id = 54798532
--    )            
--    where rownum <= 3        
),
q1 
as ( 
        -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
        select /*+ materialize dynamic_sampling (4) */  
            execution_audit_id, 
            parent_execution_audit_id,
            TOP_LEVEL_EXECUTION_AUDIT_ID,
            execution_name,
            execution_audit_status,                    
            return_result,
            task_type,
            created_on,
            updated_on,
            row_number() over(partition by execution_name order by created_on) r,
            PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((updated_on - created_on) * 24 * 60, 1)  ASC) OVER (partition by execution_name) p80_duration_mins,
            round(avg((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) avg_duration_mins,
            round(stddev((updated_on - created_on) * 24 * 60) OVER (partition by execution_name) ,1) stddev_duration_mins,
            round(min((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) min_duration_mins,
            round(max((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) max_duration_mins,
            count(distinct TOP_LEVEL_EXECUTION_AUDIT_ID||execution_name) OVER (partition by execution_name) numof_executions        
        from  owbsys.all_rt_audit_executions a
        where
            TOP_LEVEL_EXECUTION_AUDIT_ID in (  -- select flow of interest and criticality of interest(get the last run (sysdate - 1))
                                             select top_level_execution_audit_id
                                             from src
                                            )
),
q2
as (                                                                                           
    select /*+ materialize no_merge  dynamic_sampling (4) */
        src.flow_name,
        src.top_level_execution_audit_id srctop_level_exec_audit_id,
        a.top_level_execution_audit_id,
        src.node src_node,  
        CONNECT_BY_ROOT execution_name root_execution_name,
        r,
        created_on,
        updated_on,
        execution_audit_status,
        return_result,        
        level node_level,
        lpad(' ', 2*(level - 1))||REGEXP_REPLACE(REGEXP_REPLACE(execution_name, '_\d\d$', ''),'\w+:' , '' ) execution_name_short,    
        DECODE (task_type,
               'PLSQL', 'Mapping',
               'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', task_type)
        TYPE,
        lpad(' ', 2*(level - 1),'   ')||ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
        lpad(' ', 2*(level - 1),'   ')||p80_duration_mins p80_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||avg_duration_mins avg_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||stddev_duration_mins stddev_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||min_duration_mins min_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||max_duration_mins max_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||numof_executions numof_executions,
        --PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((updated_on - created_on) * 24 * 60, 1)  ASC) OVER (partition by execution_name) p80_duration_mins,        
        LEVEL owb_level,    
        CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,        
        execution_audit_id,
        execution_name,
       ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 * 60, 1) mins_until_end_of_root,
       --b.critical_ind,
       ROUND ( (CONNECT_BY_ROOT updated_on - CONNECT_BY_ROOT created_on) * 24, 1) root_duration_hrs,
       CONNECT_BY_ROOT created_on root_created_on,
       CONNECT_BY_ROOT updated_on root_updated_on,
       CONNECT_BY_ISLEAF is_leaf,
       SYS_CONNECT_BY_PATH (
          SUBSTR (execution_name, INSTR (execution_name, ':') + 1),
          '/')
          node_path  
    from  q1 a left outer join src on ( a.TOP_LEVEL_EXECUTION_AUDIT_ID = src.TOP_LEVEL_EXECUTION_AUDIT_ID 
                            and src.node = regexp_substr(a.execution_name,'\S+:(\S+)',1,1,'i',1))  
    WHERE  1=1
    START WITH src.node is not null --a.execution_name like '%'||src.node and src.top_level_execution_audit_id = a.top_level_execution_audit_id               
    CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id   
    ORDER SIBLINGS BY a.TOP_LEVEL_EXECUTION_AUDIT_ID desc, a.p80_DURATION_MINS DESC --(updated_on - created_on) DESC 
    --src.flow_name, src.node, a.TOP_LEVEL_EXECUTION_AUDIT_ID desc, (updated_on - created_on) DESC --a.p80_DURATION_MINS DESC  --a.created_on asc, a.p80_DURATION_MINS DESC
),
q3
as(
    select  --flow_name,
            regexp_substr(root_execution_name, '(\S+):(\S+)',1,1,'i',1) flow_name,
            created_on,
            updated_on,
            node_level,
            --src_node root_node,
            regexp_substr(root_execution_name, '(\S+):(\S+)',1,1,'i',2) root_node,
            execution_name_short node_name,
            type,
            p80_duration_mins,
            duration_mins,
            execution_audit_status,
            return_result,
            execution_audit_id,
            srctop_level_exec_audit_id,
            top_level_execution_audit_id,
            node_path,
            is_leaf                      
    from q2
    where 1=1
        and type in ('Procedure', 'Mapping', 'ProcessFlow', 'Function', 'Shell')
        -- 15 minutes threshold
        and p80_duration_mins > 15
    order by flow_name, top_level_execution_audit_id desc, root_node, node_level, duration_mins desc       
)
select *
from q3; 

exec dbms_stats.gather_table_stats('MONITOR_DW', upper('owb_etlpa2_tmp4'))

-- uncomment to see output
/*
select * from monitor_dw.owb_etlpa2_tmp4;
*/

-- spool result to csv
col d new_value fname4
select to_char(sysdate, 'yyyymmdd')||'_Analysis_per_Node.csv' d from dual;
@spool2csv monitor_dw owb_etlpa2_tmp4 &fname4 |


declare 
    l_table_exists    number;
begin
    select count(*) into l_table_exists
    from dba_tables 
    where owner = 'MONITOR_DW'
        and table_name = upper('owb_etlpa2_tmp5');

    if(l_table_exists > 0) then
        execute immediate 'drop table MONITOR_DW.owb_etlpa2_tmp5';
    end if;
end;
/

--drop table monitor_dw.owb_etlpa2_tmp5;

create table monitor_dw.owb_etlpa2_tmp5
as
with q1
as(
    select distinct flow_name, node_level, root_node, node_name, type, p80_duration_mins, node_path 
    from monitor_dw.owb_etlpa2_tmp4
    order by flow_name, root_node, node_level, p80_duration_mins desc   
) 
select *
from q1; 


-- uncomment to see output
/*
select * from monitor_dw.owb_etlpa2_tmp5;
*/

-- spool result to csv
col d new_value fname5
select to_char(sysdate, 'yyyymmdd')||'_Target_Nodes.csv' d from dual;
@spool2csv monitor_dw owb_etlpa2_tmp5 &fname5 |



undef flow_name
undef fname1
undef fname2
undef fname3
undef fname4
undef fname5

