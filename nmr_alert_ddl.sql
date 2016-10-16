------------------------------------------------------------------------------
--  nmr_alert_ddl.sql
--  Description:
--      Returns NMR KPIs whose value deviates significantly.
--      The values that deviate are the ones that fall outside the interval
--      [μ-N*σ, μ+N*σ] where μ is the mean value in a X weeks period
------------------------------------------------------------------------------

-- Configuration table
create table monitor_dw.nmr_alert_config (
  key varchar2(50),
  val number(3)
);
insert into monitor_dw.nmr_alert_config(key, val) values ('num_weeks', 52);
insert into monitor_dw.nmr_alert_config(key, val) values ('n', 3);
commit;

select *
from monitor_dw.nmr_alert_config;

exec dbms_stats.gather_table_stats('MONITOR_DW', 'NMR_ALERT_CONFIG')

/***********************************************************
  Weekly NMR  
************************************************************/

-- base query from vzorbas
select A.KPIMR_SNAPSHOT_DATE,  B.DEIKTISMR_CODE, deiktismr_name, deiktismr_entity, deiktismr_busarea  , SUM(A.KPIMR_RESULT) 
from KPI_DW.KPIMR_WEEKLY_FCT a 
inner join  KPI_DW.DEIKTISMR_DIM b
on A.DEIKTISMR_SK=B.DEIKTISMR_SK
AND  A.KPIMR_SNAPSHOT_DATE >sysdate-100
group by A.KPIMR_SNAPSHOT_DATE,  B.DEIKTISMR_CODE, deiktismr_name, deiktismr_entity, deiktismr_busarea
order by 1,2;

-- final view
select * from monitor_dw.v_nmrw_alert;

create or replace view monitor_dw.v_nmrw_alert as
with nmr_date as
(
  select run_date - 7 nmrdt
  from stage_dw.dw_control_table
  where
    procedure_name = 'NMR_WEEK_GLOBAL_STDT'
),
nmr_week_raw as
(
  -- base query: 1 row per KPI, per week
  select  A.KPIMR_SNAPSHOT_DATE snapdate,  
          B.DEIKTISMR_CODE kpicode, 
          deiktismr_name  kpiname, 
          deiktismr_entity kpitype, 
          deiktismr_busarea kpiprod,
          sum(A.KPIMR_RESULT) kpimr_result
  from KPI_DW.KPIMR_WEEKLY_FCT a inner join  KPI_DW.DEIKTISMR_DIM b 
        on (A.DEIKTISMR_SK=B.DEIKTISMR_SK), nmr_date c
  where 1=1
    AND  A.KPIMR_SNAPSHOT_DATE between c.nmrdt - (select val from monitor_dw.nmr_alert_config where key = 'num_weeks')*( INTERVAL '7' DAY) and c.nmrdt -- N weeks back
    group by  A.KPIMR_SNAPSHOT_DATE, B.DEIKTISMR_CODE, deiktismr_name, deiktismr_entity, deiktismr_busarea
),
nmr_week as
(
    -- base query: 1 row per KPI, per week (also calculate diff with previous week for usercount kpis)
    select  snapdate,  
            kpicode, 
            kpiname, 
            kpitype, 
            kpiprod, 
            case when kpitype = 'Usercount' 
              then
                abs(KPIMR_RESULT - lag(KPIMR_RESULT, 1) over (partition by  kpicode, kpiname, kpitype, kpiprod order by snapdate))
              else
                KPIMR_RESULT  
              end kpires,
            KPIMR_RESULT,
            lag(KPIMR_RESULT, 1) over (partition by  kpicode, kpiname, kpitype, kpiprod order by snapdate) KPIMR_RESULT_PREVWK
    from  nmr_week_raw
    where 1=1      
--    group by snapdate,  kpicode, kpiname, kpitype, kpiprod
--    order by 1 desc,2
),
nmr_max as
(
    select max(snapdate) max_snapdate from nmr_week
),
nmr_prev_tot as
(
    select  kpicode, 
            kpiname, 
            kpitype, 
            kpiprod, 
            avg(kpires) kpires_avg,
            stddev(kpires) kpires_stddev
  from nmr_week 
  where
    snapdate < (select max_snapdate from nmr_max)
  group by  kpicode, kpiname, kpitype, kpiprod
),
nmr_prev as
( -- get numbers of previous week
  select  snapdate prevwk,  
          kpicode, 
          kpiname, 
          kpitype, 
          kpiprod, 
          kpires kpires_prevwk
  from nmr_week 
  where
    snapdate = (select max_snapdate from nmr_max) - INTERVAL '7' DAY
),
nmr_cur as
(
  select  snapdate currwk,  
          kpicode, 
          kpiname, 
          kpitype, 
          kpiprod, 
          kpires kpires_currwk,
          KPIMR_RESULT,
          KPIMR_RESULT_PREVWK
  from nmr_week 
  where
    snapdate = (select max_snapdate from nmr_max)
),
final as
(
  select  a.currwk,
          a.kpicode,
          a.kpiname,
          a.kpitype,
          a.kpiprod,
          a.kpires_currwk,
          c.kpires_prevwk,
          round(b.kpires_avg) kpires_avg,
          round(b.kpires_stddev) kpires_stddev,
          a.kpimr_result,
          a.kpimr_result_prevwk
  from  nmr_cur a join nmr_prev_tot b on(a.kpicode = b.kpicode)
          join nmr_prev c on(a.kpicode = c.kpicode)
),
deviation as
(
  select  currwk,
          kpicode,
          kpiname,
          kpitype,
          kpiprod,
          kpires_currwk val_currwk,
          kpires_prevwk val_prevwk,
          kpires_avg    val_avg,
          kpires_stddev val_stddev,
          case when NOT(abs(kpires_currwk) between (abs(kpires_avg) - (select val from monitor_dw.nmr_alert_config where key = 'n')*kpires_stddev) AND (abs(kpires_avg) + (select val from monitor_dw.nmr_alert_config where key = 'n')*kpires_stddev)) then 1
                else 0
          end  deviation_ind,
          kpimr_result,
          kpimr_result_prevwk          
  from final
)
select *
from deviation
where 
  deviation_ind = 1
order by kpicode;



/***********************************************************
  Daily NMR  
************************************************************/

-- base query from vzorbas
--Για το daily, οι τελευταίες 12 μέρες 
select A.KPIMR_SNAPSHOT_DATE, a.KPIMR_GROUP_DATE,  B.DEIKTISMR_CODE, deiktismr_name, deiktismr_entity, deiktismr_busarea  , SUM(A.KPIMR_RESULT) 
from KPI_DW.KPIMR_DAILY_SNP a 
      inner join  KPI_DW.DEIKTISMR_DIM b on (A.DEIKTISMR_SK=B.DEIKTISMR_SK)
where 1=1
  AND  ( a.KPIMR_GROUP_DATE >= 
          (select run_date from stage_dw.dw_control_table where procedure_name = 'NMR_GLOBAL_RUN_DATE')-1-12 )--(select MAX(KPIMR_DAILY_SNP.KPIMR_GROUP_DATE ) from KPI_DW.KPIMR_DAILY_SNP )-12  )
group by A.KPIMR_SNAPSHOT_DATE, a.KPIMR_GROUP_DATE,  B.DEIKTISMR_CODE, deiktismr_name, deiktismr_entity, deiktismr_busarea
order by 1 desc,2 desc, 3;


-- final view
select * from monitor_dw.v_nmrd_alert;

create or replace view monitor_dw.v_nmrd_alert as
with nmr_date as
(
  select run_date - 1 nmrdt
  from stage_dw.dw_control_table
  where
    procedure_name = 'NMR_GLOBAL_RUN_DATE'
),
nmr_day_raw as
(
  -- base query: 1 row per KPI, per day
  select  A.KPIMR_SNAPSHOT_DATE snapdate, 
          a.KPIMR_GROUP_DATE daydt,  
          B.DEIKTISMR_CODE kpicode, 
          deiktismr_name kpiname, 
          deiktismr_entity kpitype, 
          deiktismr_busarea  kpiprod, 
          SUM(A.KPIMR_RESULT) kpimr_result
  from KPI_DW.KPIMR_DAILY_SNP a 
        inner join  KPI_DW.DEIKTISMR_DIM b on (A.DEIKTISMR_SK=B.DEIKTISMR_SK)
  where 1=1
    AND  ( a.KPIMR_GROUP_DATE >= 
            (select nmrdt from nmr_date)-(select val from monitor_dw.nmr_alert_config where key = 'num_weeks') )--(select MAX(KPIMR_DAILY_SNP.KPIMR_GROUP_DATE ) from KPI_DW.KPIMR_DAILY_SNP )-12  )
  group by A.KPIMR_SNAPSHOT_DATE, a.KPIMR_GROUP_DATE,  B.DEIKTISMR_CODE, deiktismr_name, deiktismr_entity, deiktismr_busarea
  --order by 1 desc,2 desc, 3
),
nmr_day as
(
    -- base query: 1 row per KPI, per day (also calculate diff with previous day for usercount kpis)
    select  snapdate,
            daydt,
            kpicode, 
            kpiname, 
            kpitype, 
            kpiprod, 
            case when kpitype = 'Usercount' 
              then
                abs(KPIMR_RESULT - lag(KPIMR_RESULT, 1) over (partition by  kpicode, kpiname, kpitype, kpiprod order by daydt))
              else
                KPIMR_RESULT  
              end kpires,
            KPIMR_RESULT,
            lag(KPIMR_RESULT, 1) over (partition by  kpicode, kpiname, kpitype, kpiprod order by daydt) KPIMR_RESULT_PREVDAY
    from  nmr_day_raw
    where 1=1      
--    group by snapdate,  kpicode, kpiname, kpitype, kpiprod
--    order by 1 desc,2
),
nmr_max as
(
  select nmrdt max_daydt from nmr_date
    --select max(daydt) max_daydt from nmr_day
),
nmr_prev_tot as
(
    select  kpicode, 
            kpiname, 
            kpitype, 
            kpiprod, 
            avg(kpires) kpires_avg,
            stddev(kpires) kpires_stddev
  from nmr_day 
  where
    daydt < (select max_daydt from nmr_max)
  group by  kpicode, kpiname, kpitype, kpiprod
),
nmr_prev as
( -- get numbers of previous day
  select  snapdate snapdate_prevday,  
          daydt prevday,
          kpicode, 
          kpiname, 
          kpitype, 
          kpiprod, 
          kpires kpires_prevday
  from nmr_day 
  where
    daydt = (select max_daydt from nmr_max) - 1
),
nmr_cur as
(
  select  snapdate currwk, 
          daydt currday,
          kpicode, 
          kpiname, 
          kpitype, 
          kpiprod, 
          kpires kpires_currday,
          KPIMR_RESULT,
          KPIMR_RESULT_PREVDAY
  from nmr_day 
  where
    daydt = (select max_daydt from nmr_max)
),
final as
(
  select  a.currwk,
          a.currday,
          a.kpicode,
          a.kpiname,
          a.kpitype,
          a.kpiprod,
          a.kpires_currday,
          c.kpires_prevday,
          round(b.kpires_avg) kpires_avg,
          round(b.kpires_stddev) kpires_stddev,
          a.kpimr_result,
          a.kpimr_result_prevday
  from  nmr_cur a join nmr_prev_tot b on(a.kpicode = b.kpicode)
          join nmr_prev c on(a.kpicode = c.kpicode)
),
deviation as
(
  select  currwk,
          currday,
          kpicode,
          kpiname,
          kpitype,
          kpiprod,
          kpires_currday val_currday,
          kpires_prevday val_prevday,
          kpires_avg    val_avg,
          kpires_stddev val_stddev,
          case when NOT(abs(kpires_currday) between (abs(kpires_avg) - (select val from monitor_dw.nmr_alert_config where key = 'n')*kpires_stddev) AND (abs(kpires_avg) + (select val from monitor_dw.nmr_alert_config where key = 'n')*kpires_stddev)) then 1
                else 0
          end  deviation_ind,
          kpimr_result,
          kpimr_result_prevday          
  from final
)
select *
from deviation
where 
  deviation_ind = 1
order by kpicode;



/*
------------ DRAFT  ---------------
select sysdate, sysdate - INTERVAL '7' DAY
from dual;

select * from dual;

select run_date - INTERVAL '7' DAY nmr_date
from stage_dw.dw_control_table
where
  procedure_name = 'NMR_WEEK_GLOBAL_STDT';
*/