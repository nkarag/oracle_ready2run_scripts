/*
    Check if KPIDW_MAIN is ready to run or if it is waiting for some other flow and which one
*/
with kpi as
(
SELECT RUN_DATE        
    FROM STAGE_DW.DW_CONTROL_TABLE        
    WHERE PROCEDURE_NAME = 'NMR_GLOBAL_RUN_DATE'
),
kpi_run as (
SELECT RUN_DATE        
    FROM STAGE_DW.DW_CONTROL_TABLE        
    WHERE PROCEDURE_NAME = 'NMR_GLOBAL_END_DATE'
)
select
case when
    (select run_date from kpi) >=
    (
    -- DAILY DWH      
      SELECT RUN_DATE + 1        
        FROM STAGE_DW.DW_CONTROL_TABLE        
        WHERE PROCEDURE_NAME = 'OTE_DW_LAST_RUN'
    ) then    'WAITING'    ELSE    'OK'   
end LEVEL0_DAILY,
case when
    (select run_date from kpi) >=
    (
-- пеяижеяеиес      
  SELECT FLOW_BASEDATE           
    FROM STAGE_PERIF.FLOW_PROGRESS_STG        
    WHERE FLOW_NAME = 'PERIF_NMR_END'    ) 
    then    'WAITING'    ELSE    'OK'   
end PER_MAIN,
case when
    (select run_date from kpi) >=
    (
-- SIEBEL    
  SELECT RUN_DATE + 1      
    FROM STAGE_DW.DW_CONTROL_TABLE        
    WHERE PROCEDURE_NAME = 'SOC_END_FORNMR_DATE'
    )
    then    'WAITING'    ELSE    'OK'
end SOC_DW_MAIN,
case when
    (select run_date from kpi) >=
    (
-- CCRM      
  SELECT RUN_DATE           
    FROM STAGE_DW.DW_CONTROL_TABLE        
    WHERE PROCEDURE_NAME = 'CCRM_RUN_DATE'
    )
    then    'WAITING'    ELSE    'OK'
end CCRM,
case when
    (select run_date from kpi_run) >  sysdate
    then    'NMR Execution in progress'    ELSE    'NOT currently executing or Waiting'
end KPIDW_MAIN                                   
from dual;