/*****************************
-- What CTO is waiting for
*****************************/

alter session set nls_date_format = 'dd-mm-yyyy'; 

col cto_to_be_loaded format a20
col temp_level0_dw format a20
col temp_wfm_date format a20
col temp_genesys_date format a20
col wfm_subflow_status format a20
col temp_per_date format a20
col temp_faults_date format a20
col faults_subflow_status format a22
col temp_soc4nmr_date format a20
col orders_subflow_status format a22

with
level0 as
(
    -- DAILY DWH    
    SELECT RUN_DATE + 1 temp_level0_dw
    FROM STAGE_DW.DW_CONTROL_TABLE      
    WHERE PROCEDURE_NAME = 'OTE_DW_LAST_RUN' 
),
genesis as
(
    -- GENESYS
    SELECT RUN_DATE + 1 temp_genesys_date    
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'GENESYS_LAST_RUN'
),
wfm as
(                 
    -- WFM
    SELECT RUN_DATE + 1 temp_wfm_date
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'WFM_LAST_RUN'
),
per_main as
(
    -- ΠΕΡΙΦΕΡΕΙΕΣ    
    SELECT FLOW_BASEDATE temp_per_date    
    FROM STAGE_PERIF.FLOW_PROGRESS_STG      
    WHERE FLOW_NAME = 'PERIF_PRESENT_AREA_END'      
),
faults as
(
    -- FAULTS
    SELECT RUN_DATE + 1 temp_faults_date    
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'FAULT_LAST_RUN'
),         
soc4nmr as
(
    -- SIEBEL    
    SELECT RUN_DATE + 1 temp_soc4nmr_date      
    FROM STAGE_DW.DW_CONTROL_TABLE        
    WHERE PROCEDURE_NAME = 'SOC_END_FORNMR_DATE'
),       
cto as
(                 
    -- CTO KPI    
    SELECT RUN_DATE + 1 temp_date_cto
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'CTO_LAST_RUN'
)         
select
    temp_date_cto CTO_TO_BE_LOADED,
    -- WFM subflow: WFM KPIs και Genesis
    temp_level0_dw,
    temp_wfm_date,
    temp_genesys_date,
    CASE WHEN 
            temp_date_cto >= temp_level0_dw OR 
            temp_date_cto >= temp_wfm_date OR
            temp_date_cto >= temp_genesys_date
        THEN    'WAITING'
        ELSE    'OK'        
    END  WFM_SUBFLOW_STATUS,              
    -- FAULTS subflow: FAULTS KPIs Siebel Faults και Προμηθέα (LL, καλωδιακές)
    temp_per_date,
    temp_level0_dw,
    temp_faults_date,
    CASE WHEN 
            temp_date_cto >= temp_per_date OR 
            temp_date_cto >= temp_level0_dw OR 
            temp_date_cto >= temp_faults_date         
        THEN    'WAITING'
        ELSE    'OK'        
    END  FAULTS_SUBFLOW_STATUS,                     
    -- ORDERS subflow: ORDER KPIs από Siebel, Woms, Προμηθέα
    temp_per_date,
    temp_level0_dw,
    temp_soc4nmr_date,
    temp_faults_date,
    CASE WHEN 
            temp_date_cto >= temp_per_date OR 
            temp_date_cto >= temp_level0_dw OR 
            temp_date_cto >= temp_soc4nmr_date  OR
            temp_date_cto >= temp_faults_date         
        THEN    'WAITING'
        ELSE    'OK'        
    END  ORDERS_SUBFLOW_STATUS
from level0, genesis, wfm, per_main, faults, soc4nmr, cto;

alter session set NLS_DATE_FORMAT='dd-mm-yyyy HH24:mi:ss';