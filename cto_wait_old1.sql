/*
 Check what CTO_MAIN is waiting for
 
 Note:
    All dates below denote data ref dates "to be loaded" or that are currently loading
*/

with 
per_main as 
(
-- пеяижеяеиес    
    SELECT FLOW_BASEDATE  dt  
        --INTO temp_date    
        FROM STAGE_PERIF.FLOW_PROGRESS_STG      
        WHERE FLOW_NAME = 'PERIF_PRESENT_AREA_END'  
),
level0 as
(
-- DAILY DWH    
    SELECT (RUN_DATE + 1) dt -- INTO temp_date_dw     
        FROM STAGE_DW.DW_CONTROL_TABLE      
        WHERE PROCEDURE_NAME = 'OTE_DW_LAST_RUN'
),
soc4nmr as
(
-- SOC for NMR
    SELECT (RUN_DATE + 1) dt 
      --INTO temp_siebel_date      
      FROM STAGE_DW.DW_CONTROL_TABLE        
      WHERE PROCEDURE_NAME = 'SOC_END_FORNMR_DATE' 
),
cmp as
(
   -- CAMAPINGS-FAULTS
    SELECT (RUN_DATE + 1) dt
        --INTO temp_crm_date    
        FROM STAGE_DW.DW_CONTROL_TABLE      
        WHERE PROCEDURE_NAME = 'CRM_LAST_RUN' 
),
gns as
(
-- GENESYS
  SELECT (RUN_DATE + 1) dt  
    --INTO temp_genesys_date    
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'GENESYS_LAST_RUN'
),    
wfm as
(
-- wfm
SELECT (RUN_DATE + 1) dt
        --INTO temp_wfm_date    
        FROM STAGE_DW.DW_CONTROL_TABLE      
        WHERE PROCEDURE_NAME = 'WFM_LAST_RUN'
),
cto as
(
 -- CTO KPI    
    SELECT (RUN_DATE + 1) dt --INTO temp_date_cto
        FROM STAGE_DW.DW_CONTROL_TABLE      
        WHERE PROCEDURE_NAME = 'CTO_LAST_RUN'
)
select
    cto.dt CTO_to_be_loaded,
    case when cto.dt >= per_main.dt then 'WAITING' else 'OK' end PER_MAIN_STATUS,
    per_main.dt PERIF_PRESENT_AREA_END,
    case when cto.dt >= level0.dt then 'WAITING' else 'OK' end LEVEL0_STATUS,
    level0.dt level0_date_plus1,
    case when cto.dt >= soc4nmr.dt then 'WAITING' else 'OK' end SOC4NMR_STATUS,
    soc4nmr.dt soc4nmr,
    case when cto.dt >= cmp.dt then 'WAITING' else 'OK' end CMP_STATUS,
    cmp.dt cmp,
    case when cto.dt >= gns.dt then 'WAITING' else 'OK' end GENESYS_STATUS,
    gns.dt genesys,   
    case when cto.dt >= wfm.dt then 'WAITING' else 'OK' end WFM_STATUS,
    wfm.dt wfm
from  per_main, level0,  soc4nmr, cmp, gns, wfm, cto;
