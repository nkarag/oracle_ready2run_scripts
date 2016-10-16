-- VoBB Compare Prom and DW 
select *
--trim(prom_source || ' ' || dw_source), count(*) 
from 
(
    (
        select prom.*, dw.* from
        (
        select distinct 'PROM' prom_source, A.ORDER_NUM prom_order_num, a.CREATED_DATE prom_created_date, A.CLI prom_cli, A.VOICE_TYPE prom_voice_type
        from REPORT_DW.V_VOBB_ORDERS_PRM a
        where
             status_cd <> 'Cancelled' and vobb_traffic_light = 'Green'
             and trunc(CREATED_DATE) >= to_date('25/5/2015', 'dd/mm/yyyy')
        ) prom,
        (
        select 'DW' dw_source, a.order_num dw_order_num, A.ORDER_CREATION_DATE_KEY dw_created_date, A.SERVICE_NUM dw_cli, A.VOICE_TYPE dw_voice_type
        from 
        ORDERS_SOC_DW.ORDER_LINE_FCT a,
        ORDERS_SOC_DW.ORDER_STATUS_DIM b,
        CONF_DIM_DW.PRODUCT_DIM c
        where A.ORDER_CREATION_DATE >= to_date('25/5/2015', 'dd/mm/yyyy')
        and A.ORDER_STATUS_SK = B.ORDER_STATUS_SK
        and B.ORDER_STATUS_DESC not in ('Cancelled')--('Pending', 'Contract Pending', 'Cancelled')
        and A.PRODUCT_SK = C.SOC_PRODUCT_SK
        and C.PRODUCT_ID = '1-65TVM'
        --and NVL (A.VOBB_FEASIBILITY_TRAFFIC, 'XXX') = 'Green'
        and (A.VOBB_ATTEMPT_CLC_PRM_IND = 1 or A.VOBB_ATTEMPT_CLC_SBL_IND = 1)
        and a.VOICE_LINE_TYPE = 'Primary Line'
        ) dw
        where prom.prom_order_num = dw.dw_order_num (+)
    )
    union
    (
        select prom.*, dw.* from
        (
        select distinct 'PROM' prom_source, A.ORDER_NUM prom_order_num, a.CREATED_DATE prom_created_date, A.CLI prom_cli, A.VOICE_TYPE prom_voice_type
        from REPORT_DW.V_VOBB_ORDERS_PRM a
        where
             status_cd <> 'Cancelled' and vobb_traffic_light = 'Green'
             and trunc(CREATED_DATE) >= to_date('25/5/2015', 'dd/mm/yyyy')        
        ) prom,
        (
        select 'DW' dw_source, a.order_num dw_order_num, A.ORDER_CREATION_DATE_KEY dw_created_date, A.SERVICE_NUM dw_cli, A.VOICE_TYPE dw_voice_type
        from 
        ORDERS_SOC_DW.ORDER_LINE_FCT a,
        ORDERS_SOC_DW.ORDER_STATUS_DIM b,
        CONF_DIM_DW.PRODUCT_DIM c
        where A.ORDER_CREATION_DATE >= to_date('25/5/2015', 'dd/mm/yyyy')
        and A.ORDER_STATUS_SK = B.ORDER_STATUS_SK
        and B.ORDER_STATUS_DESC not in  ('Cancelled') --('Pending', 'Contract Pending', 'Cancelled')
        and A.PRODUCT_SK = C.SOC_PRODUCT_SK
        and C.PRODUCT_ID = '1-65TVM'
        --and NVL (A.VOBB_FEASIBILITY_TRAFFIC, 'XXX') = 'Green'
        and (A.VOBB_ATTEMPT_CLC_PRM_IND = 1 or A.VOBB_ATTEMPT_CLC_SBL_IND = 1)
        and a.VOICE_LINE_TYPE = 'Primary Line'
        ) dw
        where dw.dw_order_num = prom.prom_order_num (+)
    )
)
order by nvl(prom_created_date, dw_created_date) desc, nvl(prom_order_num, dw_order_num) desc
--group by trim(prom_source || ' ' || dw_source)
