select *
from REPORT_DW.V_VOBB_ORDERS_REPORT;


select *
from REPORT_DW.V_VOBB_ORDERS_REPORT t
where
    T.VOBB_TRAFFIC_LIGHT = 'Green'
    and status_cd <> 'Cancelled'