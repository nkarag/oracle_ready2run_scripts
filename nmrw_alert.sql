------------------------------------------------------------------------------
--  nmrw_alert.sql
--  Description:
--      Returns NMR KPIs (from weekly NMR report) whose value deviates significantly.
--      The values that deviate are the ones that fall outside the interval
--      [ì-2*ó, ì+2*ó] where ì is the median in a 26 weeks period
------------------------------------------------------------------------------

col currwk for a10
col kpicode for a40
col kpiname for a40
col kpitype for a10
col kpiprod for a40
col VAL_CURRWK for 999G999G999
col VAL_PREVWK for 999G999G999
col VAL_AVG for 999G999G999
col VAL_STDDEV for 999G999G999
col KPIMR_RESULT for 999G999G999
col KPIMR_RESULT_PREVWK for 999G999G999

select * from monitor_dw.v_nmrw_alert;