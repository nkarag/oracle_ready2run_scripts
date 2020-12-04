set    termout off
store  set sqlplus_settings replace
save   buffer.sql replace

set colsep ';'
rem the following supresses all headers
set pagesize 0
set trimspool on
set linesize 9999
set termout off
set feedback off

col order_id format a9
col trn_id format a9
alter session set nls_date_format='dd-mm-yyyy hh24:mi:ss';

spool myfile.csv replace

-- query
    select ORDER_ID,
        TRN_ID,
        TRN_TYPE_ID,
        ORDER_TYPE_ID,
        ORDER_DESC,
        ORDER_CREATION_DATE,
        ORDER_FULLFILMENT_DATE,
        ORDER_TEK,
        POS_TYPE,
        PERIF,
        DIAM,
        TT,
        TASK_SOURCE_SYSTEM,
        TASK_NAME,
        TASK_DESC,
        TASK_STATUS,
        TASK_TYPE,
        PS_INSERT_DATE,
        TASK_START_DATE,
        TASK_END_DATE,
        TASK_USERNAME,
        TASK_IPIRESIA,
        TRN_PHONE_NUMBER,
        CREDIT_CONTROL_STATUS,
        ORDER_PHONE_NUMBER
    from vzorbas.os_Order_Status_Report
where rownum < 10
/
spool off

get    buffer.sql nolist
@sqlplus_settings
set    termout on