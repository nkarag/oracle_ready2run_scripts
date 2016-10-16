select output from table(DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_HTML(&db_id,&instance_no,&snap_start,&snap_end))
/
