select output from table(dbms_workload_repository.awr_report_text(&db_id,&instance_no,&snap_start,&snap_end))
/
