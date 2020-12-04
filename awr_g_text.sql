-- input for instance_no, in the form '1,2,3,4'
select output from table(dbms_workload_repository.awr_global_report_text(&db_id,'1,2,3,4',&snap_start,&snap_end))
/

