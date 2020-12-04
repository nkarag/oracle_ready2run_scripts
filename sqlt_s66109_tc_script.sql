REM $Header: 215187.1 sqlt_s66109_tc_script.sql 11.4.5.9 2013/07/15 carlos.sierra $

-- These are the non-default or modified CBO parameters on source system.
-- ALTER SYSTEM commands can be un-commented out on a test environment.
ALTER SESSION SET optimizer_features_enable = '11.2.0.3';
--ALTER SYSTEM SET "_pga_max_size" = 858992640 SCOPE=MEMORY;
ALTER SESSION SET db_file_multiblock_read_count = 29;
-- skip "is_recur_flags" since it is not a real parameter.;
-- skip "parallel_degree" since it is not a real parameter.;
-- skip "parallel_query_default_dop" since it is not a real parameter.;
--ALTER SYSTEM SET parallel_threads_per_cpu = 1 SCOPE=MEMORY;
-- skip "total_processor_group_count" since it is not a real parameter.;

select /* ^^unique_id */ count(*) from target_Dw.customer_Dim;
