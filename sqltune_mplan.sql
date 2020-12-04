-- ----------------------------------------------------------------------------------------------
-- Modify the plan of a sql_id (original) by providing a new plan coming from a modified version (usually with hints) of the original sql_id.
-- Input the "original sql_id", the "modified sql_id" and the "new plan" hash value. 
--
--	Note:	It is based on Carlos Sierras' coe_load_sql_baseline.sql script. The script is slightly modified (essentially the part where
--			the sql plan baseline is loaded into a staging table and exp-orted for importing to another system, is commented out.
--
-- DESCRIPTION
--   This script loads a plan from a modified SQL into the SQL
--   Plan Baseline of the original SQL.
--   If a good performing plan only reproduces with CBO Hints
--   then you can load the plan of the modified version of the
--   SQL into the SQL Plan Baseline of the orignal SQL.
--   In other words, the original SQL can use the plan that was
--   generated out of the SQL with hints.
--
-- PRE-REQUISITES
--   1. Have in cache or AWR the text for the original SQL.
--   2. Have in cache the plan for the modified SQL
--      (usually with hints).
--
-- PARAMETERS
--   1. ORIGINAL_SQL_ID (required)
--   2. MODIFIED_SQL_ID (required)
--   3. PLAN_HASH_VALUE (required)
--
-- (C) 2014 Nikos Karagiannidis - http://oradwstories.blogspot.com    
-- ----------------------------------------------------------------------------------------------

@sqlplus_settings_store

PRO Parameter 1:
PRO ORIGINAL_SQL_ID (required)
PRO
DEF original_sql_id = '&1';
PRO
PRO Parameter 2:
PRO MODIFIED_SQL_ID (required)
PRO
DEF modified_sql_id = '&2';
PRO Parameter 3:
PRO PLAN_HASH_VALUE (required)
PRO
DEF plan_hash_value = '&3';

@coe_load_sql_baseline_no_stg_table &original_sql_id &modified_sql_id &plan_hash_value

@sqlplus_get_settings