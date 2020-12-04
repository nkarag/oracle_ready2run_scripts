    select *
    from bicubes.BDA_LTE_CPS_RAW2
    
    
    select *
    from bicubes.BDA_GN_CPS_RAW2
    
    select * 
    from bicubes.BDA_LTE_UPS_RAW2

        select application, count(*)
        from bicubes.BDA_GN_UPS_RAW2
        group by application
        order by 2 desc    
    
    select *
    from bicubes.BDA_GN_UPS_RAW2

        select count(*)
        from bicubes.BDA_GN_UPS_RAW2
        where
            time > date'2015-07-19'

    
    
select * from DBA_HIVE_DATABASES

select * from DBA_HIVE_TABLES

select * from DBA_HIVE_COLUMNS


select *
from dba_external_tables
where
    type_name like '%HIVE%'
    
    
select 'grant select on ' || owner||'.'||table_name||' to BDA_ROLE;'
from dba_external_tables
where
    type_name like '%HIVE%'

----------------------- DRAFT ----------------------


select *
from dba_tables

select *
from dba_tab_privs
where grantee = 'NKARAG'

select *
from dba_role_privs
where grantee = 'NKARAG'

select *
from dba_role_privs
where grantee = 'mcif'


select *
from dba_users
    username = 'NKARAG'


select *
from dba_objects
where object_name = upper('bda_gn_cps_raw')



select *
from dba_profiles
where profile = 'SYSTEM_PROFILE_GROUP'


    
    

    
    

    
    
   select * from  hive_uri$;
   
   
   select *
   from dba_tab_privs
   where table_name = 'BDA_GN_UPS_RAW2'
   
   

