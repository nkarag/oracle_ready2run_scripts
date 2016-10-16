/*-----------------------------------------------------------------------------
    sqltune_jo
    
    Script to find the best join order.
    The script uses explain plan output to capture the various parts of the SQL
    statement. For example, 
        - participating tables
        - filters on tables
        - join conditions
/*----------------------------------------------------------------------------*/

--***** A. Capture participating tables
        delete from plan_table
            where statement_id =  'mystmnt';

        commit;    

        -- example 1 (one table)
        explain plan  
            set statement_id = 'mystmnt'
        for
            select *
            from tlarge;

        select object_owner, object_name 
        from plan_table
        where 1=1
            AND statement_id = 'mystmnt'
            AND operation = 'TABLE ACCESS'
        order by id

        /*
        OBJECT_OWNER    OBJECT_NAME
        NKARAG    TLARGE
        */

        -- example 2 (many tables)
        explain plan  
            set statement_id = 'mystmnt'
        for
            SELECT
            KPI_DW.KPIMR_MONTHLY_DET_FCT.SOC_ORDER_ID,
            KPI_DW.KPIMR_MONTHLY_DET_FCT.SOC_ORDER_LINE_ID,
            KPI_DW.KPIMR_MONTHLY_DET_FCT.PHONE_NUMBER,
            CLI_DIM.CLI_NUM,
            KPI_DW.KPIMR_MONTHLY_DET_FCT.ADSL_NUMBER,
            KPI_DW.DEIKTISMR_DIM.DEIKTISMR_NAME,
            KPI_DW.DEIKTISMR_DIM.DEIKTISMR_CODE,
            SALE_UNIT_DIM.SALE_UNIT_DESC, SALE_UNIT_DIM.SU_NMR_L1_DESCR
            FROM
            KPI_DW.KPIMR_MONTHLY_DET_FCT,
            KPI_DW.DEIKTISMR_DIM,
            target_dw.sale_unit_dim,
            target_dw.cli_dim
            WHERE
            (
            KPI_DW.KPIMR_MONTHLY_DET_FCT.DEIKTISMR_SK=KPI_DW.DEIKTISMR_DIM.DEIKTISMR_SK  )
            AND
            target_dw.sale_unit_dim.sale_unit_sk=KPIMR_MONTHLY_DET_FCT.sale_unit_sk
            AND target_dw.cli_dim.cli_sk = KPIMR_MONTHLY_DET_FCT.cli_sk
            AND KPI_DW.KPIMR_MONTHLY_DET_FCT.KPIMR_SNAPSHOT_DATE  = to_date('1/9/2015', 'dd/mm/yyyy')
            AND KPI_DW.DEIKTISMR_DIM.DEIKTISMR_CODE  IN  ( '80_2' );

        select object_owner, object_name 
        from plan_table
        where 1=1
            AND statement_id = 'mystmnt'
            AND operation = 'TABLE ACCESS'
        order by id    
        /*
        OBJECT_OWNER    OBJECT_NAME
        TARGET_DW    SALE_UNIT_DIM
        KPI_DW    DEIKTISMR_DIM
        KPI_DW    KPIMR_MONTHLY_DET_FCT
        TARGET_DW    CLI_DIM
        */

        -- example 3 (tables and views)
        --  -> The underlying tables (base tables) are returned
        explain plan  
            set statement_id = 'mystmnt'
        for
            select /*+ NO_MERGE */ *
            from CMPMGNT_DW.W_CLI_DIM join TARGET_DW.CLI_ACT using (cli_sk);
        /*
        OBJECT_OWNER    OBJECT_NAME
        TARGET_DW    CLI_ACT
        TARGET_DW    CLI_DIM
        */    


        select object_owner, object_name 
        from plan_table
        where 1=1
            AND statement_id = 'mystmnt'
            AND operation = 'TABLE ACCESS'
        order by id  

        -- example 4 (inline view)
        -- --> merged or not you get the underlying tables
        delete from plan_table
            where statement_id =  'mystmnt';

        commit;

        explain plan  
            set statement_id = 'mystmnt'
        for
        select *
        from tlarge t1, (select /*+ MERGE */ * from tsmall where id between 1 and 5) t2
        where
            t1.id = t2.id;
        /*
        OBJECT_OWNER    OBJECT_NAME
        NKARAG    TLARGE
        NKARAG    TSMALL
        */    

        explain plan  
            set statement_id = 'mystmnt'
        for
        select *
        from tlarge t1, (select /*+ NO_MERGE */ * from tsmall where id between 1 and 5) t2
        where
            t1.id = t2.id;
            
        select object_owner, object_name 
        from plan_table
        where 1=1
            AND statement_id = 'mystmnt'
            AND operation = 'TABLE ACCESS'
        order by id  
        /*
        OBJECT_OWNER    OBJECT_NAME
        NKARAG    TSMALL
        NKARAG    TLARGE

        */    


        -- example 5 (factored subquery)
        delete from plan_table
            where statement_id =  'mystmnt';

        commit;

        explain plan  
            set statement_id = 'mystmnt'
        for
        with q1
        as (select /*+ NO_MERGE */ * from tsmall where id between 1 and 5) 
        select *
        from tlarge t1, q1 t2
        where
            t1.id = t2.id;

        select object_owner, object_name 
        from plan_table
        where 1=1
            AND statement_id = 'mystmnt'
            AND operation = 'TABLE ACCESS'
        order by id
        /*
        OBJECT_OWNER    OBJECT_NAME
        NKARAG    TSMALL
        NKARAG    TLARGE
        */  

        -- example 6 (subquery - unnest or no_unnest)
        delete from plan_table
            where statement_id =  'mystmnt';

        commit;

        explain plan  
            set statement_id = 'mystmnt'
        for
            select *
            from tlarge tl
            where
                exists (select /*+ NO_UNNEST */ 1 from tsmall where id = tl.id); 

        select object_owner, object_name 
        from plan_table
        where 1=1
            AND statement_id = 'mystmnt'
            AND operation = 'TABLE ACCESS'
        order by id
        /*
        OBJECT_OWNER    OBJECT_NAME
        NKARAG    TLARGE
        NKARAG    TSMALL
        */

        -- example 7 (insert into select)

        -- example 8 (merge)

        -- example 9 (update)

        -- example 10 (delete)

        -- example 11 (create table as select)
        delete from plan_table
            where statement_id =  'mystmnt';

        commit;

        explain plan  
            set statement_id = 'mystmnt'
        for
            create table lala
            as 
               select *
                from tlarge tl
                where
                    exists (select /*+ NO_UNNEST */ 1 from tsmall where id = tl.id); 

        select object_owner, object_name 
        from plan_table
        where 1=1
            AND statement_id = 'mystmnt'
            AND operation = 'TABLE ACCESS'
        order by id
        /*
        OBJECT_OWNER    OBJECT_NAME
        NKARAG    TLARGE
        NKARAG    TSMALL
        */

        -- example 12 (set operation)
        delete from plan_table
            where statement_id =  'mystmnt';

        commit;

        explain plan  
            set statement_id = 'mystmnt'
        for
            select *
            from (
            select id
            from tlarge
            minus
            select id
            from tsmall  
            );
            
        select object_owner, object_name 
        from plan_table
        where 1=1
            AND statement_id = 'mystmnt'
            AND operation = 'TABLE ACCESS'
        order by id; 

        /*
        OBJECT_OWNER    OBJECT_NAME
        NKARAG    TLARGE
        NKARAG    TSMALL
        */   

--***** B. Table filters
        delete from plan_table
            where statement_id =  'mystmnt';

        commit;

        explain plan  
            set statement_id = 'mystmnt'
        for
            select /*+ full(t) */ *
            from tlarge t
            where id between 3 and 67
            
        select *-- object_owner, object_name 
        from plan_table
        where 1=1
            AND statement_id = 'mystmnt'
--            AND operation = 'TABLE ACCESS'
        order by id; 
                    