DROP TABLE NMR_MV;

CREATE TABLE NMR_MV (
MVIEW_OWNER VARCHAR2(30),
MVIEW_NAME  VARCHAR2(30),
VIEW_OWNER VARCHAR2(30),
VIEW_NAME      VARCHAR2(30)
);

DROP TABLE NMR_TBL_MV;

CREATE TABLE NMR_TBL_MV (
MVIEW_OWNER VARCHAR2(30),
MVIEW_NAME  VARCHAR2(30),
VIEW_OWNER VARCHAR2(30),
VIEW_NAME      VARCHAR2(30),
TABLE_OWNER VARCHAR2(30),
TABLE_NAME      VARCHAR2(30)
);

DROP TABLE NMR_TBLCOLS_MV;

CREATE TABLE NMR_TBLCOLS_MV (
MVIEW_OWNER VARCHAR2(30),
MVIEW_NAME  VARCHAR2(30),
VIEW_OWNER VARCHAR2(30),
VIEW_NAME      VARCHAR2(30),
TABLE_OWNER VARCHAR2(30),
TABLE_NAME      VARCHAR2(30),
COL_NAME            VARCHAR2(30)
);

-- 1.
-- find MVs and their VIEWs loaded from a package passes as a parameter. Package has to be created in a custom way in order to have only tyhe procedures required to be checked
TRUNCATE TABLE NMR_MV;

DECLARE
    CURSOR fet_mvs IS 
        SELECT ANAL.OWNER, ANAL.MVIEW_NAME, REL.DETAILOBJ_OWNER VIEW_ONWER, REL.DETAILOBJ_NAME VIEW_NAME 
                    FROM ALL_MVIEW_ANALYSIS ANAL,
                               ALL_MVIEW_DETAIL_RELATIONS   REL
                    WHERE LAST_REFRESH_DATE BETWEEN TRUNC(SYSDATE, 'MM') AND SYSDATE
                    AND ANAL.MVIEW_NAME = REL.MVIEW_NAME
                    AND ANAL.OWNER = REL.OWNER 
                    ORDER BY 1, 2;
    my_name VARCHAR2(2000);
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GPAPOUTSOPOULOS.NMR_MV';
--
    FOR i IN fet_mvs LOOP
        BEGIN
        SELECT DISTINCT NAME 
            INTO my_name
            FROM ALL_SOURCE
            WHERE UPPER(TEXT) LIKE '%'||i.OWNER||'.'||i.MVIEW_NAME||'%'
            AND NAME = '&A';
        INSERT INTO GPAPOUTSOPOULOS.NMR_MV (MVIEW_OWNER, MVIEW_NAME, VIEW_OWNER, VIEW_NAME)
            VALUES (i.OWNER, i.MVIEW_NAME, i.VIEW_ONWER, i.VIEW_NAME);
        COMMIT;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN 
                NULL;
        END;    
    END LOOP;
END;

-- 2.
-- find tables related with a view in DWH schemas
TRUNCATE TABLE NMR_TBL_MV;

DECLARE
  temp_string   VARCHAR2(32767);
    CURSOR fet_views IS 
        SELECT VIEWS.OWNER, VIEWS.VIEW_NAME, VIEWS.TEXT, NMR.MVIEW_NAME, NMR.MVIEW_OWNER
            FROM ALL_VIEWS VIEWS,
                       NMR_MV NMR
            WHERE TEXT_LENGTH < 32767
                        AND VIEWS.OWNER = NMR.VIEW_OWNER
                        AND VIEWS.VIEW_NAME = NMR.VIEW_NAME;
    CURSOR fet_tables IS
        SELECT OWNER, TABLE_NAME
            FROM ALL_TABLES
            WHERE (OWNER LIKE '%_DW' OR OWNER LIKE '%_PERIF');
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GPAPOUTSOPOULOS.NMR_TBL_MV';
--
    FOR i IN fet_views LOOP
        temp_string := i.TEXT;
        FOR j IN fet_tables LOOP
            IF (INSTR(temp_string, j.OWNER||'.'||j.TABLE_NAME) > 0) THEN
                INSERT 
                    INTO NMR_TBL_MV 
                        (MVIEW_OWNER, MVIEW_NAME, VIEW_OWNER, VIEW_NAME, TABLE_OWNER, TABLE_NAME)
                        VALUES (i.MVIEW_OWNER, i.MVIEW_NAME, i.OWNER, i.VIEW_NAME, j.OWNER, j.TABLE_NAME);
            END IF;
        END LOOP;
        COMMIT;
    END LOOP;
END;

-- 3.
-- find used table columns from the existent in the mvs tables
TRUNCATE TABLE NMR_TBLCOLS_MV;

DECLARE
  temp_string   VARCHAR2(32767);
    CURSOR fet_cols IS 
        SELECT NMR.MVIEW_OWNER, NMR.MVIEW_NAME, NMR.VIEW_OWNER, NMR.VIEW_NAME, NMR.TABLE_OWNER, NMR.TABLE_NAME, CLS.COLUMN_NAME,    VIEWS.TEXT
            FROM ALL_TAB_COLUMNS CLS,
                       NMR_TBL_MV NMR,
                       ALL_VIEWS VIEWS
            WHERE NMR.TABLE_OWNER = CLS.OWNER
                        AND NMR.TABLE_NAME = CLS.TABLE_NAME
                        AND VIEWS.TEXT_LENGTH < 32767
                        AND VIEWS.OWNER = NMR.VIEW_OWNER
                        AND VIEWS.VIEW_NAME = NMR.VIEW_NAME;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE GPAPOUTSOPOULOS.NMR_TBLCOLS_MV';
--
    FOR i IN fet_cols LOOP
        temp_string := i.TEXT;
        IF (INSTR(temp_string, i.COLUMN_NAME) > 0) THEN
            INSERT 
                INTO NMR_TBLCOLS_MV 
                    (MVIEW_OWNER, MVIEW_NAME, VIEW_OWNER, VIEW_NAME, TABLE_OWNER, TABLE_NAME, COL_NAME)
                    VALUES (i.MVIEW_OWNER, i.MVIEW_NAME, i.VIEW_OWNER, i.VIEW_NAME, i.TABLE_OWNER, i.TABLE_NAME, i.COLUMN_NAME);
            END IF;
        COMMIT;
    END LOOP;
END;



