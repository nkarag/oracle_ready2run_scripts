select customer_sk, customer_last_name, activation_date_key
from target_dw.customer_dim
where
    rownum < 100;
    
drop table nkarag.test_hash;    
    
create table nkarag.test_hash
as select customer_sk, customer_sk as customer_sk_h,  customer_last_name, customer_last_name as customer_last_name_h,  activation_date_key, activation_date_key as activation_date_key_h
from target_dw.customer_dim
where
    rownum < 100;    
    
select *
from nkarag.test_hash;    

select  customer_sk,
        SYS.DBMS_CRYPTO.Hash (
                    UTL_I18N.STRING_TO_RAW (customer_SK, 'AL32UTF8'),
                    3 /*HASH_SH1*/) hashed_val,  
        customer_last_name, 
        SYS.DBMS_CRYPTO.Hash (
                    UTL_I18N.STRING_TO_RAW (customer_last_name, 'AL32UTF8'),
                    3 /*HASH_SH1*/) hashed_val, 
        activation_date_key
from target_dw.customer_dim;


select  1,
        SYS.DBMS_CRYPTO.Hash (
                    UTL_I18N.STRING_TO_RAW (1, 'AL32UTF8'),
                    3 /*HASH_SH1*/) hashed_val,  
        'HELLO', 
        SYS.DBMS_CRYPTO.Hash (
                    UTL_I18N.STRING_TO_RAW ('HELLO', 'AL32UTF8'),
                    3 /*HASH_SH1*/) hashed_val,
        'τί χαμπάρια;', 
        SYS.DBMS_CRYPTO.Hash (
                    UTL_I18N.STRING_TO_RAW ('τί χαμπάρια;', 'AL32UTF8'),
                    3 /*HASH_SH1*/) hashed_val,                      
        SYSDATE,
        SYS.DBMS_CRYPTO.Hash (
                    UTL_I18N.STRING_TO_RAW (sysdate, 'AL32UTF8'),
                    3 /*HASH_SH1*/) hashed_val        
from DUAL;

----------------- DRAFT


select max(column_id) maxc from all_tab_columns where owner = upper('&table_owner') and table_name = upper('&table_name');

--   CUSTOMER_SK_H, CUSTOMER_LAST_NAME_H, ACTIVATION_DATE_KEY_H
select column_name, case when column_id < &&max_columns then
            case when regexp_instr(upper('&&hash_col_list'), '(^|\s)+'||column_name||'(,|\s|$)+') > 0 then 
                'SYS.DBMS_CRYPTO.Hash (UTL_I18N.STRING_TO_RAW ('||column_name||', ''AL32UTF8''), 3 /*HASH_SH1*/)'
                ||'||'||'''&&column_separator'''||'||'
            else
                column_name||'||'||'''&&column_separator'''||'||'
            end
        else
            case when regexp_instr(upper('&&hash_col_list'), '(^|\s)+'||column_name||'(,|\s|$)+') > 0 then 
                'SYS.DBMS_CRYPTO.Hash (UTL_I18N.STRING_TO_RAW ('||column_name||', ''AL32UTF8''), 3 /*HASH_SH1*/)'                
            else
                column_name
            end
        end
from all_tab_columns
where
    owner = upper('&&table_owner') and table_name = upper('&&table_name')
order by column_id;


customer_sk_h, customer_last_name_h, activation_date_key_h


select upper('&&hash_col_list')
from dual


select *
from dual
where
    regexp_instr(upper('customer_sk_h, customer_last_name_h, activation_date_key_h'), '(^|\s)+'||'CUSTOMER_SK_H'||'(,|\s|$)+') > 0
    
    
select
'CUSTOMER_SK'||';'||
'CUSTOMER_SK_H'||';'||
'CUSTOMER_LAST_NAME'||';'||
'CUSTOMER_LAST_NAME_H'||';'||
'ACTIVATION_DATE_KEY'||';'||
'ACTIVATION_DATE_KEY_H'
from dual
union all
select
CUSTOMER_SK                    CUSTOMER_SK||';'||
CUSTOMER_SK_H                  SYS.DBMS_CRYPTO.Hash (UTL_I18N.STRING_TO_RAW (CUSTOMER_SK_H, 'AL32UTF8'), 3 /*HASH_SH1*/)||';'||
CUSTOMER_LAST_NAME             CUSTOMER_LAST_NAME||';'||
CUSTOMER_LAST_NAME_H           SYS.DBMS_CRYPTO.Hash (UTL_I18N.STRING_TO_RAW (CUSTOMER_LAST_NAME_H, 'AL32UTF8'), 3 /*HASH_SH1*/)||';'||
ACTIVATION_DATE_KEY            ACTIVATION_DATE_KEY||';'||
ACTIVATION_DATE_KEY_H          SYS.DBMS_CRYPTO.Hash (UTL_I18N.STRING_TO_RAW (ACTIVATION_DATE_KEY_H, 'AL32UTF8'), 3 /*HASH_SH1*/)
from nkarag.test_hash;
    