alter session set nls_date_format='dd-mm-yyyy hh24:mi:ss'
/

spool test_hash.csv replace

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
CUSTOMER_SK||';'||
SYS.DBMS_CRYPTO.Hash (UTL_I18N.STRING_TO_RAW (CUSTOMER_SK_H, 'AL32UTF8'), 3 /*HASH_SH1*/)||';'||
CUSTOMER_LAST_NAME||';'||
SYS.DBMS_CRYPTO.Hash (UTL_I18N.STRING_TO_RAW (CUSTOMER_LAST_NAME_H, 'AL32UTF8'), 3 /*HASH_SH1*/)||';'||
ACTIVATION_DATE_KEY||';'||
SYS.DBMS_CRYPTO.Hash (UTL_I18N.STRING_TO_RAW (ACTIVATION_DATE_KEY_H, 'AL32UTF8'), 3 /*HASH_SH1*/)
from nkarag.test_hash;

spool off

