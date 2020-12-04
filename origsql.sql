select OWNER, OBJECT_NAME, CREATED
from test_mplan
where
object_type = 'WINDOW'
/
