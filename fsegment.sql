select *
from dba_extents
where file_id = &FILE
            and &BLOCK between block_id and block_id + blocks - 1
/			
