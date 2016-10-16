SELECT inst_id, tablespace_name, segment_file, total_blocks,
used_blocks, free_blocks, max_used_blocks, max_sort_blocks
FROM gv$sort_segment
order by 1
/