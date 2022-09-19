DROP STREAM IF EXISTS skip_idx_comp_parts;
create stream skip_idx_comp_parts (a int, b int, index b_idx b TYPE minmax GRANULARITY 4)
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity=256, merge_max_block_size=100;

SYSTEM STOP MERGES skip_idx_comp_parts;

INSERT INTO skip_idx_comp_parts SELECT number, number FROM numbers(200);
INSERT INTO skip_idx_comp_parts SELECT number, number FROM numbers(200);
INSERT INTO skip_idx_comp_parts SELECT number, number FROM numbers(200);
INSERT INTO skip_idx_comp_parts SELECT number, number FROM numbers(200);

SYSTEM START MERGES skip_idx_comp_parts;
OPTIMIZE STREAM skip_idx_comp_parts FINAL;

SELECT count() FROM skip_idx_comp_parts WHERE b > 100;

DROP STREAM skip_idx_comp_parts;
