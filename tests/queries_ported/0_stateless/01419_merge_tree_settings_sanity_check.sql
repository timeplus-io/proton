DROP STREAM IF EXISTS mytable_local;

CREATE STREAM mytable_local
(
    created          DateTime,
    eventday         Date,
    user_id          uint32
)
ENGINE = MergeTree()
PARTITION BY to_YYYYMM(eventday)
ORDER BY (eventday, user_id)
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 100; -- { serverError 36 }

CREATE STREAM mytable_local
(
    created          DateTime,
    eventday         Date,
    user_id          uint32
)
ENGINE = MergeTree()
PARTITION BY to_YYYYMM(eventday)
ORDER BY (eventday, user_id)
SETTINGS number_of_free_entries_in_pool_to_lower_max_size_of_merge = 100; -- { serverError 36 }

CREATE STREAM mytable_local
(
    created          DateTime,
    eventday         Date,
    user_id          uint32
)
ENGINE = MergeTree()
PARTITION BY to_YYYYMM(eventday)
ORDER BY (eventday, user_id);

ALTER STREAM mytable_local MODIFY SETTING number_of_free_entries_in_pool_to_execute_mutation = 100;  -- { serverError 36 }

DROP STREAM mytable_local;
