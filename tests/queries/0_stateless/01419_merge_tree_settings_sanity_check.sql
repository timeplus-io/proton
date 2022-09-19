DROP STREAM IF EXISTS mytable_local;

create stream mytable_local
(
    created          DateTime,
    eventday         date,
    user_id          uint32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(eventday)
ORDER BY (eventday, user_id)
SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 100; -- { serverError 36 }

create stream mytable_local
(
    created          DateTime,
    eventday         date,
    user_id          uint32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(eventday)
ORDER BY (eventday, user_id)
SETTINGS number_of_free_entries_in_pool_to_lower_max_size_of_merge = 100; -- { serverError 36 }

create stream mytable_local
(
    created          DateTime,
    eventday         date,
    user_id          uint32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(eventday)
ORDER BY (eventday, user_id);

ALTER STREAM mytable_local MODIFY SETTING number_of_free_entries_in_pool_to_execute_mutation = 100;  -- { serverError 36 }

DROP STREAM mytable_local;
