-- Tags: not_supported, blocked_by_SummingMergeTree

create stream xx
(
    `date` date, 
    `id` int64, 
    `clicks` int64, 
    `price` float64, 
    `spend` float64
)
ENGINE = SummingMergeTree([price, spend])
PARTITION BY toYYYYMM(date)
ORDER BY id
SAMPLE BY id
SETTINGS index_granularity = 8192; -- { serverError 223 }
