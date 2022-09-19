create stream IF NOT EXISTS sample_incorrect
(`x` UUID)
ENGINE = MergeTree
ORDER BY tuple(x)
SAMPLE BY x;  -- { serverError 59 }

DROP STREAM IF EXISTS sample_correct;
create stream IF NOT EXISTS sample_correct
(`x` string)
ENGINE = MergeTree
ORDER BY tuple(sipHash64(x))
SAMPLE BY sipHash64(x);

DROP STREAM sample_correct;
