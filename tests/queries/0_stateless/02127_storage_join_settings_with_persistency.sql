DROP STREAM IF EXISTS 02127_join_settings_with_persistency_1;
create stream 02127_join_settings_with_persistency_1 (k uint64, s string) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent=1, join_any_take_last_row=0;
SHOW create stream 02127_join_settings_with_persistency_1;
DROP STREAM IF EXISTS 02127_join_settings_with_persistency_0;
create stream 02127_join_settings_with_persistency_0 (k uint64, s string) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent=0, join_any_take_last_row=0;
SHOW create stream 02127_join_settings_with_persistency_0;
