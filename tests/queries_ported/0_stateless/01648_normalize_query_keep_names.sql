SELECT normalize_query_keep_names('SELECT 1 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`'),
    normalized_query_hash_keep_names('SELECT 1 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`')
  = normalized_query_hash_keep_names('SELECT 2 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeef`');
