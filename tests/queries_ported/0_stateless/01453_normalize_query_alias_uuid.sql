SELECT normalize_query('SELECT 1 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`'),
    normalized_query_hash('SELECT 1 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`')
  = normalized_query_hash('SELECT 2 AS `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeef`');
