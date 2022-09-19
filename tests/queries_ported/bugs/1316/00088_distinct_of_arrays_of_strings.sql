-- Tags: bug, #1316

SELECT DISTINCT array_filter(x -> not_empty(x), array_join([[''], ['is_registred'], ['registration_month','user_login','is_registred'], ['is_registred'], ['is_registred'], ['']]));
