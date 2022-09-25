WITH to_datetime(1509138000) + number * 300 AS t SELECT to_hour(t, 'Asia/Kolkata') AS h, to_string(to_start_of_hour(t, 'Asia/Kolkata'), 'Asia/Kolkata') AS h_start FROM system.numbers LIMIT 12;
