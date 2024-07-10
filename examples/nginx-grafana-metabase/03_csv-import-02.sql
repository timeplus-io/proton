set input_format_skip_unknown_fields = 1;

INSERT INTO nginx_ipinfo (ip, city, region, country, country_name, country_flag_emoji, country_flag_unicode, continent_name, isEU, loc) 
FORMAT CSVWithNames
