SELECT path('www.example.com:443/a/b/c') AS Path;
SELECT decode_url_component(materialize(path_full('www.example.com/?query=hello%20world+foo%2Bbar'))) AS Path;
