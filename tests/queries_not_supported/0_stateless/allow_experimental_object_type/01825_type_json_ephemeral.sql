-- Tags: no-fasttest

SET allow_experimental_object_type = 1;

DROP STREAM IF EXISTS t_github_json;

CREATE stream t_github_json
(
    event_type low_cardinality(string) DEFAULT JSONExtractString(message_raw, 'type'),
    repo_name low_cardinality(string) DEFAULT JSONExtractString(message_raw, 'repo', 'name'),
    message JSON DEFAULT message_raw,
    message_raw string EPHEMERAL
) ENGINE = MergeTree ORDER BY (event_type, repo_name);

INSERT INTO t_github_json (message_raw) FORMAT JSONEachRow {"message_raw": "{\"type\":\"PushEvent\", \"created_at\": \"2022-01-04 07:00:00\", \"actor\":{\"avatar_url\":\"https://avatars.githubusercontent.com/u/123213213?\",\"display_login\":\"github-actions\",\"gravatar_id\":\"\",\"id\":123123123,\"login\":\"github-actions[bot]\",\"url\":\"https://api.github.com/users/github-actions[bot]\"},\"repo\":{\"id\":1001001010101,\"name\":\"some-repo\",\"url\":\"https://api.github.com/repos/some-repo\"}}"}

SELECT * FROM t_github_json ORDER BY event_type, repo_name;

DROP STREAM t_github_json;
