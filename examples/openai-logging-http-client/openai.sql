-- create a stream used to mointor all http call to LLM
CREATE STREAM llm_calls
(
  `request_method` string,
  `request_url` string,
  `request_headers` map(string, string),
  `request_content` string,
  `request_time` datetime64,
  `response_time` datetime64,
  `response_status_code` int,
  `response_text` string
)

-- monitor llm call latency by model name
WITH llm_latency AS
  (
    SELECT
      request_content:model AS model, date_diff('ms', request_time, response_time ) AS latency
    FROM
      table(llm_calls)
  )
SELECT
  max(latency), min(latency), avg(latency), p90(latency), model
FROM
  llm_latency
GROUP BY
  model