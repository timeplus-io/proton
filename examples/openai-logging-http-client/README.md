# OpenAI LLM Call Tracker

This example provides a Python implementation to track all HTTP requests made to OpenAI's API, log these requests and responses into a Proton stream for monitoring purposes, and analyze the latency by model using SQL queries.

## Overview

This solution utilizes the following technologies:

- **httpx.Client**: For making and intercepting HTTP requests.
- **Proton**: To store and monitor HTTP call logs.
- **Proton Driver**: To insert data into Proton.

### Key Features:
1. Intercept OpenAI API calls.
2. Log each call’s request and response metadata into a Timeplus stream.
3. Analyze the latency of the requests by model name using SQL.

## Getting Started

### Prerequisites

1. **Proton Driver** for interacting with Timeplus.
2. **OpenAI Python library** for making API calls.
3. **Proton** account and stream setup.

### Quick start

1. run `pip install openai proton-driver` to install all python dependency
2. run following SQL to create the log stream
```sql
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
```
3. setup related environment 
```bash
export PROTON_HOST=localhost
export PROTON_USER=user_name
export PROTON_PASSWORD=password
export OPENAI_API_KEY=sk-xxx

```

3. create an instance of `TimeplusStreamLogHTTPClient` and use that as the http client when create openAI client

```python
log_client = TimeplusStreamLogHTTPClient()
client = OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        http_client=log_client
    )
```

And when the the client call openAI API, all the request response will be logged in stream `llm_calls`

here is an sample of how to analysis the latency of all the LLM requests

```sql
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
```

