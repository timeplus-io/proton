# The One Billion Row Challenge with Proton

Back in January, Gunnar Morling kicked off a data aggregation task involving up to a billion rows nicknamed the `1brc` ([One Billion Row Challenge](https://www.morling.dev/blog/one-billion-row-challenge/)):

> “Your mission, should you decide to accept it, is deceptively simple: write a Java program for retrieving temperature measurement values from a text file and calculating the min, mean, and max temperature per weather station. There’s just one caveat: the file has 1,000,000,000 rows!”

`1brc` was intended to generate interest in the new language features that many Java developers may not be aware of and it turned out to be hugely popular amongst developers of all stripes.

The aggregation task garnered a lot of interest from other programming languages even though solutions were to be written in Java. Solutions for the challenge were written in a wide variety of languages including C, C++, C#, Dart, Elixir, Erlang, Go, JavaScript, Lua, Perl, Python, R, Rust, Scala, Swift, Zig and even less popular languages like COBOL, Crystal and KDB/Q. 

There were multiple solutions written in SQL for different SQL dialects. This isn't surprising since SQL is a general purpose language for data aggregation. 

Solutions were shared in the following SQL dialects:
* ClickHouse SQL
* Databend Cloud SQL
* DuckDB SQL
* MySQL SQL
* Postgres SQL
* Snowflake SQL



Robin Moffat shared an SQL-based solution based on DuckDB[^1], while Francesco Tisiot shared another SQL-based solution based on Postgres and ClickHouse[^2].

This article will undertake the challenge using SQL written for the open-source Proton database engine.


## Proton
Because Proton is a purpose-built streaming analytics engine, its actually comes bundled with [two data stores](https://docs.timeplus.com/proton-architecture#data-storage): 
- Timeplus NativeLog data store for real-time streaming queries and;
- ClickHouse data store for historical queries.

Note that the input data for the `1brc` is not a streaming data source. It is a static 13GB CSV file that we will query using the ClickHouse data store. 







[^1]: [1️⃣🐝🏎️🦆 (1BRC in SQL with DuckDB)](https://rmoff.net/2024/01/03/1%EF%B8%8F%E2%83%A3%EF%B8%8F-1brc-in-sql-with-duckdb/) 
[^2]: [1 billion rows challenge in PostgreSQL and ClickHouse](https://ftisiot.net/posts/1brows/)
