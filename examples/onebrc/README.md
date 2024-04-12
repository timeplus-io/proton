# The One Billion Row Challenge with Proton

Back in January, Gunnar Morling kicked off the `1brc` ([One Billion Row Challenge](https://www.morling.dev/blog/one-billion-row-challenge/)) to generate interest in the new language features that many Java developers may not be aware of.

> “Your mission, should you decide to accept it, is deceptively simple: write a Java program for retrieving temperature measurement values from a text file and calculating the min, mean, and max temperature per weather station. There’s just one caveat: the file has 1,000,000,000 rows!”

Even though the primary language for the data aggregation challenge was Java, the challenge still garnered a lot of interest from other programming languages. Solutions for the aggregation task were written in a variety of languages like C, Go, R, Rust and .NET. 

Not surprisingly, there were quite a few solutions that used plain old SQL since it was expressly designed as a general purpose language for data aggregation. Robin Moffat shared an SQL-based solution based on DuckDB, while Francesco Tisiot shared another SQL-based solution based on Postgres and ClickHouse.

This article will undertake the challenge using SQL written for the open-source Proton database engine.


## Proton
Because Proton is a purpose-built streaming analytics engine, its actually comes bundled with [two data stores](https://docs.timeplus.com/proton-architecture#data-storage): 
- Timeplus NativeLog data store for real-time streaming queries and;
- ClickHouse data store for historical queries.

Note that the input data for the `1brc` is not a streaming data source. It is a static 13GB CSV file that we will query using the ClickHouse data store. 


