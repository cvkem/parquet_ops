# Parquet experiment

In this project we are working with Parquet files. The parquet-tooling from project [arrow-rs](https://github.com/apache/arrow-rs) is used. In this project you find:
* Examples of how parquet-tools from arrow-rs can be used.
* A generic merge-tool that merges two sorted parquet-files in a new sorted parquet files
* Some tooling that generates test-files with random data.

in the examples you find a series of examples which you can run with the command `cargo run --example <example-label>`. The examples are:
* write_2NO: generate two ordered test-files, one contains rows having an even id number, and the other contains the rows with an odd id.
* merge: Take 2 or more ordered files and merge them into a single file that retains the ordering.


These tools are used to experiment with a real and often occuring use-case where we have a stream of data which is time-ordered, for example user-orders, while for querying we need data ordered by user. For large files and long-time intervals this might be challenging to get a fast search. Unless you reorder the data, however, this reording might be challenging due to the large volume of data.
