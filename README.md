[![GoDoc](https://godoc.org/github.com/turbobytes/infreqdb?status.svg)](https://godoc.org/github.com/turbobytes/infreqdb)

# infreqdb
S3 backed key/value database for infrequent read access

## Use-Cases

infreqdb might be useful if :-

1. Your database is quite large.
2. You mostly do bulk updates.
3. Most of the data is cold, i.e. only a small subset of data is typically queried.
4. You are able to partition your data in a way such that hot and cold objects live on different partitions.
5. Your data can fit in key/value model

## Architecture

The source of truth of all data is a bucket in S3. The data is split into multiple partitions. Each partition is a [Bolt](https://github.com/boltdb/bolt/) database file. infreqdb caches partitions on disk. Changes to a partition is done by re-writing and uploading an entire partition. The partitions are stored gzipped.

## Motivation

I have a PostgreSQL database(mostly time series) thats consuming about 500GB (and growing) of storage. The data is output of batch processing scripts, which process an hour worth of data each time and merge it in the database. The queries are mostly for fresh data.

500GB of might take 1500 GB disk storage - 2 replicas for HA and 250GB extra per replica to accommodate growth. Whereas the same data compressed might be 300GB (I haven't done an export yet) on S3. S3 is already replicated.

For comparison.

- 1500 GB EBS(gp2) costs $150/month
- 300 GB on S3 costs $6.9/month + extra for requests, no bandwidth charges if running in EC2.

There are additional charges for requests when using S3. If the data/partition structure is not optimized, it can end up costing a lot.

$0.005 per 1,000 requests for PUT, COPY, POST, or LIST Requests
$0.004 per 10,000 requests for GET and all other Requests

In my use-case, I expect to do a maximum of 100 PUTs per hour = 100 * 24 * 31 = 74400/month costing $0.372/month

GET/HEAD should cost even less, depends on cache HIT ratio I can achieve.

## Usage

infreqdb is a library, not a database server.

Example: [toyexample](examples/toyexample).

## Ideas

1. Make storage pluggable.
2. Make cluster that can gossip evictions, take ownership of a portion of data.
3. Allow cached partitions to persist across restarts.

## Disclaimer

I have not yet used infreqdb for anything large, just the [toyexample](examples/toyexample/main.go)

Backwards compatibility is not guaranteed. I am making changes to the API as I start using this library for real-world application.
