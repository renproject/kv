# KV


[![CircleCI](https://circleci.com/gh/renproject/kv/tree/master.svg?style=shield)](https://circleci.com/gh/renproject/kv/tree/master)
![Go Report](https://goreportcard.com/badge/github.com/renproject/kv)
[![Coverage Status](https://coveralls.io/repos/github/renproject/kv/badge.svg?branch=master)](https://coveralls.io/github/renproject/kv?branch=master)

A flexible and extensible library for key-value storage.

- [x] Multiple encoding/decoding formats
- [x] Persistent database drivers
- [x] In-memory database drivers
- [x] Time-to-live caching
- [x] Safe for concurrent use

### Benchmarks results

| Database | Number of iterations run | Time (ns/op) | Memory (bytes/op) |
|----------|:------------------------:|-------------:|-------------------|
| LevelDB  |           2000           |     10784337 | 4397224           |
| BadgerDB |            100           |    200012411 | 200012411         |


Built with ❤ by Ren.
