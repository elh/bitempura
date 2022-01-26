## TODO:
- [x] [API v1 done](https://github.com/elh/bitempura/blob/main/db.go). [In-memory implementation](https://github.com/elh/bitempura/blob/main/memory/db.go)
    - [x] Get
    - [x] List
    - [x] Set
    - [x] Delete
- [x] [XTDB, Robinhood example tests pass](https://github.com/elh/bitempura/blob/main/memory/db_examples_test.go)
- [x] Split out in-memory implementation
- [x] History API?
    - [ ] ReadOpt's for History
- [x] Thread safe writes
    - [x] Show issue with race detector
- [ ] Exported ReadOpt and WriteOpt handling
- [ ] Exported DB test harness
- [ ] Performance/memory usage benchmarking
    - [ ] Profiling
- [ ] Visualizations. Interactive?

Candidates
- [ ] Write about new intuition about mutations + the 2D time graph
    - [ ] Valid time management as a custom "version rule"?
    - [ ] "Domain time"?
    - [ ] Explore geographical map idea. 2D of data + transaction time => 3 dimensions?
- [ ] Separate "db" and "storage" models? first pass was blending XTDB APIs with Snodgrass style records and things are getting muddled. Storage layer will inform choices for querying ability at DB layer.
    - [ ] Should data read and write APIs return tx time and valid time context at all?
- [ ] SQL backed implementation?
- [ ] Consider Datomic accumulate and retract event style. Immutable storage layer?
