# bitemporal ⌛

Building intuition about bitemporal databases by building one for myself.

### TODO:
- [ ] API v1 done
    - [x] Find
    - [x] List
    - [ ] Put
    - [ ] Delete
- [ ] History API?
- [ ] Document new intuition about mutations + the 2D time graph
- [ ] Separate "db" and "storage" models. first pass was blending XTDB APIs with Snodgrass style records and things are getting muddled.
- [ ] Consider Datomic accumulate and retract event style
- [ ] Consider immutable "storage" style
- [ ] Should data read and write APIs return tx time and valid time context at all. maybe that is relegated to "history" APIs only
- [ ] SQL backed impl
- [ ] Visualization
