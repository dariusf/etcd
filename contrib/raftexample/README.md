
- Clone and build (see [the original readme](https://github.com/etcd-io/etcd/tree/main/contrib/raftexample); tested on Go 1.16.3)
- [Use TLC to generate a trace (or use a pre-generated one)](https://github.com/dranov/cs6213-project/tree/master/traces)
- Run the interpreter with the trace as input

```sh
# Start from a clean state
rm -rf raftexample-*

./raftexample --nodes 3 --file trace.json --debug
```
