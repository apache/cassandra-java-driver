## Frequently Asked Questions

### How do I implement paging?

When using [native protocol](../features/native_protocol/) version 2 or
higher, the driver automatically pages large result sets under the hood.
You can also save the paging state to resume iteration later. See [this
page](../features/paging/) for more information.

Native protocol v1 does not support paging, but you can emulate it in
CQL with `LIMIT` and the `token()` function. See
[this conversation](https://groups.google.com/a/lists.datastax.com/d/msg/java-driver-user/U2KzAHruWO4/6vDmUVDDkOwJ) on the mailing list.

There is no trivial solution for offset queries (e.g. jump to page 20
directly). Cassandra does not implement them out of the box, see
[CASSANDRA-6511](https://issues.apache.org/jira/browse/CASSANDRA-6511).
