## Metadata

The driver exposes metadata about the Cassandra cluster via the [Cluster#getMetadata] method. It
returns a [Metadata] object, which contains three types of information:

* [node metadata](node/)
* [schema metadata](schema/)
* [token metadata](token/)

Metadata is mostly **immutable** (except for the fields of the [Node] class, see the "node metadata"
link above for details). Each call to `getMetadata()` will return a **new copy** if something has
changed since the last call. Do not cache the result across usages:

```java
Metadata metadata = cluster.getMetadata();

session.execute("CREATE TABLE test.foo (k int PRIMARY KEY)");

// WRONG: the metadata was retrieved before the CREATE TABLE call, it does not reflect the new table 
TableMetadata fooMetadata =
    metadata
        .getKeyspace(CqlIdentifier.fromCql("test"))
        .getTable(CqlIdentifier.fromCql("foo"));
assert fooMetadata == null;
```

On the other hand, the advantage of immutability is that a `Metadata` instance provides a
**consistent view** of the cluster at a given point in time. In other words, the token map is
guaranteed to be in sync with the node and schema metadata:

```java
Metadata metadata = cluster.getMetadata();
// Pick up any node and keyspace:
Node node = metadata.getNodes().values().iterator().next();
KeyspaceMetadata keyspace = metadata.getKeyspaces().values().iterator().next();

TokenMap tokenMap = metadata.getTokenMap().get();
// The token map is guaranteed to have the corresponding data:
Set<TokenRange> tokenRanges = tokenMap.getTokenRanges(keyspace.getName(), node);
```

This is a big improvement over previous versions of the driver, where it was possible to observe a
new keyspace in the schema metadata before the token metadata was updated.

[Cluster#getMetadata]:                          http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/Cluster.html#getMetadata--
[Metadata]:                                     http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/metadata/Metadata.html
[Node]:                                         http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/metadata/Node.html