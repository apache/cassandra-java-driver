<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## Token metadata

### Quick overview

[session.getMetadata().getTokenMap()][Metadata#getTokenMap]

* used for token-aware routing or analytics clients.
* immutable (must invoke again to observe changes).
* `advanced.metadata.token-map.enabled` in the configuration (defaults to true).

-----

[Metadata#getTokenMap] returns information about the tokens used for data replication. It is used
internally by the driver to send requests to the optimal coordinator when token-aware routing is
enabled. Another typical use case is data analytics clients, for example fetching a large range of
keys in parallel by sending sub-queries to each replica. 

Because token metadata can be disabled, the resulting [TokenMap] object is wrapped in an `Optional`;
to access it, you can use either a functional pattern, or more traditionally test first with
`isPresent` and then unwrap:  

```java
Metadata metadata = session.getMetadata();

metadata.getTokenMap().ifPresent(tokenMap -> {
  // do something with the map
});

if (metadata.getTokenMap().isPresent()) {
  TokenMap tokenMap = metadata.getTokenMap().get();
  // do something with the map
}
```


### `TokenMap` methods

For illustration purposes, let's consider a fictitious ring with 6 tokens, and a cluster of 3 nodes
that each own two tokens:

```ditaa
             node1
             /---\
        /=---+ 12+---=\
        :    \---/    :
        |             |
      /-+-\         /-+-\
node3 | 10|         | 2 | node2
      \-+-/         \-+-/
        :             :
        |             |
      /---\         /-+-\
node2 | 8 |         | 4 | node3
      \-+-/         \-+-/
        :             :
        |    /---\    |
        \=---+ 6 +---=/
             \---/
             node1
```

The first thing you can do is retrieve all the ranges, in other words describe the ring:

```java
Set<TokenRange> ring = tokenMap.getTokenRanges();
// Returns [Murmur3TokenRange(Murmur3Token(12), Murmur3Token(2)),
//          Murmur3TokenRange(Murmur3Token(2), Murmur3Token(4)),
//          Murmur3TokenRange(Murmur3Token(4), Murmur3Token(6)),
//          Murmur3TokenRange(Murmur3Token(6), Murmur3Token(8)),
//          Murmur3TokenRange(Murmur3Token(8), Murmur3Token(10)),
//          Murmur3TokenRange(Murmur3Token(10), Murmur3Token(12))]
```

Note: `Murmur3Token` is an implementation detail. The actual class depends on the partitioner
you configured in Cassandra, but in general you don't need to worry about that. `TokenMap` provides
a few utility methods to parse tokens and create new instances: `parse`, `format`, `newToken` and
`newTokenRange`.

You can also retrieve the ranges and tokens owned by a specific replica:

```java
tokenMap.getTokenRanges(node1);
// [Murmur3TokenRange(Murmur3Token(10), Murmur3Token(12)),
//  Murmur3TokenRange(Murmur3Token(4), Murmur3Token(6))]

tokenMap.getTokens(node1);
// [Murmur3Token(12)), Murmur3Token(6))]
``` 

As shown here, the node owns the ranges that *end* with its tokens; this is because ranges are
start-exclusive and end-inclusive: `]10, 12]` and `]4, 6]`.

Next, you can retrieve keyspace-specific information. To illustrate this, let's use two keyspaces
with different replication settings:

```
// RF = 1: each range is only stored on the primary replica
CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

// RF = 2: each range is stored on the primary replica, and replicated on the next node in the ring
CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};
```

`getReplicas` finds the nodes that have the data in a given range:

```java
TokenRange firstRange = tokenMap.getTokenRanges().iterator().next();
// Murmur3TokenRange(Murmur3Token(12), Murmur3Token(2))

Set<Node> nodes1 = tokenMap.getReplicas(CqlIdentifier.fromCql("ks1"), firstRange);
// [node2] (only the primary replica)

Set<Node> nodes2 = tokenMap.getReplicas(CqlIdentifier.fromCql("ks2"), firstRange);
// [node2, node3] (the primary replica, and the next node on the ring)
```

There is a also a variant that takes a primary key, to find the replicas for a particular row. In
the following example, let's assume that the key hashes to the token "1" with the current
partitioner:

```java
String pk = "johndoe@example.com";
// You need to manually encode the key as binary:
ByteBuffer encodedPk = TypeCodecs.TEXT.encode(pk, session.getContext().getProtocolVersion());

Set<Node> nodes1 = tokenMap.getReplicas(CqlIdentifier.fromInternal("ks1"), encodedPk);
// Assuming the key hashes to "1", it is in the ]12, 2] range
// => [node2] (only the primary replica)

Set<Node> nodes2 = tokenMap.getReplicas(CqlIdentifier.fromCql("ks2"), encodedPk);
// [node2, node3] (the primary replica, and the next node on the ring)
```

Finally, you can go the other way, and find the token ranges that a node stores for a given
keyspace:

```java
Set<TokenRange> ranges1 = tokenMap.getTokenRanges(CqlIdentifier.fromCql("ks1"), node1);
// [Murmur3TokenRange(Murmur3Token(4), Murmur3Token(6)),
//  Murmur3TokenRange(Murmur3Token(10), Murmur3Token(12))]
// (only its primary ranges)

Set<TokenRange> ranges2 = tokenMap.getTokenRanges(CqlIdentifier.fromCql("ks2"), node1);
// [Murmur3TokenRange(Murmur3Token(2), Murmur3Token(4)),
//  Murmur3TokenRange(Murmur3Token(4), Murmur3Token(6)),
//  Murmur3TokenRange(Murmur3Token(8), Murmur3Token(10)),
//  Murmur3TokenRange(Murmur3Token(10), Murmur3Token(12))]
// (its primary ranges, and a replica of the primary ranges of node3, the previous node on the ring)
```

### Configuration

#### Enabling/disabling

You can disable token metadata globally from the configuration:

```
datastax-java-driver.advanced.metadata.token-map.enabled = false
```

If it is disabled at startup, [Metadata#getTokenMap] will stay empty, and token-aware routing won't
work (requests will be sent to a non-optimal coordinator). If you disable it at runtime, it will
keep the value of the last refresh, and token-aware routing might operate on stale data.

#### Relation to schema metadata

The keyspace-specific information in `TokenMap` (all methods with a `CqlIdentifier` argument) relies
on [schema metadata](../schema/). If schema metadata is disabled or filtered, token metadata will
also be unavailable for the excluded keyspaces.


[Metadata#getTokenMap]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/metadata/Metadata.html#getTokenMap--
[TokenMap]:             https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/metadata/TokenMap.html
