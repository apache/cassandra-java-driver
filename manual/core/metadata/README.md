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

## Metadata

### Quick overview

[session.getMetadata()][Session#getMetadata]: node states, schema and token map.

* immutable, provides a consistent view at a given point in time (e.g. token map always matches
  schema).
* pitfall: holding onto a stale instance; must call `session.getMetadata()` again to observe
  changes.

-----

The driver exposes metadata about the Cassandra cluster via the [Session#getMetadata] method. It
returns a [Metadata] object, which contains three types of information:

* [node metadata](node/)
* [schema metadata](schema/)
* [token metadata](token/)

Metadata is mostly **immutable** (except for the fields of the [Node] class, see the "node metadata"
link above for details). Each call to `getMetadata()` will return a **new copy** if something has
changed since the last call. Do not cache the result across usages:

```java
Metadata metadata = session.getMetadata();

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
Metadata metadata = session.getMetadata();
// Pick up any node and keyspace:
Node node = metadata.getNodes().values().iterator().next();
KeyspaceMetadata keyspace = metadata.getKeyspaces().values().iterator().next();

TokenMap tokenMap = metadata.getTokenMap().get();
// The token map is guaranteed to have the corresponding data:
Set<TokenRange> tokenRanges = tokenMap.getTokenRanges(keyspace.getName(), node);
```

This is a big improvement over previous versions of the driver, where it was possible to observe a
new keyspace in the schema metadata before the token metadata was updated.

Schema and node state events are debounced. This allows you to control how often the metadata gets
refreshed. See the [Performance](../performance/#debouncing) page for more details.

[Session#getMetadata]:                          https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/session/Session.html#getMetadata--
[Metadata]:                                     https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/metadata/Metadata.html
[Node]:                                         https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/metadata/Node.html

```eval_rst
.. toctree::
   :hidden:
   :glob:
   
   node/*
   schema/*
   token/*
```
