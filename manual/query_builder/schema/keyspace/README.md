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

## Keyspace

A keyspace is a top-level namespace that defines a name, replication strategy and configurable
options. [SchemaBuilder] offers API methods for creating, altering and dropping keyspaces.

### Creating a Keyspace (CREATE KEYSPACE)

To start a `CREATE KEYSPACE` query, use `createKeyspace` in [SchemaBuilder]:

```java
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

CreateKeyspaceStart create = createKeyspace("cycling");
```

Like all other `CREATE` queries, one may supply `ifNotExists()` to require that the keyspace should
only be created if it doesn't already exist, i.e.:

```java
CreateKeyspaceStart create = createKeyspace("cycling").ifNotExists();
```

Note that, at this stage, the query cannot be completed yet.  You need to provide at least a
replication strategy.  The two most widely used ones are SimpleStrategy and NetworkTopologyStrategy.

To provide a replication strategy, use one of the following API methods on `CreateKeyspaceStart`:

* `withSimpleStrategy(int replicationFactor)`
* `withNetworkTopologyStrategy(Map<String, Integer> replications)`
* `withReplicationOptions(Map<String, Object> replicationOptions)`

For example, the following builds a completed `CreateKeyspace` using `NetworkTopologyStrategy` with
a replication factor of 2 in `east` and 3 in `west`:

```java
CreateKeyspace create = createKeyspace("cycling")
    .withNetworkTopologyStrategy(ImmutableMap.of("east", 2, "west", 3));
// CREATE KEYSPACE cycling WITH replication={'class':'NetworkTopologyStrategy','east':2,'west':3}
```

Optionally, once a replication factor is provided, one may provide additional configuration when
creating a keyspace:

* `withDurableWrites(boolean durableWrites)`
* `withOption(String name, Object value)`

### Altering a Keyspace (ALTER KEYSPACE)

To start an `ALTER KEYSPACE` query, use `alterKeyspace`:

```java
AlterKeyspaceStart alterKeyspace = alterKeyspace("cycling");
```

From here, you can modify the keyspace's replication and other settings:

* `withSimpleStrategy(int replicationFactor)`
* `withNetworkTopologyStrategy(Map<String, Integer> replications)`
* `withReplicationOptions(Map<String, Object> replicationOptions)`
* `withDurableWrites(boolean durableWrites)`
* `withOption(String name, Object value)`

At least one of these operations must be used to return a completed `AlterKeyspace`, i.e.:

```java
alterKeyspace("cycling").withDurableWrites(true);
// ALTER KEYSPACE cycling WITH durable_writes=true
```

### Dropping a keyspace (DROP KEYSPACE)

To create a `DROP KEYSPACE` query, use `dropKeyspace`:

```java
dropKeyspace("cycling");
// DROP KEYSPACE cycling
```

You may also specify `ifExists`:

```java
dropKeyspace("cycling").ifExists();
// DROP KEYSPACE IF EXISTS cycling
```

[SchemaBuilder]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/SchemaBuilder.html


