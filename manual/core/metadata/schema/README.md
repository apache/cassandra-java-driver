## Schema metadata

### Quick overview

[session.getMetadata().getKeyspaces()][Metadata#getKeyspaces]

* immutable (must invoke again to observe changes).
* getting notifications:
  [CqlSession.builder().addSchemaChangeListener][SessionBuilder#addSchemaChangeListener].
* enabling/disabling: `advanced.metadata.schema.enabled` in the configuration, or
  [session.setSchemaMetadataEnabled()][Session#setSchemaMetadataEnabled].
* filtering: `advanced.metadata.schema.refreshed-keyspaces` in the configuration.
* schema agreement: wait for the schema to replicate to all nodes (may add latency to DDL
  statements).

-----

[Metadata#getKeyspaces] returns a client-side representation of the database schema:

```java
Map<CqlIdentifier, KeyspaceMetadata> keyspaces = session.getMetadata().getKeyspaces();
KeyspaceMetadata system = keyspaces.get(CqlIdentifier.fromCql("system"));
System.out.println("The system keyspace contains the following tables:");
for (TableMetadata table : system.getTables().values()) {
  System.out.printf(
      "  %s (%d columns)%n", table.getName().asCql(true), table.getColumns().size());
}
```

Schema metadata is fully immutable (both the map and all the objects it contains). It represents a
snapshot of the database at the time of the last metadata refresh, and is consistent with the
[token map](../token/) of its parent `Metadata` object. Keep in mind that `Metadata` is itself
immutable; if you need to get the latest schema, be sure to call
`session.getMetadata().getKeyspaces()` again (and not just `getKeyspaces()` on a stale `Metadata`
reference).


### DSE

All schema metadata interfaces accessible through `Metadata.getKeyspaces()` have a DSE-specific
subtype in the package [com.datastax.dse.driver.api.core.metadata.schema]. The objects returned by
the DSE driver implement those types, so you can safely cast:

```java
for (KeyspaceMetadata keyspace : session.getMetadata().getKeyspaces().values()) {
  DseKeyspaceMetadata dseKeyspace = (DseKeyspaceMetadata) keyspace;
}
```

If you're calling a method that returns an optional and want to keep the result wrapped, use this
pattern:

```java
Optional<DseFunctionMetadata> f =
    session
        .getMetadata()
        .getKeyspace("ks")
        .flatMap(ks -> ks.getFunction("f"))
        .map(DseFunctionMetadata.class::cast);
```

For future extensibility, there is a `DseXxxMetadata` subtype for every OSS type. But currently (DSE
6.7), the only types that really add extra information are:

* [DseFunctionMetadata]: add support for the `DETERMINISTIC` and `MONOTONIC` keywords; 
* [DseAggregateMetadata]: add support for the `MONOTONIC` keyword.

All other types (keyspaces, tables, etc.) are identical to their OSS counterparts.

### Notifications

If you need to follow schema changes, you don't need to poll the metadata manually; instead,
you can register one or more listeners to get notified when changes occur:

```java
SchemaChangeListener listener =
    new SchemaChangeListenerBase() {
      @Override
      public void onTableCreated(TableMetadata table) {
        System.out.println("New table: " + table.getName().asCql(true));
      }
    };
CqlSession session = CqlSession.builder()
    .addSchemaChangeListener(listener)
    .build();

session.execute("CREATE TABLE test.foo (k int PRIMARY KEY)");
```

See [SchemaChangeListener] for the list of available methods. [SchemaChangeListenerBase] is a
convenience implementation with empty methods, for when you only need to override a few of them.

It is also possible to register one or more listeners via the configuration:

```hocon
datastax-java-driver {
  advanced {
    schema-change-listener.classes = [com.example.app.MySchemaChangeListener1,com.example.app.MySchemaChangeListener2]
  }
}
```

Listeners registered via configuration will be instantiated with reflection; they must have a public
constructor taking a `DriverContext` argument.

The two registration methods (programmatic and via the configuration) can be used simultaneously.

### Configuration

#### Enabling/disabling

You can disable schema metadata globally from the configuration:

```
datastax-java-driver.advanced.metadata.schema.enabled = false
```

If it is disabled at startup, [Metadata#getKeyspaces] will stay empty. If you disable it at runtime,
it will keep the value of the last refresh.

You can achieve the same thing programmatically with [Session#setSchemaMetadataEnabled]: if you call
it with `true` or `false`, it overrides the configuration; if you pass `null`, it reverts to the
value defined in the configuration. One case where that could come in handy is if you are sending a
large number of DDL statements from your code:

```java
// Disable temporarily, we'll do a single refresh once we're done 
session.setSchemaMetadataEnabled(false);

for (int i = 0; i < 100; i++) {
  session.execute(String.format("CREATE TABLE test.foo%d (k int PRIMARY KEY)", i));
}

session.setSchemaMetadataEnabled(null);
```

Whenever schema metadata was disabled and becomes enabled again (either through the configuration or
the API), a refresh is triggered immediately.


#### Filtering

You can also limit the metadata to a subset of keyspaces: 

```
datastax-java-driver.advanced.metadata.schema.refreshed-keyspaces = [ "users", "products" ]
```

Each element in the list can be one of the following:

1. An exact name inclusion, for example `"Ks1"`. If the name is case-sensitive, it must appear in
   its exact case.
2. An exact name exclusion, for example `"!Ks1"`.
3. A regex inclusion, enclosed in slashes, for example `"/^Ks.*/"`. The part between the slashes
   must follow the syntax rules of [java.util.regex.Pattern]. The regex must match the entire
   keyspace name (no partial matching).
4. A regex exclusion, for example `"!/^Ks.*/"`.

If the list is empty, or the option is unset, all keyspaces will match. Otherwise:

* If a keyspace matches an exact name inclusion, it is always included, regardless of what any other
  rule says.
* Otherwise, if it matches an exact name exclusion, it is always excluded, regardless of what any
  regex rule says.
* Otherwise, if there are regex rules:

  * if they're only inclusions, the keyspace must match at least one of them.
  * if they're only exclusions, the keyspace must match none of them.
  * if they're both, the keyspace must match at least one inclusion and none of the
    exclusions.

For example, given the keyspaces `system`, `ks1`, `ks2`, `data1` and `data2`, here's the outcome of
a few filters:

|Filter|Outcome|Translation|
|---|---|---|
| `[]` | `system`, `ks1`, `ks2`, `data1`, `data2` | Include all. |
| `["ks1", "ks2"]` | `ks1`, `ks2` | Include ks1 and ks2 (recommended, see explanation below). |
| `["!system"]` | `ks1`, `ks2`, `data1`, `data2` | Include all except system. |
| `["/^ks.*/"]` | `ks1`, `ks2` | Include all that start with ks. |
| `["!/^ks.*/"]` | `system`, `data1`, `data2` | Exclude all that start with ks (and include everything else). |
| `["system", "/^ks.*/"]` | `system`, `ks1`, `ks2` | Include system, and all that start with ks. |
| `["/^ks.*/", "!ks2"]` | `ks1` | Include all that start with ks, except ks2. |
| `["!/^ks.*/", "ks1"]` | `system`, `ks1`, `data1`, `data2` | Exclude all that start with ks, except ks1 (and also include everything else). |
| `["/^s.*/", /^ks.*/", "!/.*2$/"]` | `system`, `ks1` | Include all that start with s or ks, except if they end with 2. |


If an element is malformed, or if its regex has a syntax error, a warning is logged and that single
element is ignored.

The default configuration (see [reference.conf](../../configuration/reference/)) excludes all
Cassandra and DSE system keyspaces.

Try to use only exact name inclusions if possible. This allows the driver to filter on the server
side with a `WHERE IN` clause. If you use any other rule, it has to fetch all system rows and filter
on the client side.

Note that, if you change the list at runtime, `onKeyspaceAdded`/`onKeyspaceDropped` will be invoked
on your schema listeners for the newly included/excluded keyspaces. 


#### Schema agreement

Due to the distributed nature of Cassandra, schema changes made on one node might not be immediately
visible to others. If left unaddressed, this could create race conditions when successive queries
get routed to different coordinators: 

```ditaa
 Application             Driver             Node 1             Node 2
------+--------------------+------------------+------------------+---
      |                    |                  |                  |
      |  CREATE TABLE foo  |                  |                  |
      |------------------->|                  |                  |
      |                    |   send request   |                  |
      |                    |----------------->|                  |
      |                    |                  |                  |
      |                    |     success      |                  |
      |                    |<-----------------|                  |
      |   complete query   |                  |                  |
      |<-------------------|                  |                  |
      |                    |                  |                  |
      |  SELECT k FROM foo |                  |                  |
      |------------------->|                  |                  |
      |                    |   send request                      |
      |                    |------------------------------------>| schema changes not
      |                    |                                     | replicated yet
      |                    |   unconfigured table foo            |
      |                    |<------------------------------------|
      |   ERROR!           |                  |                  |
      |<-------------------|                  |                  |
      |                    |                  |                  |
```

To avoid this issue, the driver waits until all nodes agree on a common schema version:

```ditaa
 Application             Driver             Node 1
------+--------------------+------------------+-----
      |                    |                  |
      |  CREATE TABLE...   |                  |
      |------------------->|                  |
      |                    |   send request   |
      |                    |----------------->|
      |                    |                  |
      |                    |     success      |
      |                    |<-----------------|
      |                    |                  |
      |          /--------------------\       |
      |          :Wait until all nodes+------>|
      |          :agree (or timeout)  :       |
      |          \--------------------/       |
      |                    |        ^         |
      |                    |        |         |
      |                    |        +---------|
      |                    |                  |
      |   complete query   |                  |
      |<-------------------|                  |
      |                    |                  |
```

Schema agreement is checked:
 
* before a schema refresh;
* before completing a successful schema-altering query (like in our example above).

It is done by querying system tables to find out the schema version of all nodes that are currently
UP. If all the versions match, the check succeeds, otherwise it is retried periodically, until a
given timeout. This process is tunable in the driver's configuration:

```
datastax-java-driver.advanced.control-connection.schema-agreement {
  interval = 200 milliseconds
  timeout = 10 seconds
  warn-on-failure = true
}
```

After executing a statement, you can check whether schema agreement was successful or timed out with
[ExecutionInfo#isSchemaInAgreement]:

```java
ResultSet rs = session.execute("CREATE TABLE...");
if (rs.getExecutionInfo().isSchemaInAgreement()) {
  ...
}
```

You can also perform an on-demand check at any time with [Session#checkSchemaAgreementAsync] \(or
its synchronous counterpart):

```java
if (session.checkSchemaAgreement()) {
  ...
}
```

A schema agreement failure is not fatal, but it might produce unexpected results (as explained at
the beginning of this section).


##### Schema agreement in mixed-version clusters

If you're operating a cluster with different major/minor server releases (for example, Cassandra 2.1
and 2.2), schema agreement will never succeed. This is because the way the schema version is
computed changes across releases, so the nodes will report different versions even though they
actually agree (see [JAVA-750] for the technical details).

This issue would be hard to fix in a reliable way, and shouldn't be that much of a problem in
practice anyway: if you're in the middle of a rolling upgrade, you're probably not applying schema
changes at the same time.  


### Relation to token metadata

Some of the data in the [token map](../token/) relies on keyspace metadata (any method that takes a
`CqlIdentifier` argument). If schema metadata is disabled or filtered, token metadata will also be
unavailable for the excluded keyspaces.

### Performing schema updates from the client

If you issue schema-altering requests from the driver (e.g. `session.execute("CREATE TABLE ..")`),
take a look at the [Performance](../../performance/#schema-updates) page for a few tips.

[Metadata#getKeyspaces]:             https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/metadata/Metadata.html#getKeyspaces--
[SchemaChangeListener]:              https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/metadata/schema/SchemaChangeListener.html
[SchemaChangeListenerBase]:          https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/metadata/schema/SchemaChangeListenerBase.html
[Session#setSchemaMetadataEnabled]:  https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/session/Session.html#setSchemaMetadataEnabled-java.lang.Boolean-
[Session#checkSchemaAgreementAsync]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/session/Session.html#checkSchemaAgreementAsync--
[SessionBuilder#addSchemaChangeListener]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/session/SessionBuilder.html#addSchemaChangeListener-com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener-
[ExecutionInfo#isSchemaInAgreement]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/ExecutionInfo.html#isSchemaInAgreement--
[com.datastax.dse.driver.api.core.metadata.schema]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/metadata/schema/package-frame.html
[DseFunctionMetadata]:  https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/metadata/schema/DseFunctionMetadata.html
[DseAggregateMetadata]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/metadata/schema/DseAggregateMetadata.html

[JAVA-750]: https://datastax-oss.atlassian.net/browse/JAVA-750
[java.util.regex.Pattern]: https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
