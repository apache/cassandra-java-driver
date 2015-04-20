## Metadata

The driver maintains global information about the Cassandra cluster it
is connected to. It is available via `Cluster#getMetadata()`.

### Schema metadata

Use `getKeyspace(String)` or `getKeyspaces()` to get keyspace-level
metadata. From there you can access the keyspace's objects (tables, and
UDTs and UDFs if relevant).

#### Refreshes

Schema metadata gets refreshed in the following circumstances:

* **schema changes via the driver**: after successfully executing a
  schema-altering query (ex: `CREATE TABLE`), the driver waits for
  schema agreement (see below), then refreshes the schema.
* **third-party schema changes**: if another client (cqlsh, other driver
  instance...) changes the schema, the driver gets notified by Cassandra
  via a push notification. It refreshes the schema directly (there is no
  need to wait for schema agreement since Cassandra has already done it).

#### Schema agreement

Schema changes need to be propagated to all nodes in the cluster. Once
they have settled on a common version, we say that they are in
agreement.

As explained above, the driver waits for schema agreement after
executing a schema-altering query. This is to ensure that subsequent
requests (which might get routed to different nodes) see an up-to-date
version of the schema.

```ditaa
 Application             Driver           Cassandra
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
      |                    |  refresh schema  |
      |                    |----------------->|
      |                    |<-----------------|
      |   complete query   |                  |
      |<-------------------|                  |
      |                    |                  |
```

The schema agreement wait is performed synchronously, so the `execute`
call -- or the completion of the `ResultSetFuture` if you use the async
API -- will only return after it has completed.

The check is implemented by repeatedly querying system tables for the
schema version reported by each node, until they all converge to the
same value. If that doesn't happen within a given timeout, the driver
will give up waiting.  The default timeout is 10 seconds, it can be
customized when building your cluster:

```java
Cluster cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .withMaxSchemaAgreementWaitSeconds(20)
    .build();
```

After executing a statement, you can check whether schema agreement was
successful or timed out:

```java
ResultSet rs = session.execute("CREATE TABLE...");
if (rs.getExecutionInfo().isSchemaInAgreement()) {
    // schema is in agreement
} else {
    // schema agreement timed out
}
```

You can also perform an on-demand check at any time:

```java
if (cluster.getMetadata().checkSchemaAgreement()) {
    // schema is in agreement
} else {
    // schema is not in agreement
}
```

The on-demand check does not retry, it only queries system tables once
(so `maxSchemaAgreementWaitSeconds` does not apply). If you need
retries, you'll have to schedule them yourself (for example with a
custom executor).
