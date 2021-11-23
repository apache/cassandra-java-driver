## Per-query keyspace

### Quick overview

Specify the keyspace separately instead of hardcoding it in the query string.

* Cassandra 4+ / DSE 6+.
* only works with simple statements.

-----

Sometimes it is convenient to send the keyspace separately from the query string, and without
switching the whole session to that keyspace either. For example, you might have a multi-tenant
setup where identical requests are executed against different keyspaces.

**This feature is only available with Cassandra 4.0 or above** ([CASSANDRA-10145]). Make sure you
are using [native protocol](../../native_protocol/) v5 or above to connect.

If you try against an older version, you will get an error:

```
Exception in thread "main" java.lang.IllegalArgumentException: Can't use per-request keyspace with protocol V4
```

*Note: at the time of writing, Cassandra 4 is not released yet. If you want to test those examples
against the development version, keep in mind that native protocol v5 is still in beta, so you'll
need to force it in the configuration: `datastax-java-driver.protocol.version = V5`*.

### Basic usage

To use a per-query keyspace, set it on your statement instance:

```java
CqlSession session = CqlSession.builder().build();
CqlIdentifier keyspace = CqlIdentifier.fromCql("test");
SimpleStatement statement =
    SimpleStatement.newInstance("SELECT * FROM foo WHERE k = 1").setKeyspace(keyspace);
session.execute(statement);
```

You can do this on [simple](../simple/), [prepared](../prepared) or [batch](../batch/) statements.

If the session is connected to another keyspace, the per-query keyspace takes precedence:

```java
CqlIdentifier keyspace1 = CqlIdentifier.fromCql("test1");
CqlIdentifier keyspace2 = CqlIdentifier.fromCql("test2");

CqlSession session = CqlSession.builder().withKeyspace(keyspace1).build();

// Will query test2.foo:
SimpleStatement statement =
    SimpleStatement.newInstance("SELECT * FROM foo WHERE k = 1").setKeyspace(keyspace2);
session.execute(statement);
```

On the other hand, if a keyspace is hard-coded in the query, it takes precedence over the per-query
keyspace:

```java
// Will query test1.foo:
SimpleStatement statement =
    SimpleStatement.newInstance("SELECT * FROM test1.foo WHERE k = 1").setKeyspace(keyspace2);
```

### Bound statements

Bound statements can't have a per-query keyspace; they only inherit the one that was set on the
prepared statement:

```java
CqlIdentifier keyspace = CqlIdentifier.fromCql("test");
PreparedStatement pst =
    session.prepare(
        SimpleStatement.newInstance("SELECT * FROM foo WHERE k = ?").setKeyspace(keyspace));

// Will query test.foo:
BoundStatement bs = pst.bind(1);
```

The rationale is that prepared statements hold metadata about the target table; if Cassandra allowed
execution against different keyspaces, it would be under the assumption that all tables have the
same exact schema, which could create issues if this turned out not to be true at runtime.

Therefore you'll have to prepare against every target keyspace. A good strategy is to do this lazily
with a cache. Here is a simple example using Guava:

```java
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

LoadingCache<CqlIdentifier, PreparedStatement> cache =
    CacheBuilder.newBuilder()
        .build(
            new CacheLoader<CqlIdentifier, PreparedStatement>() {
              @Override
              public PreparedStatement load(CqlIdentifier keyspace) throws Exception {
                return session.prepare(
                    SimpleStatement.newInstance("SELECT * FROM foo WHERE k = ?")
                        .setKeyspace(keyspace));
              }
            });
CqlIdentifier keyspace = CqlIdentifier.fromCql("test");
BoundStatement bs = cache.get(keyspace).bind(1);
```

### Relation to the routing keyspace

Statements have another keyspace-related method: `Statement.setRoutingKeyspace()`. However, the
routing keyspace is only used for [token-aware routing], as a hint to help the driver send requests
to the best replica. It does not affect the query string itself.   

If you are using a per-query keyspace, the routing keyspace becomes obsolete: the driver will use
the per-query keyspace as the routing keyspace.

```java
SimpleStatement statement =
    SimpleStatement.newInstance("SELECT * FROM foo WHERE k = 1")
       .setKeyspace(keyspace)
       .setRoutingKeyspace(keyspace); // NOT NEEDED: will be ignored
```

At some point in the future, when Cassandra 4 becomes prevalent and using a per-query keyspace is
the norm, we'll probably deprecate `setRoutingKeyspace()`.

[token-aware routing]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/session/Request.html#getRoutingKey--

[CASSANDRA-10145]: https://issues.apache.org/jira/browse/CASSANDRA-10145