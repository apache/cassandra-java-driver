# Configuration

## How Configuring the Java driver works

The two basic components in the Java driver are the `Cluster` and the `Session`.
The `Cluster` is the object to create first, and on to which all global configurations
apply. Connecting to the `Cluster` creates a `Session`. Queries are executed 
through the `Session`.

The `Cluster` object then is to be viewed as the equivalent of the `AstyanaxContext`
object. "Starting" an `AstyanaxContext` object typically returns a `Keyspace`
object, the `Keyspace` object is the equivalent of the *Java driver*’s `Session`.

Configuring a `Cluster` works with the *Builder* pattern. The `Builder` takes all
the configurations into account before building the `Cluster`.

Following are some examples of the most important configurations that were 
possible with *Astyanax* and how to translate them into *DataStax Java driver*
configurations. Please note that the Java driver has been optimized to handle most use
cases at best and even though the following sections show how to tune some various 
options, the driver should provide the best performances with the default configurations 
and these options should not be changed unless there is a good reason to.

## Connection pools

Configuration of connection pools in *Astyanax* are made through the
`ConnectionPoolConfigurationImpl`. This object gathers important configurations
that the *Java driver* has categorized in multiple *Option* and *Policy* kinds.

### Connections pools internals
Everything concerning the internal pools of connections to the *Cassandra nodes*
will be gathered in the Java driver in the [`PoolingOptions`](../../../manual/pooling):

*Astyanax*:

```java
ConnectionPoolConfigurationImpl cpool =
       new ConnectionPoolConfigurationImpl("myConnectionPool")
               .setInitConnsPerHost(2)
               .setMaxConnsPerHost(3)
```

*Java driver*:

```java
PoolingOptions poolingOptions =
       new PoolingOptions()
           .setConnectionsPerHost(HostDistance.LOCAL, 2, 3)
```
The first number is the initial number of connections, the second is the maximum number
of connections the driver is allowed to create for each host.

Note that the *Java driver* allows multiple simultaneous requests on one single
connection, as it is based upon the [*Native protocol*](../../../manual/native_protocol),
an asynchronous binary protocol that can handle up to 32768 simultaneous requests on a 
single connection. The Java driver is able to manage and distribute simultaneous requests
by itself even under high contention, and changing the default `PoolingOptions` is not
necessary most of the time except for very [specific use cases](../../../manual/pooling/#tuning-protocol-v3-for-very-high-throughputs).

### Timeouts

Timeouts concerning requests, or connections will be part of the `SocketOptions`.

*Astyanax*:

```java
ConnectionPoolConfigurationImpl cpool =
       new ConnectionPoolConfigurationImpl("myConnectionPool")
               .setSocketTimeout(3000)
               .setConnectTimeout(3000)
```

*Java Driver:*

```java
SocketOptions so =
       new SocketOptions()
           .setReadTimeoutMillis(3000)
           .setConnectTimeoutMillis(3000);
```

Changing the client timeout options might have more impacts than expected, **please make
sure to properly document before changing these options.**

## Load Balancing
Both *Astyanax* and the *Java driver* connect to multiple nodes of the *Cassandra*
cluster. Distributing requests through all the nodes plays an important role in 
the good operation of the `Cluster` and for best performances. With *Astyanax*, 
requests (or “operations”) can be sent directly to replicas that have a copy of 
the data targeted by the *“Row key”* specified in the operation. Since the *Thrift* API is
low-level, it forces the user to provide *Row keys*, known as the `TokenAware` 
connection pool type. This setting is also present in the *Java driver*, however 
the configuration is different and provides more options to tweak.

Load balancing in the *Java driver* is a *Policy*, it is a class that will be
plugged in the *Java driver*’s code and the Driver will call its methods when it 
needs to. The *Java driver* comes with a preset of specific load balancing policies. 
Here’s an equivalent code:

*Astyanax*:

```java
final ConnectionPoolType poolType = ConnectionPoolType.TOKEN_AWARE;
final NodeDiscoveryType discType = NodeDiscoveryType.RING_DESCRIBE;
ConnectionPoolConfigurationImpl cpool =
       new ConnectionPoolConfigurationImpl("myConnectionPool")
               .setLocalDatacenter("myDC")
AstyanaxConfigurationImpl aconf =
       new AstyanaxConfigurationImpl()
               .setConnectionPoolType(poolType)
               .setDiscoveryType(discType)
```

*Java driver*:

```java
LoadBalancingPolicy lbp = new TokenAwarePolicy(
       DCAwareRoundRobinPolicy.builder()
       .withLocalDc("myDC")
       .build()
);
```

*By default* the *Java driver* will instantiate the exact Load balancing policy 
shown above, with the `LocalDC` being the DC of the first host the driver connects 
to. So to get the same behaviour than the *TokenAware* pool type of *Astyanax*, 
users shouldn’t need to specify a load balancing policy since the default one 
should cover it.

Important: Note that since *CQL* is an abstraction of the Cassandra’s architecture, a simple 
query needs to have the *Row key* specified explicitly on a `Statement` in order 
to benefit from the *TokenAware* routing (the *Row key* in the *Java driver* is 
referenced as *Routing Key*), unlike the *Astyanax* driver. 
Some differences occur related to the different kinds of `Statements` the *Java
driver* provides. Please see [this link](../../../manual/load_balancing/#token-aware-policy) 
for specific information.

Custom load balancing policies can easily be implemented by users, and supplied to 
the *Java driver* for specific use cases. All information necessary is available
in the [Load balaning policies docs](../../../manual/load_balancing).

## Consistency levels
Consistency levels can be set per-statement, or globally through the `QueryOptions`.

*Astyanax*:

```java
AstyanaxConfigurationImpl aconf =
       new AstyanaxConfigurationImpl()
               .setDefaultReadConsistencyLevel(ConsistencyLevel.CL*ALL)
               .setDefaultWriteConsistencyLevel(ConsistencyLevel.CL*ALL)
```

*Java driver*:

```java
QueryOptions qo = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ALL);
```

Since the *Java driver* only executes *CQL* statements, which can be either reads
or writes to *Cassandra*, it is not possible to globally configure the
Consistency Level for only reads or only writes. To do so, since the Consistency 
Level can be set per-statement, you can either set it on every statement, or use 
`PreparedStatements` (if queries are to be repeated with different values): in
this case, setting the CL on the `PreparedStatement`, causes the `BoundStatements` to 
inherit the CL from the prepared statements they were prepared from. More
informations about how `Statement`s work in the *Java driver* are detailed
in the [“Queries and Results” section](../queries_and_results/).


## Authentication

Authentication settings are managed by the `AuthProvider` class in the *Java driver*.
It can be highly customizable, but also comes with default simple implementations:

*Astyanax*:

```java
AuthenticationCredentials authCreds = new SimpleAuthenticationCredentials("username", "password");
ConnectionPoolConfigurationImpl cpool =
       new ConnectionPoolConfigurationImpl("myConnectionPool")
               .setAuthenticationCredentials(authCreds)
```

*Java driver*:

```java
AuthProvider authProvider = new PlainTextAuthProvider("username", "password");
```

The class `AuthProvider` can be easily implemented to suit the user’s needs,
documentation about the classes needed is [available there](../../../manual/auth/).

## Hosts and ports

Setting the “seeds” or first hosts to connect to can be done directly on the 
Cluster configuration Builder:

*Astyanax*:

```java
ConnectionPoolConfigurationImpl cpool =
       new ConnectionPoolConfigurationImpl("myConnectionPool")
               .setSeeds("127.0.0.1")
               .setPort(9160)
```

*Java driver*:

```java
Cluster cluster = Cluster.builder()
       .addContactPoint("127.0.0.1")
       .withPort(9042)
```

The *Java driver* by default connects to port *9042*, hence you can supply only
host names with the `addContactPoints(String...)` method. Note that the contact
points are only the entry points to the `Cluster` for the *Automatic discovery
phase*.

## Building the Cluster
With all options previously presented, one may configure and create the
`Cluster` object this way:

*Java driver*:

```java
Cluster cluster = Cluster.builder()
       .addContactPoint("127.0.0.1")
       .withAuthProvider(authProvider)
       .withLoadBalancingPolicy(lbp)
       .withSocketOptions(so)
       .withPoolingOptions(poolingOptions)
       .withQueryOptions(qo)
       .build();
Session session = cluster.connect();
```

## Best Practices

A few best practices are summed up in [this blog post](http://www.datastax.com/dev/blog/4-simple-rules-when-using-the-datastax-drivers-for-cassandra).

Concerning connection pools, the Java driver’s default settings should allow 
most of the users to get the best out of the driver in terms of throughput, 
they have been thoroughly tested and tweaked to accommodate the users’ needs. 
If one still wishes to change those, first [Monitoring the pools](../../../manual/pooling/#monitoring-and-tuning-the-pool) is
advised, then a [deep dive in the Pools management mechanism](../../../manual/pooling/) should
provide enough insight.

A lot more options are available in the different `XxxxOption`s classes, policies are
also highly customizable since the base Java driver's implementations can easily be
extended and implement user-specific actions.
