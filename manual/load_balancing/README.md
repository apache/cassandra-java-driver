## Load balancing

A Cassandra cluster is typically composed of multiple hosts; the [LoadBalancingPolicy] \(sometimes abbreviated LBP) is a
central component that determines:

* which hosts the driver will communicate with;
* for each new query, which coordinator to pick, and which hosts to use as failover.

The policy is configured when initializing the cluster:

```java
Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withLoadBalancingPolicy(new RoundRobinPolicy())
        .build();
```

Once the cluster has been built, you can't change the policy, but you may inspect it at runtime:

```java
LoadBalancingPolicy lbp = cluster.getConfiguration().getPolicies().getLoadBalancingPolicy();
```

If you don't explicitly configure the policy, you get the default, which is a
[datacenter-aware](#dc-aware-round-robin-policy), [token-aware](#token-aware-policy) policy:

```java
new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build());
```

### Common concepts

Before we review the available implementations, we need to introduce two concepts:

#### Host distance

For each host, the policy computes a **[distance][HostDistance]** that determines how the driver will establish connections
to it:

* `LOCAL` and `REMOTE` are "active" distances, meaning that the driver will keep open connections to the host. They
  differ in the number of connections opened, depending on your [pooling options](../pooling/). Also, the
  [control connection](../control_connection/) will  favor local nodes if possible.
* `IGNORED`, as the name suggests, means that the driver will not attempt to connect.

Typically, the distance will reflect network topology (e.g. local vs. remote datacenter), although that is entirely up
to your policy. The distance can be dynamic: the driver re-checks it whenever connection pools are created (e.g. at
startup or if a node was down and just came back up); you can also trigger it with [refreshConnectedHosts]:

```java
// Re-evaluate all host distances:
cluster.getConfiguration().getPoolingOptions().refreshConnectedHosts();

// Re-evaluate the distance for a given host:
cluster.getConfiguration().getPoolingOptions().refreshConnectedHost(host);
```

#### Query plan

Each time the driver executes a query, it asks the policy to compute a **query plan**, which is a list of hosts. The
driver will then try each host in sequence, according to the [retry policy](../retries/) and
[speculative execution policy](../speculative_execution).

The contents and order of query plans are entirely up to your policy, but implementations typically return plans that:

* are different for each query, in order to balance the load across the cluster;
* only contain hosts that are known to be able to process queries, i.e. neither ignored nor down;
* favor local hosts over remote ones.

The next sections describe the implementations that are provided with the driver.


### [RoundRobinPolicy]

```java
Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withLoadBalancingPolicy(new RoundRobinPolicy())
        .build();
```

This is the most straightforward implementation. It returns query plans that include all hosts, and shift for each
query in a round-robin fashion. For example:

* query 1: host1, host2, host3
* query 2: host2, host3, host1
* query 3: host3, host1, host2
* query 4: host1, host2, host3
* etc.

All hosts are at distance `LOCAL`.

This works well for simple deployments. If you have multiple datacenters, it will be inefficient and you probably want
to switch to a DC-aware policy.


### [DCAwareRoundRobinPolicy]

```java
Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withLoadBalancingPolicy(
                DCAwareRoundRobinPolicy.builder()
                        .withLocalDc("myLocalDC")
                        .build()
        ).build();
```

This policy queries nodes of the local data-center in a round-robin fashion.

Call `withLocalDc` to specify the name of your local datacenter. You can also leave it out, and the driver will use the
datacenter of the first contact point that was reached [at initialization](../#cluster-initialization). However,
remember that the driver shuffles the initial list of contact points, so this assumes that all contact points are in the
local datacenter. In general, providing the datacenter name explicitly is a safer option.

Hosts belonging to the local datacenter are at distance `LOCAL`, and appear first in query plans (in a round-robin
fashion).

### [TokenAwarePolicy]

```java
Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withLoadBalancingPolicy(new TokenAwarePolicy(anotherPolicy))
        .build();
```

This policy adds **token awareness** on top of another policy: requests will be routed in priority to the local replicas
that own the data that is being queried.

#### Requirements

In order for token awareness to work, you should first ensure that metadata is enabled in the driver. That is the case
by default, unless it's been explicitly disabled by [QueryOptions#setMetadataEnabled][setMetadataEnabled].

Then you need to consider whether routing information (provided by [Statement#getKeyspace] and
[Statement#getRoutingKey]) can be computed automatically for your statements; if not, you may provide it yourself (if a
statement has no routing information, the query will still be executed, but token awareness will not work, so the driver
might not pick the best coordinator).

The examples assume the following CQL schema:

```
CREATE TABLE testKs.sensor_data(id int, year int, ts timestamp, data double,
                                PRIMARY KEY ((id, year), ts));
```

For [simple statements](../statements/simple/), routing information can never be computed automatically:

```java
SimpleStatement statement = new SimpleStatement(
        "SELECT * FROM testKs.sensor_data WHERE id = 1 and year = 2016");

// No routing info available:
assert statement.getKeyspace() == null;
assert statement.getRoutingKey() == null;

// Set the keyspace manually:
statement.setKeyspace("testKs");

// Set the routing key manually: serialize each partition key component to its target CQL type
ProtocolVersion protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersionEnum();
statement.setRoutingKey(
        TypeCodec.cint().serialize(1, protocolVersion),
        TypeCodec.cint().serialize(2016, protocolVersion));

session.execute(statement);
```

For [built statements](../statements/built/), the keyspace is available if it was provided while building the query; the
routing key is available only if the statement was built using the table metadata, and all components of the partition
key appear in the query:

```java
TableMetadata tableMetadata = cluster.getMetadata()
        .getKeyspace("testKs")
        .getTable("sensor_data");

// Built from metadata: all info available
BuiltStatement statement1 = select().from(tableMetadata)
        .where(eq("id", 1))
        .and(eq("year", 2016));

assert statement1.getKeyspace() != null;
assert statement1.getRoutingKey() != null;

// Built from keyspace and table name: only keyspace available
BuiltStatement statement2 = select().from("testKs", "sensor")
        .where(eq("id", 1))
        .and(eq("year", 2016));

assert statement2.getKeyspace() != null;
assert statement2.getRoutingKey() == null;
```

For [bound statements](../statements/prepared/), the keyspace is always available; the routing key is only available if
all components of the partition key are bound as variables:

```java
// All components bound: all info available
PreparedStatement pst1 = session.prepare("SELECT * FROM testKs.sensor_data WHERE id = :id and year = :year");
BoundStatement statement1 = pst1.bind(1, 2016);

assert statement1.getKeyspace() != null;
assert statement1.getRoutingKey() != null;

// 'id' hard-coded, only 'year' is bound: only keyspace available
PreparedStatement pst2 = session.prepare("SELECT * FROM testKs.sensor_data WHERE id = 1 and year = :year");
BoundStatement statement2 = pst2.bind(2016);

assert statement2.getKeyspace() != null;
assert statement2.getRoutingKey() == null;
```

For [batch statements](../statements/batch/), the routing information of each child statement is inspected; the first
non-null keyspace is used as the keyspace of the batch, and the first non-null routing key as its routing key (the idea
is that all childs should have the same routing information, since batches are supposed to operate on a single
partition). All children might have null information, in which case you need to provide the information manually as
shown previously.

#### Behavior

For any host, the distance returned by `TokenAwarePolicy` is always the same as its child policy.

When the policy computes a query plan, it will first inspect the statement's routing information. If there is none, the
policy simply acts as a pass-through, and returns the query plan computed by its child policy.

If the statement has routing information, the policy uses it to determine the replicas that hold the corresponding data.
Then it returns a query plan containing:

1. the replicas for which the child policy returns distance `LOCAL`, shuffled in a random order;
2. followed by the query plan of the child policy, skipping any host that were already returned by the previous step.

Finally, the `shuffleReplicas` constructor parameter allows you to control whether the policy shuffles the replicas in
step 1:

```java
new TokenAwarePolicy(anotherPolicy, false); // no shuffling
```

Shuffling will distribute writes better, and can alleviate hotspots caused by "fat" partitions. On the other hand,
setting it to `false` might increase the effectiveness of caching, since data will always be retrieved from the
"primary" replica. Shuffling is enabled by default.

### [LatencyAwarePolicy]

```java
Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withLoadBalancingPolicy(
                LatencyAwarePolicy.builder(anotherPolicy)
                        .withExclusionThreshold(2.0)
                        .withScale(100, TimeUnit.MILLISECONDS)
                        .withRetryPeriod(10, TimeUnit.SECONDS)
                        .withUpdateRate(100, TimeUnit.MILLISECONDS)
                        .withMininumMeasurements(50)
                        .build()
        ).build();
```

This policy adds **latency awareness** on top of another policy: it collects the latencies of queries to each host, and
will exclude the worst-performing hosts from query plans.

The builder allow you to customize various aspects of the policy:

* the [exclusion threshold][withExclusionThreshold] controls how much worse a host must perform (compared to the fastest
  host) in order to be excluded. For example, 2 means that hosts that are twice slower will be excluded;
* since a host's performance can vary over time, its score is computed with a time-weighted average; the
  [scale][withScale] controls how fast the weight given to older latencies decreases over time;
* the [retry period][withRetryPeriod] is the duration for which a slow host will be penalized;
* the [update rate][withUpdateRate] defines how often the minimum average latency (i.e. the fastest host) is recomputed;
* the [minimum measurements][withMininumMeasurements] threshold guarantees that we have enough measurements before we
  start excluding a host. This prevents skewing the measurements during a node restart, where JVM warm-up will influence
  latencies.

For any host, the distance returned by the policy is always the same as its child policy.

Query plans are based on the child policy's, except that hosts that are currently excluded for being too slow are moved
to the end of the plan.

[withExclusionThreshold]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/LatencyAwarePolicy.Builder.html#withExclusionThreshold-double-
[withMininumMeasurements]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/LatencyAwarePolicy.Builder.html#withMininumMeasurements-int-
[withRetryPeriod]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/LatencyAwarePolicy.Builder.html#withRetryPeriod-long-java.util.concurrent.TimeUnit-
[withScale]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/LatencyAwarePolicy.Builder.html#withScale-long-java.util.concurrent.TimeUnit-
[withUpdateRate]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/LatencyAwarePolicy.Builder.html#withUpdateRate-long-java.util.concurrent.TimeUnit-

### Filtering policies

[WhiteListPolicy] wraps another policy with a white list, to ensure that the driver will only ever connect to a
pre-defined subset of the cluster. The distance will be that of the child policy for hosts that are in the white list,
and `IGNORED` otherwise. Query plans are guaranteed to only contain white-listed hosts.

[HostFilterPolicy] is a generalization of that concept, where you provide the predicate that will determine if a host is
included or not.

### Implementing your own

If none of the provided policies fit your use case, you can write your own. This is an advanced topic, so we recommend
studying the existing implementations first: `RoundRobinPolicy` is a good place to start, then you can look at more
complex ones like `DCAwareRoundRobinPolicy`.


[LoadBalancingPolicy]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/LoadBalancingPolicy.html
[RoundRobinPolicy]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/RoundRobinPolicy.html
[DCAwareRoundRobinPolicy]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/DCAwareRoundRobinPolicy.html
[TokenAwarePolicy]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/TokenAwarePolicy.html
[LatencyAwarePolicy]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/LatencyAwarePolicy.html
[HostFilterPolicy]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/HostFilterPolicy.html
[WhiteListPolicy]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/policies/WhiteListPolicy.html
[HostDistance]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/HostDistance.html
[refreshConnectedHosts]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/PoolingOptions.html#refreshConnectedHosts--
[setMetadataEnabled]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/QueryOptions.html#setMetadataEnabled-boolean-
[Statement#getKeyspace]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/Statement.html#getKeyspace--
[Statement#getRoutingKey]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/Statement.html#getRoutingKey--
