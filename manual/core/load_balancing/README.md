## Load balancing

A Cassandra cluster is typically composed of multiple nodes; the *load balancing policy* (sometimes
abbreviated LBP) is a central component that determines:

* which nodes the driver will communicate with;
* for each new query, which coordinator to pick, and which nodes to use as failover.

It is defined in the [configuration](../configuration/):

```
datastax-java-driver.basic.load-balancing-policy {
  class = DefaultLoadBalancingPolicy
}
```

### Concepts

#### Node distance

For each node, the policy computes a *distance* that determines how connections will be established:

* `LOCAL` and `REMOTE` are "active" distances, meaning that the driver will keep open connections to
  this node. [Connection pools](../pooling/) can be sized independently for each distance.
* `IGNORED` means that the driver will never attempt to connect.

Typically, the distance will reflect network topology (e.g. local vs. remote datacenter), although
that is entirely up to each policy implementation. It can also change over time.

#### Query plan

Each time the driver executes a query, it asks the policy to compute a *query plan*, in other words
a list of nodes. The driver then tries each node in sequence, moving down the plan according to the
[retry policy](../retries/) and [speculative execution policy](../speculative_execution/).

The contents and order of query plans are entirely implementation-specific, but policies typically
return plans that:

* are different for each query, in order to balance the load across the cluster;
* only contain nodes that are known to be able to process queries, i.e. neither ignored nor down;
* favor local nodes over remote ones.

### Default policy

In previous versions, the driver provided a wide variety of built-in load balancing policies; in
addition, they could be nested into each other, yielding an even higher number of choices. In our
experience, this has proven to be too complicated: it's not obvious which policy(ies) to choose for
a given use case, and nested policies can sometimes affect each other's effects in subtle and hard
to predict ways.  

In driver 4+, we are taking a more opinionated approach: we provide a single load balancing policy,
that we consider the best choice for most cases. You can still write a
[custom implementation](#custom-implementation) if you have special requirements.

#### Local only

The default policy **only connects to a single datacenter**. The rationale is that a typical
multi-region deployment will collocate one or more application instances with each Cassandra
datacenter:

```ditaa
                  /----+----\
                  | client  |
                  \----+----/
                       |
                       v
               /---------------\
               | load balancer |
               \-------+-------/
                       |
          +------------+------------+
          |                         |
+---------|---------+     +---------|---------+
| Region1 v         |     | Region2 v         |
|    /---------\    |     |    /---------\    |
|    |   app1  |    |     |    |   app2  |    |
|    \----+----/    |     |    \----+----/    |
|         |         |     |         |         |
|         v         |     |         v         |
|   +-----------+   |     |   +-----------+   |
|   | {s}       |   |     |   | {s}       |   |
|   | Cassandra +------=------+ Cassandra |   |
|   |    DC1    |   |     |   |    DC2    |   |
|   +-----------+   |     |   +-----------+   |
|                   |     |                   |
+-------------------+     +-------------------+
```

In previous driver versions, you could configure application-level failover, such as: "if all the
Cassandra nodes in DC1 are down, allow app1 to connect to the nodes in DC2". We now believe that
this is not the right place to handle this: if a whole datacenter went down at once, it probably
means a catastrophic failure happened in Region1, and the application node is down as well.
Failover should be cross-region instead (handled by the load balancer in this example).

Therefore the default policy does not allow remote nodes; it only ever assigns the `LOCAL` or
`IGNORED` distance, based on the local datacenter name specified in the configuration:

```
datastax-java-driver.basic.load-balancing-policy {
  class = DefaultLoadBalancingPolicy
  local-datacenter = datacenter1
}
```

This option is required, except when you didn't specify any contact points and let the driver
default to 127.0.0.1:9042 (this is mostly for convenience during the development phase).

#### Token-aware

The default policy is **token-aware** by default: requests will be routed in priority to the
replicas that own the data being queried.

##### Providing routing information

First make sure that [token metadata](../metadata/token/#configuration) is enabled.

Then your statements need to provide:

* a keyspace: if you use a [per-query keyspace](../statements/per_query_keyspace/), then it will be
  used for routing as well. Otherwise, the driver relies on [getRoutingKeyspace()];
* a routing key: it can be provided either by [getRoutingKey()] \(raw binary data) or
  [getRoutingToken()] \(already hashed as a token).

Depending on the type of statement, some of this information may be computed automatically,
otherwise you have to set it manually. The examples below assume the following CQL schema:

```
CREATE TABLE testKs.sensor_data(id int, year int, ts timestamp, data double,
                                PRIMARY KEY ((id, year), ts));
```

For [simple statements](../statements/simple/), routing information is never computed
automatically:

```java
SimpleStatement statement =
    SimpleStatement.newInstance(
        "SELECT * FROM testKs.sensor_data WHERE id = 1 and year = 2016");

// No routing info available:
assert statement.getRoutingKeyspace() == null;
assert statement.getRoutingKey() == null;

// Set the keyspace manually (skip this if using a per-query keyspace):
statement = statement.setRoutingKeyspace("testKs");

// Set the routing key manually: serialize each partition key component to its target CQL type
statement = statement.setRoutingKey(
    TypeCodecs.INT.encodePrimitive(1, session.getContext().protocolVersion()),
    TypeCodecs.INT.encodePrimitive(2016, session.getContext().protocolVersion()));

session.execute(statement);
```

For [bound statements](../statements/prepared/), the keyspace is always available; the routing key
is only available if all components of the partition key are bound as variables:

```java
// All components bound: all info available
PreparedStatement pst1 =
    session.prepare("SELECT * FROM testKs.sensor_data WHERE id = :id and year = :year");
BoundStatement statement1 = pst1.bind(1, 2016);

assert statement1.getRoutingKeyspace() != null;
assert statement1.getRoutingKey() != null;

// 'id' hard-coded, only 'year' is bound: only keyspace available
PreparedStatement pst2 =
    session.prepare("SELECT * FROM testKs.sensor_data WHERE id = 1 and year = :year");
BoundStatement statement2 = pst2.bind(2016);

assert statement2.getRoutingKeyspace() != null;
assert statement2.getRoutingKey() == null;
```

For [batch statements](../statements/batch/), the routing information of each child statement is
inspected; the first non-null keyspace is used as the keyspace of the batch, and the first non-null
routing key as its routing key (the idea is that all children should have the same routing
information, since batches are supposed to operate on a single partition). If no child has any
routing information, you need to provide it manually.

##### Policy behavior 

When the policy computes a query plan, it first inspects the statement's routing information. If
there isn't any, the query plan is a simple round-robin shuffle of all connected nodes.

If the statement has routing information, the policy uses it to determine the replicas that hold the
corresponding data. Then it returns a query plan containing the replicas shuffled in random order,
followed by a round-robin shuffle of the rest of the nodes.

#### Optional node filtering

<!-- TODO JAVA-1798 -->

Finally, the default policy accepts an optional node filter that gets applied just after the test
for inclusion in the local DC. If a node doesn't pass this test, it will be set at distance
`IGNORED` and the driver will never try to connect to it.

```
datastax-java-driver.basic.load-balancing-policy {
  class = DefaultLoadBalancingPolicy
  local-datacenter = datacenter1
  filter-class = com.acme.MyNodeFilter
}
```

This is a good way to exclude nodes on some custom criteria.

### Custom implementation

You can use your own implementation by specifying its fully-qualified name in the configuration.

Study the [LoadBalancingPolicy] interface and the default implementation for the low-level details.

### Using multiple policies

The load balancing policy can be overridden in [configuration profiles](../configuration/#profiles):

```
datastax-java-driver {
  basic.load-balancing-policy {
    class = DefaultLoadBalancingPolicy
  }
  profiles {
    custom-lbp {
      basic.load-balancing-policy {
        class = CustomLoadBalancingPolicy
      }
    }
    slow {
      request.timeout = 30 seconds
    }
  }
}
```

The `custom-lbp` profile uses a dedicated policy. The `slow` profile inherits the default profile's.
Note that this goes beyond configuration inheritance: the driver only creates a single
`DefaultLoadBalancingPolicy` instance and reuses it (this also occurs if two sibling profiles have
the same configuration).

For query plans, each request uses its declared profile's policy. If it doesn't declare any profile,
or if the profile doesn't have a dedicated policy, then the default profile's policy is used.

For node distances, the driver remembers the last distance suggested by each policy for each node.
Then it uses the "closest" distance for any given node. For example:

* for node1, policy1 suggests distance LOCAL and policy2 suggests REMOTE. node1 is set to LOCAL;
* policy1 changes its suggestion to IGNORED. node1 is set to REMOTE;
* policy1 changes its suggestion to REMOTE. node1 stays at REMOTE.

[LoadBalancingPolicy]:  https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/loadbalancing/LoadBalancingPolicy.html
[getRoutingKeyspace()]: https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/session/Request.html#getRoutingKeyspace--
[getRoutingToken()]:    https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/session/Request.html#getRoutingToken--
[getRoutingKey()]:      https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/session/Request.html#getRoutingKey-- 