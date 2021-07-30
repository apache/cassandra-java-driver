## Load balancing

### Quick overview

Which nodes the driver talks to, and in which order they are tried.

* `basic.load-balancing-policy` in the configuration.
* defaults to `DefaultLoadBalancingPolicy` (opinionated best practices).
* can have per-profile policies. 

-----

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

The driver built-in policies only ever assign the `LOCAL` or `IGNORED` distance, to avoid cross-
datacenter traffic (see below to understand how to change this behavior).

#### Query plan

Each time the driver executes a query, it asks the policy to compute a *query plan*, in other words
a list of nodes. The driver then tries each node in sequence, moving down the plan according to the
[retry policy](../retries/) and [speculative execution policy](../speculative_execution/).

The contents and order of query plans are entirely implementation-specific, but policies typically
return plans that:

* are different for each query, in order to balance the load across the cluster;
* only contain nodes that are known to be able to process queries, i.e. neither ignored nor down;
* favor local nodes over remote ones.

### Built-in policies

In previous versions, the driver provided a wide variety of built-in load balancing policies; in
addition, they could be nested into each other, yielding an even higher number of choices. In our
experience, this has proven to be too complicated: it's not obvious which policy(ies) to choose for
a given use case, and nested policies can sometimes affect each other's effects in subtle and hard-
to-predict ways.  

In driver 4+, we are taking a different approach: we provide only a handful of load balancing
policies, that we consider the best choices for most cases:

- `DefaultLoadBalancingPolicy` should almost always be used; it requires a local datacenter to be 
  specified either programmatically when creating the session, or via the configuration (see below). 
  It can also use a highly efficient slow replica avoidance mechanism, which is by default enabled.
- `DcInferringLoadBalancingPolicy` is similar to `DefaultLoadBalancingPolicy`, but does not require 
  a local datacenter to be defined, in which case it will attempt to infer the local datacenter from 
  the provided contact points. If that's not possible, it will throw an error during session 
  initialization. This policy is intended mostly for ETL tools and is not recommended for normal 
  applications.
- `BasicLoadBalancingPolicy` is similar to `DefaultLoadBalancingPolicy`, but does not have the slow 
  replica avoidance mechanism. More importantly, it is the only policy capable of operating without 
  local datacenter defined, in which case it will consider nodes in the cluster in a datacenter-
  agnostic way. Beware that this could cause spikes in cross-datacenter traffic! This policy is 
  provided mostly as a starting point for users wishing to implement their own load balancing 
  policy; it should not be used as is in normal applications.
  
You can still write a [custom implementation](#custom-implementation) if you have special
requirements.

#### Datacenter locality

By default, both `DefaultLoadBalancingPolicy` and `DcInferringLoadBalancingPolicy` **only connect to 
a single datacenter**. The rationale is that a typical multi-region deployment will collocate one or 
more application instances with each Cassandra datacenter:

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

When using these policies you **must** provide a local datacenter name, either in the configuration:

```
datastax-java-driver.basic.load-balancing-policy {
  local-datacenter = datacenter1
}
```

Or programmatically when building the session:

```java
CqlSession session = CqlSession.builder()
    .withLocalDatacenter("datacenter1")
    .build();
```

If both are provided, the programmatic value takes precedence.

For convenience, the local datacenter name may be omitted if no contact points were provided: in
that case, the driver will connect to 127.0.0.1:9042, and use that node's datacenter. This is just
for a better out-of-the-box experience for users who have just downloaded the driver; beyond that
initial development phase, you should provide explicit contact points and a local datacenter.

##### Finding the local datacenter

To check which datacenters are defined in a given cluster, you can run [`nodetool status`]. It will 
print information about each node in the cluster, grouped by datacenters. Here is an example: 

```
$ nodetool status
Datacenter: DC1
===============
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address    Load       Tokens  Owns  Host ID  Rack
UN  <IP1>      1.5 TB     256     ?     <ID1>    rack1
UN  <IP2>      1.5 TB     256     ?     <ID2>    rack2
UN  <IP3>      1.5 TB     256     ?     <ID3>    rack3

Datacenter: DC2
===============
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address    Load       Tokens  Owns  Host ID  Rack
UN  <IP4>      1.5 TB     256     ?     <ID4>    rack1
UN  <IP5>      1.5 TB     256     ?     <ID5>    rack2
UN  <IP6>      1.5 TB     256     ?     <ID6>    rack3
```

To find out which datacenter should be considered local, you need to first determine which nodes the 
driver is going to be co-located with, then choose their datacenter as local. In case of doubt, you
can also use [cqlsh]; if cqlsh is co-located too in the same datacenter, simply run the command 
below:

```
cqlsh> select data_center from system.local;

data_center
-------------
DC1 
```

#### Cross-datacenter failover

Since the driver by default only contacts nodes in the local datacenter, what happens if the whole
datacenter is down? Resuming the example shown in the diagram above, shouldn't the driver
temporarily allow app1 to connect to the nodes in DC2?

We believe that, while appealing by its simplicity, such ability is not the right way to handle a
datacenter failure: resuming our example above, if the whole DC1 datacenter went down at once, it
probably means a catastrophic failure happened in Region1, and the application node is down as well.
Failover should be cross-region instead (handled by the load balancer in the above example).

However, due to popular demand, starting with driver 4.10, we re-introduced cross-datacenter
failover in the driver built-in load balancing policies.

Cross-datacenter failover is enabled with the following configuration option:

```
datastax-java-driver.advanced.load-balancing-policy.dc-failover {
  max-nodes-per-remote-dc = 2
}
```

The default for `max-nodes-per-remote-dc` is zero, which means that failover is disabled. Setting
this option to any value greater than zero will have the following effects:

- The load balancing policies will assign the `REMOTE` distance to that many nodes *in each remote
  datacenter*.
- The driver will then attempt to open connections to those nodes. The actual number of connections
  to open to each one of those nodes is configurable, see [Connection pools](../pooling/) for
  more details. By default, the driver opens only one connection to each node.
- Those remote nodes (and only those) will then become eligible for inclusion in query plans,
  effectively enabling cross-datacenter failover.
  
Beware that enabling such failover can result in cross-datacenter network traffic spikes, if the
local datacenter is down or experiencing high latencies!

Cross-datacenter failover can also have unexpected consequences when using local consistency levels
(LOCAL_ONE, LOCAL_QUORUM and LOCAL_SERIAL). Indeed, a local consistency level may have different
semantics depending on the replication factor (RF) in use in each datacenter: if the local DC has
RF=3 for a given keyspace, but the remote DC has RF=1 for it, achieving LOCAL_QUORUM in the local DC
means 2 replicas required, but in the remote DC, only one will be required.

For this reason, cross-datacenter failover for local consistency levels is disabled by default. If
you want to enable this and understand the consequences, then set the following option to true:

```
datastax-java-driver.advanced.load-balancing-policy.dc-failover {
  allow-for-local-consistency-levels = true
}
```

##### Alternatives to driver-level cross-datacenter failover

Before you jump into the failover technique explained above, please also consider the following
alternatives:

1. **Application-level failover**: instead of letting the driver do the failover, implement the
failover logic in your application. Granted, this solution wouldn't be much better if the
application servers are co-located with the Cassandra datacenter itself. It's also a bit more work,
but at least, you would have full control over the failover procedure: you could for example decide,
based on the exact error that prevented the local datacenter from fulfilling a given request,
whether a failover would make sense, and which remote datacenter to use for that specific request.
Such a fine-grained logic is not possible with a driver-level failover. Besides, if you opt for this
approach, execution profiles can come in handy. See "Using multiple policies" below and also check
our [application-level failover example] for a good starting point.
   
2. **Infrastructure-level failover**: in this scenario, the failover is handled by the
infrastructure. To resume our example above, if Region1 goes down, the load balancers in your
infrastructure would transparently switch all the traffic intended for that region to Region2,
possibly scaling up its bandwidth to cope with the network traffic spike. This is by far the best
solution for the cross-datacenter failover issue in general, but we acknowledge that it also
requires a purpose-built infrastructure. To help you explore this option, read our [white paper].

[application-level failover example]: https://github.com/datastax/java-driver/blob/4.x/examples/src/main/java/com/datastax/oss/driver/examples/failover/CrossDatacenterFailover.java
[white paper]: https://www.datastax.com/sites/default/files/content/whitepaper/files/2019-09/Designing-Fault-Tolerant-Applications-DataStax.pdf

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
    TypeCodecs.INT.encodePrimitive(1, session.getContext().getProtocolVersion()),
    TypeCodecs.INT.encodePrimitive(2016, session.getContext().getProtocolVersion()));

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
there isn't any, the query plan is a simple round-robin shuffle of all connected nodes that are
located in the local datacenter.

If the statement has routing information, the policy uses it to determine the *local* replicas that
hold the corresponding data. Then it returns a query plan containing these replicas shuffled in
random order, followed by a round-robin shuffle of the rest of the nodes.

If cross-datacenter failover has been activated as explained above, some remote nodes may appear in
query plans as well. With the driver built-in policies, remote nodes always come after local nodes
in query plans: this way, if the local datacenter is up, local nodes will be tried first, and remote
nodes are unlikely to ever be queried. If the local datacenter goes down however, all the local
nodes in query plans will likely fail, causing the query plans to eventually try remote nodes
instead. If the local datacenter unavailability persists, local nodes will be eventually marked down
and will be removed from query plans completely from query plans, until they are back up again.

#### Customizing node distance assignment

Finally, all the driver the built-in policies accept an optional node distance evaluator that gets
invoked each time a node is added to the cluster or comes back up. If the evaluator returns a
non-null distance for the node, that distance will be used, otherwise the driver will use its
built-in logic to assign a default distance to it. This is a good way to exclude nodes or to adjust
their distance according to custom, dynamic criteria.

You can pass the node distance evaluator through the configuration:

```
datastax-java-driver.basic.load-balancing-policy {
  class = DefaultLoadBalancingPolicy
  local-datacenter = datacenter1
  evaluator.class = com.acme.MyNodeDistanceEvaluator
}
```

The node distance evaluator class must implement [NodeDistanceEvaluator], and have a public
constructor that takes a [DriverContext] argument: `public MyNodeDistanceEvaluator(DriverContext
context)`.

Sometimes it's more convenient to pass the evaluator programmatically; you can do that with
`SessionBuilder.withNodeDistanceEvaluator`:

```java
Map<Node, NodeDistance> distances = ...
CqlSession session = CqlSession.builder()
    .withNodeDistanceEvaluator((node, dc) -> distances.get(node))
    .build();
```

If a programmatic node distance evaluator evaluator is provided, the configuration option is
ignored.

### Custom implementation

You can use your own implementation by specifying its fully-qualified name in the configuration.

Study the [LoadBalancingPolicy] interface and the built-in [BasicLoadingBalancingPolicy] for the
low-level details. Feel free to extend `BasicLoadingBalancingPolicy` and override only the methods
that you wish to modify – but keep in mind that it may be simpler to just start from scratch.

### Using multiple policies

The load balancing policy can be overridden in [execution profiles](../configuration/#profiles):

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

[DriverContext]:        https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/context/DriverContext.html
[LoadBalancingPolicy]:  https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/loadbalancing/LoadBalancingPolicy.html
[BasicLoadBalancingPolicy]: https://github.com/datastax/java-driver/blob/4.x/core/src/main/java/com/datastax/oss/driver/internal/core/loadbalancing/BasicLoadBalancingPolicy.java
[getRoutingKeyspace()]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/session/Request.html#getRoutingKeyspace--
[getRoutingToken()]:    https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/session/Request.html#getRoutingToken--
[getRoutingKey()]:      https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/session/Request.html#getRoutingKey--
[NodeDistanceEvaluator]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/loadbalancing/NodeDistanceEvaluator.html
[`nodetool status`]: https://docs.datastax.com/en/dse/6.7/dse-dev/datastax_enterprise/tools/nodetool/toolsStatus.html 
[cqlsh]: https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/startCqlshStandalone.html
