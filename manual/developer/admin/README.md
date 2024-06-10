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

## Administrative tasks

Aside from the main task of [executing user requests](../request_execution), the driver also needs
to track cluster state and metadata. This is done with a number of administrative components:

```ditaa
                                 +---------------+
                                 | DriverChannel |
                                 +-------+-------+
                                         |1
                                         |                 topology
+-----------------+    query   +---------+---------+        events
| TopologyMonitor +------+---->| ControlConnection +-----------------+
+-----------------+      |     +---------+---------+                 |
         ^               |               |                           |
         |               |               |     topology+channel      V
   get   |     +---------+        refresh|          events      +----------+
node info|     |                  schema |         +------------+ EventBus |
         |     |                         |         |            +-+--------+
+--------+-----+--+                      |         |              ^      ^
| MetadataManager |<-------+-------------+         |          node|      |
+--------+-------++        |                       |         state|      |
         |       |         | add/remove            v        events|      |
         |1      |         |   node     +------------------+      |      |
   +-----+----+  |         +------------+ NodeStateManager +------+      |
   | Metadata |  |                      +------------------+             |
   +----------+  |                                                       |
                 +-------------------------------------------------------+
                               metadata changed events
```

Note: the event bus is covered in the [common infrastructure](../common/event_bus) section.

### Control connection

The goal of the control connection is to maintain a dedicated `DriverChannel` instance, used to:

* listen for server-side protocol events:
  * topology events (`NEW_NODE`, `REMOVED_NODE`) and status events (`UP`, `DOWN`) are published on
    the event bus, to be processed by other components;
  * schema events are propagated directly to the metadata manager, to trigger a refresh;
* provide a way to query system tables. In practice, this is used by:
  * the topology monitor, to read node information from `system.local` and `system.peers`;
  * the metadata manager, to read schema metadata from `system_schema.*`.

It has its own reconnection mechanism (if the channel goes down, a new one will be opened to another
node in the cluster) and some logic for initialization and shutdown.

Note that the control connection is really just an implementation detail of the metadata manager and
topology monitor: if those components are overridden with custom versions that use other means to
get their data, the driver will detect it and not initialize the control connection (at the time of
writing, the session also references the control connection directly, but that's a bug:
[JAVA-2473](https://datastax-oss.atlassian.net/browse/JAVA-2473)).

### Metadata manager

This component is responsible for maintaining the contents of
[session.getMetadata()](../../core/metadata/).

One big improvement in driver 4 is that the `Metadata` object is immutable and updated atomically;
this guarantees a consistent view of the cluster at a given point in time. For example, if a
keyspace name is referenced in the token map, there will always be a corresponding
`KeyspaceMetadata` in the schema metadata.

`MetadataManager` keeps the current `Metadata` instance in a volatile field. Each transition is
managed by a `MetadataRefresh` object that computes the new metadata, along with an optional list of
events to publish on the bus (e.g. table created, keyspace removed, etc.) The new metadata is then
written back to the volatile field. `MetadataManager` follows the [confined inner
class](../common/concurrency/#cold-path) pattern to ensure that all refreshes are applied serially,
from a single admin thread. This guarantees that two refreshes can't start from the same initial
state and overwrite each other.

There are various types of refreshes targeting nodes, the schema or the token map.

Note that, unlike driver 3, we only do full schema refreshes. This simplifies the code considerably,
and thanks to debouncing this should not affect performance. The schema refresh process uses a few
auxiliary components that may have different implementations depending on the Cassandra version:

* `SchemaQueries`: launches the schema queries asynchronously, and assemble the result in a
  `SchemaRows`;
* `SchemaParser`: turns the `SchemaRows` into the `SchemaRefresh`. 

When the metadata manager needs node-related data, it queries the topology monitor. When it needs
schema-related data, it uses the control connection directly to issue its queries.

### Topology monitor

`TopologyMonitor` abstracts how we get information about nodes in the cluster:

* refresh the list of nodes;
* refresh an individual node, or load the information of a newly added node;
* check schema agreement;
* emit `TopologyEvent` instances on the bus when we get external signals suggesting topology changes
  (node added or removed), or status changes (node down or up).

The built-in implementation uses the control connection to query `system.local` and `system.peers`,
and listen to gossip events.

### Node state manager

`NodeStateManager` tracks the state of the nodes in the cluster.

We can't simply trust gossip events because they are not always reliable (the coordinator can become
isolated and think other nodes are down). Instead, the driver uses more elaborate rules that combine
external signals with observed internal state:

* as long as we have an active connection to a node, it is considered up, whatever gossip events
  say;
* if all connections to a node are lost, and its pool has started reconnecting, it gets marked down
  (we check the reconnection because the pool could have shut down for legitimate reasons, like the
  node distance changing to IGNORED);
* a node is marked back up when the driver has successfully reopened at least one connection;
* if the driver is not actively trying to connect to a node (for example if it is at distance
  IGNORED), then gossip events are applied directly.
  
See the javadocs of `NodeState` and `TopologyEvent`, as well as the `NodeStateManager`
implementation itself, for more details.

#### Topology events vs. node state events

These two event types are related, but they're used at different stages:

* `TopologyEvent` is an external signal about the state of a node (by default, a `TOPOLOGY_CHANGE`
  or `STATUS_CHANGE` gossip event received on the control connection). This is considered as a mere
  suggestion, that the driver may or may not decide to follow;
* `NodeStateEvent` is an actual decision made by the driver to change a node to a given state.

`NodeStateManager` essentially transforms topology events, as well as other internal signals, into
node state events.

In general, other driver components only react to node state events, but there are a few exceptions:
for example, if a connection pool is reconnecting and the next attempt is scheduled in 5 minutes,
but a SUGGEST_UP topology event is emitted, the pool tries to reconnect immediately.

The best way to find where each event is used is to do a usage search of the event type.

### How admin components work together

Most changes to the cluster state will involve the coordinated effort of multiple admin components.
Here are a few examples:

#### A new node gets added

```ditaa
+-----------------+   +--------+ +----------------+ +---------------+ +---------------+
|ControlConnection|   |EventBus| |NodeStateManager| |MetadataManager| |TopologyMonitor|
+--------+--------+   +---+----+ +--------+-------+ +-------+-------+ +-------+-------+
         |                |               |                 |                 |
+--------+-------+        |               |                 |                 |
|Receive NEW_NODE|        |               |                 |                 |
|gossip event    |        |               |                 |                 |
|             {d}|        |               |                 |                 |
+--------+-------+        |               |                 |                 |
         |                |               |                 |                 |
         |TopologyEvent(  |               |                 |                 |
         |  SUGGEST_ADDED)|               |                 |                 |
         +--------------->|               |                 |                 |
         |                |onTopologyEvent|                 |                 |
         |                +-------------->|                 |                 |
         |                |        +------+-------+         |                 |
         |                |        |check node not|         |                 |
         |                |        |known already |         |                 |
         |                |        |           {d}|         |                 |
         |                |        +------+-------+         |                 |
         |                |               |                 |                 |
         |                |               |     addNode     |                 |
         |                |               +---------------->|                 |
         |                |               |                 |  getNewNodeInfo |
         |                |               |                 +---------------->|
         |                |               |                 |                 |
         |                 query(SELECT FROM system.peers)                    |
         |<-------------------------------------------------------------------+
         +------------------------------------------------------------------->|
         |                |               |                 |<----------------+
         |                |               |         +-------+--------+        |
         |                |               |         |create and apply|        |   
         |                |               |         |AddNodeRefresh  |        |
         |                |               |         |             {d}|        |
         |                |               |         +-------+--------+        |
         |                |               |                 |                 |
         |                |      NodeChangeEvent(ADDED)     |                 |
         |                |<--------------------------------+                 |
         |                |               |                 |                 |
```

At this point, other driver components listening on the event bus will get notified of the addition.
For example, `DefaultSession` will initialize a connection pool to the new node.

#### A new table gets created

```ditaa
  +-----------------+               +---------------+     +---------------+ +--------+
  |ControlConnection|               |MetadataManager|     |TopologyMonitor| |EventBus|
  +--------+--------+               +-------+-------+     +-------+-------+ +---+----+
           |                                |                     |             |
+----------+----------+                     |                     |             |
|Receive SCHEMA_CHANGE|                     |                     |             |
|gossip event         |                     |                     |             |
|             {d}     |                     |                     |             |
+----------+----------+                     |                     |             |
           |                                |                     |             |
           |            refreshSchema       |                     |             |
           +------------------------------->|                     |             |
           |                                |checkSchemaAgreement |             |
           |                                +-------------------->|             |
           |                                |                     |             |           
           |         query(SELECT FROM system.local/peers)        |             |
           |<-----------------------------------------------------+             |
           +----------------------------------------------------->|             |
           |                                |                     |             |
           |                                |<--------------------+             |
           |query(SELECT FROM system_schema)|                     |             |
           |<-------------------------------+                     |             |
           +------------------------------->|                     |             |
           |                        +-------+--------+            |             |
           |                        |Parse results   |            |             |
           |                        |Create and apply|            |             |
           |                        |SchemaRefresh   |            |             |
           |                        |             {d}|            |             |
           |                        +-------+--------+            |             |
           |                                |                     |             |
           |                                |   TableChangeEvent(CREATED)       |
           |                                +---------------------------------->|
           |                                |                     |             |
```

#### The last connection to an active node drops

```ditaa
  +-----------+              +--------+   +----------------+     +----+ +---------------+
  |ChannelPool|              |EventBus|   |NodeStateManager|     |Node| |MetadataManager|
  +-----+-----+              +---+----+   +-------+--------+     +-+--+ +-------+-------+
        |                        |                |                |            |
        |ChannelEvent(CLOSED)    |                |                |            |
        +----------------------->|                |                |            |
        |                        |onChannelEvent  |                |            |
 +------+-----+                  +--------------->|                |            |
 |   start    |                  |                |decrement       |            |
 |reconnecting|                  |                |openConnections |            |
 |         {d}|                  |                +--------------->|            |
 +------+-----+                  |                |                |            |
        |ChannelEvent(           |                |                |            |
        |  RECONNECTION_STARTED) |                |                |            |
        +----------------------->|                |                |            |
        |                        |onChannelEvent  |                |            |
        |                        +--------------->|                |            |
        |                        |                |increment       |            |
        |                        |                |reconnections   |            |
        |                        |                +--------------->|            |
        |                        |                |                |            |
        |                        |       +--------+--------+       |            |
        |                        |       |detect node has  |       |            |
        |                        |       |0 connections and|       |            |
        |                        |       |is reconnecting  |       |            |
        |                        |       |           {d}   |       |            |
        |                        |       +--------+--------+       |            |
        |                        |                |set state DOWN  |            |
        |                        |                +--------------->|            |
        |                        |NodeStateEvent( |                |            |
        |                        |  DOWN)         |                |            |
 +------+-----+                  |<---------------+                |            |
 |reconnection|                  |                |                |            |
 | succeeds   |                  |                |                |            |
 |         {d}|                  |                |                |            |
 +------+-----+                  |                |                |            |
        |ChannelEvent(OPENED)    |                |                |            |
        +----------------------->|                |                |            |
        |                        |onChannelEvent  |                |            |
        |                        +--------------->|                |            |
        |                        |                |increment       |            |
        |                        |                |openConnections |            |
        |                        |                +--------------->|            |
        |                        |                |                |            |
        |                        |       +--------+--------+       |            |
        |                        |       |detect node has  |       |            |
        |                        |       |1 connection     |       |            |
        |                        |       |           {d}   |       |            |
        |                        |       +--------+--------+       |            |
        |                        |                | refreshNode    |            |
        |                        |                +---------------------------->|
        |                        |                |                |            |
        |                        |                |set state UP    |            |
        |                        |                +--------------->|            |
        |                        |NodeStateEvent( |                |            |
        |                        |  UP)           |                |            |
        |                        |<---------------+                |            |
        |ChannelEvent(           |                |                |            |
        |  RECONNECTION_STOPPED) |                |                |            |
        +----------------------->|                |                |            |
        |                        |onChannelEvent  |                |            |
        |                        +--------------->|                |            |
        |                        |                |decrement       |            |
        |                        |                |reconnections   |            |
        |                        |                +--------------->|            |
        |                        |                |                |            |
```

### Extension points

#### TopologyMonitor

This is a standalone component because some users have asked for a way to use their own discovery
service instead of relying on system tables and gossip (see
[JAVA-1082](https://datastax-oss.atlassian.net/browse/JAVA-1082)).

A custom implementation can be plugged by [extending the
context](../common/context/#overriding-a-context-component) and overriding `buildTopologyMonitor`.
It should:

* implement the methods of `TopologyMonitor` by querying the discovery service;
* use some notification mechanism (or poll the service periodically) to detect when nodes go up or
  down, or get added or removed, and emit the corresponding `TopologyEvent` instances on the bus.
   
Read the javadocs for more details; in particular, `NodeInfo` explains how the driver uses the
information returned by the topology monitor.

#### MetadataManager

It's less likely that this will be overridden directly. But the schema querying and parsing logic is
abstracted behind two factories that handle the differences between Cassandra versions:
`SchemaQueriesFactory` and `SchemaParserFactory`. These are pluggable by [extending the
context](../common/context/#overriding-a-context-component) and overriding the corresponding
`buildXxx` methods.
