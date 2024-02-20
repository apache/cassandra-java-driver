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

## Changelog

<!-- Note: contrary to 3.x, insert new entries *first* in their section -->

### 4.18.0

- [improvement] PR 1689: Add support for publishing percentile time series for the histogram metrics (nparaddi-walmart)
- [improvement] JAVA-3104: Do not eagerly pre-allocate array when deserializing CqlVector
- [improvement] JAVA-3111: upgrade jackson-databind to 2.13.4.2 to address gradle dependency issue
- [improvement] PR 1617: Improve ByteBufPrimitiveCodec readBytes (chibenwa)
- [improvement] JAVA-3095: Fix CREATE keyword in vector search example in upgrade guide
- [improvement] JAVA-3100: Update jackson-databind to 2.13.4.1 and jackson-jaxrs-json-provider to 2.13.4 to address recent CVEs
- [improvement] JAVA-3089: Forbid wildcard imports

### 4.17.0

- [improvement] JAVA-3070: Make CqlVector and CqlDuration serializable
- [improvement] JAVA-3085: Initialize c.d.o.d.i.core.util.Dependency at Graal native image build-time
- [improvement] JAVA-3061: CqlVector API improvements, add support for accessing vectors directly as float arrays
- [improvement] JAVA-3042: Enable automated testing for Java17
- [improvement] JAVA-3050: Upgrade Netty to 4.1.94

### 4.16.0

- [improvement] JAVA-3058: Clear prepared statement cache on UDT type change event
- [improvement] JAVA-3060: Add vector type, codec + support for parsing CQL type
- [improvement] DOC-2813: Add error handling guidance linking to a helpful blog post
- [improvement] JAVA-3045: Fix GraalVM native image support for GraalVM 22.2

### 4.15.0

- [improvement] JAVA-3041: Update Guava session sample code to use ProgrammaticArguments
- [improvement] JAVA-3022: Implement AddressTranslator for AWS PrivateLink
- [bug] JAVA-3021: Update table SchemaBuilder page to replace withPrimaryKey with withPartitionKey
- [bug] JAVA-3005: Node list refresh behavior in 4.x is different from 3.x
- [bug] JAVA-3002: spring-boot app keeps connecting to IP of replaced node
- [improvement] JAVA-3023 Upgrade Netty to 4.1.77
- [improvement] JAVA-2995: CodecNotFoundException doesn't extend DriverException

### 4.14.1

- [improvement] JAVA-3013: Upgrade dependencies to address CVEs and other security issues, 4.14.1 edition
- [improvement] JAVA-2977: Update Netty to resolve higher-priority CVEs
- [improvement] JAVA-3003: Update jnr-posix to address CVE-2014-4043

### 4.14.0

- [bug] JAVA-2976: Support missing protocol v5 error codes CAS_WRITE_UNKNOWN, CDC_WRITE_FAILURE
- [bug] JAVA-2987: BasicLoadBalancingPolicy remote computation assumes local DC is up and live
- [bug] JAVA-2992: Include options into DefaultTableMetadata equals and hash methods
- [improvement] JAVA-2982: Switch Esri geometry lib to an optional dependency
- [improvement] JAVA-2959: Don't throw NoNodeAvailableException when all connections busy

### 4.13.0

- [improvement] JAVA-2940: Add GraalVM native image build configurations
- [improvement] JAVA-2953: Promote ProgrammaticPlainTextAuthProvider to the public API and add
  credentials hot-reload
- [improvement] JAVA-2951: Accept multiple node state listeners, schema change listeners and request
  trackers

Merged from 4.12.x:

- [bug] JAVA-2949: Provide mapper support for CompletionStage<Stream<T>>
- [bug] JAVA-2950: Remove reference to Reflection class from DependencyCheck

### 4.12.1

Merged from 4.11.x:

- [bug] JAVA-2949: Provide mapper support for CompletionStage<Stream<T>>
- [bug] JAVA-2950: Remove reference to Reflection class from DependencyCheck

### 4.12.0

- [improvement] JAVA-2935: Make GetEntity and SetEntity methods resilient to incomplete data
- [improvement] JAVA-2944: Upgrade MicroProfile Metrics to 3.0

Merged from 4.11.x:

- [bug] JAVA-2932: Make DefaultDriverConfigLoader.close() resilient to terminated executors
- [bug] JAVA-2945: Reinstate InternalDriverContext.getNodeFilter method
- [bug] JAVA-2947: Release buffer after decoding multi-slice frame
- [bug] JAVA-2946: Make MapperResultProducerService instances be located with user-provided class loader
- [bug] JAVA-2942: GraphStatement.setConsistencyLevel() is not effective
- [bug] JAVA-2941: Cannot add a single static column with the alter table API
- [bug] JAVA-2943: Prevent session leak with wrong keyspace name
- [bug] JAVA-2938: OverloadedException message is misleading

### 4.11.3

- [bug] JAVA-2949: Provide mapper support for CompletionStage<Stream<T>>
- [bug] JAVA-2950: Remove reference to Reflection class from DependencyCheck

### 4.11.2

- [bug] JAVA-2932: Make DefaultDriverConfigLoader.close() resilient to terminated executors
- [bug] JAVA-2945: Reinstate InternalDriverContext.getNodeFilter method
- [bug] JAVA-2947: Release buffer after decoding multi-slice frame
- [bug] JAVA-2946: Make MapperResultProducerService instances be located with user-provided class loader
- [bug] JAVA-2942: GraphStatement.setConsistencyLevel() is not effective
- [bug] JAVA-2941: Cannot add a single static column with the alter table API
- [bug] JAVA-2943: Prevent session leak with wrong keyspace name
- [bug] JAVA-2938: OverloadedException message is misleading

### 4.11.1

- [bug] JAVA-2910: Add a configuration option to support strong values for prepared statements cache
- [bug] JAVA-2936: Support Protocol V6
- [bug] JAVA-2934: Handle empty non-final pages in ReactiveResultSetSubscription

### 4.11.0

- [improvement] JAVA-2930: Allow Micrometer to record histograms for timers
- [improvement] JAVA-2914: Transform node filter into a more flexible node distance evaluator
- [improvement] JAVA-2929: Revisit node-level metric eviction
- [new feature] JAVA-2830: Add mapper support for Java streams
- [bug] JAVA-2928: Generate counter increment/decrement constructs compatible with legacy C* 
  versions
- [new feature] JAVA-2872: Ability to customize metric names and tags
- [bug] JAVA-2925: Consider protocol version unsupported when server requires USE_BETA flag for it
- [improvement] JAVA-2704: Remove protocol v5 beta status, add v6-beta
- [improvement] JAVA-2916: Annotate generated classes with `@SuppressWarnings`
- [bug] JAVA-2927: Make Dropwizard truly optional
- [improvement] JAVA-2917: Include GraalVM substitutions for request processors and geo codecs
- [bug] JAVA-2918: Exclude invalid peers from schema agreement checks

### 4.10.0

- [improvement] JAVA-2907: Switch Tinkerpop to an optional dependency
- [improvement] JAVA-2904: Upgrade Jackson to 2.12.0 and Tinkerpop to 3.4.9
- [bug] JAVA-2911: Prevent control connection from scheduling too many reconnections
- [bug] JAVA-2902: Consider computed values when validating constructors for immutable entities
- [new feature] JAVA-2899: Re-introduce cross-DC failover in driver 4
- [new feature] JAVA-2900: Re-introduce consistency downgrading retries
- [new feature] JAVA-2903: BlockHound integration
- [improvement] JAVA-2877: Allow skipping validation for individual mapped entities
- [improvement] JAVA-2871: Allow keyspace exclusions in the metadata, and exclude system keyspaces
  by default
- [improvement] JAVA-2449: Use non-cryptographic random number generation in Uuids.random()
- [improvement] JAVA-2893: Allow duplicate keys in DefaultProgrammaticDriverConfigLoaderBuilder
- [documentation] JAVA-2894: Clarify usage of Statement.setQueryTimestamp
- [bug] JAVA-2889: Remove TypeSafe imports from DriverConfigLoader
- [bug] JAVA-2883: Use root locale explicitly when changing string case
- [bug] JAVA-2890: Fix off-by-one error in UdtCodec
- [improvement] JAVA-2905: Prevent new connections from using a protocol version higher than the negotiated one
- [bug] JAVA-2647: Handle token types in QueryBuilder.literal()
- [bug] JAVA-2887: Handle composite profiles with more than one key and/or backed by only one profile

### 4.9.0

- [documentation] JAVA-2823: Make Astra more visible in the docs
- [documentation] JAVA-2869: Advise against using 4.5.x-4.6.0 in the upgrade guide
- [documentation] JAVA-2868: Cover reconnect-on-init in the manual
- [improvement] JAVA-2827: Exclude unused Tinkerpop transitive dependencies
- [improvement] JAVA-2827: Remove dependency to Tinkerpop gremlin-driver
- [task] JAVA-2859: Upgrade Tinkerpop to 3.4.8
- [bug] JAVA-2726: Fix Tinkerpop incompatibility with JPMS
- [bug] JAVA-2842: Remove security vulnerabilities introduced by Tinkerpop
- [bug] JAVA-2867: Revisit compressor substitutions
- [improvement] JAVA-2870: Optimize memory usage of token map
- [improvement] JAVA-2855: Allow selection of the metrics framework via the config
- [improvement] JAVA-2864: Revisit mapper processor's messaging
- [new feature] JAVA-2816: Support immutability and fluent accessors in the mapper
- [new feature] JAVA-2721: Add counter support in the mapper
- [bug] JAVA-2863: Reintroduce mapper processor dependency to SLF4J

### 4.8.0

- [improvement] JAVA-2811: Add aliases for driver 3 method names
- [new feature] JAVA-2808: Provide metrics bindings for Micrometer and MicroProfile
- [new feature] JAVA-2773: Support new protocol v5 message format
- [improvement] JAVA-2841: Raise timeouts during connection initialization
- [bug] JAVA-2331: Unregister old metrics when a node gets removed or changes RPC address
- [improvement] JAVA-2850: Ignore credentials in secure connect bundle [DataStax Astra]
- [improvement] JAVA-2813: Don't fail when secure bundle is specified together with other options
- [bug] JAVA-2800: Exclude SLF4J from mapper-processor dependencies
- [new feature] JAVA-2819: Add DriverConfigLoader.fromString
- [improvement] JAVA-2431: Set all occurrences when bound variables are used multiple times
- [improvement] JAVA-2829: Log protocol negotiation messages at DEBUG level
- [bug] JAVA-2846: Give system properties the highest precedence in DefaultDriverConfigLoader
- [new feature] JAVA-2691: Provide driver 4 support for extra codecs
- [improvement] Allow injection of CodecRegistry on session builder
- [improvement] JAVA-2828: Add safe paging state wrapper
- [bug] JAVA-2835: Correctly handle unresolved addresses in DefaultEndPoint.equals
- [bug] JAVA-2838: Avoid ConcurrentModificationException when closing connection
- [bug] JAVA-2837: make StringCodec strict about unicode in ascii

### 4.7.2

- [bug] JAVA-2821: Can't connect to DataStax Astra using driver 4.7.x

### 4.7.1

- [bug] JAVA-2818: Remove root path only after merging non-programmatic configs

### 4.7.0

- [improvement] JAVA-2301: Introduce OSGi tests for the mapper
- [improvement] JAVA-2658: Refactor OSGi tests
- [bug] JAVA-2657: Add ability to specify the class loader to use for application-specific classpath resources
- [improvement] JAVA-2803: Add Graal substitutions for protocol compression
- [documentation] JAVA-2666: Document BOM and driver modules
- [documentation] JAVA-2613: Improve connection pooling documentation
- [new feature] JAVA-2793: Add composite config loader
- [new feature] JAVA-2792: Allow custom results in the mapper
- [improvement] JAVA-2663: Add Graal substitutions for native functions
- [improvement] JAVA-2747: Revisit semantics of Statement.setExecutionProfile/Name

### 4.6.1

- [bug] JAVA-2676: Don't reschedule write coalescer after empty runs

### 4.6.0

- [improvement] JAVA-2741: Make keyspace/table metadata impls serializable
- [bug] JAVA-2740: Extend peer validity check to include datacenter, rack and tokens
- [bug] JAVA-2744: Recompute token map when node is added
- [new feature] JAVA-2614: Provide a utility to emulate offset paging on the client side
- [new feature] JAVA-2718: Warn when the number of sessions exceeds a configurable threshold
- [improvement] JAVA-2664: Add a callback to inject the session in listeners
- [bug] JAVA-2698: TupleCodec and UdtCodec give wrong error message when parsing fails
- [improvement] JAVA-2435: Add automatic-module-names to the manifests
- [new feature] JAVA-2054: Add now_in_seconds to protocol v5 query messages
- [bug] JAVA-2711: Fix handling of UDT keys in the mapper
- [improvement] JAVA-2631: Add getIndex() shortcuts to TableMetadata
- [improvement] JAVA-2679: Add port information to QueryTrace and TraceEvent
- [improvement] JAVA-2184: Refactor DescribeIT to improve maintainability
- [new feature] JAVA-2600: Add map-backed config loader
- [new feature] JAVA-2105: Add support for transient replication
- [new feature] JAVA-2670: Provide base class for mapped custom codecs
- [new feature] JAVA-2633: Add execution profile argument to DAO mapper factory methods
- [improvement] JAVA-2667: Add ability to fail the build when integration tests fail
- [bug] JAVA-1861: Add Metadata.getClusterName()

### 4.5.1

- [bug] JAVA-2673: Fix mapper generated code for UPDATE with TTL and IF condition

### 4.5.0

- [bug] JAVA-2654: Make AdminRequestHandler handle integer serialization
- [improvement] JAVA-2618: Improve error handling in request handlers
- [new feature] JAVA-2064: Add support for DSE 6.8 graph options in schema builder
- [documentation] JAVA-2559: Fix GraphNode javadocs
- [improvement] JAVA-2281: Extend GraphBinaryDataTypesTest to other graph protocols
- [new feature] JAVA-2498: Add support for reactive graph queries
- [bug] JAVA-2572: Prevent race conditions when cancelling a continuous paging query
- [improvement] JAVA-2566: Introduce specific metrics for Graph queries
- [improvement] JAVA-2556: Make ExecutionInfo compatible with any Request type
- [improvement] JAVA-2571: Revisit usages of DseGraph.g
- [improvement] JAVA-2558: Revisit GraphRequestHandler
- [bug] JAVA-2508: Preserve backward compatibility in schema metadata types
- [bug] JAVA-2465: Avoid requesting 0 page when executing continuous paging queries
- [improvement] JAVA-2472: Enable speculative executions for paged graph queries
- [improvement] JAVA-1579: Change default result format to latest GraphSON format
- [improvement] JAVA-2496: Revisit timeouts for paged graph queries
- [bug] JAVA-2510: Fix GraphBinaryDataTypesTest Codec registry initialization
- [bug] JAVA-2492: Parse edge metadata using internal identifiers
- [improvement] JAVA-2282: Remove GraphSON3 support
- [new feature] JAVA-2098: Add filter predicates for collections
- [improvement] JAVA-2245: Rename graph engine Legacy to Classic and Modern to Core
- [new feature] JAVA-2099: Enable Paging Through DSE Driver for Gremlin Traversals (2.x)
- [new feature] JAVA-1898: Expose new table-level graph metadata
- [bug] JAVA-2642: Fix default value of max-orphan-requests
- [bug] JAVA-2644: Revisit channel selection when pool size > 1
- [bug] JAVA-2630: Correctly handle custom classes in IndexMetadata.describe
- [improvement] JAVA-1556: Publish Maven Bill Of Materials POM
- [improvement] JAVA-2637: Bump Netty to 4.1.45
- [bug] JAVA-2617: Reinstate generation of deps.txt for Insights
- [new feature] JAVA-2625: Provide user-friendly programmatic configuration for kerberos
- [improvement] JAVA-2624: Expose a config option for the connect timeout
- [improvement] JAVA-2592: Make reload support parameterizable for DefaultDriverConfigLoader
- [new feature] JAVA-2263: Add optional schema validation to the mapper

### 4.4.0

This version brings in all functionality that was formerly only in the DataStax Enterprise driver, 
such as the built-in support for reactive programming. Going forward, all new features will be 
implemented in this single driver (for past DataStax Enterprise driver versions before the merge,
refer to the [DSE driver
changelog](https://docs.datastax.com/en/developer/java-driver-dse/latest/changelog/)).

- [documentation] JAVA-2607: Improve visibility of driver dependencies section
- [documentation] JAVA-1975: Document the importance of using specific TinkerPop version
- [improvement] JAVA-2529: Standardize optional/excludable dependency checks
- [bug] JAVA-2598: Do not use context class loader when attempting to load classes
- [improvement] JAVA-2582: Don't propagate a future into SchemaQueriesFactory
- [documentation] JAVA-2542: Improve the javadocs of methods in CqlSession
- [documentation] JAVA-2609: Add docs for proxy authentication to unified driver
- [improvement] JAVA-2554: Improve efficiency of InsightsClient by improving supportsInsights check
- [improvement] JAVA-2601: Inject Google Tag Manager scripts in generated API documentation
- [improvement] JAVA-2551: Improve support for DETERMINISTIC and MONOTONIC functions
- [documentation] JAVA-2446: Revisit continuous paging javadocs
- [improvement] JAVA-2550: Remove warnings in ContinuousCqlRequestHandler when coordinator is not replica
- [improvement] JAVA-2569: Make driver compatible with Netty < 4.1.34 again
- [improvement] JAVA-2541: Improve error messages during connection initialization
- [improvement] JAVA-2530: Expose shortcuts for name-based UUIDs
- [improvement] JAVA-2547: Add method DriverConfigLoader.fromPath
- [improvement] JAVA-2528: Store suppressed exceptions in AllNodesFailedException
- [new feature] JAVA-2581: Add query builder support for indexed list assignments
- [improvement] JAVA-2596: Consider collection removals as idempotent in query builder
- [bug] JAVA-2555: Generate append/prepend constructs compatible with legacy C* versions
- [bug] JAVA-2584: Ensure codec registry is able to create codecs for collections of UDTs and tuples
- [bug] JAVA-2583: IS NOT NULL clause should be idempotent
- [improvement] JAVA-2442: Don't check for schema agreement twice when completing a DDL query
- [improvement] JAVA-2473: Don't reconnect control connection if protocol is downgraded
- [bug] JAVA-2556: Make ExecutionInfo compatible with any Request type
- [new feature] JAVA-2532: Add BoundStatement ReturnType for insert, update, and delete DAO methods
- [improvement] JAVA-2107: Add XML formatting plugin
- [bug] JAVA-2527: Allow AllNodesFailedException to accept more than one error per node
- [improvement] JAVA-2546: Abort schema refresh if a query fails

### 4.3.1

- [bug] JAVA-2557: Accept any negative length when decoding elements of tuples and UDTs

### 4.3.0

- [improvement] JAVA-2497: Ensure nodes and exceptions are serializable
- [bug] JAVA-2464: Fix initial schema refresh when reconnect-on-init is enabled
- [improvement] JAVA-2516: Enable hostname validation with Cloud
- [documentation]: JAVA-2460: Document how to determine the local DC
- [improvement] JAVA-2476: Improve error message when codec registry inspects a collection with a
  null element
- [documentation] JAVA-2509: Mention file-based approach for Cloud configuration in the manual
- [improvement] JAVA-2447: Mention programmatic local DC method in Default LBP error message
- [improvement] JAVA-2459: Improve extensibility of existing load balancing policies
- [documentation] JAVA-2428: Add developer docs
- [documentation] JAVA-2503: Migrate Cloud "getting started" page to driver manual
- [improvement] JAVA-2484: Add errors for cloud misconfiguration
- [improvement] JAVA-2490: Allow to read the secure bundle from an InputStream
- [new feature] JAVA-2478: Allow to provide the secure bundle via URL
- [new feature] JAVA-2356: Support for DataStax Cloud API
- [improvement] JAVA-2407: Improve handling of logback configuration files in IDEs
- [improvement] JAVA-2434: Add support for custom cipher suites and host name validation to ProgrammaticSslEngineFactory
- [improvement] JAVA-2480: Upgrade Jackson to 2.10.0
- [documentation] JAVA-2505: Annotate Node.getHostId() as nullable
- [improvement] JAVA-1708: Support DSE "everywhere" replication strategy
- [improvement] JAVA-2471: Consider DSE version when parsing the schema
- [improvement] JAVA-2444: Add method setRoutingKey(ByteBuffer...) to StatementBuilder
- [improvement] JAVA-2398: Improve support for optional dependencies in OSGi
- [improvement] JAVA-2452: Allow "none" as a compression option
- [improvement] JAVA-2419: Allow registration of user codecs at runtime
- [documentation] JAVA-2384: Add quick overview section to each manual page
- [documentation] JAVA-2412: Cover DDL query debouncing in FAQ and upgrade guide
- [documentation] JAVA-2416: Update paging section in the manual
- [improvement] JAVA-2402: Add setTracing(boolean) to StatementBuilder
- [bug] JAVA-2466: Set idempotence to null in BatchStatement.newInstance

### 4.2.2

- [bug] JAVA-2475: Fix message size when query string contains Unicode surrogates
- [bug] JAVA-2470: Fix Session.OSS_DRIVER_COORDINATES for shaded JAR

### 4.2.1

- [bug] JAVA-2454: Handle "empty" CQL type while parsing schema
- [improvement] JAVA-2455: Improve logging of schema refresh errors
- [documentation] JAVA-2429: Document expected types on DefaultDriverOption
- [documentation] JAVA-2426: Fix month pattern in CqlDuration documentation
- [bug] JAVA-2451: Make zero a valid estimated size for PagingIterableSpliterator
- [bug] JAVA-2443: Compute prepared statement PK indices for protocol v3
- [bug] JAVA-2430: Use variable metadata to infer the routing keyspace on bound statements

### 4.2.0

- [improvement] JAVA-2390: Add methods to set the SSL engine factory programmatically
- [improvement] JAVA-2379: Fail fast if prepared id doesn't match when repreparing on the fly
- [bug] JAVA-2375: Use per-request keyspace when repreparing on the fly
- [improvement] JAVA-2370: Remove auto-service plugin from mapper processor
- [improvement] JAVA-2377: Add a config option to make driver threads daemon
- [improvement] JAVA-2371: Handle null elements in collections on the decode path
- [improvement] JAVA-2351: Add a driver example for the object mapper
- [bug] JAVA-2323: Handle restart of a node with same host_id but a different address
- [improvement] JAVA-2303: Ignore peer rows matching the control host's RPC address
- [improvement] JAVA-2236: Add methods to set the auth provider programmatically
- [improvement] JAVA-2369: Change mapper annotations retention to runtime
- [improvement] JAVA-2365: Redeclare default constants when an enum is abstracted behind an
  interface
- [improvement] JAVA-2302: Better target mapper errors and warnings for inherited methods
- [improvement] JAVA-2336: Expose byte utility methods in the public API 
- [improvement] JAVA-2338: Revisit toString() for data container types
- [bug] JAVA-2367: Fix column names in EntityHelper.updateByPrimaryKey
- [bug] JAVA-2358: Fix list of reserved CQL keywords
- [improvement] JAVA-2359: Allow default keyspace at the mapper level
- [improvement] JAVA-2306: Clear security tokens from memory immediately after use
- [improvement] JAVA-2320: Expose more attributes on mapper Select for individual query clauses
- [bug] JAVA-2332: Destroy connection pool when a node gets removed
- [bug] JAVA-2324: Add support for primitive shorts in mapper
- [bug] JAVA-2325: Allow "is" prefix for boolean getters in mapped entities
- [improvement] JAVA-2308: Add customWhereClause to `@Delete`
- [improvement] JAVA-2247: PagingIterable implementations should implement spliterator()
- [bug] JAVA-2312: Handle UDTs with names that clash with collection types
- [improvement] JAVA-2307: Improve `@Select` and `@Delete` by not requiring full primary key
- [improvement] JAVA-2315: Improve extensibility of session builder
- [bug] JAVA-2394: BaseCcmRule DseRequirement max should use DseVersion, not Cassandra version

### 4.1.0

- [documentation] JAVA-2294: Fix wrong examples in manual page on batch statements
- [bug] JAVA-2304: Avoid direct calls to ByteBuffer.array()
- [new feature] JAVA-2078: Add object mapper
- [improvement] JAVA-2297: Add a NettyOptions method to set socket options
- [bug] JAVA-2280: Ignore peer rows with missing host id or RPC address
- [bug] JAVA-2264: Adjust HashedWheelTimer tick duration from 1 to 100 ms
- [bug] JAVA-2260: Handle empty collections in PreparedStatement.bind(...)
- [improvement] JAVA-2278: Pass the request's log prefix to RequestTracker
- [bug] JAVA-2253: Don't strip trailing zeros in ByteOrderedToken
- [improvement] JAVA-2207: Add bulk value assignment to QueryBuilder Insert
- [bug] JAVA-2234: Handle terminated executor when the session is closed twice
- [documentation] JAVA-2220: Emphasize that query builder is now a separate artifact in root README
- [documentation] JAVA-2217: Cover contact points and local datacenter earlier in the manual
- [improvement] JAVA-2242: Allow skipping all integration tests with -DskipITs
- [improvement] JAVA-2241: Make DefaultDriverContext.cycleDetector protected
- [bug] JAVA-2226: Support IPv6 contact points in the configuration

### 4.0.1

- [new feature] JAVA-2201: Expose a public API for programmatic config
- [new feature] JAVA-2205: Expose public factory methods for alternative config loaders
- [bug] JAVA-2214: Fix flaky RequestLoggerIT test
- [bug] JAVA-2203: Handle unresolved addresses in DefaultEndPoint
- [bug] JAVA-2210: Add ability to set TTL for modification queries
- [improvement] JAVA-2212: Add truncate to QueryBuilder 
- [improvement] JAVA-2211: Upgrade Jersey examples to fix security issue sid-3606
- [bug] JAVA-2193: Fix flaky tests in ExecutionInfoWarningsIT
- [improvement] JAVA-2197: Skip deployment of examples and integration tests to Maven central

### 4.0.0

- [improvement] JAVA-2192: Don't return generic types with wildcards
- [improvement] JAVA-2148: Add examples
- [bug] JAVA-2189: Exclude virtual keyspaces from token map computation
- [improvement] JAVA-2183: Enable materialized views when testing against Cassandra 4
- [improvement] JAVA-2182: Add insertInto().json() variant that takes an object in QueryBuilder
- [improvement] JAVA-2161: Annotate mutating methods with `@CheckReturnValue`
- [bug] JAVA-2177: Don't exclude down nodes when initializing LBPs
- [improvement] JAVA-2143: Rename Statement.setTimestamp() to setQueryTimestamp()
- [improvement] JAVA-2165: Abstract node connection information
- [improvement] JAVA-2090: Add support for additional_write_policy and read_repair table options
- [improvement] JAVA-2164: Rename statement builder methods to setXxx
- [bug] JAVA-2178: QueryBuilder: Alias after function column is not included in a query
- [improvement] JAVA-2158: Allow BuildableQuery to build statement with values
- [improvement] JAVA-2150: Improve query builder error message on unsupported literal type
- [documentation] JAVA-2149: Improve Term javadocs in the query builder

### 4.0.0-rc1

- [improvement] JAVA-2106: Log server side warnings returned from a query
- [improvement] JAVA-2151: Drop "Dsl" suffix from query builder main classes
- [new feature] JAVA-2144: Expose internal API to hook into the session lifecycle
- [improvement] JAVA-2119: Add PagingIterable abstraction as a supertype of ResultSet
- [bug] JAVA-2063: Normalize authentication logging
- [documentation] JAVA-2034: Add performance recommendations in the manual
- [improvement] JAVA-2077: Allow reconnection policy to detect first connection attempt
- [improvement] JAVA-2067: Publish javadocs JAR for the shaded module
- [improvement] JAVA-2103: Expose partitioner name in TokenMap API
- [documentation] JAVA-2075: Document preference for LZ4 over Snappy

### 4.0.0-beta3

- [bug] JAVA-2066: Array index range error when fetching routing keys on bound statements
- [documentation] JAVA-2061: Add section to upgrade guide about updated type mappings
- [improvement] JAVA-2038: Add jitter to delays between reconnection attempts
- [improvement] JAVA-2053: Cache results of session.prepare()
- [improvement] JAVA-2058: Make programmatic config reloading part of the public API
- [improvement] JAVA-1943: Fail fast in execute() when the session is closed
- [improvement] JAVA-2056: Reduce HashedWheelTimer tick duration
- [bug] JAVA-2057: Do not create pool when SUGGEST\_UP topology event received
- [improvement] JAVA-2049: Add shorthand method to SessionBuilder to specify local DC
- [bug] JAVA-2037: Fix NPE when preparing statement with no bound variables
- [improvement] JAVA-2014: Schedule timeouts on a separate Timer
- [bug] JAVA-2029: Handle schema refresh failure after a DDL query
- [bug] JAVA-1947: Make schema parsing more lenient and allow missing system_virtual_schema
- [bug] JAVA-2028: Use CQL form when parsing UDT types in system tables
- [improvement] JAVA-1918: Document temporal types
- [improvement] JAVA-1914: Optimize use of System.nanoTime in CqlRequestHandlerBase
- [improvement] JAVA-1945: Document corner cases around UDT and tuple attachment
- [improvement] JAVA-2026: Make CqlDuration implement TemporalAmount
- [improvement] JAVA-2017: Slightly optimize conversion methods on the hot path
- [improvement] JAVA-2010: Make dependencies to annotations required again
- [improvement] JAVA-1978: Add a config option to keep contact points unresolved
- [bug] JAVA-2000: Fix ConcurrentModificationException during channel shutdown
- [improvement] JAVA-2002: Reimplement TypeCodec.accepts to improve performance
- [improvement] JAVA-2011: Re-add ResultSet.getAvailableWithoutFetching() and isFullyFetched()
- [improvement] JAVA-2007: Make driver threads extend FastThreadLocalThread
- [bug] JAVA-2001: Handle zero timeout in admin requests

### 4.0.0-beta2

- [new feature] JAVA-1919: Provide a timestamp <=> ZonedDateTime codec
- [improvement] JAVA-1989: Add BatchStatement.newInstance(BatchType, Iterable<BatchableStatement>)
- [improvement] JAVA-1988: Remove pre-fetching from ResultSet API
- [bug] JAVA-1948: Close session properly when LBP fails to initialize
- [improvement] JAVA-1949: Improve error message when contact points are wrong
- [improvement] JAVA-1956: Add statementsCount accessor to BatchStatementBuilder
- [bug] JAVA-1946: Ignore protocol version in equals comparison for UdtValue/TupleValue
- [new feature] JAVA-1932: Send Driver Name and Version in Startup message
- [new feature] JAVA-1917: Add ability to set node on statement
- [improvement] JAVA-1916: Base TimestampCodec.parse on java.util.Date.
- [improvement] JAVA-1940: Clean up test resources when CCM integration tests finish
- [bug] JAVA-1938: Make CassandraSchemaQueries classes public
- [improvement] JAVA-1925: Rename context getters
- [improvement] JAVA-1544: Check API compatibility with Revapi
- [new feature] JAVA-1900: Add support for virtual tables

### 4.0.0-beta1

- [new feature] JAVA-1869: Add DefaultDriverConfigLoaderBuilder
- [improvement] JAVA-1913: Expose additional counters on Node
- [improvement] JAVA-1880: Rename "config profile" to "execution profile"
- [improvement] JAVA-1889: Upgrade dependencies to the latest minor versions
- [improvement] JAVA-1819: Propagate more attributes to bound statements
- [improvement] JAVA-1897: Improve extensibility of schema metadata classes
- [improvement] JAVA-1437: Enable SSL hostname validation by default
- [improvement] JAVA-1879: Duplicate basic.request options as Request/Statement attributes
- [improvement] JAVA-1870: Use sensible defaults in RequestLogger if config options are missing
- [improvement] JAVA-1877: Use a separate reconnection schedule for the control connection
- [improvement] JAVA-1763: Generate a binary tarball as part of the build process
- [improvement] JAVA-1884: Add additional methods from TypeToken to GenericType
- [improvement] JAVA-1883: Use custom queue implementation for LBP's query plan
- [improvement] JAVA-1890: Add more configuration options to DefaultSslEngineFactory
- [bug] JAVA-1895: Rename PreparedStatement.getPrimaryKeyIndices to getPartitionKeyIndices
- [bug] JAVA-1891: Allow null items when setting values in bulk
- [improvement] JAVA-1767: Improve message when column not in result set
- [improvement] JAVA-1624: Expose ExecutionInfo on exceptions where applicable
- [improvement] JAVA-1766: Revisit nullability
- [new feature] JAVA-1860: Allow reconnection at startup if no contact point is available
- [improvement] JAVA-1866: Make all public policies implement AutoCloseable
- [new feature] JAVA-1762: Build alternate core artifact with Netty shaded
- [new feature] JAVA-1761: Add OSGi descriptors
- [bug] JAVA-1560: Correctly propagate policy initialization errors
- [improvement] JAVA-1865: Add RelationMetadata.getPrimaryKey()
- [improvement] JAVA-1862: Add ConsistencyLevel.isDcLocal and isSerial
- [improvement] JAVA-1858: Implement Serializable in implementations, not interfaces
- [improvement] JAVA-1830: Surface response frame size in ExecutionInfo
- [improvement] JAVA-1853: Add newValue(Object...) to TupleType and UserDefinedType
- [improvement] JAVA-1815: Reorganize configuration into basic/advanced categories
- [improvement] JAVA-1848: Add logs to DefaultRetryPolicy
- [new feature] JAVA-1832: Add Ec2MultiRegionAddressTranslator
- [improvement] JAVA-1825: Add remaining Typesafe config primitive types to DriverConfigProfile
- [new feature] JAVA-1846: Add ConstantReconnectionPolicy
- [improvement] JAVA-1824: Make policies overridable in profiles
- [bug] JAVA-1569: Allow null to be used in positional and named values in statements
- [new feature] JAVA-1592: Expose request's total Frame size through API
- [new feature] JAVA-1829: Add metrics for bytes-sent and bytes-received 
- [improvement] JAVA-1755: Normalize usage of DEBUG/TRACE log levels
- [improvement] JAVA-1803: Log driver version on first use
- [improvement] JAVA-1792: Add AuthProvider callback to handle missing challenge from server
- [improvement] JAVA-1775: Assume default packages for built-in policies
- [improvement] JAVA-1774: Standardize policy locations
- [improvement] JAVA-1798: Allow passing the default LBP filter as a session builder argument
- [new feature] JAVA-1523: Add query logger
- [improvement] JAVA-1801: Revisit NodeStateListener and SchemaChangeListener APIs
- [improvement] JAVA-1759: Revisit metrics API
- [improvement] JAVA-1776: Use concurrency annotations
- [improvement] JAVA-1799: Use CqlIdentifier for simple statement named values
- [new feature] JAVA-1515: Add query builder
- [improvement] JAVA-1773: Make DriverConfigProfile enumerable
- [improvement] JAVA-1787: Use standalone shaded Guava artifact
- [improvement] JAVA-1769: Allocate exact buffer size for outgoing requests
- [documentation] JAVA-1780: Add manual section about case sensitivity
- [new feature] JAVA-1536: Add request throttling
- [improvement] JAVA-1772: Revisit multi-response callbacks
- [new feature] JAVA-1537: Add remaining socket options
- [bug] JAVA-1756: Propagate custom payload when preparing a statement
- [improvement] JAVA-1847: Add per-node request tracking

### 4.0.0-alpha3

- [new feature] JAVA-1518: Expose metrics
- [improvement] JAVA-1739: Add host_id and schema_version to node metadata
- [improvement] JAVA-1738: Convert enums to allow extensibility
- [bug] JAVA-1727: Override DefaultUdtValue.equals
- [bug] JAVA-1729: Override DefaultTupleValue.equals
- [improvement] JAVA-1720: Merge Cluster and Session into a single interface
- [improvement] JAVA-1713: Use less nodes in DefaultLoadBalancingPolicyIT
- [improvement] JAVA-1707: Add test infrastructure for running DSE clusters with CCM
- [bug] JAVA-1715: Propagate unchecked exceptions to CompletableFuture in SyncAuthenticator methods
- [improvement] JAVA-1714: Make replication strategies pluggable
- [new feature] JAVA-1647: Handle metadata_changed flag in protocol v5
- [new feature] JAVA-1633: Handle per-request keyspace in protocol v5
- [improvement] JAVA-1678: Warn if auth is configured on the client but not the server
- [improvement] JAVA-1673: Remove schema agreement check when repreparing on up
- [new feature] JAVA-1526: Provide a single load balancing policy implementation
- [improvement] JAVA-1680: Improve error message on batch log write timeout
- [improvement] JAVA-1675: Remove dates from copyright headers
- [improvement] JAVA-1645: Don't log stack traces at WARN level
- [new feature] JAVA-1524: Add query trace API
- [improvement] JAVA-1646: Provide a more readable error when connecting to Cassandra 2.0 or lower
- [improvement] JAVA-1662: Raise default request timeout
- [improvement] JAVA-1566: Enforce API rules automatically
- [bug] JAVA-1584: Validate that no bound values are unset in protocol v3

### 4.0.0-alpha2

- [new feature] JAVA-1525: Handle token metadata
- [new feature] JAVA-1638: Check schema agreement
- [new feature] JAVA-1494: Implement Snappy and LZ4 compression
- [new feature] JAVA-1514: Port Uuids utility class
- [new feature] JAVA-1520: Add node state listeners
- [new feature] JAVA-1493: Handle schema metadata
- [improvement] JAVA-1605: Refactor request execution model
- [improvement] JAVA-1597: Fix raw usages of Statement
- [improvement] JAVA-1542: Enable JaCoCo code coverage
- [improvement] JAVA-1295: Auto-detect best protocol version in mixed cluster
- [bug] JAVA-1565: Mark node down when it loses its last connection and was already reconnecting
- [bug] JAVA-1594: Don't create pool if node comes back up but is ignored
- [bug] JAVA-1593: Reconnect control connection if current node is removed, forced down or ignored
- [bug] JAVA-1595: Don't use system.local.rpc_address when refreshing node list
- [bug] JAVA-1568: Handle Reconnection#reconnectNow/stop while the current attempt is still in 
  progress
- [improvement] JAVA-1585: Add GenericType#where
- [improvement] JAVA-1590: Properly skip deployment of integration-tests module
- [improvement] JAVA-1576: Expose AsyncResultSet's iterator through a currentPage() method
- [improvement] JAVA-1591: Add programmatic way to get driver version

### 4.0.0-alpha1

- [improvement] JAVA-1586: Throw underlying exception when codec not found in cache
- [bug] JAVA-1583: Handle write failure in ChannelHandlerRequest
- [improvement] JAVA-1541: Reorganize configuration
- [improvement] JAVA-1577: Set default consistency level to LOCAL_ONE
- [bug] JAVA-1548: Retry idempotent statements on READ_TIMEOUT and UNAVAILABLE
- [bug] JAVA-1562: Fix various issues around heart beats
- [improvement] JAVA-1546: Make all statement implementations immutable
- [bug] JAVA-1554: Include VIEW and CDC in WriteType
- [improvement] JAVA-1498: Add a cache above Typesafe config
- [bug] JAVA-1547: Abort pending requests when connection dropped
- [new feature] JAVA-1497: Port timestamp generators from 3.x
- [improvement] JAVA-1539: Configure for deployment to Maven central
- [new feature] JAVA-1519: Close channel if number of orphan stream ids exceeds a configurable 
  threshold
- [new feature] JAVA-1529: Make configuration reloadable
- [new feature] JAVA-1502: Reprepare statements on newly added/up nodes
- [new feature] JAVA-1530: Add ResultSet.wasApplied
- [improvement] JAVA-1531: Merge CqlSession and Session
- [new feature] JAVA-1513: Handle batch statements
- [improvement] JAVA-1496: Improve log messages
- [new feature] JAVA-1501: Reprepare on the fly when we get an UNPREPARED response
- [bug] JAVA-1499: Wait for load balancing policy at cluster initialization
- [new feature] JAVA-1495: Add prepared statements

## 3.11.5
- [improvement] JAVA-3114: Shade io.dropwizard.metrics:metrics-core in shaded driver
- [improvement] JAVA-3115: SchemaChangeListener#onKeyspaceChanged can fire when keyspace has not changed if using SimpleStrategy replication

## 3.11.4
- [improvement] JAVA-3079: Upgrade Netty to 4.1.94, 3.x edition
- [improvement] JAVA-3082: Fix maven build for Apple-silicon
- [improvement] PR 1671: Fix LatencyAwarePolicy scale docstring

## 3.11.3

- [improvement] JAVA-3023: Upgrade Netty to 4.1.77, 3.x edition

## 3.11.2

- [improvement] JAVA-3008: Upgrade Netty to 4.1.75, 3.x edition
- [improvement] JAVA-2984: Upgrade Jackson to resolve high-priority CVEs

## 3.11.1

- [bug] JAVA-2967: Support native transport peer information for DSE 6.8.
- [bug] JAVA-2976: Support missing protocol v5 error codes CAS_WRITE_UNKNOWN, CDC_WRITE_FAILURE.

## 3.11.0

- [improvement] JAVA-2705: Remove protocol v5 beta status, add v6-beta.
- [bug] JAVA-2923: Detect and use Guava's new HostAndPort.getHost method.
- [bug] JAVA-2922: Switch to modern framing format inside a channel handler.
- [bug] JAVA-2924: Consider protocol version unsupported when server requires USE_BETA flag for it.

### 3.10.2

- [bug] JAVA-2860: Avoid NPE if channel initialization crashes.

### 3.10.1

- [bug] JAVA-2857: Fix NPE when built statements without parameters are logged at TRACE level.
- [bug] JAVA-2843: Successfully parse DSE table schema in OSS driver.

### 3.10.0

- [improvement] JAVA-2676: Don't reschedule flusher after empty runs
- [new feature] JAVA-2772: Support new protocol v5 message format

### 3.9.0

- [bug] JAVA-2627: Avoid logging error message including stack trace in request handler.
- [new feature] JAVA-2706: Add now_in_seconds to protocol v5 query messages.
- [improvement] JAVA-2730: Add support for Cassandra® 4.0 table options
- [improvement] JAVA-2702: Transient Replication Support for Cassandra® 4.0

### 3.8.0

- [new feature] JAVA-2356: Support for DataStax Cloud API.
- [improvement] JAVA-2483: Allow to provide secure bundle via URL.
- [improvement] JAVA-2499: Allow to read the secure bundle from an InputStream.
- [improvement] JAVA-2457: Detect CaaS and change default consistency.
- [improvement] JAVA-2485: Add errors for Cloud misconfiguration.
- [documentation] JAVA-2504: Migrate Cloud "getting started" page to driver manual.
- [improvement] JAVA-2516: Enable hostname validation with Cloud
- [bug] JAVA-2515: NEW_NODE and REMOVED_NODE events should trigger ADDED and REMOVED.


### 3.7.2

- [bug] JAVA-2249: Stop stripping trailing zeros in ByteOrderedTokens.
- [bug] JAVA-1492: Don't immediately reuse busy connections for another request.
- [bug] JAVA-2198: Handle UDTs with names that clash with collection types.
- [bug] JAVA-2204: Avoid memory leak when client holds onto a stale TableMetadata instance.


### 3.7.1

- [bug] JAVA-2174: Metadata.needsQuote should accept empty strings.
- [bug] JAVA-2193: Fix flaky tests in WarningsTest.


### 3.7.0

- [improvement] JAVA-2025: Include exception message in Abstract\*Codec.accepts(null).
- [improvement] JAVA-1980: Use covariant return types in RemoteEndpointAwareJdkSSLOptions.Builder methods.
- [documentation] JAVA-2062: Document frozen collection preference with Mapper.
- [bug] JAVA-2071: Fix NPE in ArrayBackedRow.toString().
- [bug] JAVA-2070: Call onRemove instead of onDown when rack and/or DC information changes for a host.
- [improvement] JAVA-1256: Log parameters of BuiltStatement in QueryLogger.
- [documentation] JAVA-2074: Document preference for LZ4 over Snappy.
- [bug] JAVA-1612: Include netty-common jar in binary tarball.
- [improvement] JAVA-2003: Simplify CBUtil internal API to improve performance.
- [improvement] JAVA-2002: Reimplement TypeCodec.accepts to improve performance.
- [documentation] JAVA-2041: Deprecate cross-DC failover in DCAwareRoundRobinPolicy.
- [documentation] JAVA-1159: Document workaround for using tuple with udt field in Mapper.
- [documentation] JAVA-1964: Complete remaining "Coming Soon" sections in docs.
- [improvement] JAVA-1950: Log server side warnings returned from a query.
- [improvement] JAVA-2123: Allow to use QueryBuilder for building queries against Materialized Views.
- [bug] JAVA-2082: Avoid race condition during cluster close and schema refresh.


### 3.6.0

- [improvement] JAVA-1394: Add request-queue-depth metric.
- [improvement] JAVA-1857: Add Statement.setHost.
- [bug] JAVA-1920: Use nanosecond precision in LocalTimeCodec#format().
- [bug] JAVA-1794: Driver tries to create a connection array of size -1.
- [new feature] JAVA-1899: Support virtual tables.
- [bug] JAVA-1908: TableMetadata.asCQLQuery does not add table option 'memtable_flush_period_in_ms' in the generated query.
- [bug] JAVA-1924: StatementWrapper setters should return the wrapping statement.
- [new feature] JAVA-1532: Add Codec support for Java 8's LocalDateTime and ZoneId.
- [improvement] JAVA-1786: Use Google code formatter.
- [bug] JAVA-1871: Change LOCAL\_SERIAL.isDCLocal() to return true.
- [documentation] JAVA-1902: Clarify unavailable & request error in DefaultRetryPolicy javadoc.
- [new feature] JAVA-1903: Add WhiteListPolicy.ofHosts.
- [bug] JAVA-1928: Fix GuavaCompatibility for Guava 26.
- [bug] JAVA-1935: Add null check in QueryConsistencyException.getHost.
- [improvement] JAVA-1771: Send driver name and version in STARTUP message.
- [improvement] JAVA-1388: Add dynamic port discovery for system.peers\_v2.
- [documentation] JAVA-1810: Note which setters are not propagated to PreparedStatement.
- [bug] JAVA-1944: Surface Read and WriteFailureException to RetryPolicy.
- [bug] JAVA-1211: Fix NPE in cluster close when cluster init fails.
- [bug] JAVA-1220: Fail fast on cluster init if previous init failed.
- [bug] JAVA-1929: Preempt session execute queries if session was closed.

Merged from 3.5.x:

- [bug] JAVA-1872: Retain table's views when processing table update.


### 3.5.0

- [improvement] JAVA-1448: TokenAwarePolicy should respect child policy ordering.
- [bug] JAVA-1751: Include defaultTimestamp length in encodedSize for protocol version >= 3.
- [bug] JAVA-1770: Fix message size when using Custom Payload.
- [documentation] JAVA-1760: Add metrics documentation.
- [improvement] JAVA-1765: Update dependencies to latest patch versions.
- [improvement] JAVA-1752: Deprecate DowngradingConsistencyRetryPolicy.
- [improvement] JAVA-1735: Log driver version on first use.
- [documentation] JAVA-1380: Add FAQ entry for errors arising from incompatibilities.
- [improvement] JAVA-1748: Support IS NOT NULL and != in query builder.
- [documentation] JAVA-1740: Mention C*2.2/3.0 incompatibilities in paging state manual.
- [improvement] JAVA-1725: Add a getNodeCount method to CCMAccess for easier automation.
- [new feature] JAVA-708: Add means to measure request sizes.
- [documentation] JAVA-1788: Add example for enabling host name verification to SSL docs.
- [improvement] JAVA-1791: Revert "JAVA-1677: Warn if auth is configured on the client but not the server."
- [bug] JAVA-1789: Account for flags in Prepare encodedSize.
- [bug] JAVA-1797: Use jnr-ffi version required by jnr-posix.


### 3.4.0

- [improvement] JAVA-1671: Remove unnecessary test on prepared statement metadata.
- [bug] JAVA-1694: Upgrade to jackson-databind 2.7.9.2 to address CVE-2015-15095.
- [documentation] JAVA-1685: Clarify recommendation on preparing SELECT *.
- [improvement] JAVA-1679: Improve error message on batch log write timeout.
- [improvement] JAVA-1672: Remove schema agreement check when repreparing on up.
- [improvement] JAVA-1677: Warn if auth is configured on the client but not the server.
- [new feature] JAVA-1651: Add NO_COMPACT startup option.
- [improvement] JAVA-1683: Add metrics to track writes to nodes.
- [new feature] JAVA-1229: Allow specifying the keyspace for individual queries.
- [improvement] JAVA-1682: Provide a way to record latencies for cancelled speculative executions.
- [improvement] JAVA-1717: Add metrics to latency-aware policy.
- [improvement] JAVA-1675: Remove dates from copyright headers.

Merged from 3.3.x:

- [bug] JAVA-1555: Include VIEW and CDC in WriteType.
- [bug] JAVA-1599: exportAsString improvements (sort, format, clustering order)
- [improvement] JAVA-1587: Deterministic ordering of columns used in Mapper#saveQuery
- [improvement] JAVA-1500: Add a metric to report number of in-flight requests.
- [bug] JAVA-1438: QueryBuilder check for empty orderings.
- [improvement] JAVA-1490: Allow zero delay for speculative executions.
- [documentation] JAVA-1607: Add FAQ entry for netty-transport-native-epoll.
- [bug] JAVA-1630: Fix Metadata.addIfAbsent.
- [improvement] JAVA-1619: Update QueryBuilder methods to support Iterable input.
- [improvement] JAVA-1527: Expose host_id and schema_version on Host metadata.
- [new feature] JAVA-1377: Add support for TWCS in SchemaBuilder.
- [improvement] JAVA-1631: Publish a sources jar for driver-core-tests.
- [improvement] JAVA-1632: Add a withIpPrefix(String) method to CCMBridge.Builder.
- [bug] JAVA-1639: VersionNumber does not fullfill equals/hashcode contract.
- [bug] JAVA-1613: Fix broken shaded Netty detection in NettyUtil.
- [bug] JAVA-1666: Fix keyspace export when a UDT has case-sensitive field names.
- [improvement] JAVA-1196: Include hash of result set metadata in prepared statement id.
- [improvement] JAVA-1670: Support user-provided JMX ports for CCMBridge.
- [improvement] JAVA-1661: Avoid String.toLowerCase if possible in Metadata.
- [improvement] JAVA-1659: Expose low-level flusher tuning options.
- [improvement] JAVA-1660: Support netty-transport-native-epoll in OSGi container.


### 3.3.2

- [bug] JAVA-1666: Fix keyspace export when a UDT has case-sensitive field names.
- [improvement] JAVA-1196: Include hash of result set metadata in prepared statement id.
- [improvement] JAVA-1670: Support user-provided JMX ports for CCMBridge.
- [improvement] JAVA-1661: Avoid String.toLowerCase if possible in Metadata.
- [improvement] JAVA-1659: Expose low-level flusher tuning options.
- [improvement] JAVA-1660: Support netty-transport-native-epoll in OSGi container.


### 3.3.1

- [bug] JAVA-1555: Include VIEW and CDC in WriteType.
- [bug] JAVA-1599: exportAsString improvements (sort, format, clustering order)
- [improvement] JAVA-1587: Deterministic ordering of columns used in Mapper#saveQuery
- [improvement] JAVA-1500: Add a metric to report number of in-flight requests.
- [bug] JAVA-1438: QueryBuilder check for empty orderings.
- [improvement] JAVA-1490: Allow zero delay for speculative executions.
- [documentation] JAVA-1607: Add FAQ entry for netty-transport-native-epoll.
- [bug] JAVA-1630: Fix Metadata.addIfAbsent.
- [improvement] JAVA-1619: Update QueryBuilder methods to support Iterable input.
- [improvement] JAVA-1527: Expose host_id and schema_version on Host metadata.
- [new feature] JAVA-1377: Add support for TWCS in SchemaBuilder.
- [improvement] JAVA-1631: Publish a sources jar for driver-core-tests.
- [improvement] JAVA-1632: Add a withIpPrefix(String) method to CCMBridge.Builder.
- [bug] JAVA-1639: VersionNumber does not fullfill equals/hashcode contract.
- [bug] JAVA-1613: Fix broken shaded Netty detection in NettyUtil.


### 3.3.0

- [bug] JAVA-1469: Update LoggingRetryPolicy to deal with SLF4J-353.
- [improvement] JAVA-1203: Upgrade Metrics to allow usage in OSGi.
- [bug] JAVA-1407: KeyspaceMetadata exportAsString should export user types in topological sort order.
- [bug] JAVA-1455: Mapper support using unset for null values.
- [bug] JAVA-1464: Allow custom codecs with non public constructors in @Param.
- [bug] JAVA-1470: Querying multiple pages overrides WrappedStatement.
- [improvement] JAVA-1428: Upgrade logback and jackson dependencies.
- [documentation] JAVA-1463: Revisit speculative execution docs.
- [documentation] JAVA-1466: Revisit timestamp docs.
- [documentation] JAVA-1445: Clarify how nodes are penalized in LatencyAwarePolicy docs.
- [improvement] JAVA-1446: Support 'DEFAULT UNSET' in Query Builder JSON Insert.
- [improvement] JAVA-1443: Add groupBy method to Select statement.
- [improvement] JAVA-1458: Check thread in mapper sync methods.
- [improvement] JAVA-1488: Upgrade Netty to 4.0.47.Final.
- [improvement] JAVA-1460: Add speculative execution number to ExecutionInfo
- [improvement] JAVA-1431: Improve error handling during pool initialization.


### 3.2.0

- [new feature] JAVA-1347: Add support for duration type.
- [new feature] JAVA-1248: Implement "beta" flag for native protocol v5.
- [new feature] JAVA-1362: Send query options flags as [int] for Protocol V5+.
- [new feature] JAVA-1364: Enable creation of SSLHandler with remote address information.
- [improvement] JAVA-1367: Make protocol negotiation more resilient.
- [bug] JAVA-1397: Handle duration as native datatype in protocol v5+.
- [improvement] JAVA-1308: CodecRegistry performance improvements.
- [improvement] JAVA-1287: Add CDC to TableOptionsMetadata and Schema Builder.
- [improvement] JAVA-1392: Reduce lock contention in RPTokenFactory.
- [improvement] JAVA-1328: Provide compatibility with Guava 20.
- [improvement] JAVA-1247: Disable idempotence warnings.
- [improvement] JAVA-1286: Support setting and retrieving udt fields in QueryBuilder.
- [bug] JAVA-1415: Correctly report if a UDT column is frozen.
- [bug] JAVA-1418: Make Guava version detection more reliable.
- [new feature] JAVA-1174: Add ifNotExists option to mapper.
- [improvement] JAVA-1414: Optimize Metadata.escapeId and Metadata.handleId.
- [improvement] JAVA-1310: Make mapper's ignored properties configurable.
- [improvement] JAVA-1316: Add strategy for resolving properties into CQL names.
- [bug] JAVA-1424: Handle new WRITE_FAILURE and READ_FAILURE format in v5 protocol.

Merged from 3.1.x branch:

- [bug] JAVA-1371: Reintroduce connection pool timeout.
- [bug] JAVA-1313: Copy SerialConsistencyLevel to PreparedStatement.
- [documentation] JAVA-1334: Clarify documentation of method `addContactPoints`.
- [improvement] JAVA-1357: Document that getReplicas only returns replicas of the last token in range.
- [bug] JAVA-1404: Fix min token handling in TokenRange.contains.
- [bug] JAVA-1429: Prevent heartbeats until connection is fully initialized.


### 3.1.4

Merged from 3.0.x branch:

- [bug] JAVA-1371: Reintroduce connection pool timeout.
- [bug] JAVA-1313: Copy SerialConsistencyLevel to PreparedStatement.
- [documentation] JAVA-1334: Clarify documentation of method `addContactPoints`.
- [improvement] JAVA-1357: Document that getReplicas only returns replicas of the last token in range.


### 3.1.3

Merged from 3.0.x branch:

- [bug] JAVA-1330: Add un/register for SchemaChangeListener in DelegatingCluster
- [bug] JAVA-1351: Include Custom Payload in Request.copy.
- [bug] JAVA-1346: Reset heartbeat only on client reads (not writes).
- [improvement] JAVA-866: Support tuple notation in QueryBuilder.eq/in.


### 3.1.2

- [bug] JAVA-1321: Wrong OSGi dependency version for Guava.

Merged from 3.0.x branch:

- [bug] JAVA-1312: QueryBuilder modifies selected columns when manually selected.
- [improvement] JAVA-1303: Add missing BoundStatement.setRoutingKey(ByteBuffer...)
- [improvement] JAVA-262: Make internal executors customizable


### 3.1.1

- [bug] JAVA-1284: ClockFactory should check system property before attempting to load Native class.
- [bug] JAVA-1255: Allow nested UDTs to be used in Mapper.
- [bug] JAVA-1279: Mapper should exclude Groovy's "metaClass" property when looking for mapped properties

Merged from 3.0.x branch:

- [improvement] JAVA-1246: Driver swallows the real exception in a few cases
- [improvement] JAVA-1261: Throw error when attempting to page in I/O thread.
- [bug] JAVA-1258: Regression: Mapper cannot map a materialized view after JAVA-1126.
- [bug] JAVA-1101: Batch and BatchStatement should consider inner statements to determine query idempotence
- [improvement] JAVA-1262: Use ParseUtils for quoting & unquoting.
- [improvement] JAVA-1275: Use Netty's default thread factory
- [bug] JAVA-1285: QueryBuilder routing key auto-discovery should handle case-sensitive column names.
- [bug] JAVA-1283: Don't cache failed query preparations in the mapper.
- [improvement] JAVA-1277: Expose AbstractSession.checkNotInEventLoop.
- [bug] JAVA-1272: BuiltStatement not able to print its query string if it contains mapped UDTs.
- [bug] JAVA-1292: 'Adjusted frame length' error breaks driver's ability to read data.
- [improvement] JAVA-1293: Make DecoderForStreamIdSize.MAX_FRAME_LENGTH configurable.
- [improvement] JAVA-1053: Add a metric for authentication errors
- [improvement] JAVA-1263: Eliminate unnecessary memory copies in FrameCompressor implementations.
- [improvement] JAVA-893: Make connection pool non-blocking


### 3.1.0

- [new feature] JAVA-1153: Add PER PARTITION LIMIT to Select QueryBuilder.
- [improvement] JAVA-743: Add JSON support to QueryBuilder.
- [improvement] JAVA-1233: Update HdrHistogram to 2.1.9.
- [improvement] JAVA-1233: Update Snappy to 1.1.2.6.
- [bug] JAVA-1161: Preserve full time zone info in ZonedDateTimeCodec and DateTimeCodec.
- [new feature] JAVA-1157: Allow asynchronous paging of Mapper Result.
- [improvement] JAVA-1212: Don't retry non-idempotent statements by default.
- [improvement] JAVA-1192: Make EventDebouncer settings updatable at runtime.
- [new feature] JAVA-541: Add polymorphism support to object mapper.
- [new feature] JAVA-636: Allow @Column annotations on getters/setters as well as fields.
- [new feature] JAVA-984: Allow non-void setters in object mapping.
- [new feature] JAVA-1055: Add ErrorAware load balancing policy.

Merged from 3.0.x branch:

- [bug] JAVA-1179: Request objects should be copied when executed.
- [improvement] JAVA-1182: Throw error when synchronous call made on I/O thread.
- [bug] JAVA-1184: Unwrap StatementWrappers when extracting column definitions.
- [bug] JAVA-1132: Executing bound statement with no variables results in exception with protocol v1.
- [improvement] JAVA-1040: SimpleStatement parameters support in QueryLogger.
- [improvement] JAVA-1151: Fail fast if HdrHistogram is not in the classpath.
- [improvement] JAVA-1154: Allow individual Statement to cancel the read timeout.
- [bug] JAVA-1074: Fix documentation around default timestamp generator.
- [improvement] JAVA-1109: Document SSLOptions changes in upgrade guide.
- [improvement] JAVA-1065: Add method to create token from partition key values.
- [improvement] JAVA-1136: Enable JDK signature check in module driver-extras.
- [improvement] JAVA-866: Support tuple notation in QueryBuilder.eq/in.
- [bug] JAVA-1140: Use same connection to check for schema agreement after a DDL query.
- [improvement] JAVA-1113: Support Cassandra 3.4 LIKE operator in QueryBuilder.
- [improvement] JAVA-1086: Support Cassandra 3.2 CAST function in QueryBuilder.
- [bug] JAVA-1095: Check protocol version for custom payload before sending the query.
- [improvement] JAVA-1133: Add OSGi headers to cassandra-driver-extras.
- [bug] JAVA-1137: Incorrect string returned by DataType.asFunctionParameterString() for collections and tuples.
- [bug] JAVA-1046: (Dynamic)CompositeTypes need to be parsed as string literal, not blob.
- [improvement] JAVA-1164: Clarify documentation on Host.listenAddress and broadcastAddress.
- [improvement] JAVA-1171: Add Host method to determine if DSE Graph is enabled.
- [improvement] JAVA-1069: Bootstrap driver-examples module.
- [documentation] JAVA-1150: Add example and FAQ entry about ByteBuffer/BLOB.
- [improvement] JAVA-1011: Expose PoolingOptions default values.
- [improvement] JAVA-630: Don't process DOWN events for nodes that have active connections.
- [improvement] JAVA-851: Improve UUIDs javadoc with regard to user-provided timestamps.
- [improvement] JAVA-979: Update javadoc for RegularStatement toString() and getQueryString() to indicate that consistency level and other parameters are not maintained in the query string.
- [bug] JAVA-1068: Unwrap StatementWrappers when hashing the paging state.
- [improvement] JAVA-1021: Improve error message when connect() is called with an invalid keyspace name.
- [improvement] JAVA-879: Mapper.map() accepts mapper-generated and user queries.
- [bug] JAVA-1100: Exception when connecting with shaded java driver in OSGI
- [bug] JAVA-1064: getTable create statement doesn't properly handle quotes in primary key.
- [bug] JAVA-1089: Set LWT made from BuiltStatements to non-idempotent.
- [improvement] JAVA-923: Position idempotent flag on object mapper queries.
- [bug] JAVA-1070: The Mapper should not prepare queries synchronously.
- [new feature] JAVA-982: Introduce new method ConsistencyLevel.isSerial().
- [bug] JAVA-764: Retry with the normal consistency level (not the serial one) when a write times out on the Paxos phase.
- [improvement] JAVA-852: Ignore peers with null entries during discovery.
- [bug] JAVA-1005: DowngradingConsistencyRetryPolicy does not work with EACH_QUORUM when 1 DC is down.
- [bug] JAVA-1002: Avoid deadlock when re-preparing a statement on other hosts.
- [bug] JAVA-1072: Ensure defunct connections are properly evicted from the pool.
- [bug] JAVA-1152: Fix NPE at ControlConnection.refreshNodeListAndTokenMap().

Merged from 2.1 branch:

- [improvement] JAVA-1038: Fetch node info by rpc_address if its broadcast_address is not in system.peers.
- [improvement] JAVA-888: Add cluster-wide percentile tracker.
- [improvement] JAVA-963: Automatically register PercentileTracker from components that use it.
- [new feature] JAVA-1019: SchemaBuilder support for CREATE/ALTER/DROP KEYSPACE.
- [bug] JAVA-727: Allow monotonic timestamp generators to drift in the future + use microsecond precision when possible.
- [improvement] JAVA-444: Add Java process information to UUIDs.makeNode() hash.


### 3.0.7

- [bug] JAVA-1371: Reintroduce connection pool timeout.
- [bug] JAVA-1313: Copy SerialConsistencyLevel to PreparedStatement.
- [documentation] JAVA-1334: Clarify documentation of method `addContactPoints`.
- [improvement] JAVA-1357: Document that getReplicas only returns replicas of the last token in range.


### 3.0.6

- [bug] JAVA-1330: Add un/register for SchemaChangeListener in DelegatingCluster
- [bug] JAVA-1351: Include Custom Payload in Request.copy.
- [bug] JAVA-1346: Reset heartbeat only on client reads (not writes).
- [improvement] JAVA-866: Support tuple notation in QueryBuilder.eq/in.


### 3.0.5

- [bug] JAVA-1312: QueryBuilder modifies selected columns when manually selected.
- [improvement] JAVA-1303: Add missing BoundStatement.setRoutingKey(ByteBuffer...)
- [improvement] JAVA-262: Make internal executors customizable
- [bug] JAVA-1320: prevent unnecessary task creation on empty pool


### 3.0.4

- [improvement] JAVA-1246: Driver swallows the real exception in a few cases
- [improvement] JAVA-1261: Throw error when attempting to page in I/O thread.
- [bug] JAVA-1258: Regression: Mapper cannot map a materialized view after JAVA-1126.
- [bug] JAVA-1101: Batch and BatchStatement should consider inner statements to determine query idempotence
- [improvement] JAVA-1262: Use ParseUtils for quoting & unquoting.
- [improvement] JAVA-1275: Use Netty's default thread factory
- [bug] JAVA-1285: QueryBuilder routing key auto-discovery should handle case-sensitive column names.
- [bug] JAVA-1283: Don't cache failed query preparations in the mapper.
- [improvement] JAVA-1277: Expose AbstractSession.checkNotInEventLoop.
- [bug] JAVA-1272: BuiltStatement not able to print its query string if it contains mapped UDTs.
- [bug] JAVA-1292: 'Adjusted frame length' error breaks driver's ability to read data.
- [improvement] JAVA-1293: Make DecoderForStreamIdSize.MAX_FRAME_LENGTH configurable.
- [improvement] JAVA-1053: Add a metric for authentication errors
- [improvement] JAVA-1263: Eliminate unnecessary memory copies in FrameCompressor implementations.
- [improvement] JAVA-893: Make connection pool non-blocking


### 3.0.3

- [improvement] JAVA-1147: Upgrade Netty to 4.0.37.
- [bug] JAVA-1213: Allow updates and inserts to BLOB column using read-only ByteBuffer.
- [bug] JAVA-1209: ProtocolOptions.getProtocolVersion() should return null instead of throwing NPE if Cluster has not
        been init'd.
- [improvement] JAVA-1204: Update documentation to indicate tcnative version requirement.
- [bug] JAVA-1186: Fix duplicated hosts in DCAwarePolicy warn message.
- [bug] JAVA-1187: Fix warning message when local CL used with RoundRobinPolicy.
- [improvement] JAVA-1175: Warn if DCAwarePolicy configuration is inconsistent.
- [bug] JAVA-1139: ConnectionException.getMessage() throws NPE if address is null.
- [bug] JAVA-1202: Handle null rpc_address when checking schema agreement.
- [improvement] JAVA-1198: Document that BoundStatement is not thread-safe.
- [improvement] JAVA-1200: Upgrade LZ4 to 1.3.0.
- [bug] JAVA-1232: Fix NPE in IdempotenceAwareRetryPolicy.isIdempotent.
- [improvement] JAVA-1227: Document "SELECT *" issue with prepared statement.
- [bug] JAVA-1160: Fix NPE in VersionNumber.getPreReleaseLabels().
- [improvement] JAVA-1126: Handle schema changes in Mapper.
- [bug] JAVA-1193: Refresh token and replica metadata synchronously when schema is altered.
- [bug] JAVA-1120: Skip schema refresh debouncer when checking for agreement as a result of schema change made by client.
- [improvement] JAVA-1242: Fix driver-core dependency in driver-stress
- [improvement] JAVA-1235: Move the query to the end of "re-preparing .." log message as a key value.


### 3.0.2

Merged from 2.1 branch:

- [bug] JAVA-1179: Request objects should be copied when executed.
- [improvement] JAVA-1182: Throw error when synchronous call made on I/O thread.
- [bug] JAVA-1184: Unwrap StatementWrappers when extracting column definitions.


### 3.0.1

- [bug] JAVA-1132: Executing bound statement with no variables results in exception with protocol v1.
- [improvement] JAVA-1040: SimpleStatement parameters support in QueryLogger.
- [improvement] JAVA-1151: Fail fast if HdrHistogram is not in the classpath.
- [improvement] JAVA-1154: Allow individual Statement to cancel the read timeout.
- [bug] JAVA-1074: Fix documentation around default timestamp generator.
- [improvement] JAVA-1109: Document SSLOptions changes in upgrade guide.
- [improvement] JAVA-1065: Add method to create token from partition key values.
- [improvement] JAVA-1136: Enable JDK signature check in module driver-extras.
- [improvement] JAVA-866: Support tuple notation in QueryBuilder.eq/in.
- [bug] JAVA-1140: Use same connection to check for schema agreement after a DDL query.
- [improvement] JAVA-1113: Support Cassandra 3.4 LIKE operator in QueryBuilder.
- [improvement] JAVA-1086: Support Cassandra 3.2 CAST function in QueryBuilder.
- [bug] JAVA-1095: Check protocol version for custom payload before sending the query.
- [improvement] JAVA-1133: Add OSGi headers to cassandra-driver-extras.
- [bug] JAVA-1137: Incorrect string returned by DataType.asFunctionParameterString() for collections and tuples.
- [bug] JAVA-1046: (Dynamic)CompositeTypes need to be parsed as string literal, not blob.
- [improvement] JAVA-1164: Clarify documentation on Host.listenAddress and broadcastAddress.
- [improvement] JAVA-1171: Add Host method to determine if DSE Graph is enabled.
- [improvement] JAVA-1069: Bootstrap driver-examples module.
- [documentation] JAVA-1150: Add example and FAQ entry about ByteBuffer/BLOB.

Merged from 2.1 branch:

- [improvement] JAVA-1011: Expose PoolingOptions default values.
- [improvement] JAVA-630: Don't process DOWN events for nodes that have active connections.
- [improvement] JAVA-851: Improve UUIDs javadoc with regard to user-provided timestamps.
- [improvement] JAVA-979: Update javadoc for RegularStatement toString() and getQueryString() to indicate that consistency level and other parameters are not maintained in the query string.
- [bug] JAVA-1068: Unwrap StatementWrappers when hashing the paging state.
- [improvement] JAVA-1021: Improve error message when connect() is called with an invalid keyspace name.
- [improvement] JAVA-879: Mapper.map() accepts mapper-generated and user queries.
- [bug] JAVA-1100: Exception when connecting with shaded java driver in OSGI
- [bug] JAVA-1064: getTable create statement doesn't properly handle quotes in primary key.
- [bug] JAVA-1089: Set LWT made from BuiltStatements to non-idempotent.
- [improvement] JAVA-923: Position idempotent flag on object mapper queries.
- [bug] JAVA-1070: The Mapper should not prepare queries synchronously.
- [new feature] JAVA-982: Introduce new method ConsistencyLevel.isSerial().
- [bug] JAVA-764: Retry with the normal consistency level (not the serial one) when a write times out on the Paxos phase.
- [improvement] JAVA-852: Ignore peers with null entries during discovery.
- [bug] JAVA-1005: DowngradingConsistencyRetryPolicy does not work with EACH_QUORUM when 1 DC is down.
- [bug] JAVA-1002: Avoid deadlock when re-preparing a statement on other hosts.
- [bug] JAVA-1072: Ensure defunct connections are properly evicted from the pool.
- [bug] JAVA-1152: Fix NPE at ControlConnection.refreshNodeListAndTokenMap().


### 3.0.0

- [bug] JAVA-1034: fix metadata parser for collections of custom types.
- [improvement] JAVA-1035: Expose host broadcast_address and listen_address if available.
- [new feature] JAVA-1037: Allow named parameters in simple statements.
- [improvement] JAVA-1033: Allow per-statement read timeout.
- [improvement] JAVA-1042: Include DSE version and workload in Host data.

Merged from 2.1 branch:

- [improvement] JAVA-1030: Log token to replica map computation times.
- [bug] JAVA-1039: Minor bugs in Event Debouncer.


### 3.0.0-rc1

- [bug] JAVA-890: fix mapper for case-sensitive UDT.


### 3.0.0-beta1

- [bug] JAVA-993: Support for "custom" types after CASSANDRA-10365.
- [bug] JAVA-999: Handle unset parameters in QueryLogger.
- [bug] JAVA-998: SchemaChangeListener not invoked for Functions or Aggregates having UDT arguments.
- [bug] JAVA-1009: use CL ONE to compute query plan when reconnecting
  control connection.
- [improvement] JAVA-1003: Change default consistency level to LOCAL_ONE (amends JAVA-926).
- [improvement] JAVA-863: Idempotence propagation in prepared statements.
- [improvement] JAVA-996: Make CodecRegistry available to ProtocolDecoder.
- [bug] JAVA-819: Driver shouldn't retry on client timeout if statement is not idempotent.
- [improvement] JAVA-1007: Make SimpleStatement and QueryBuilder "detached" again.

Merged from 2.1 branch:

- [improvement] JAVA-989: Include keyspace name when invalid replication found when generating token map.
- [improvement] JAVA-664: Reduce heap consumption for TokenMap.
- [bug] JAVA-994: Don't call on(Up|Down|Add|Remove) methods if Cluster is closed/closing.


### 3.0.0-alpha5

- [improvement] JAVA-958: Make TableOrView.Order visible.
- [improvement] JAVA-968: Update metrics to the latest version.
- [improvement] JAVA-965: Improve error handling for when a non-type 1 UUID is given to bind() on a timeuuid column.
- [improvement] JAVA-885: Pass the authenticator name from the server to the auth provider.
- [improvement] JAVA-961: Raise an exception when an older version of guava (<16.01) is found.
- [bug] JAVA-972: TypeCodec.parse() implementations should be case insensitive when checking for keyword NULL.
- [bug] JAVA-971: Make type codecs invariant.
- [bug] JAVA-986: Update documentation links to reference 3.0.
- [improvement] JAVA-841: Refactor SSLOptions API.
- [improvement] JAVA-948: Don't limit cipher suites by default.
- [improvement] JAVA-917: Document SSL configuration.
- [improvement] JAVA-936: Adapt schema metadata parsing logic to new storage format of CQL types in C* 3.0.
- [new feature] JAVA-846: Provide custom codecs library as an extra module.
- [new feature] JAVA-742: Codec Support for JSON.
- [new feature] JAVA-606: Codec support for Java 8.
- [new feature] JAVA-565: Codec support for Java arrays.
- [new feature] JAVA-605: Codec support for Java enums.
- [bug] JAVA-884: Fix UDT mapper to process fields in the correct order.

Merged from 2.1 branch:

- [bug] JAVA-854: avoid early return in Cluster.init when a node doesn't support the protocol version.
- [bug] JAVA-978: Fix quoting issue that caused Mapper.getTableMetadata() to return null.
- [improvement] JAVA-920: Downgrade "error creating pool" message to WARN.
- [bug] JAVA-954: Don't trigger reconnection before initialization complete.
- [improvement] JAVA-914: Avoid rejected tasks at shutdown.
- [improvement] JAVA-921: Add SimpleStatement.getValuesCount().
- [bug] JAVA-901: Move call to connection.release() out of cancelHandler.
- [bug] JAVA-960: Avoid race in control connection shutdown.
- [bug] JAVA-656: Fix NPE in ControlConnection.updateLocationInfo.
- [bug] JAVA-966: Count uninitialized connections in conviction policy.
- [improvement] JAVA-917: Document SSL configuration.
- [improvement] JAVA-652: Add DCAwareRoundRobinPolicy builder.
- [improvement] JAVA-808: Add generic filtering policy that can be used to exclude specific DCs.
- [bug] JAVA-988: Metadata.handleId should handle escaped double quotes.
- [bug] JAVA-983: QueryBuilder cannot handle collections containing function calls.


### 3.0.0-alpha4

- [improvement] JAVA-926: Change default consistency level to LOCAL_QUORUM.
- [bug] JAVA-942: Fix implementation of UserType.hashCode().
- [improvement] JAVA-877: Don't delay UP/ADDED notifications if protocol version = V4.
- [improvement] JAVA-938: Parse 'extensions' column in table metadata.
- [bug] JAVA-900: Fix Configuration builder to allow disabled metrics.
- [new feature] JAVA-902: Prepare API for async query trace.
- [new feature] JAVA-930: Add BoundStatement#unset.
- [bug] JAVA-946: Make table metadata options class visible.
- [bug] JAVA-939: Add crcCheckChance to TableOptionsMetadata#equals/hashCode.
- [bug] JAVA-922: Make TypeCodec return mutable collections.
- [improvement] JAVA-932: Limit visibility of codec internals.
- [improvement] JAVA-934: Warn if a custom codec collides with an existing one.
- [improvement] JAVA-940: Allow typed getters/setters to target any CQL type.
- [bug] JAVA-950: Fix Cluster.connect with a case-sensitive keyspace.
- [bug] JAVA-953: Fix MaterializedViewMetadata when base table name is case sensitive.


### 3.0.0-alpha3

- [new feature] JAVA-571: Support new system tables in C* 3.0.
- [improvement] JAVA-919: Move crc_check_chance out of compressions options.

Merged from 2.0 branch:

- [improvement] JAVA-718: Log streamid at the trace level on sending request and receiving response.
- [bug] JAVA-796: Fix SpeculativeExecutionPolicy.init() and close() are never called.
- [improvement] JAVA-710: Suppress unnecessary warning at shutdown.
- [improvement] #340: Allow DNS name with multiple A-records as contact point.
- [bug] JAVA-794: Allow tracing across multiple result pages.
- [bug] JAVA-737: DowngradingConsistencyRetryPolicy ignores write timeouts.
- [bug] JAVA-736: Forbid bind marker in QueryBuilder add/append/prepend.
- [bug] JAVA-712: Prevent QueryBuilder.quote() from applying duplicate double quotes.
- [bug] JAVA-688: Prevent QueryBuilder from trying to serialize raw string.
- [bug] JAVA-679: Support bind marker in QueryBuilder DELETE's list index.
- [improvement] JAVA-475: Improve QueryBuilder API for SELECT DISTINCT.
- [improvement] JAVA-225: Create values() function for Insert builder using List.
- [improvement] JAVA-702: Warn when ReplicationStrategy encounters invalid
  replication factors.
- [improvement] JAVA-662: Add PoolingOptions method to set both core and max
  connections.
- [improvement] JAVA-766: Do not include epoll JAR in binary distribution.
- [improvement] JAVA-726: Optimize internal copies of Request objects.
- [bug] JAVA-815: Preserve tracing across retries.
- [improvement] JAVA-709: New RetryDecision.tryNextHost().
- [bug] JAVA-733: Handle function calls and raw strings as non-idempotent in QueryBuilder.
- [improvement] JAVA-765: Provide API to retrieve values of a Parameterized SimpleStatement.
- [improvement] JAVA-827: implement UPDATE .. IF EXISTS in QueryBuilder.
- [improvement] JAVA-618: Randomize contact points list to prevent hotspots.
- [improvement] JAVA-720: Surface the coordinator used on query failure.
- [bug] JAVA-792: Handle contact points removed during init.
- [improvement] JAVA-719: Allow PlainTextAuthProvider to change its credentials at runtime.
- [new feature] JAVA-151: Make it possible to register for SchemaChange Events.
- [improvement] JAVA-861: Downgrade "Asked to rebuild table" log from ERROR to INFO level.
- [improvement] JAVA-797: Provide an option to prepare statements only on one node.
- [improvement] JAVA-658: Provide an option to not re-prepare all statements in onUp.
- [improvement] JAVA-853: Customizable creation of netty timer.
- [bug] JAVA-859: Avoid quadratic ring processing with invalid replication factors.
- [improvement] JAVA-657: Debounce control connection queries.
- [bug] JAVA-784: LoadBalancingPolicy.distance() called before init().
- [new feature] JAVA-828: Make driver-side metadata optional.
- [improvement] JAVA-544: Allow hosts to remain partially up.
- [improvement] JAVA-821, JAVA-822: Remove internal blocking calls and expose async session
  creation.
- [improvement] JAVA-725: Use parallel calls when re-preparing statement on other
  hosts.
- [bug] JAVA-629: Don't use connection timeout for unrelated internal queries.
- [bug] JAVA-892: Fix NPE in speculative executions when metrics disabled.


### 3.0.0-alpha2

- [new feature] JAVA-875, JAVA-882: Move secondary index metadata out of column definitions.

Merged from 2.2 branch:

- [bug] JAVA-847: Propagate CodecRegistry to nested UDTs.
- [improvement] JAVA-848: Ability to store a default, shareable CodecRegistry
  instance.
- [bug] JAVA-880: Treat empty ByteBuffers as empty values in TupleCodec and
  UDTCodec.


### 3.0.0-alpha1

- [new feature] JAVA-876: Support new system tables in C* 3.0.0-alpha1.

Merged from 2.2 branch:

- [improvement] JAVA-810: Rename DateWithoutTime to LocalDate.
- [bug] JAVA-816: DateCodec does not format values correctly.
- [bug] JAVA-817: TimeCodec does not format values correctly.
- [bug] JAVA-818: TypeCodec.getDataTypeFor() does not handle LocalDate instances.
- [improvement] JAVA-836: Make ResultSet#fetchMoreResult return a
  ListenableFuture<ResultSet>.
- [improvement] JAVA-843: Disable frozen checks in mapper.
- [improvement] JAVA-721: Allow user to register custom type codecs.
- [improvement] JAVA-722: Support custom type codecs in mapper.


### 2.2.0-rc3

- [bug] JAVA-847: Propagate CodecRegistry to nested UDTs.
- [improvement] JAVA-848: Ability to store a default, shareable CodecRegistry
  instance.
- [bug] JAVA-880: Treat empty ByteBuffers as empty values in TupleCodec and
  UDTCodec.


### 2.2.0-rc2

- [improvement] JAVA-810: Rename DateWithoutTime to LocalDate.
- [bug] JAVA-816: DateCodec does not format values correctly.
- [bug] JAVA-817: TimeCodec does not format values correctly.
- [bug] JAVA-818: TypeCodec.getDataTypeFor() does not handle LocalDate instances.
- [improvement] JAVA-836: Make ResultSet#fetchMoreResult return a
  ListenableFuture<ResultSet>.
- [improvement] JAVA-843: Disable frozen checks in mapper.
- [improvement] JAVA-721: Allow user to register custom type codecs.
- [improvement] JAVA-722: Support custom type codecs in mapper.

Merged from 2.1 branch:

- [bug] JAVA-834: Special case check for 'null' string in index_options column.
- [improvement] JAVA-835: Allow accessor methods with less parameters in case
  named bind markers are repeated.
- [improvement] JAVA-475: Improve QueryBuilder API for SELECT DISTINCT.
- [improvement] JAVA-715: Make NativeColumnType a top-level class.
- [improvement] JAVA-700: Expose ProtocolVersion#toInt.
- [bug] JAVA-542: Handle void return types in accessors.
- [improvement] JAVA-225: Create values() function for Insert builder using List.
- [improvement] JAVA-713: HashMap throws an OOM Exception when logging level is set to TRACE.
- [bug] JAVA-679: Support bind marker in QueryBuilder DELETE's list index.
- [improvement] JAVA-732: Expose KEYS and FULL indexing options in IndexMetadata.
- [improvement] JAVA-589: Allow @Enumerated in Accessor method parameters.
- [improvement] JAVA-554: Allow access to table metadata from Mapper.
- [improvement] JAVA-661: Provide a way to map computed fields.
- [improvement] JAVA-824: Ignore missing columns in mapper.
- [bug] JAVA-724: Preserve default timestamp for retries and speculative executions.
- [improvement] JAVA-738: Use same pool implementation for protocol v2 and v3.
- [improvement] JAVA-677: Support CONTAINS / CONTAINS KEY in QueryBuilder.
- [improvement] JAVA-477/JAVA-540: Add USING options in mapper for delete and save
  operations.
- [improvement] JAVA-473: Add mapper option to configure whether to save null fields.

Merged from 2.0 branch:

- [bug] JAVA-737: DowngradingConsistencyRetryPolicy ignores write timeouts.
- [bug] JAVA-736: Forbid bind marker in QueryBuilder add/append/prepend.
- [bug] JAVA-712: Prevent QueryBuilder.quote() from applying duplicate double quotes.
- [bug] JAVA-688: Prevent QueryBuilder from trying to serialize raw string.
- [bug] JAVA-679: Support bind marker in QueryBuilder DELETE's list index.
- [improvement] JAVA-475: Improve QueryBuilder API for SELECT DISTINCT.
- [improvement] JAVA-225: Create values() function for Insert builder using List.
- [improvement] JAVA-702: Warn when ReplicationStrategy encounters invalid
  replication factors.
- [improvement] JAVA-662: Add PoolingOptions method to set both core and max
  connections.
- [improvement] JAVA-766: Do not include epoll JAR in binary distribution.
- [improvement] JAVA-726: Optimize internal copies of Request objects.
- [bug] JAVA-815: Preserve tracing across retries.
- [improvement] JAVA-709: New RetryDecision.tryNextHost().
- [bug] JAVA-733: Handle function calls and raw strings as non-idempotent in QueryBuilder.


### 2.2.0-rc1

- [new feature] JAVA-783: Protocol V4 enum support.
- [new feature] JAVA-776: Use PK columns in protocol v4 PREPARED response.
- [new feature] JAVA-777: Distinguish NULL and UNSET values.
- [new feature] JAVA-779: Add k/v payload for 3rd party usage.
- [new feature] JAVA-780: Expose server-side warnings on ExecutionInfo.
- [new feature] JAVA-749: Expose new read/write failure exceptions.
- [new feature] JAVA-747: Expose function and aggregate metadata.
- [new feature] JAVA-778: Add new client exception for CQL function failure.
- [improvement] JAVA-700: Expose ProtocolVersion#toInt.
- [new feature] JAVA-404: Support new C* 2.2 CQL date and time types.

Merged from 2.1 branch:

- [improvement] JAVA-782: Unify "Target" enum for schema elements.


### 2.1.10.2

Merged from 2.0 branch:

- [bug] JAVA-1179: Request objects should be copied when executed.
- [improvement] JAVA-1182: Throw error when synchronous call made on I/O thread.
- [bug] JAVA-1184: Unwrap StatementWrappers when extracting column definitions.


### 2.1.10.1

- [bug] JAVA-1152: Fix NPE at ControlConnection.refreshNodeListAndTokenMap().
- [bug] JAVA-1156: Fix NPE at TableMetadata.equals().


### 2.1.10

- [bug] JAVA-988: Metadata.handleId should handle escaped double quotes.
- [bug] JAVA-983: QueryBuilder cannot handle collections containing function calls.
- [improvement] JAVA-863: Idempotence propagation in PreparedStatements.
- [bug] JAVA-937: TypeCodec static initializers not always correctly executed.
- [improvement] JAVA-989: Include keyspace name when invalid replication found when generating token map.
- [improvement] JAVA-664: Reduce heap consumption for TokenMap.
- [improvement] JAVA-1030: Log token to replica map computation times.
- [bug] JAVA-1039: Minor bugs in Event Debouncer.
- [improvement] JAVA-843: Disable frozen checks in mapper.
- [improvement] JAVA-833: Improve message when a nested type can't be serialized.
- [improvement] JAVA-1011: Expose PoolingOptions default values.
- [improvement] JAVA-630: Don't process DOWN events for nodes that have active connections.
- [improvement] JAVA-851: Improve UUIDs javadoc with regard to user-provided timestamps.
- [improvement] JAVA-979: Update javadoc for RegularStatement toString() and getQueryString() to indicate that consistency level and other parameters are not maintained in the query string.
- [improvement] JAVA-1038: Fetch node info by rpc_address if its broadcast_address is not in system.peers.
- [improvement] JAVA-974: Validate accessor parameter types against bound statement.
- [bug] JAVA-1068: Unwrap StatementWrappers when hashing the paging state.
- [bug] JAVA-831: Mapper can't load an entity where the PK is a UDT.
- [improvement] JAVA-1021: Improve error message when connect() is called with an invalid keyspace name.
- [improvement] JAVA-879: Mapper.map() accepts mapper-generated and user queries.
- [bug] JAVA-1100: Exception when connecting with shaded java driver in OSGI
- [bug] JAVA-819: Expose more errors in RetryPolicy + provide idempotent-aware wrapper.
- [improvement] JAVA-1040: SimpleStatement parameters support in QueryLogger.
- [bug] JAVA-1064: getTable create statement doesn't properly handle quotes in primary key.
- [improvement] JAVA-888: Add cluster-wide percentile tracker.
- [improvement] JAVA-963: Automatically register PercentileTracker from components that use it.
- [bug] JAVA-1089: Set LWT made from BuiltStatements to non-idempotent.
- [improvement] JAVA-923: Position idempotent flag on object mapper queries.
- [new feature] JAVA-1019: SchemaBuilder support for CREATE/ALTER/DROP KEYSPACE.
- [bug] JAVA-1070: The Mapper should not prepare queries synchronously.
- [new feature] JAVA-982: Introduce new method ConsistencyLevel.isSerial().
- [bug] JAVA-764: Retry with the normal consistency level (not the serial one) when a write times out on the Paxos phase.
- [bug] JAVA-727: Allow monotonic timestamp generators to drift in the future + use microsecond precision when possible.
- [improvement] JAVA-444: Add Java process information to UUIDs.makeNode() hash.
- [improvement] JAVA-977: Preserve original cause when BuiltStatement value can't be serialized.
- [bug] JAVA-1094: Backport TypeCodec parse and format fixes from 3.0.
- [improvement] JAVA-852: Ignore peers with null entries during discovery.
- [bug] JAVA-1132: Executing bound statement with no variables results in exception with protocol v1.
- [bug] JAVA-1005: DowngradingConsistencyRetryPolicy does not work with EACH_QUORUM when 1 DC is down.
- [bug] JAVA-1002: Avoid deadlock when re-preparing a statement on other hosts.

Merged from 2.0 branch:

- [bug] JAVA-994: Don't call on(Up|Down|Add|Remove) methods if Cluster is closed/closing.
- [improvement] JAVA-805: Document that metrics are null until Cluster is initialized.
- [bug] JAVA-1072: Ensure defunct connections are properly evicted from the pool.


### 2.1.9

- [bug] JAVA-942: Fix implementation of UserType.hashCode().
- [bug] JAVA-854: avoid early return in Cluster.init when a node doesn't support the protocol version.
- [bug] JAVA-978: Fix quoting issue that caused Mapper.getTableMetadata() to return null.

Merged from 2.0 branch:

- [bug] JAVA-950: Fix Cluster.connect with a case-sensitive keyspace.
- [improvement] JAVA-920: Downgrade "error creating pool" message to WARN.
- [bug] JAVA-954: Don't trigger reconnection before initialization complete.
- [improvement] JAVA-914: Avoid rejected tasks at shutdown.
- [improvement] JAVA-921: Add SimpleStatement.getValuesCount().
- [bug] JAVA-901: Move call to connection.release() out of cancelHandler.
- [bug] JAVA-960: Avoid race in control connection shutdown.
- [bug] JAVA-656: Fix NPE in ControlConnection.updateLocationInfo.
- [bug] JAVA-966: Count uninitialized connections in conviction policy.
- [improvement] JAVA-917: Document SSL configuration.
- [improvement] JAVA-652: Add DCAwareRoundRobinPolicy builder.
- [improvement] JAVA-808: Add generic filtering policy that can be used to exclude specific DCs.


### 2.1.8

Merged from 2.0 branch:

- [improvement] JAVA-718: Log streamid at the trace level on sending request and receiving response.

- [bug] JAVA-796: Fix SpeculativeExecutionPolicy.init() and close() are never called.
- [improvement] JAVA-710: Suppress unnecessary warning at shutdown.
- [improvement] #340: Allow DNS name with multiple A-records as contact point.
- [bug] JAVA-794: Allow tracing across multiple result pages.
- [bug] JAVA-737: DowngradingConsistencyRetryPolicy ignores write timeouts.
- [bug] JAVA-736: Forbid bind marker in QueryBuilder add/append/prepend.
- [bug] JAVA-712: Prevent QueryBuilder.quote() from applying duplicate double quotes.
- [bug] JAVA-688: Prevent QueryBuilder from trying to serialize raw string.
- [bug] JAVA-679: Support bind marker in QueryBuilder DELETE's list index.
- [improvement] JAVA-475: Improve QueryBuilder API for SELECT DISTINCT.
- [improvement] JAVA-225: Create values() function for Insert builder using List.
- [improvement] JAVA-702: Warn when ReplicationStrategy encounters invalid
  replication factors.
- [improvement] JAVA-662: Add PoolingOptions method to set both core and max
  connections.
- [improvement] JAVA-766: Do not include epoll JAR in binary distribution.
- [improvement] JAVA-726: Optimize internal copies of Request objects.
- [bug] JAVA-815: Preserve tracing across retries.
- [improvement] JAVA-709: New RetryDecision.tryNextHost().
- [bug] JAVA-733: Handle function calls and raw strings as non-idempotent in QueryBuilder.
- [improvement] JAVA-765: Provide API to retrieve values of a Parameterized SimpleStatement.
- [improvement] JAVA-827: implement UPDATE .. IF EXISTS in QueryBuilder.
- [improvement] JAVA-618: Randomize contact points list to prevent hotspots.
- [improvement] JAVA-720: Surface the coordinator used on query failure.
- [bug] JAVA-792: Handle contact points removed during init.
- [improvement] JAVA-719: Allow PlainTextAuthProvider to change its credentials at runtime.
- [new feature] JAVA-151: Make it possible to register for SchemaChange Events.
- [improvement] JAVA-861: Downgrade "Asked to rebuild table" log from ERROR to INFO level.
- [improvement] JAVA-797: Provide an option to prepare statements only on one node.
- [improvement] JAVA-658: Provide an option to not re-prepare all statements in onUp.
- [improvement] JAVA-853: Customizable creation of netty timer.
- [bug] JAVA-859: Avoid quadratic ring processing with invalid replication factors.
- [improvement] JAVA-657: Debounce control connection queries.
- [bug] JAVA-784: LoadBalancingPolicy.distance() called before init().
- [new feature] JAVA-828: Make driver-side metadata optional.
- [improvement] JAVA-544: Allow hosts to remain partially up.
- [improvement] JAVA-821, JAVA-822: Remove internal blocking calls and expose async session
  creation.
- [improvement] JAVA-725: Use parallel calls when re-preparing statement on other
  hosts.
- [bug] JAVA-629: Don't use connection timeout for unrelated internal queries.
- [bug] JAVA-892: Fix NPE in speculative executions when metrics disabled.


### 2.1.7.1

- [bug] JAVA-834: Special case check for 'null' string in index_options column.
- [improvement] JAVA-835: Allow accessor methods with less parameters in case
  named bind markers are repeated.


### 2.1.7

- [improvement] JAVA-475: Improve QueryBuilder API for SELECT DISTINCT.
- [improvement] JAVA-715: Make NativeColumnType a top-level class.
- [improvement] JAVA-782: Unify "Target" enum for schema elements.
- [improvement] JAVA-700: Expose ProtocolVersion#toInt.
- [bug] JAVA-542: Handle void return types in accessors.
- [improvement] JAVA-225: Create values() function for Insert builder using List.
- [improvement] JAVA-713: HashMap throws an OOM Exception when logging level is set to TRACE.
- [bug] JAVA-679: Support bind marker in QueryBuilder DELETE's list index.
- [improvement] JAVA-732: Expose KEYS and FULL indexing options in IndexMetadata.
- [improvement] JAVA-589: Allow @Enumerated in Accessor method parameters.
- [improvement] JAVA-554: Allow access to table metadata from Mapper.
- [improvement] JAVA-661: Provide a way to map computed fields.
- [improvement] JAVA-824: Ignore missing columns in mapper.
- [bug] JAVA-724: Preserve default timestamp for retries and speculative executions.
- [improvement] JAVA-738: Use same pool implementation for protocol v2 and v3.
- [improvement] JAVA-677: Support CONTAINS / CONTAINS KEY in QueryBuilder.
- [improvement] JAVA-477/JAVA-540: Add USING options in mapper for delete and save
  operations.
- [improvement] JAVA-473: Add mapper option to configure whether to save null fields.

Merged from 2.0 branch:

- [bug] JAVA-737: DowngradingConsistencyRetryPolicy ignores write timeouts.
- [bug] JAVA-736: Forbid bind marker in QueryBuilder add/append/prepend.
- [bug] JAVA-712: Prevent QueryBuilder.quote() from applying duplicate double quotes.
- [bug] JAVA-688: Prevent QueryBuilder from trying to serialize raw string.
- [bug] JAVA-679: Support bind marker in QueryBuilder DELETE's list index.
- [improvement] JAVA-475: Improve QueryBuilder API for SELECT DISTINCT.
- [improvement] JAVA-225: Create values() function for Insert builder using List.
- [improvement] JAVA-702: Warn when ReplicationStrategy encounters invalid
  replication factors.
- [improvement] JAVA-662: Add PoolingOptions method to set both core and max
  connections.
- [improvement] JAVA-766: Do not include epoll JAR in binary distribution.
- [improvement] JAVA-726: Optimize internal copies of Request objects.
- [bug] JAVA-815: Preserve tracing across retries.
- [improvement] JAVA-709: New RetryDecision.tryNextHost().
- [bug] JAVA-733: Handle function calls and raw strings as non-idempotent in QueryBuilder.


### 2.1.6

Merged from 2.0 branch:

- [new feature] JAVA-584: Add getObject to BoundStatement and Row.
- [improvement] JAVA-419: Improve connection pool resizing algorithm.
- [bug] JAVA-599: Fix race condition between pool expansion and shutdown.
- [improvement] JAVA-622: Upgrade Netty to 4.0.27.
- [improvement] JAVA-562: Coalesce frames before flushing them to the connection.
- [improvement] JAVA-583: Rename threads to indicate that they are for the driver.
- [new feature] JAVA-550: Expose paging state.
- [new feature] JAVA-646: Slow Query Logger.
- [improvement] JAVA-698: Exclude some errors from measurements in LatencyAwarePolicy.
- [bug] JAVA-641: Fix issue when executing a PreparedStatement from another cluster.
- [improvement] JAVA-534: Log keyspace xxx does not exist at WARN level.
- [improvement] JAVA-619: Allow Cluster subclasses to delegate to another instance.
- [new feature] JAVA-669: Expose an API to check for schema agreement after a
  schema-altering statement.
- [improvement] JAVA-692: Make connection and pool creation fully async.
- [improvement] JAVA-505: Optimize connection use after reconnection.
- [improvement] JAVA-617: Remove "suspected" mechanism.
- [improvement] reverts JAVA-425: Don't mark connection defunct on client timeout.
- [new feature] JAVA-561: Speculative query executions.
- [bug] JAVA-666: Release connection before completing the ResultSetFuture.
- [new feature BETA] JAVA-723: Percentile-based variant of query logger and speculative
  executions.
- [bug] JAVA-734: Fix buffer leaks when compression is enabled.
- [improvement] JAVA-756: Use Netty's pooled ByteBufAllocator by default.
- [improvement] JAVA-759: Expose "unsafe" paging state API.
- [bug] JAVA-768: Prevent race during pool initialization.


### 2.1.5

- [bug] JAVA-575: Authorize Null parameter in Accessor method.
- [improvement] JAVA-570: Support C* 2.1.3's nested collections.
- [bug] JAVA-612: Fix checks on mapped collection types.
- [bug] JAVA-672: Fix QueryBuilder.putAll() when the collection contains UDTs.

Merged from 2.0 branch:

- [new feature] JAVA-518: Add AddressTranslater for EC2 multi-region deployment.
- [improvement] JAVA-533: Add connection heartbeat.
- [improvement] JAVA-568: Reduce level of logs on missing rpc_address.
- [improvement] JAVA-312, JAVA-681: Expose node token and range information.
- [bug] JAVA-595: Fix cluster name mismatch check at startup.
- [bug] JAVA-620: Fix guava dependency when using OSGI.
- [bug] JAVA-678: Fix handling of DROP events when ks name is case-sensitive.
- [improvement] JAVA-631: Use List<?> instead of List<Object> in QueryBuilder API.
- [improvement] JAVA-654: Exclude Netty POM from META-INF in shaded JAR.
- [bug] JAVA-655: Quote single quotes contained in table comments in asCQLQuery method.
- [bug] JAVA-684: Empty TokenRange returned in a one token cluster.
- [improvement] JAVA-687: Expose TokenRange#contains.
- [bug] JAVA-614: Prevent race between cancellation and query completion.
- [bug] JAVA-632: Prevent cancel and timeout from cancelling unrelated ResponseHandler if
  streamId was already released and reused.
- [bug] JAVA-642: Fix issue when newly opened pool fails before we could mark the node UP.
- [bug] JAVA-613: Fix unwanted LBP notifications when a contact host is down.
- [bug] JAVA-651: Fix edge cases where a connection was released twice.
- [bug] JAVA-653: Fix edge cases in query cancellation.


### 2.1.4

Merged from 2.0 branch:

- [improvement] JAVA-538: Shade Netty dependency.
- [improvement] JAVA-543: Target schema refreshes more precisely.
- [bug] JAVA-546: Don't check rpc_address for control host.
- [improvement] JAVA-409: Improve message of NoHostAvailableException.
- [bug] JAVA-556: Rework connection reaper to avoid deadlock.
- [bug] JAVA-557: Avoid deadlock when multiple connections to the same host get write
  errors.
- [improvement] JAVA-504: Make shuffle=true the default for TokenAwarePolicy.
- [bug] JAVA-577: Fix bug when SUSPECT reconnection succeeds, but one of the pooled
  connections fails while bringing the node back up.
- [bug] JAVA-419: JAVA-587: Prevent faulty control connection from ignoring reconnecting hosts.
- temporarily revert "Add idle timeout to the connection pool".
- [bug] JAVA-593: Ensure updateCreatedPools does not add pools for suspected hosts.
- [bug] JAVA-594: Ensure state change notifications for a given host are handled serially.
- [bug] JAVA-597: Ensure control connection reconnects when control host is removed.


### 2.1.3

- [bug] JAVA-510: Ignore static fields in mapper.
- [bug] JAVA-509: Fix UDT parsing at init when using the default protocol version.
- [bug] JAVA-495: Fix toString, equals and hashCode on accessor proxies.
- [bug] JAVA-528: Allow empty name on Column and Field annotations.

Merged from 2.0 branch:

- [bug] JAVA-497: Ensure control connection does not trigger concurrent reconnects.
- [improvement] JAVA-472: Keep trying to reconnect on authentication errors.
- [improvement] JAVA-463: Expose close method on load balancing policy.
- [improvement] JAVA-459: Allow load balancing policy to trigger refresh for a single host.
- [bug] JAVA-493: Expose an API to cancel reconnection attempts.
- [bug] JAVA-503: Fix NPE when a connection fails during pool construction.
- [improvement] JAVA-423: Log datacenter name in DCAware policy's init when it is explicitly provided.
- [improvement] JAVA-504: Shuffle the replicas in TokenAwarePolicy.newQueryPlan.
- [improvement] JAVA-507: Make schema agreement wait tuneable.
- [improvement] JAVA-494: Document how to inject the driver metrics into another registry.
- [improvement] JAVA-419: Add idle timeout to the connection pool.
- [bug] JAVA-516: LatencyAwarePolicy does not shutdown executor on invocation of close.
- [improvement] JAVA-451: Throw an exception when DCAwareRoundRobinPolicy is built with
  an explicit but null or empty local datacenter.
- [bug] JAVA-511: Fix check for local contact points in DCAware policy's init.
- [improvement] JAVA-457: Make timeout on saturated pool customizable.
- [improvement] JAVA-521: Downgrade Guava to 14.0.1.
- [bug] JAVA-526: Fix token awareness for case-sensitive keyspaces and tables.
- [bug] JAVA-515: Check maximum number of values passed to SimpleStatement.
- [improvement] JAVA-532: Expose the driver version through the API.
- [improvement] JAVA-522: Optimize session initialization when some hosts are not
  responsive.


### 2.1.2

- [improvement] JAVA-361, JAVA-364, JAVA-467: Support for native protocol v3.
- [bug] JAVA-454: Fix UDT fields of type inet in QueryBuilder.
- [bug] JAVA-455: Exclude transient fields from Frozen checks.
- [bug] JAVA-453: Fix handling of null collections in mapper.
- [improvement] JAVA-452: Make implicit column names case-insensitive in mapper.
- [bug] JAVA-433: Fix named bind markers in QueryBuilder.
- [bug] JAVA-458: Fix handling of BigInteger in object mapper.
- [bug] JAVA-465: Ignore synthetic fields in mapper.
- [improvement] JAVA-451: Throw an exception when DCAwareRoundRobinPolicy is built with
  an explicit but null or empty local datacenter.
- [improvement] JAVA-469: Add backwards-compatible DataType.serialize methods.
- [bug] JAVA-487: Handle null enum fields in object mapper.
- [bug] JAVA-499: Handle null UDT fields in object mapper.

Merged from 2.0 branch:

- [bug] JAVA-449: Handle null pool in PooledConnection.release.
- [improvement] JAVA-425: Defunct connection on request timeout.
- [improvement] JAVA-426: Try next host when we get a SERVER_ERROR.
- [bug] JAVA-449, JAVA-460, JAVA-471: Handle race between query timeout and completion.
- [bug] JAVA-496: Fix DCAwareRoundRobinPolicy datacenter auto-discovery.


### 2.1.1

- [new] JAVA-441: Support for new "frozen" keyword.

Merged from 2.0 branch:

- [bug] JAVA-397: Check cluster name when connecting to a new node.
- [bug] JAVA-326: Add missing CAS delete support in QueryBuilder.
- [bug] JAVA-363: Add collection and data length checks during serialization.
- [improvement] JAVA-329: Surface number of retries in metrics.
- [bug] JAVA-428: Do not use a host when no rpc_address found for it.
- [improvement] JAVA-358: Add ResultSet.wasApplied() for conditional queries.
- [bug] JAVA-349: Fix negative HostConnectionPool open count.
- [improvement] JAVA-436: Log more connection details at trace and debug levels.
- [bug] JAVA-445: Fix cluster shutdown.


### 2.1.0

- [bug] JAVA-408: ClusteringColumn annotation not working with specified ordering.
- [improvement] JAVA-410: Fail BoundStatement if null values are not set explicitly.
- [bug] JAVA-416: Handle UDT and tuples in BuiltStatement.toString.

Merged from 2.0 branch:

- [bug] JAVA-407: Release connections on ResultSetFuture#cancel.
- [bug] JAVA-393: Fix handling of SimpleStatement with values in query builder
  batches.
- [bug] JAVA-417: Ensure pool is properly closed in onDown.
- [bug] JAVA-415: Fix tokenMap initialization at startup.
- [bug] JAVA-418: Avoid deadlock on close.


### 2.1.0-rc1

Merged from 2.0 branch:

- [bug] JAVA-394: Ensure defunct connections are completely closed.
- [bug] JAVA-342, JAVA-390: Fix memory and resource leak on closed Sessions.


### 2.1.0-beta1

- [new] Support for User Defined Types and tuples
- [new] Simple object mapper

Merged from 2.0 branch: everything up to 2.0.3 (included), and the following.

- [improvement] JAVA-204: Better handling of dead connections.
- [bug] JAVA-373: Fix potential NPE in ControlConnection.
- [bug] JAVA-291: Throws NPE when passed null for a contact point.
- [bug] JAVA-315: Avoid LoadBalancingPolicy onDown+onUp at startup.
- [bug] JAVA-343: Avoid classloader leak in Tomcat.
- [bug] JAVA-387: Avoid deadlock in onAdd/onUp.
- [bug] JAVA-377, JAVA-391: Make metadata parsing more lenient.


### 2.0.12.2

- [bug] JAVA-1179: Request objects should be copied when executed.
- [improvement] JAVA-1182: Throw error when synchronous call made on I/O thread.
- [bug] JAVA-1184: Unwrap StatementWrappers when extracting column definitions.


### 2.0.12.1

- [bug] JAVA-994: Don't call on(Up|Down|Add|Remove) methods if Cluster is closed/closing.
- [improvement] JAVA-805: Document that metrics are null until Cluster is initialized.
- [bug] JAVA-1072: Ensure defunct connections are properly evicted from the pool.


### 2.0.12

- [bug] JAVA-950: Fix Cluster.connect with a case-sensitive keyspace.
- [improvement] JAVA-920: Downgrade "error creating pool" message to WARN.
- [bug] JAVA-954: Don't trigger reconnection before initialization complete.
- [improvement] JAVA-914: Avoid rejected tasks at shutdown.
- [improvement] JAVA-921: Add SimpleStatement.getValuesCount().
- [bug] JAVA-901: Move call to connection.release() out of cancelHandler.
- [bug] JAVA-960: Avoid race in control connection shutdown.
- [bug] JAVA-656: Fix NPE in ControlConnection.updateLocationInfo.
- [bug] JAVA-966: Count uninitialized connections in conviction policy.
- [improvement] JAVA-917: Document SSL configuration.
- [improvement] JAVA-652: Add DCAwareRoundRobinPolicy builder.
- [improvement] JAVA-808: Add generic filtering policy that can be used to exclude specific DCs.


### 2.0.11

- [improvement] JAVA-718: Log streamid at the trace level on sending request and receiving response.
- [bug] JAVA-796: Fix SpeculativeExecutionPolicy.init() and close() are never called.
- [improvement] JAVA-710: Suppress unnecessary warning at shutdown.
- [improvement] #340: Allow DNS name with multiple A-records as contact point.
- [bug] JAVA-794: Allow tracing across multiple result pages.
- [bug] JAVA-737: DowngradingConsistencyRetryPolicy ignores write timeouts.
- [bug] JAVA-736: Forbid bind marker in QueryBuilder add/append/prepend.
- [bug] JAVA-712: Prevent QueryBuilder.quote() from applying duplicate double quotes.
- [bug] JAVA-688: Prevent QueryBuilder from trying to serialize raw string.
- [bug] JAVA-679: Support bind marker in QueryBuilder DELETE's list index.
- [improvement] JAVA-475: Improve QueryBuilder API for SELECT DISTINCT.
- [improvement] JAVA-225: Create values() function for Insert builder using List.
- [improvement] JAVA-702: Warn when ReplicationStrategy encounters invalid
  replication factors.
- [improvement] JAVA-662: Add PoolingOptions method to set both core and max
  connections.
- [improvement] JAVA-766: Do not include epoll JAR in binary distribution.
- [improvement] JAVA-726: Optimize internal copies of Request objects.
- [bug] JAVA-815: Preserve tracing across retries.
- [improvement] JAVA-709: New RetryDecision.tryNextHost().
- [bug] JAVA-733: Handle function calls and raw strings as non-idempotent in QueryBuilder.
- [improvement] JAVA-765: Provide API to retrieve values of a Parameterized SimpleStatement.
- [improvement] JAVA-827: implement UPDATE .. IF EXISTS in QueryBuilder.
- [improvement] JAVA-618: Randomize contact points list to prevent hotspots.
- [improvement] JAVA-720: Surface the coordinator used on query failure.
- [bug] JAVA-792: Handle contact points removed during init.
- [improvement] JAVA-719: Allow PlainTextAuthProvider to change its credentials at runtime.
- [new feature] JAVA-151: Make it possible to register for SchemaChange Events.
- [improvement] JAVA-861: Downgrade "Asked to rebuild table" log from ERROR to INFO level.
- [improvement] JAVA-797: Provide an option to prepare statements only on one node.
- [improvement] JAVA-658: Provide an option to not re-prepare all statements in onUp.
- [improvement] JAVA-853: Customizable creation of netty timer.
- [bug] JAVA-859: Avoid quadratic ring processing with invalid replication factors.
- [improvement] JAVA-657: Debounce control connection queries.
- [bug] JAVA-784: LoadBalancingPolicy.distance() called before init().
- [new feature] JAVA-828: Make driver-side metadata optional.
- [improvement] JAVA-544: Allow hosts to remain partially up.
- [improvement] JAVA-821, JAVA-822: Remove internal blocking calls and expose async session
  creation.
- [improvement] JAVA-725: Use parallel calls when re-preparing statement on other
  hosts.
- [bug] JAVA-629: Don't use connection timeout for unrelated internal queries.
- [bug] JAVA-892: Fix NPE in speculative executions when metrics disabled.

Merged from 2.0.10_fixes branch:

- [improvement] JAVA-756: Use Netty's pooled ByteBufAllocator by default.
- [improvement] JAVA-759: Expose "unsafe" paging state API.
- [bug] JAVA-767: Fix getObject by name.
- [bug] JAVA-768: Prevent race during pool initialization.


### 2.0.10.1

- [improvement] JAVA-756: Use Netty's pooled ByteBufAllocator by default.
- [improvement] JAVA-759: Expose "unsafe" paging state API.
- [bug] JAVA-767: Fix getObject by name.
- [bug] JAVA-768: Prevent race during pool initialization.


### 2.0.10

- [new feature] JAVA-518: Add AddressTranslater for EC2 multi-region deployment.
- [improvement] JAVA-533: Add connection heartbeat.
- [improvement] JAVA-568: Reduce level of logs on missing rpc_address.
- [improvement] JAVA-312, JAVA-681: Expose node token and range information.
- [bug] JAVA-595: Fix cluster name mismatch check at startup.
- [bug] JAVA-620: Fix guava dependency when using OSGI.
- [bug] JAVA-678: Fix handling of DROP events when ks name is case-sensitive.
- [improvement] JAVA-631: Use List<?> instead of List<Object> in QueryBuilder API.
- [improvement] JAVA-654: Exclude Netty POM from META-INF in shaded JAR.
- [bug] JAVA-655: Quote single quotes contained in table comments in asCQLQuery method.
- [bug] JAVA-684: Empty TokenRange returned in a one token cluster.
- [improvement] JAVA-687: Expose TokenRange#contains.
- [new feature] JAVA-547: Expose values of BoundStatement.
- [new feature] JAVA-584: Add getObject to BoundStatement and Row.
- [improvement] JAVA-419: Improve connection pool resizing algorithm.
- [bug] JAVA-599: Fix race condition between pool expansion and shutdown.
- [improvement] JAVA-622: Upgrade Netty to 4.0.27.
- [improvement] JAVA-562: Coalesce frames before flushing them to the connection.
- [improvement] JAVA-583: Rename threads to indicate that they are for the driver.
- [new feature] JAVA-550: Expose paging state.
- [new feature] JAVA-646: Slow Query Logger.
- [improvement] JAVA-698: Exclude some errors from measurements in LatencyAwarePolicy.
- [bug] JAVA-641: Fix issue when executing a PreparedStatement from another cluster.
- [improvement] JAVA-534: Log keyspace xxx does not exist at WARN level.
- [improvement] JAVA-619: Allow Cluster subclasses to delegate to another instance.
- [new feature] JAVA-669: Expose an API to check for schema agreement after a
  schema-altering statement.
- [improvement] JAVA-692: Make connection and pool creation fully async.
- [improvement] JAVA-505: Optimize connection use after reconnection.
- [improvement] JAVA-617: Remove "suspected" mechanism.
- [improvement] reverts JAVA-425: Don't mark connection defunct on client timeout.
- [new feature] JAVA-561: Speculative query executions.
- [bug] JAVA-666: Release connection before completing the ResultSetFuture.
- [new feature BETA] JAVA-723: Percentile-based variant of query logger and speculative
  executions.
- [bug] JAVA-734: Fix buffer leaks when compression is enabled.

Merged from 2.0.9_fixes branch:

- [bug] JAVA-614: Prevent race between cancellation and query completion.
- [bug] JAVA-632: Prevent cancel and timeout from cancelling unrelated ResponseHandler if
  streamId was already released and reused.
- [bug] JAVA-642: Fix issue when newly opened pool fails before we could mark the node UP.
- [bug] JAVA-613: Fix unwanted LBP notifications when a contact host is down.
- [bug] JAVA-651: Fix edge cases where a connection was released twice.
- [bug] JAVA-653: Fix edge cases in query cancellation.


### 2.0.9.2

- [bug] JAVA-651: Fix edge cases where a connection was released twice.
- [bug] JAVA-653: Fix edge cases in query cancellation.


### 2.0.9.1

- [bug] JAVA-614: Prevent race between cancellation and query completion.
- [bug] JAVA-632: Prevent cancel and timeout from cancelling unrelated ResponseHandler if
  streamId was already released and reused.
- [bug] JAVA-642: Fix issue when newly opened pool fails before we could mark the node UP.
- [bug] JAVA-613: Fix unwanted LBP notifications when a contact host is down.


### 2.0.9

- [improvement] JAVA-538: Shade Netty dependency.
- [improvement] JAVA-543: Target schema refreshes more precisely.
- [bug] JAVA-546: Don't check rpc_address for control host.
- [improvement] JAVA-409: Improve message of NoHostAvailableException.
- [bug] JAVA-556: Rework connection reaper to avoid deadlock.
- [bug] JAVA-557: Avoid deadlock when multiple connections to the same host get write
  errors.
- [improvement] JAVA-504: Make shuffle=true the default for TokenAwarePolicy.
- [bug] JAVA-577: Fix bug when SUSPECT reconnection succeeds, but one of the pooled
  connections fails while bringing the node back up.
- [bug] JAVA-419: JAVA-587: Prevent faulty control connection from ignoring reconnecting hosts.
- temporarily revert "Add idle timeout to the connection pool".
- [bug] JAVA-593: Ensure updateCreatedPools does not add pools for suspected hosts.
- [bug] JAVA-594: Ensure state change notifications for a given host are handled serially.
- [bug] JAVA-597: Ensure control connection reconnects when control host is removed.


### 2.0.8

- [bug] JAVA-526: Fix token awareness for case-sensitive keyspaces and tables.
- [bug] JAVA-515: Check maximum number of values passed to SimpleStatement.
- [improvement] JAVA-532: Expose the driver version through the API.
- [improvement] JAVA-522: Optimize session initialization when some hosts are not
  responsive.


### 2.0.7

- [bug] JAVA-449: Handle null pool in PooledConnection.release.
- [improvement] JAVA-425: Defunct connection on request timeout.
- [improvement] JAVA-426: Try next host when we get a SERVER_ERROR.
- [bug] JAVA-449, JAVA-460, JAVA-471: Handle race between query timeout and completion.
- [bug] JAVA-496: Fix DCAwareRoundRobinPolicy datacenter auto-discovery.
- [bug] JAVA-497: Ensure control connection does not trigger concurrent reconnects.
- [improvement] JAVA-472: Keep trying to reconnect on authentication errors.
- [improvement] JAVA-463: Expose close method on load balancing policy.
- [improvement] JAVA-459: Allow load balancing policy to trigger refresh for a single host.
- [bug] JAVA-493: Expose an API to cancel reconnection attempts.
- [bug] JAVA-503: Fix NPE when a connection fails during pool construction.
- [improvement] JAVA-423: Log datacenter name in DCAware policy's init when it is explicitly provided.
- [improvement] JAVA-504: Shuffle the replicas in TokenAwarePolicy.newQueryPlan.
- [improvement] JAVA-507: Make schema agreement wait tuneable.
- [improvement] JAVA-494: Document how to inject the driver metrics into another registry.
- [improvement] JAVA-419: Add idle timeout to the connection pool.
- [bug] JAVA-516: LatencyAwarePolicy does not shutdown executor on invocation of close.
- [improvement] JAVA-451: Throw an exception when DCAwareRoundRobinPolicy is built with
  an explicit but null or empty local datacenter.
- [bug] JAVA-511: Fix check for local contact points in DCAware policy's init.
- [improvement] JAVA-457: Make timeout on saturated pool customizable.
- [improvement] JAVA-521: Downgrade Guava to 14.0.1.


### 2.0.6

- [bug] JAVA-397: Check cluster name when connecting to a new node.
- [bug] JAVA-326: Add missing CAS delete support in QueryBuilder.
- [bug] JAVA-363: Add collection and data length checks during serialization.
- [improvement] JAVA-329: Surface number of retries in metrics.
- [bug] JAVA-428: Do not use a host when no rpc_address found for it.
- [improvement] JAVA-358: Add ResultSet.wasApplied() for conditional queries.
- [bug] JAVA-349: Fix negative HostConnectionPool open count.
- [improvement] JAVA-436: Log more connection details at trace and debug levels.
- [bug] JAVA-445: Fix cluster shutdown.
- [improvement] JAVA-439: Expose child policy in chainable load balancing policies.


### 2.0.5

- [bug] JAVA-407: Release connections on ResultSetFuture#cancel.
- [bug] JAVA-393: Fix handling of SimpleStatement with values in query builder
  batches.
- [bug] JAVA-417: Ensure pool is properly closed in onDown.
- [bug] JAVA-415: Fix tokenMap initialization at startup.
- [bug] JAVA-418: Avoid deadlock on close.


### 2.0.4

- [improvement] JAVA-204: Better handling of dead connections.
- [bug] JAVA-373: Fix potential NPE in ControlConnection.
- [bug] JAVA-291: Throws NPE when passed null for a contact point.
- [bug] JAVA-315: Avoid LoadBalancingPolicy onDown+onUp at startup.
- [bug] JAVA-343: Avoid classloader leak in Tomcat.
- [bug] JAVA-387: Avoid deadlock in onAdd/onUp.
- [bug] JAVA-377, JAVA-391: Make metadata parsing more lenient.
- [bug] JAVA-394: Ensure defunct connections are completely closed.
- [bug] JAVA-342, JAVA-390: Fix memory and resource leak on closed Sessions.


### 2.0.3

- [new] The new AbsractSession makes mocking of Session easier.
- [new] JAVA-309: Allow to trigger a refresh of connected hosts.
- [new] JAVA-265: New Session#getState method allows to grab information on
  which nodes a session is connected to.
- [new] JAVA-327: Add QueryBuilder syntax for tuples in where clauses (syntax
  introduced in Cassandra 2.0.6).
- [improvement] JAVA-359: Properly validate arguments of PoolingOptions methods.
- [bug] JAVA-368: Fix bogus rejection of BigInteger in 'execute with values'.
- [bug] JAVA-367: Signal connection failure sooner to avoid missing them.
- [bug] JAVA-337: Throw UnsupportedOperationException for protocol batch
  setSerialCL.

Merged from 1.0 branch:

- [bug] JAVA-325: Fix periodic reconnection to down hosts.


### 2.0.2

- [api] The type of the map key returned by NoHostAvailable#getErrors has changed from
  InetAddress to InetSocketAddress. Same for Initializer#getContactPoints return and
  for AuthProvider#newAuthenticator.
- [api] JAVA-296: The default load balacing policy is now DCAwareRoundRobinPolicy, and the local
  datacenter is automatically picked based on the first connected node. Furthermore,
  the TokenAwarePolicy is also used by default.
- [new] JAVA-145: New optional AddressTranslater.
- [bug] JAVA-321: Don't remove quotes on keyspace in the query builder.
- [bug] JAVA-320: Fix potential NPE while cluster undergo schema changes.
- [bug] JAVA-319: Fix thread-safety of page fetching.
- [bug] JAVA-318: Fix potential NPE using fetchMoreResults.

Merged from 1.0 branch:

- [new] JAVA-179: Expose the name of the partitioner in use in the cluster metadata.
- [new] Add new WhiteListPolicy to limit the nodes connected to a particular list.
- [improvement] JAVA-289: Do not hop DC for LOCAL_* CL in DCAwareRoundRobinPolicy.
- [bug] JAVA-313: Revert back to longs for dates in the query builder.
- [bug] JAVA-314: Don't reconnect to nodes ignored by the load balancing policy.


### 2.0.1

- [improvement] JAVA-278: Handle the static columns introduced in Cassandra 2.0.6.
- [improvement] JAVA-208: Add Cluster#newSession method to create Session without connecting
  right away.
- [bug] JAVA-279: Add missing iso8601 patterns for parsing dates.
- [bug] Properly parse BytesType as the blob type.
- [bug] JAVA-280: Potential NPE when parsing schema of pre-CQL tables of C* 1.2 nodes.

Merged from 1.0 branch:

- [bug] JAVA-275: LatencyAwarePolicy.Builder#withScale doesn't set the scale.
- [new] JAVA-114: Add methods to check if a Cluster/Session instance has been closed already.


### 2.0.0

- [api] JAVA-269: Case sensitive identifier by default in Metadata.
- [bug] JAVA-274: Fix potential NPE in Cluster#connect.

Merged from 1.0 branch:

- [bug] JAVA-263: Always return the PreparedStatement object that is cache internally.
- [bug] JAVA-261: Fix race when multiple connect are done in parallel.
- [bug] JAVA-270: Don't connect at all to nodes that are ignored by the load balancing
  policy.


### 2.0.0-rc3

- [improvement] The protocol version 1 is now supported (features only supported by the
  version 2 of the protocol throw UnsupportedFeatureException).
- [improvement] JAVA-195: Make most main objects interface to facilitate testing/mocking.
- [improvement] Adds new getStatements and clear methods to BatchStatement.
- [api] JAVA-247: Renamed shutdown to closeAsync and ShutdownFuture to CloseFuture. Clustering
  and Session also now implement Closeable.
- [bug] JAVA-232: Fix potential thread leaks when shutting down Metrics.
- [bug] JAVA-231: Fix potential NPE in HostConnectionPool.
- [bug] JAVA-244: Avoid NPE when node is in an unconfigured DC.
- [bug] JAVA-258: Don't block for scheduled reconnections on Cluster#close.

Merged from 1.0 branch:

- [new] JAVA-224: Added Session#prepareAsync calls.
- [new] JAVA-249: Added Cluster#getLoggedKeyspace.
- [improvement] Avoid preparing a statement multiple time per host with multiple sessions.
- [bug] JAVA-255: Make sure connections are returned to the right pools.
- [bug] JAVA-264: Use date string in query build to work-around CASSANDRA-6718.


### 2.0.0-rc2

- [new] JAVA-207: Add LOCAL_ONE consistency level support (requires using C* 2.0.2+).
- [bug] JAVA-219: Fix parsing of counter types.
- [bug] JAVA-218: Fix missing whitespace for IN clause in the query builder.
- [bug] JAVA-221: Fix replicas computation for token aware balancing.

Merged from 1.0 branch:

- [bug] JAVA-213: Fix regression from JAVA-201.
- [improvement] New getter to obtain a snapshot of the scores maintained by
  LatencyAwarePolicy.


### 2.0.0-rc1

- [new] JAVA-199: Mark compression dependencies optional in maven.
- [api] Renamed TableMetadata#getClusteringKey to TableMetadata#getClusteringColumns.

Merged from 1.0 branch:

- [new] JAVA-142: OSGi bundle.
- [improvement] JAVA-205: Make collections returned by Row immutable.
- [improvement] JAVA-203: Limit internal thread pool size.
- [bug] JAVA-201: Don't retain unused PreparedStatement in memory.
- [bug] Add missing clustering order info in TableMetadata
- [bug] JAVA-196: Allow bind markers for collections in the query builder.


### 2.0.0-beta2

- [api] BoundStatement#setX(String, X) methods now set all values (if there is
  more than one) having the provided name, not just the first occurence.
- [api] The Authenticator interface now has a onAuthenticationSuccess method that
  allows to handle the potential last token sent by the server.
- [new] The query builder don't serialize large values to strings anymore by
  default by making use the new ability to send values alongside the query string.
- [new] JAVA-140: The query builder has been updated for new CQL features.
- [bug] Fix exception when a conditional write timeout C* side.
- [bug] JAVA-182: Ensure connection is created when Cluster metadata are asked for.
- [bug] JAVA-187: Fix potential NPE during authentication.


### 2.0.0-beta1

- [api] The 2.0 version is an API-breaking upgrade of the driver. While most
  of the breaking changes are minor, there are too numerous to be listed here
  and you are encouraged to look at the Upgrade_guide_to_2.0 file that describe
  those changes in details.
- [new] LZ4 compression is supported for the protocol.
- [new] JAVA-39: The driver does not depend on cassandra-all anymore.
- [new] New BatchStatement class allows to execute batch other statements.
- [new] Large ResultSet are now paged (incrementally fetched) by default.
- [new] SimpleStatement support values for bind-variables, to allow
  prepare+execute behavior with one roundtrip.
- [new] Query parameters defaults (Consistency level, page size, ...) can be
  configured globally.
- [new] New Cassandra 2.0 SERIAL and LOCAL_SERIAL consistency levels are
  supported.
- [new] JAVA-116: Cluster#shutdown now waits for ongoing queries to complete by default.
- [new] Generic authentication through SASL is now exposed.
- [bug] JAVA-88: TokenAwarePolicy now takes all replica into account, instead of only the
  first one.


### 1.0.5

- [new] JAVA-142: OSGi bundle.
- [new] JAVA-207: Add support for ConsistencyLevel.LOCAL_ONE; note that this
  require Cassandra 1.2.12+.
- [improvement] JAVA-205: Make collections returned by Row immutable.
- [improvement] JAVA-203: Limit internal thread pool size.
- [improvement] New getter to obtain a snapshot of the scores maintained by
  LatencyAwarePolicy.
- [improvement] JAVA-222: Avoid synchronization when getting codec for collection
  types.
- [bug] JAVA-201, JAVA-213: Don't retain unused PreparedStatement in memory.
- [bug] Add missing clustering order info in TableMetadata
- [bug] JAVA-196: Allow bind markers for collections in the query builder.


### 1.0.4

- [api] JAVA-163: The Cluster.Builder#poolingOptions and Cluster.Builder#socketOptions
  are now deprecated. They are replaced by the new withPoolingOptions and
  withSocketOptions methods.
- [new] JAVA-129: A new LatencyAwarePolicy wrapping policy has been added, allowing to
  add latency awareness to a wrapped load balancing policy.
- [new] JAVA-161: Cluster.Builder#deferInitialization: Allow defering cluster initialization.
- [new] JAVA-117: Add truncate statement in query builder.
- [new] JAVA-106: Support empty IN in the query builder.
- [bug] JAVA-166: Fix spurious "No current pool set; this should not happen" error
  message.
- [bug] JAVA-184: Fix potential overflow in RoundRobinPolicy and correctly errors if
  a balancing policy throws.
- [bug] Don't release Stream ID for timeouted queries (unless we do get back
  the response)
- [bug] Correctly escape identifiers and use fully qualified table names when
  exporting schema as string.


### 1.0.3

- [api] The query builder now correctly throw an exception when given a value
  of a type it doesn't know about.
- [new] SocketOptions#setReadTimeout allows to set a timeout on how long we
  wait for the answer of one node. See the javadoc for more details.
- [new] New Session#prepare method that takes a Statement.
- [bug] JAVA-143: Always take per-query CL, tracing, etc. into account for QueryBuilder
  statements.
- [bug] Temporary fixup for TimestampType when talking to C* 2.0 nodes.


### 1.0.2

- [api] Host#getMonitor and all Host.HealthMonitor methods have been
  deprecated. The new Host#isUp method is now prefered to the method
  in the monitor and you should now register Host.StateListener against
  the Cluster object directly (registering against a host HealthMonitor
  was much more limited anyway).
- [new] JAVA-92: New serialize/deserialize methods in DataType to serialize/deserialize
  values to/from bytes.
- [new] JAVA-128: New getIndexOf() method in ColumnDefinitions to find the index of
  a given column name.
- [bug] JAVA-131: Fix a bug when thread could get blocked while setting the current
  keyspace.
- [bug] JAVA-136: Quote inet addresses in the query builder since CQL3 requires it.


### 1.0.1

- [api] JAVA-100: Function call handling in the query builder has been modified in a
  backward incompatible way. Function calls are not parsed from string values
  anymore as this wasn't safe. Instead the new 'fcall' method should be used.
- [api] Some typos in method names in PoolingOptions have been fixed in a
  backward incompatible way before the API get widespread.
- [bug] JAVA-123: Don't destroy composite partition key with BoundStatement and
  TokenAwarePolicy.
- [new] null values support in the query builder.
- [new] JAVA-5: SSL support (requires C* >= 1.2.1).
- [new] JAVA-113: Allow generating unlogged batch in the query builder.
- [improvement] Better error message when no host are available.
- [improvement] Improves performance of the stress example application been.


### 1.0.0

- [api] The AuthInfoProvider has be (temporarily) removed. Instead, the
  Cluster builder has a new withCredentials() method to provide a username
  and password for use with Cassandra's PasswordAuthenticator. Custom
  authenticator will be re-introduced in a future version but are not
  supported at the moment.
- [api] The isMetricsEnabled() method in Configuration has been replaced by
  getMetricsOptions(). An option to disabled JMX reporting (on by default)
  has been added.
- [bug] JAVA-91: Don't make default load balancing policy a static singleton since it
  is stateful.


### 1.0.0-RC1

- [new] JAVA-79: Null values are now supported in BoundStatement (but you will need at
  least Cassandra 1.2.3 for it to work). The API of BoundStatement has been
  slightly changed so that not binding a variable is not an error anymore,
  the variable is simply considered null by default. The isReady() method has
  been removed.
- [improvement] JAVA-75: The Cluster/Session shutdown methods now properly block until
  the shutdown is complete. A version with at timeout has been added.
- [bug] JAVA-44: Fix use of CQL3 functions in the query builder.
- [bug] JAVA-77: Fix case where multiple schema changes too quickly wouldn't work
  (only triggered when 0.0.0.0 was used for the rpc_address on the Cassandra
  nodes).
- [bug] JAVA-72: Fix IllegalStateException thrown due to a reconnection made on an I/O
  thread.
- [bug] JAVA-82: Correctly reports errors during authentication phase.


### 1.0.0-beta2

- [new] JAVA-51, JAVA-60, JAVA-58: Support blob constants, BigInteger, BigDecimal and counter batches in
  the query builder.
- [new] JAVA-61: Basic support for custom CQL3 types.
- [new] JAVA-65: Add "execution infos" for a result set (this also move the query
  trace in the new ExecutionInfos object, so users of beta1 will have to
  update).
- [bug] JAVA-62: Fix failover bug in DCAwareRoundRobinPolicy.
- [bug] JAVA-66: Fix use of bind markers for routing keys in the query builder.


### 1.0.0-beta1

- initial release
