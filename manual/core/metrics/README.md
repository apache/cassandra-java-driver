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

## Metrics

### Quick overview

* `advanced.metrics` in the configuration. All metrics disabled by default. To enable, select the
  metrics library to use, then define which individual metrics to activate.
* some metrics are per node, others global to the session, or both.
* unlike driver 3, JMX is not provided out of the box. You need to add the dependency manually.

-----

The driver is able to report measurements of its internal behavior to a variety of metrics
libraries, and ships with bindings for three popular ones: [Dropwizard Metrics] , [Micrometer
Metrics] and [MicroProfile Metrics].

### Selecting a Metrics Library

#### Dropwizard Metrics

Dropwizard is the driver's default metrics library; there is no additional configuration nor any
extra dependency to add if you wish to use Dropwizard.

#### Micrometer

To use Micrometer you must:

1. Define `MicrometerMetricsFactory` as the metrics factory to use in the driver configuration: 

```
datastax-java-driver.advanced.metrics {
  factory.class = MicrometerMetricsFactory
}
```

2. Add a dependency to `java-driver-metrics-micrometer` in your application. This separate driver
module contains the actual bindings for Micrometer, and depends itself on the Micrometer library:

```xml
<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>java-driver-metrics-micrometer</artifactId>
  <version>${driver.version}</version>
</dependency>
```

3. You should also exclude Dropwizard and HdrHistogram, which are two transitive dependencies of the
driver, because they are not relevant when using Micrometer:

```xml
<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>java-driver-core</artifactId>
  <exclusions>
    <exclusion>
      <groupId>io.dropwizard.metrics</groupId>
      <artifactId>metrics-core</artifactId>
    </exclusion>
    <exclusion>
      <groupId>org.hdrhistogram</groupId>
      <artifactId>HdrHistogram</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

#### MicroProfile Metrics

To use MicroProfile Metrics you must:

1. Define `MicroProfileMetricsFactory` as the metrics factory to use in the driver configuration:

```
datastax-java-driver.advanced.metrics {
  factory.class = MicroProfileMetricsFactory
}
```

2. Add a dependency to `java-driver-metrics-microprofile` in your application. This separate driver
module contains the actual bindings for MicroProfile, and depends itself on the MicroProfile Metrics
library:

```xml
<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>java-driver-metrics-microprofile</artifactId>
  <version>${driver.version}</version>
</dependency>
```

3. You should also exclude Dropwizard and HdrHistogram, which are two transitive dependencies of the
driver, because they are not relevant when using MicroProfile Metrics:

```xml
<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>java-driver-core</artifactId>
  <exclusions>
    <exclusion>
      <groupId>io.dropwizard.metrics</groupId>
      <artifactId>metrics-core</artifactId>
    </exclusion>
    <exclusion>
      <groupId>org.hdrhistogram</groupId>
      <artifactId>HdrHistogram</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

#### Other Metrics libraries

Other metrics libraries can also be used. However, you will need to provide a custom
metrics factory. Simply implement the
`com.datastax.oss.driver.internal.core.metrics.MetricsFactory` interface for your library of choice,
then pass the fully-qualified name of that implementation class to the driver using the
`advanced.metrics.factory.class` option. See the [reference configuration].

You will certainly need to add the metrics library as a dependency to your application as well.
It is also recommended excluding Dropwizard and HdrHistogram, as shown above.

### Enabling specific driver metrics

Now that the metrics library is configured, you need to activate the driver metrics you are
interested in.

There are two categories of driver metrics:

* session-level: the measured data is global to a `Session` instance. For example, `connected-nodes`
  measures the number of nodes to which we have connections.
* node-level: the data is specific to a node (and therefore there is one metric instance per node).
  For example, `pool.open-connections` measures the number of connections open to this particular
  node.

To find out which metrics are available, see the [reference configuration]. It contains a
commented-out line for each metric, with detailed explanations on its intended usage.

By default, all metrics are disabled. You can turn them on individually in the configuration, by
adding their name to these lists:

```
datastax-java-driver.advanced.metrics {
  session.enabled = [ connected-nodes, cql-requests ]
  node.enabled = [ pool.open-connections, pool.in-flight ]
}
```

If you specify a metric that doesn't exist, it will be ignored, and a warning will be logged.

Finally, if you are using Dropwizard or Micrometer and enabled any metric of timer type, such as
`cql-requests`, it is also possible to provide additional configuration to fine-tune the underlying
histogram's characteristics and precision, such as its highest expected latency, its number of
significant digits to use, and its refresh interval. Again, see the [reference configuration] for
more details.

### Selecting a metric identifier style

Most metric libraries uniquely identify a metric by a name and, optionally, by a set of key-value
pairs, usually called tags.

The `advanced.metrics.id-generator.class` option is used to customize how the driver generates
metric identifiers. The driver ships with two built-in implementations:

- `DefaultMetricIdGenerator`: generates identifiers composed solely of (unique) metric names; it
  does not generate tags. All metric names start with the name of the session (see `session-name` in
  the configuration), and in the case of node-level metrics, this is followed by `.nodes.`, followed
  by a textual representation of the node's address. All names end with the metric distinctive name.
  See below for examples. This generator is mostly suitable for use with metrics libraries that do
  not support tags, like Dropwizard.

- `TaggingMetricIdGenerator`: generates identifiers composed of a name and one or two tags.
  Session-level metric names start with the `session.` prefix followed by the metric distinctive
  name; node-level metric names start with the `nodes.` prefix followed by the metric distinctive
  name. Session-level tags will include a `session` tag whose value is the session name (see
  `session-name` in the configuration); node-level tags will include the same `session` tag, and
  also a `node` tag whose value is the node's address. See below for examples. This generator is
  mostly suitable for use with metrics libraries that support tags, like Micrometer or MicroProfile
  Metrics.

For example, here is how each one of them generates identifiers for the session metric "bytes-sent",
assuming that the session is named "s0":

- `DefaultMetricIdGenerator`:
  - name:`s0.bytes-sent`
  - tags: `{}`
- `TaggingMetricIdGenerator`:
  - name: `session.bytes-sent`
  - tags: `{ "session" : "s0" }`

Here is how each one of them generates identifiers for the node metric "bytes-sent", assuming that
the session is named "s0", and the node's broadcast address is 10.1.2.3:9042:

- `DefaultMetricIdGenerator`:
  - name : `s0.nodes.10_1_2_3:9042.bytes-sent`
  - tags: `{}`
- `TaggingMetricIdGenerator`:
  - name `nodes.bytes-sent`
  - tags: `{ "session" : "s0", "node" : "\10.1.2.3:9042" }`

As shown above, both built-in implementations generate names that are path-like structures separated
by dots. This is indeed the most common expected format by reporting tools.

Finally, it is also possible to define a global prefix for all metric names; this can be done with
the `advanced.metrics.id-generator.prefix` option.

The prefix should not start nor end with a dot or any other path separator; the following are two
valid examples: `cassandra` or `myapp.prod.cassandra`.

For example, if this prefix is set to `cassandra`, here is how the session metric "bytes-sent" would
be named, assuming that the session is named "s0":

- with `DefaultMetricIdGenerator`: `cassandra.s0.bytes-sent`
- with `TaggingMetricIdGenerator`: `cassandra.session.bytes-sent`

Here is how the node metric "bytes-sent" would be named, assuming that the session is named "s0",
and the node's broadcast address is 10.1.2.3:9042:

- with `DefaultMetricIdGenerator`: `cassandra.s0.nodes.10_1_2_3:9042.bytes-sent`
- with `TaggingMetricIdGenerator`: `cassandra.nodes.bytes-sent`

### Using an external metric registry

Regardless of which metrics library is used, you can provide an external metric registry object when
building a session. This allows the driver to transparently export its operational metrics to
whatever reporting system you want to use.

To pass a metric registry object to the session, use the `CqlSessionBuilder.withMetricRegistry()`
method:

```java
CqlSessionBuilder builder = CqlSession.builder();
builder.withMetricRegistry(myRegistryObject);
CqlSession session = builder.build();
```

Beware that the driver does not inspect the provided object, it simply passes it to the metrics
factory in use; it is the user's responsibility to provide registry objects compatible with the
metrics library in use. For reference, here are the expected base types for the three built-in
metrics libraries:

* Dropwizard:   `com.codahale.metrics.MetricRegistry`
* Micrometer:   `io.micrometer.core.instrument.MeterRegistry`
* MicroProfile: `org.eclipse.microprofile.metrics.MetricRegistry`

**NOTE:** MicroProfile **requires** an external instance of its registry to be provided. For
Micrometer, if no registry object is provided, Micrometer's `globalRegistry` will be used. For
Dropwizard, if no registry object is provided, an instance of `MetricRegistry` will be created and
used (in which case, it can be retrieved programmatically if needed, see below).

### Programmatic access to driver metrics

Programmatic access to driver metrics is only available when using Dropwizard Metrics. Users of
other libraries are encouraged to provide an external registry when creating the driver session (see
above), then use it to gain programmatic access to the driver metrics.

The Dropwizard `MetricRegistry` object is exposed in the driver API via
`session.getMetrics().getRegistry()`. You can retrieve it and, for example, configure a `Reporter`
to send the metrics to a monitoring tool.

**NOTE:** Beware that `session.getMetrics()` is not available when using other metrics libraries,
and will throw a `NoClassDefFoundError` at runtime if accessed in such circumstances.

### Exposing driver metrics with JMX

Unlike previous driver versions, JMX support is not included out of the box.

The way to add JMX support to your application depends largely on the metrics library being used. We
show below instructions for Dropwizard only. Micrometer also has support for JMX: please refer to
its [official documentation][Micrometer JMX].

#### Dropwizard Metrics

Add the following dependency to your application (make sure the version matches the `metrics-core`
dependency of the driver):

```xml
<dependency>
  <groupId>io.dropwizard.metrics</groupId>
  <artifactId>metrics-jmx</artifactId>
  <version>4.1.2</version>
</dependency>
```

Then create a JMX reporter for the registry:

```java
MetricRegistry registry = session.getMetrics()
    .orElseThrow(() -> new IllegalStateException("Metrics are disabled"))
    .getRegistry();

JmxReporter reporter =
    JmxReporter.forRegistry(registry)
        .inDomain("com.datastax.oss.driver")
        .build();
reporter.start();
```

Note: by default, the JMX reporter exposes all metrics in a flat structure (for example,
`pool.open-connections` and `pool.in-flight` appear as root elements). If you prefer a hierarchical
structure (`open-connections` and `in-flight` nested into a `pool` sub-domain), use a custom object
factory:

```java
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jmx.ObjectNameFactory;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

ObjectNameFactory objectNameFactory = (type, domain, name) -> {
  StringBuilder objectName = new StringBuilder(domain).append(':');
  List<String> nameParts = Splitter.on('.').splitToList(name);
  int i = 0;
  for (String namePart : nameParts) {
    boolean isLast = (i == nameParts.size() - 1);
    String key =
        isLast ? "name" : Strings.padStart(Integer.toString(i), 2, '0');
    objectName.append(key).append('=').append(namePart);
    if (!isLast) {
      objectName.append(',');
    }
    i += 1;
  }
  try {
    return new ObjectName(objectName.toString());
  } catch (MalformedObjectNameException e) {
    throw new RuntimeException(e);
  }
};

JmxReporter reporter =
    JmxReporter.forRegistry(registry)
        .inDomain("com.datastax.oss.driver")
        .createsObjectNamesWith(objectNameFactory)
        .build();
reporter.start();
```

### Exporting metrics with other protocols

Dropwizard Metrics has built-in reporters for other output formats: JSON (via a servlet), stdout,
CSV files, SLF4J logs and Graphite. Refer to their [manual][Dropwizard manual] for more details.

[Dropwizard Metrics]: https://metrics.dropwizard.io/4.1.2
[Dropwizard Manual]: https://metrics.dropwizard.io/4.1.2/getting-started.html
[Micrometer Metrics]: https://micrometer.io/docs
[Micrometer JMX]: https://micrometer.io/docs/registry/jmx
[MicroProfile Metrics]: https://github.com/eclipse/microprofile-metrics
[reference configuration]: ../configuration/reference/