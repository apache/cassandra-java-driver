## Metrics

### Quick overview

* `advanced.metrics` in the configuration. All disabled by default, can be selected individually.
* some metrics are per node, others global to the session, or both.
* unlike driver 3, JMX is not provided out of the box. You need to add the dependency manually.

-----

The driver exposes measurements of its internal behavior through the popular [Dropwizard Metrics]
library. Application developers can select which metrics are enabled, and export them to a
monitoring tool. 

### Structure
 
There are two categories of metrics:

* session-level: the measured data is global to a `Session` instance. For example, `connected-nodes`
  measures the number of nodes to which we have connections.
* node-level: the data is specific to a node (and therefore there is one metric instance per node).
  For example, `pool.open-connections` measures the number of connections open to this particular
  node.
  
Metric names are path-like, dot-separated strings. The driver prefixes them with the name of the
session (see `session-name` in the configuration), and in the case of node-level metrics, `nodes`
followed by a textual representation of the node's address. For example:

```
s0.connected-nodes => 2
s0.nodes.127_0_0_1:9042.pool.open-connections => 2
s0.nodes.127_0_0_2:9042.pool.open-connections => 1
```  

### Configuration

By default, all metrics are disabled. You can turn them on individually in the configuration, by
adding their name to these lists:

```
datastax-java-driver.advanced.metrics {
  session.enabled = [ connected-nodes, cql-requests ]
  node.enabled = [ pool.open-connections, pool.in-flight ]
}
```

To find out which metrics are available, see the [reference configuration]. It contains a
commented-out line for each metric, with detailed explanations on its intended usage.

If you specify a metric that doesn't exist, it will be ignored and a warning will be logged.

The `metrics` section may also contain additional configuration for some specific metrics; again,
see the [reference configuration] for more details.

### Export

The Dropwizard `MetricRegistry` is exposed via `session.getMetrics()`. You can retrieve it and
configure a `Reporter` to send the metrics to a monitoring tool.

#### JMX

Unlike previous driver versions, JMX support is not included out of the box.

Add the following dependency to your application (make sure the version matches the `metrics-core`
dependency of the driver):

```
<dependency>
  <groupId>io.dropwizard.metrics</groupId>
  <artifactId>metrics-jmx</artifactId>
  <version>4.0.2</version>
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

#### Other protocols

Dropwizard Metrics has built-in reporters for other output formats: JSON (via a servlet), stdout,
CSV files, SLF4J logs and Graphite. Refer to their [manual][Dropwizard manual] for more details.


[Dropwizard Metrics]: http://metrics.dropwizard.io/4.0.0/manual/index.html
[Dropwizard Manual]: http://metrics.dropwizard.io/4.0.0/getting-started.html#reporting-via-http
[reference configuration]: ../configuration/reference/