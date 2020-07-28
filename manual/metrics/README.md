## Metrics

The driver exposes measurements of its internal behavior through the popular [Dropwizard Metrics]
library.  Developers can access these metrics and choose to export them to a monitoring tool.

The driver depends on Metrics 3.2.x, but is compatible with newer versions of Dropwizard Metrics.
For using Metrics 4.x with the driver, see [Metrics 4 Compatibility](#metrics-4-compatibility).

### Structure

Metric names are path-like, dot-separated strings.  Metrics are measured at the `Cluster`-level,
thus the driver prefixes them with the name of the `Cluster` they are associated with (see [withClusterName]
for how to configure this), suffixed by `-metrics`.  For example:

```
cluster1-metrics.connected-to
cluster1-metrics.connection-errors
...
```

### Configuration

By default, metrics are enabled and exposed via JMX as [MXBeans].

Some users may find that they don't want the driver to record and expose metrics.  To disable
metrics collection, use the [withoutMetrics] builder method, i.e.:

```java
Cluster cluster = Cluster.builder()
        .withoutMetrics()
        .build();
```

Note that if you decide to disable metrics, you may also consider excluding metrics as a dependency.
To do this in a maven project:

```xml
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-core</artifactId>
  <version>3.10.0</version>
  <exclusions>
    <exclusion>
      <groupId>io.dropwizard.metrics</groupId>
      <artifactId>metrics-core</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

Alternatively, one may not want to expose metrics using JMX.  Disabling JMX reporting is simple
as using the [withoutJMXReporting] builder method, i.e.:

```java
Cluster cluster = Cluster.builder()
        .withoutJMXReporting()
        .build();
```

### Accessing Cluster Metrics

`Cluster` metrics may be accessed via the [getMetrics] method.  The [Metrics] class offers
direct access to all metrics recorded for the `Cluster` via getter methods.  Refer to
the [Metrics javadoc][Metrics] for more details about the metrics offered.

It is very common for applications to record their own metrics.  You can add all metrics
recorded for a `Cluster` to your applications' [MetricRegistry] in the following manner:

```java
MetricRegistry myRegistery = new MetricRegistry();
myRegistry.registerAll(cluster.getMetrics().getRegistry());
```

### Registering a Custom Reporter

Dropwizard Metrics offers a variety of [Reporters] for exporting metrics.  To enable reporting,
access the `Cluster`'s metrics via the [getMetrics] method.  For example, to enable CSV reporting
every 30 seconds:

```java
import com.codahale.metrics.*;

import java.io.File;
import java.util.concurrent.TimeUnit;

Metrics metrics = cluster.getMetrics();

CsvReporter csvReporter = CsvReporter.forRegistry(metrics.getRegistry())
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .build(new File("."));

csvReporter.start(30, TimeUnit.SECONDS);
```

### Metrics 4 Compatibility

While the driver depends on Metrics 3.2.x, it also works with Metrics 4, with some caveats.

In Metrics 4, JMX reporting was moved to a separate module, `metrics-jmx`.  Because of this you are
likely to encounter the following exception at runtime when initializing a `Cluster`:

```
Exception in thread "main" java.lang.NoClassDefFoundError: com/codahale/metrics/JmxReporter
	at com.datastax.driver.core.Metrics.<init>(Metrics.java:103)
	at com.datastax.driver.core.Cluster$Manager.init(Cluster.java:1402)
	at com.datastax.driver.core.Cluster.init(Cluster.java:159)
	at com.datastax.driver.core.Cluster.connectAsync(Cluster.java:330)
	at com.datastax.driver.core.Cluster.connectAsync(Cluster.java:305)
	at com.datastax.durationtest.core.DurationTest.createSessions(DurationTest.java:360)
        ....
Caused by: java.lang.ClassNotFoundException: com.codahale.metrics.JmxReporter
	at java.base/jdk.internal.loader.BuiltinClassLoader.loadClass(BuiltinClassLoader.java:582)
	at java.base/jdk.internal.loader.ClassLoaders$AppClassLoader.loadClass(ClassLoaders.java:185)
	at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:496)
	... 8 more
```

To fix this, use [withoutJMXReporting] when constructing your `Cluster`.  If you still desire JMX
reporting, add `metrics-jmx` as a dependency:

```xml
<dependency>
  <groupId>io.dropwizard.metrics</groupId>
  <artifactId>metrics-jmx</artifactId>
  <version>4.0.2</version>
</dependency>
```

Then create your `Cluster` and `JmxReporter` in the following manner:

```java
Cluster cluster = Cluster.builder()
        .withoutJMXReporting()
        .build();

JmxReporter reporter =
    JmxReporter.forRegistry(cluster.getMetrics().getRegistry())
        .inDomain(cluster.getClusterName() + "-metrics")
        .build();

reporter.start();
```

[Dropwizard Metrics]: http://metrics.dropwizard.io/3.2.2/manual/index.html
[Reporters]: http://metrics.dropwizard.io/3.2.2/manual/core.html#reporters
[MetricRegistry]: http://metrics.dropwizard.io/3.2.2/apidocs/com/codahale/metrics/MetricRegistry.html
[MXBeans]: https://docs.oracle.com/javase/tutorial/jmx/mbeans/mxbeans.html
[withClusterName]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/Cluster.Builder.html#withClusterName-java.lang.String-
[withoutMetrics]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/Cluster.Builder.html#withoutMetrics--
[withoutJMXReporting]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/Cluster.Builder.html#withoutJMXReporting--
[getMetrics]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/Cluster.html#getMetrics--
[Metrics]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/Metrics.html
