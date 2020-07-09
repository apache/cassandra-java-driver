## Metrics

[Driver Metrics](../../../core/metrics/) are reported via [Dropwizard Metrics] by default.

With a bit of custom code, it is possible to switch to a different framework: we provide
alternative implementations for [Micrometer] and [Eclipse MicroProfile Metrics].

### Adding Metrics Framework Dependency

Each implementation lives in a dedicated driver module. Add the following dependency to use
Micrometer:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-metrics-micrometer</artifactId>
  <version>${driver.version}</version>
</dependency>
```

or the following for MicroProfile:

```xml
<dependency>
  <groupId>com.datastax.oss</groupId>
  <artifactId>java-driver-metrics-microprofile</artifactId>
  <version>${driver.version}</version>
</dependency>
```

### Enabling Metrics Framework On A Session

Once the dependency has been added, you need to
[override the context component](../context/#overriding-a-context-component) `MetricsFactory`. If
this is the only customization you have, we provide context classes out of the box, so you just need
to write a custom session builder.

For Micrometer:

```java
import com.datastax.oss.driver.internal.metrics.micrometer.MicrometerDriverContext;
import io.micrometer.core.instrument.MeterRegistry;

public class CustomSessionBuilder extends SessionBuilder<CustomBuilder, CqlSession> {

  private final MeterRegistry registry;

  public CustomSessionBuilder(MeterRegistry registry){
    this.registry = registry;
  }

  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
    return new MicrometerDriverContext(configLoader, programmaticArguments, registry);
  }

  @Override
  protected CqlSession wrap(@NonNull CqlSession defaultSession) {
    // Nothing to do here, nothing changes on the session type
    return defaultSession;
  }
}
```

Or for MicroProfile:

```java
import com.datastax.oss.driver.internal.metrics.microprofile.MicroProfileDriverContext;
import org.eclipse.microprofile.metrics.MetricRegistry;

public class CustomSessionBuilder extends SessionBuilder<CustomBuilder, CqlSession> {

  private final MetricRegistry registry;

  public CustomSessionBuilder(MetricRegistry registry){
    this.registry = registry;
  }

  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
    return new MicroProfileDriverContext(configLoader, programmaticArguments, registry);
  }

  @Override
  protected CqlSession wrap(@NonNull CqlSession defaultSession) {
    // Nothing to do here, nothing changes on the session type
    return defaultSession;
  }
}
```

Use the new builder class to create your driver session:

```java
CqlSession session = new CustomSessionBuilder()
    .addContactPoint(new InetSocketAddress("1.2.3.4", 9042))
    .withLocalDatacenter("datacenter1")
    .build();
```

Notes:

* For both Micrometer and MicroProfile metrics, your application will need to provide a Registry
  implementation to which driver metrics will be registered. Some environments may provide access to
  available instances of the registry
  ([Spring](https://micrometer.io/docs/ref/spring/1.5#_configuring), for example, provides many
  implementations of Micrometer MeterRegistry instances) that can be used.
* `Session.getMetrics()` will only work with the built-in implementation. Our `Metrics` interface
  references DropWizard types directly, we didn't want to make it generic because it would
  over-complicate the driver API. If you use another framework and need programmatic access to the
  metrics, you'll need to find your own way to expose the registry.

[Dropwizard Metrics]: http://metrics.dropwizard.io/4.0.0/manual/index.html
[Micrometer]: https://micrometer.io/
[Eclipse MicroProfile Metrics]: https://projects.eclipse.org/projects/technology.microprofile/releases/metrics-2.3