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

## Configuration

The driver's configuration is composed of options, organized in a hierarchical manner. Optionally,
it can define *profiles* that customize a set of options for a particular kind of request.

The default implementation is based on the Typesafe Config framework. It can be completely
overridden if needed.

For a complete list of built-in options, see the [reference configuration][reference.conf].


### Concepts

#### Options

Essentially, an option is a path in the configuration with an expected type, for example
`basic.request.timeout`, representing a duration.

#### Execution profiles

Imagine an application that does both transactional and analytical requests. Transactional requests
are simpler and must return quickly, so they will typically use a short timeout, let's say 100 
milliseconds; analytical requests are more complex and less frequent so a higher SLA is acceptable,
for example 5 seconds. In addition, maybe you want to use a different consistency level.

Instead of manually adjusting the options on every request, you can create execution profiles:

```
datastax-java-driver {
  profiles {
    oltp {
      basic.request.timeout = 100 milliseconds
      basic.request.consistency = ONE
    }
    olap {
      basic.request.timeout = 5 seconds
      basic.request.consistency = QUORUM
    }
}
```

Now each request only needs a profile name:

```java
SimpleStatement s =
  SimpleStatement.builder("SELECT name FROM user WHERE id = 1")
      .setExecutionProfileName("oltp")
      .build();
session.execute(s);
```

The configuration has an anonymous *default profile* that is always present. It can define an
arbitrary number of named profiles. They inherit from the default profile, so you only need to
override the options that have a different value.


### Default implementation: Typesafe Config

Out of the box, the driver uses [Typesafe Config]. 

It looks at the following locations, according to the [standard behavior][config standard behavior]
of that library:

* system properties
* `application.conf` (all resources on the classpath with this name)
* `application.json` (all resources on the classpath with this name)
* `application.properties` (all resources on the classpath with this name)
* `reference.conf` (all resources on the classpath with this name)

The driver ships with a [reference.conf] that defines sensible defaults for all the options. That
file is heavily documented, so refer to it for details about each option. It is included in the core
driver JAR, so it is in your application's classpath. If you need to customize something, add an
`application.conf` to the classpath. There are various ways to do it:
 
* place the file in a directory that is on your application or application server's classpath
  ([example for Apache Tomcat](https://stackoverflow.com/questions/1300780/adding-a-directory-to-tomcat-classpath));
* if you use Maven, place it in the `src/main/resources` directory.

Since `application.conf` inherits from `reference.conf`, you only need to redeclare what you
override:

```
# Sample application.conf: overrides one option and adds a profile
datastax-java-driver {
  advanced.protocol.version = V4
  profiles {
    slow {
      basic.request.timeout = 10 seconds
    }
  }
}
```

`.conf` files are in the *HOCON* format, an improved superset of JSON; refer to the
[HOCON spec][HOCON] for details. 

By default, configuration files are reloaded regularly, and the driver will adjust to the new values
(on a "best effort" basis: some options, like protocol version and policy configurations, cannot be
changed at runtime and will be ignored). The reload interval is defined in the configuration:

```
# To disable periodic reloading, set this to 0.
datastax-java-driver.basic.config-reload-interval = 5 minutes
```

As mentioned previously, system properties can also be used to override individual options. This is
great for temporary changes, for example in your development environment:  
 
```
# Increase heartbeat interval to limit the amount of debug logs:
java -Ddatastax-java-driver.advanced.heartbeat.interval="5 minutes" ...
```

For array options, provide each element separately by appending an index to the path:

```
-Ddatastax-java-driver.basic.contact-points.0="127.0.0.1:9042"
-Ddatastax-java-driver.basic.contact-points.1="127.0.0.2:9042"
```

We recommend reserving system properties for the early phases of the project; in production, having
all the configuration in one place will make it easier to manage and review.

As shown so far, all options live under a `datastax-java-driver` prefix. This can be changed, for
example if you need multiple driver instances in the same VM with different configurations. See the
[Advanced topics](#changing-the-config-prefix) section.

#### Alternate application config locations

If loading `application.conf` from the classpath doesn't work for you, other loader implementations
are available:

* [DriverConfigLoader.fromClasspath]: still load from the classpath, but use a different resource
  name. For example "config" will try to load `config.conf`, `config.json` or `config.properties`.
* [DriverConfigLoader.fromFile]: load from a file on the local filesystem.
* [DriverConfigLoader.fromUrl]: load from a URL.

To use any of those loaders, pass it to the session builder:

```java
File file = new File("/path/to/application.conf");
CqlSession session = CqlSession.builder()
    .withConfigLoader(DriverConfigLoader.fromFile(file))
    .build();
```

Apart from application-specific configuration, they work exactly like the default loader: they
fall back to the driver's built-in `reference.conf` for defaults, accept overrides via system
properties, and reload at the interval specified by the `basic.config-reload-interval` option. 

#### Programmatic application config

Alternatively, you can use [DriverConfigLoader.programmaticBuilder] to specify configuration options
programmatically instead of loading them from a static resource:

```java
DriverConfigLoader loader =
    DriverConfigLoader.programmaticBuilder()
        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5))
        .startProfile("slow")
        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
        .endProfile()
        .build();
CqlSession session = CqlSession.builder().withConfigLoader(loader).build();
```

This is useful for frameworks and tools that already have their own configuration mechanism.

### The configuration API

You don't need the configuration API for everyday usage of the driver, but it can be useful if:

* you're writing custom policies or a custom config implementation;
* use dynamic profiles (see below);
* or simply want to read configuration options at runtime.

#### Basics 

The driver's context exposes a [DriverConfig] instance:

```java
DriverConfig config = session.getContext().getConfig();
DriverExecutionProfile defaultProfile = config.getDefaultProfile();
DriverExecutionProfile olapProfile = config.getProfile("olap");

config.getProfiles().forEach((name, profile) -> ...);
```

[DriverExecutionProfile] has typed option getters:

```java
Duration requestTimeout = defaultProfile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT);
int maxRequestsPerConnection = defaultProfile.getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS);
```

#### Manual reloading

In addition to periodic reloading, you can trigger a reload programmatically. This returns a
`CompletionStage` that you can use for example to register a callback when the reload is complete: 

```java
DriverConfigLoader loader = session.getContext().getConfigLoader();
if (loader.supportsReloading()) {
  CompletionStage<Boolean> reloaded = loader.reload();
  reloaded.whenComplete(
      (configChanged, error) -> {
        if (error != null) {
          // handle error
        } else if (configChanged) {
          // do something after the config change
        }
      });
}
```

Manual reloading is optional, this can be checked with `supportsReloading()`; the driver's built-in
loader supports it.

#### Derived profiles

Execution profiles are hard-coded in the configuration, and can't be changed at runtime (except
by modifying and reloading the files). What if you want to adjust an option for a single request,
without having a dedicated profile for it?

To allow this, you start from an existing profile in the configuration and build a *derived profile*
that overrides a subset of options:

```java
DriverExecutionProfile defaultProfile = session.getContext().getConfig().getDefaultProfile();
DriverExecutionProfile dynamicProfile =
  defaultProfile.withString(
      DefaultDriverOption.REQUEST_CONSISTENCY, DefaultConsistencyLevel.EACH_QUORUM.name());
SimpleStatement s =
    SimpleStatement.builder("SELECT name FROM user WHERE id = 1")
        .setExecutionProfile(dynamicProfile)
        .build();
session.execute(s);
```

A derived profile keeps a reference to its base profile, and reflects the change if the
configuration gets reloaded.

Do not overuse derived profiles, as they can have an impact on performance: each `withXxx` method
creates a new copy, and propagating the changes from the base profile also has an overhead. We
strongly suggest defining all your profiles ahead of time in the configuration file; at the very
least, try to cache derived profiles if you reuse them multiple times.


### Advanced topics

*Note: all the features described in this section use the driver's internal API, which is subject to
the restrictions explained in [API conventions]*.

#### Changing the config prefix

As mentioned earlier, all configuration options are looked up under the `datastax-java-driver`
prefix. This might be a problem if you have multiple instances of the driver executing in the same
VM, but with different configurations. What you want instead is separate option trees, like this:

```
# application.conf
session1 {
  basic.session-name = "session1"
  advanced.protocol-version = V4
  // etc.
}
session2 {
  basic.session-name = "session2"
  advanced.protocol-version = V3
  // etc.
}
```

To achieve that, first write a method that loads the configuration under your prefix, and uses the
driver's `reference.conf` as a fallback:

```java
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

private static Config loadConfig(String prefix) {
  // Make sure we see the changes when reloading: 
  ConfigFactory.invalidateCaches();

  // Every config file in the classpath, without stripping the prefixes 
  Config root = ConfigFactory.load();

  // The driver's built-in defaults, under the default prefix in reference.conf:
  Config reference = root.getConfig("datastax-java-driver");

  // Everything under your custom prefix in application.conf:
  Config application = root.getConfig(prefix);

  return application.withFallback(reference);
}
```

Next, create a `DriverConfigLoader`. This is the component that abstracts the configuration
implementation to the rest of the driver. Here we use the built-in class, but tell it to load the
Typesafe Config object with the previous method:

```java
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;

DriverConfigLoader session1ConfigLoader =
    new DefaultDriverConfigLoader(
        () -> loadConfig("session1"), DefaultDriverOption.values());
```

Finally, pass the config loader when building the driver:

```java
CqlSession session1 =
    CqlSession.builder()
        .withConfigLoader(session1ConfigLoader)
        .build();
```

#### Loading from a different source

If you don't want to use a config file, you can write custom code to create the Typesafe `Config`
object (refer to the [documentation][Typesafe Config] for more details).

Then reuse the examples from the previous section to merge it with the driver's reference file, and
pass it to the driver. Here's a contrived example that loads the configuration from a string:

```java
String configSource = "protocol.version = V3";
DriverConfigLoader loader =
    new DefaultDriverConfigLoader(
        () -> {
          ConfigFactory.invalidateCaches();
          Config reference = ConfigFactory.load().getConfig("datastax-java-driver");
          Config application = ConfigFactory.parseString(configSource);
          return application.withFallback(reference);
        },
        DefaultDriverOption.values());

CqlSession session = CqlSession.builder().withConfigLoader(loader).build();
```

#### Bypassing Typesafe Config

If Typesafe Config doesn't work for you, it is possible to get rid of it entirely.

Start by excluding Typesafe Config from the list of dependencies required by the driver; if you are 
using Maven, this can be achieved as follows:

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.cassandra</groupId>
        <artifactId>java-driver-core</artifactId>
        <version>...</version>
        <exclusions>
            <exclusion>
                <groupId>com.typesafe</groupId>
                <artifactId>config</artifactId>
            </exclusion>
        </exclusions>
    </dependency>
</dependencies>

```
Next, you will need to provide your own implementations of [DriverConfig] and 
[DriverExecutionProfile]. Then write a [DriverConfigLoader] and pass it to the session at 
initialization, as shown in the previous sections. Study the built-in implementation (package
`com.datastax.oss.driver.internal.core.config.typesafe`) for reference.

Reloading is not mandatory: you can choose not to implement it, and the driver will simply keep
using the initial configuration.

Note that the option getters (`DriverExecutionProfile.getInt` and similar) are invoked very
frequently on the hot code path; if your implementation is slow, consider caching the results
between reloads.

#### Configuration change event

If you're writing your own policies, you might want them to be reactive to configuration changes.
You can register a callback to `ConfigChangeEvent`, which gets emitted any time a manual or periodic
reload detects changes since the last reload:

```java
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.config.ConfigChangeEvent;

InternalDriverContext context = (InternalDriverContext) session.getContext();

Object key =
    eventBus.register(
        ConfigChangeEvent.class, (e) -> {
          System.out.println("The configuration changed");
          // re-read the config option(s) you're interested in, and apply changes if needed
        });

// If your component has a shorter lifecycle than the driver, make sure to unregister when it closes
eventBus.unregister(key, ConfigChangeEvent.class);
```

For example, the driver uses this mechanism internally to resize connection pools if you change the
options in `advanced.connection.pool`.

The event is emitted by the config loader. If you write a custom loader, study the source of
`DefaultDriverConfigLoader` to reproduce the behavior.

#### Policies

The preferred way to instantiate policies (load balancing policy, retry policy, etc.) is via the
configuration:

```
datastax-java-driver {
  basic.load-balancing-policy.class = DefaultLoadBalancingPolicy
  advanced.reconnection-policy {
    class = ExponentialReconnectionPolicy
    base-delay = 1 second
    max-delay = 60 seconds
  }
}
```

When the driver encounters such a declaration, it will load the class and use reflection to invoke a
constructor with the following signature:

* for policies that can be overridden in a profile (load balancing policy, retry policy, speculative
  execution policy):
  
    ```java
    public DefaultLoadBalancingPolicy(DriverContext context, String profileName)
    ```

* for session-wide policies (all the others):

    ```java
    public ExponentialReconnectionPolicy(DriverContext context)
    ```

Where [DriverContext] is the object returned by `session.getContext()`, which allows the policy to
access other driver components (for example the configuration).
 
If you write custom policy implementations, you should follow that same pattern; it provides an
elegant way to switch policies without having to recompile the application (if your policy needs
custom options, see the next section). Study the built-in implementations for reference.

If for some reason you really can't use reflection, there is a way out; subclass
`DefaultDriverContext` and override the corresponding method:

```java
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;

public class MyDriverContext extends DefaultDriverContext {

  public MyDriverContext(DriverConfigLoader configLoader, List<TypeCodec<?>> typeCodecs) {
    super(configLoader, typeCodecs);
  }

  @Override
  protected ReconnectionPolicy buildReconnectionPolicy() {
    return myReconnectionPolicy;
  }
}
```

Then you'll need to pass an instance of this context to `DefaultSession.init`. You can either do so
directly, or subclass `SessionBuilder` and override the `buildContext` method. 

#### Custom options

You can add your own options to the configuration. This is useful for custom components, or even as
a way to associate arbitrary key/value pairs with the session instance.

First, write an enum that implements [DriverOption]:

```java
public enum MyCustomOption implements DriverOption {

  ADMIN_NAME("admin.name"),
  ADMIN_EMAIL("admin.email"),
  AWESOMENESS_FACTOR("awesomeness-factor"),
  ;

  private final String path;

  MyCustomOption(String path) {
    this.path = path;
  }

  @Override
  public String getPath() {
    return path;
  }
}
```

You can now add the options to your configuration:

```
datastax-java-driver {
  admin {
    name = "Bob"
    email = "bob@example.com"
  }
  awesomeness-factor = 11
}
```

And access them from the code:

```java
DriverConfig config = session.getContext().getConfig();
config.getDefaultProfile().getString(MyCustomOption.ADMIN_EMAIL);
config.getDefaultProfile().getInt(MyCustomOption.AWESOMENESS_FACTOR);
```

[DriverConfig]:                           https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/config/DriverConfig.html
[DriverExecutionProfile]:                 https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/config/DriverExecutionProfile.html
[DriverContext]:                          https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/context/DriverContext.html
[DriverOption]:                           https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/config/DriverOption.html
[DefaultDriverOption]:                    https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/config/DefaultDriverOption.html
[DriverConfigLoader]:                     https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/config/DriverConfigLoader.html
[DriverConfigLoader.fromClasspath]:       https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/config/DriverConfigLoader.html#fromClasspath-java.lang.String-
[DriverConfigLoader.fromFile]:            https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/config/DriverConfigLoader.html#fromFile-java.io.File-
[DriverConfigLoader.fromUrl]:             https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/config/DriverConfigLoader.html#fromUrl-java.net.URL-
[DriverConfigLoader.programmaticBuilder]: https://docs.datastax.com/en/drivers/java/4.2/com/datastax/oss/driver/api/core/config/DriverConfigLoader.html#programmaticBuilder--

[Typesafe Config]: https://github.com/typesafehub/config
[config standard behavior]: https://github.com/typesafehub/config#standard-behavior
[reference.conf]: reference/
[HOCON]: https://github.com/typesafehub/config/blob/master/HOCON.md
[API conventions]: ../../api_conventions
