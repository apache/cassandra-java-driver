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

## Driver context

The context holds the driver's internal components. It is exposed in the public API as
`DriverContext`, accessible via `session.getContext()`. Internally, the child interface
`InternalDriverContext` adds access to more components; finally, `DefaultDriverContext` is the
implementing class.

### The dependency graph

Most components initialize lazily (see `LazyReference`). They also reference each other, typically
by taking the context as a constructor argument, and extracting the dependencies they need:

```java
public DefaultTopologyMonitor(InternalDriverContext context) {
  ...
  this.controlConnection = context.getControlConnection();
}
```

This avoids having to handle the initialization order ourselves. It is also convenient for unit
tests: you can run a component in isolation by mocking all of its dependencies. 

Obviously, things won't go well if there are cyclic dependencies; if you make changes to the
context, you can set a system property to check the dependency graph, it will throw if a cycle is 
detected (see `CycleDetector`):

```
-Dcom.datastax.oss.driver.DETECT_CYCLES=true
```

This is disabled by default, because we don't expect it to be very useful outside of testing cycles.

### Why not use a DI framework?

As should be clear by now, the context is a poor man's Dependency Injection framework. We
deliberately avoided third-party solutions:
 
* to keep things as simple as possible,
* to avoid an additional library dependency,
* to allow end users to access components and add their own (which wouldn't work well with
  compile-time approaches like Dagger).

### Overriding a context component

The basic approach to plug in a custom internal component is to subclass the context.

For example, let's say you wrote a custom `NettyOptions` implementation (maybe you have multiple
sessions, and want to reuse the event loop groups instead of recreating them every time):

```java
public class CustomNettyOptions implements NettyOptions {
  ...
} 
```

In the default context, here's how the component is managed:
 
```java
public class DefaultDriverContext {
  
  // some content omitted for brevity
  
  private final LazyReference<NettyOptions> nettyOptionsRef =
      new LazyReference<>("nettyOptions", this::buildNettyOptions, cycleDetector);

  protected NettyOptions buildNettyOptions() {
    return new DefaultNettyOptions(this);
  }

  @NonNull
  @Override
  public NettyOptions getNettyOptions() {
    return nettyOptionsRef.get();
  }
}
```
 
To switch in your implementation, you only need to override the build method:

```java
public class CustomContext extends DefaultDriverContext {

  public CustomContext(DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
    super(configLoader, programmaticArguments);
  }

  @Override
  protected NettyOptions buildNettyOptions() {
    return new CustomNettyOptions(this);
  }
}
```

Then you need a way to create a session that uses your custom context. The session builder is
extensible as well:

```java
public class CustomBuilder extends SessionBuilder<CustomBuilder, CqlSession> {

  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
    return new CustomContext(configLoader, programmaticArguments);
  }

  @Override
  protected CqlSession wrap(@NonNull CqlSession defaultSession) {
    // Nothing to do here, nothing changes on the session type
    return defaultSession;
  }
}
```

Finally, you can use your custom builder like the regular `CqlSession.builder()`, it inherits all
the methods:

```java
CqlSession session = new CustomBuilder()
    .addContactPoint(new InetSocketAddress("1.2.3.4", 9042))
    .withLocalDatacenter("datacenter1")
    .build();
```
