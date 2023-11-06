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

## Event bus

`EventBus` is a bare-bones messaging mechanism, to decouple components from each other, and
broadcast messages to more than one component at a time.

Producers fire events on the bus; consumers register to be notified for a particular event class.
For example, `DefaultDriverConfigLoader` reloads the config periodically, and fires an event if it
detects a change:

```java
boolean changed = driverConfig.reload(configSupplier.get());
if (changed) {
  LOG.info("[{}] Detected a configuration change", logPrefix);
  eventBus.fire(ConfigChangeEvent.INSTANCE);
}
```

This allows other components, such as `ChannelPool`, to react to config changes dynamically: 

```java
eventBus.register(
    ConfigChangeEvent.class, RunOrSchedule.on(adminExecutor, this::onConfigChanged));

private void onConfigChanged(ConfigChangeEvent event) {
  assert adminExecutor.inEventLoop();
  // resize re-reads the pool size from the configuration and does nothing if it hasn't changed,
  // which is exactly what we want.
  resize(distance);
}
```

For simplicity, the implementation makes the following assumptions:

* events are propagated synchronously: if their processing needs to be delayed or rescheduled to
  another thread, it's the consumer's responsibility (see how the pool uses `RunOrSchedule` in the
  example above);
* callbacks are not polymorphic: you must register for the exact event class. For example, if you
  have `eventBus.register(B.class, callback)` and fire an `A extends B`, the callback won't catch
  it (internally, this allows direct lookups instead of traversing all registered callbacks with an
  `instanceof` check).

Those choices have been good enough for the needs of the driver. That's why we use a custom
implementation rather than something more sophisticated like Guava's event bus.
