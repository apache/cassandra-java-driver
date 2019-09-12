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
