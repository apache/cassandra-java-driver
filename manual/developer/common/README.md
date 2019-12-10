## Common infrastructure

This covers utilities or concept that are shared throughout the codebase.

The [context](context/) is what glues everything together, and your primary entry point to extend
the driver.

We then explain the two major approaches to deal with [concurrency](concurrency/) in the driver.

Lastly, we briefly touch on the [event bus](event_bus/), which is used to decouple some of the
internal components through asynchronous messaging.