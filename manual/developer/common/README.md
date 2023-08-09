## Common infrastructure

This covers utilities or concept that are shared throughout the codebase:

* the [context](context/) is what glues everything together, and your primary entry point to extend
  the driver.
* we explain the two major approaches to deal with [concurrency](concurrency/) in the driver.
* the [event bus](event_bus/) is used to decouple some of the internal components through
  asynchronous messaging.
