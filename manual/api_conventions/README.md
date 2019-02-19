## API conventions

In previous versions, the driver relied solely on Java visibility rules: everything was either
private or part of the public API. This made it hard to cleanly organize the code, and things ended
up all together in one monolithic package; it also created a dilemma between providing useful hooks
for advanced users, or keeping them hidden to limit the API surface.

Starting with 4.0, we adopt a package naming convention to address those issues:

* Everything under `com.datastax.oss.driver.api` is part of the "official" public API of the driver,
  intended for regular client applications to execute queries. It follows [semantic versioning]:
  binary compatibility is guaranteed across minor and patch versions.
  
* Everything under `com.datastax.oss.driver.internal` is the "internal" API, intended primarily for
  internal communication between driver components, and secondarily for advanced customization. If
  you use it from your code, the rules are:
    1. with great power comes great responsibility: this stuff is more involved, and has the
       potential to break the driver. You should probably have some familiarity with the source
       code.
    2. backward compatibility is "best-effort" only: we'll try to preserve it as much as possible,
       but it's not formally guaranteed.

The public API never exposes internal types (this is enforced automatically by our build). You'll
generally have to go through an explicit cast:

```java
import com.datastax.oss.driver.api.core.context.DriverContext;

import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;

// Public API:
DriverContext context = session.getContext();

// Switch to the internal API to force a node down:
InternalDriverContext internalContext = (InternalDriverContext) context;
InetSocketAddress address = new InetSocketAddress("127.0.0.1", 9042);
internalContext.getEventBus().fire(TopologyEvent.forceDown(address));
```

So the risk of unintentionally using the internal API is very low. To double-check, you can always
grep `import com.datastax.oss.driver.internal` in your source files.

[semantic versioning]: http://semver.org/