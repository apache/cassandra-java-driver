## API conventions

In previous versions, the driver relied solely on Java visibility rules: everything was either
private or part of the public API. This made it hard to cleanly organize the code, and things ended
up all together in one monolithic package; it also created a dilemma between providing useful hooks
for advanced users, or keeping them hidden to limit the API surface.

Starting with 4.0, we adopt a package naming convention to address those issues:

* Everything under `com.datastax.oss.driver.api` is part of the "official" public API of the driver,
  that can be used by regular client applications to execute queries. It follows
  [semantic versioning] and binary compatibility is guaranteed across minor and patch versions.
* Everything under `com.datastax.oss.driver.internal` is the "internal" API, primarily used by 
  driver components to communicate with each other. It also exposes hooks for expert tweaking or
  framework implementors. We'll do our best to also keep that API stable, but full compatibility is
  not strictly guaranteed.


[semantic versioning]: http://semver.org/