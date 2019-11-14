## DSE-specific features

Some driver features only work with Datastax Enterprise:

* [Graph](graph/);
* [Geospatial types](geotypes/);
* Proxy and GSSAPI authentication (covered in the [Authentication](../authentication/) page).

Note that, if you don't use these features, you might be able to exclude certain dependencies in
order to limit the number of JARs in your classpath. See the
[Integration](../integration/#driver-dependencies) page.