## Manual

Driver modules:

* [Core](core/): the main entry point, deals with connectivity and query execution.
* [Query builder](query_builder/): a fluent API to create CQL queries programmatically.
* [Mapper](mapper/): generates the boilerplate to execute queries and convert the results into
  application-level objects.
* [Developer docs](developer/): explains the codebase and internal extension points for advanced
  customization.

Common topics:

* [API conventions](api_conventions/)
* [Case sensitivity](case_sensitivity/)
* [OSGi](osgi/)
* [Cloud](cloud/)