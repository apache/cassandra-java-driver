## TRUNCATE

To create a TRUNCATE query, use one of the `truncate` methods in [QueryBuilder]. There are several
variants depending on whether your table name is qualified, and whether you use
[identifiers](../../case_sensitivity/) or raw strings:

```java
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

Truncate truncate = truncate("ks", "mytable");
// TRUNCATE ks.mytable

Truncate truncate2 = truncate(CqlIdentifier.fromCql("mytable"));
// TRUNCATE mytable
```

Note that, at this stage, the query is ready to build. After creating a TRUNCATE query it does not
take any values.

[QueryBuilder]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/querybuilder/QueryBuilder.html
