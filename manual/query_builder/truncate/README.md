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

[QueryBuilder]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/querybuilder/QueryBuilder.html
