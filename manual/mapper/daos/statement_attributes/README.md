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

## Statement attributes

The [@Delete](../delete/), [@Insert](../insert/), [@Query](../query/), [@Select](../select/) and
[@Update](../update/) annotations allow you to control some aspects of the execution of the
underlying statement, such as the consistency level, timeout, etc.

### As a parameter

If the **last** parameter of any of those methods is a `Function<BoundStatementBuilder,
BoundStatementBuilder>` (or `UnaryOperator<BoundStatementBuilder>`), the mapper will apply that
function to the statement before executing it:

```java
@Dao
public interface ProductDao {
  @Select
  Product findById(
      int productId, Function<BoundStatementBuilder, BoundStatementBuilder> setAttributes);
}

Function<BoundStatementBuilder, BoundStatementBuilder> statementFunction =
    builder -> builder.setConsistencyLevel(DefaultConsistencyLevel.ONE).setPageSize(500);

Product product = dao.findById(1, statementFunction);
``` 

Use this if you need to execute the same DAO methods with different configurations that can change
dynamically.

If you reuse the same set of attributes often, you can store the function as a constant to reduce
allocation costs.

### As an annotation

Attributes can also be provided statically by annotating the method with [@StatementAttributes]:

```java
@Dao
public interface ProductDao {
  @Select
  @StatementAttributes(consistencyLevel = "ONE", pageSize = 500)
  Product findById(int productId);
}
```

It's possible to have both the annotation and the function parameter; in that case, the annotation
will be applied first, and the function second:

```java
@Dao
public interface ProductDao {
  @Select
  @StatementAttributes(consistencyLevel = "ONE", pageSize = 500)
  Product findById(
      int productId, Function<BoundStatementBuilder, BoundStatementBuilder> setAttributes);
}

// Will use CL = QUORUM, page size = 500
Product product =
    dao.findById(1, builder -> builder.setConsistencyLevel(DefaultConsistencyLevel.QUORUM));
```

[@StatementAttributes]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/mapper/annotations/StatementAttributes.html
