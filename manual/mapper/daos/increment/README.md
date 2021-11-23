## Increment methods

Annotate a DAO method with [@Increment] to generate a query that updates a counter table that is
mapped to an entity:

```java
// CREATE TABLE votes(article_id int PRIMARY KEY, up_votes counter, down_votes counter);

@Entity
public class Votes {
  @PartitionKey private int articleId;
  private long upVotes;
  private long downVotes;
  ... // constructor(s), getters and setters, etc.
}

@Dao
public interface VotesDao {
  @Increment(entityClass = Votes.class)
  void incrementUpVotes(int articleId, long upVotes);

  @Increment(entityClass = Votes.class)
  void incrementDownVotes(int articleId, long downVotes);
  
  @Select
  Votes findById(int articleId);
}
```

### Parameters

The entity class must be specified with `entityClass` in the annotation.

The method's parameters must start with the [full primary key](../../entities/#primary-key-columns),
in the exact order (as defined by the [@PartitionKey] and [@ClusteringColumn] annotations in the
entity class). The parameter names don't necessarily need to match the names of the columns, but the
types must match. Unlike other methods like [@Select](../select/) or [@Delete](../delete/), counter
updates cannot operate on a whole partition, they need to target exactly one row; so all the
partition key and clustering columns must be specified.

Then must follow one or more parameters representing counter increments. Their type must be
`long` or `java.lang.Long`. The name of the parameter must match the name of the entity
property that maps to the counter (that is, the name of the getter without "get" and
decapitalized). Alternatively, you may annotate a parameter with [@CqlName] to specify the
raw column name directly; in that case, the name of the parameter does not matter:

```java
@Increment(entityClass = Votes.class)
void incrementUpVotes(int articleId, @CqlName("up_votes") long foobar);
```

When you invoke the method, each parameter value is interpreted as a **delta** that will be applied
to the counter. In other words, if you pass 1, the counter will be incremented by 1. Negative values
are allowed. If you are using Cassandra 2.2 or above, you can use `Long` and pass `null` for some of
the parameters, they will be ignored (following [NullSavingStrategy#DO_NOT_SET](../null_saving/)
semantics). If you are using Cassandra 2.1, `null` values will trigger a runtime error.

A `Function<BoundStatementBuilder, BoundStatementBuilder>` or `UnaryOperator<BoundStatementBuilder>`
can be added as the **last** parameter. It will be applied to the statement before execution. This
allows you to customize certain aspects of the request (page size, timeout, etc) at runtime. See
[statement attributes](../statement_attributes/).

### Return type

The method can return `void`, a void [CompletionStage] or [CompletableFuture], or a
[ReactiveResultSet].

### Target keyspace and table

If a keyspace was specified [when creating the DAO](../../mapper/#dao-factory-methods), then the
generated query targets that keyspace. Otherwise, it doesn't specify a keyspace, and will only work
if the mapper was built from a session that has a [default keyspace] set.

If a table was specified when creating the DAO, then the generated query targets that table.
Otherwise, it uses the default table name for the entity (which is determined by the name of the
entity class and the naming convention).

[@Increment]:        https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/Increment.html
[ReactiveResultSet]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/dse/driver/api/core/cql/reactive/ReactiveResultSet.html
[default keyspace]:  https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withKeyspace-com.datastax.oss.driver.api.core.CqlIdentifier-
[@ClusteringColumn]:      https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/ClusteringColumn.html
[@PartitionKey]:          https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/PartitionKey.html
[@CqlName]:             https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/annotations/CqlName.html

[CompletionStage]:   https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletionStage.html
[CompletableFuture]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html
