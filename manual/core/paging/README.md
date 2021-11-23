## Paging

### Quick overview

How the server splits large result sets into multiple network responses.

* `basic.request.page-size` in the configuration.
* transparent in the synchronous API (`session.execute`): the driver fetches new pages in the
  background as you iterate.
* explicit in the asynchronous API (`session.executeAsync`):
  [AsyncResultSet.hasMorePages()][AsyncPagingIterable.hasMorePages] and
  [AsyncResultSet.fetchNextPage()][AsyncPagingIterable.fetchNextPage].
* paging state: record the current position and reuse it later (forward only).
* offset queries: emulated client-side with [OffsetPager] \(**this comes with important performance
  trade-offs, make sure you read and understand the full documentation below**).

-----

When a query returns many rows, it would be inefficient to return them as a single response message.
Instead, the driver breaks the results into *pages* which get returned as they are needed.


### Setting the page size

The page size specifies how many rows the server will return in each network frame. You can set it
in the configuration:

```
datastax-java-driver.basic.request.page-size = 5000
```

It can be changed at runtime (the new value will be used for requests issued after the change). If
you have categories of queries that require different page sizes, use
[configuration profiles](../configuration#profiles).

Note that the page size is merely a hint; the server will not always return the exact number of
rows, it might decide to return slightly more or less.


### Synchronous paging

The fetch size limits the number of results that are returned in one page; if you iterate past that,
the driver uses background queries to fetch subsequent pages. Here's an example with a fetch size of
20:

```java
ResultSet rs = session.execute("SELECT * FROM my_table WHERE k = 1");
for (Row row : rs) {
  // process the row
}
```

```ditaa
    client         Session                          Cassandra
    --+--------------+---------------------------------+-----
      |execute(query)|                                 |
      |------------->|                                 |
      |              | query rows 1 to 20              |
      |              |-------------------------------->|
      |              |                                 |
      |              |create                           |
      |              |------>ResultSet                 |
      |                          |                     |
+-----+--------+-----------------+-+                   |
|For i in 1..20|                 | |                   |
+--------------+                 | |                   |
|     |        get next row      | |                   |
|     |------------------------->| |                   |
|     |          row i           | |                   |
|     |<-------------------------| |                   |
|     |                          | |                   |
+-----+--------------------------+-+                   |
      |                          |                     |
      |                          |                     |
      |        get next row      |                     |
      |------------------------->|                     |
      |                          | query rows 21 to 40 |
      |                          |-------------------->|
      |          row 21          |                     |
      |<------------------------ |                     |
```


### Asynchronous paging

In previous versions of the driver, the synchronous and asynchronous APIs returned the same
`ResultSet` type. This made asynchronous paging very tricky, because it was very easy to
accidentally trigger background synchronous queries (which would defeat the whole purpose of async,
and potentially introduce deadlocks).

To avoid this problem, the driver's asynchronous API now returns a dedicated [AsyncResultSet];
iteration only yields the current page, and the next page must be fetched explicitly. To iterate a
result set in a fully asynchronous manner, you need to compose page futures using the methods of
[CompletionStage]. Here's an example that prints each row on the command line:

```java
CompletionStage<AsyncResultSet> resultSetFuture =
    session.executeAsync("SELECT * FROM myTable WHERE id = 1");
// The returned stage will complete once all the rows have been printed:
CompletionStage<Void> printRowsFuture = resultSetFuture.thenCompose(this::printRows);

private CompletionStage<Void> printRows(AsyncResultSet resultSet) {
  for (Row row : resultSet.currentPage()) {
    System.out.println(row.getFormattedContents());
  }
  if (resultSet.hasMorePages()) {
    return resultSet.fetchNextPage().thenCompose(this::printRows);
  } else {
    return CompletableFuture.completedFuture(null);
  }
}
```

If you need to propagate state throughout the iteration, add parameters to the callback. Here's an
example that counts the number of rows (obviously this is contrived, you would use `SELECT COUNT(*)`
instead of doing this client-side, but it illustrates the basic principle):

```java
CompletionStage<AsyncResultSet> resultSetFuture =
    session.executeAsync("SELECT * FROM myTable WHERE id = 1");
CompletionStage<Integer> countFuture = resultSetFuture.thenCompose(rs -> countRows(rs, 0));

private CompletionStage<Integer> countRows(AsyncResultSet resultSet, int previousPagesCount) {
  int count = previousPagesCount;
  for (Row row : resultSet.currentPage()) {
    count += 1;
  }
  if (resultSet.hasMorePages()) {
    int finalCount = count; // need a final variable to use in the lambda below
    return resultSet.fetchNextPage().thenCompose(rs -> countRows(rs, finalCount));
  } else {
    return CompletableFuture.completedFuture(count);
  }
}
```

See [Asynchronous programming](../async/) for more tips about the async API.

### Saving and reusing the paging state

Sometimes it is convenient to interrupt paging and resume it later. For example, this could be
used for a stateless web service that displays a list of results with a link to the next page. When
the user clicks that link, we want to run the exact same query, except that the iteration should
start where we stopped the last time.

The driver exposes a *paging state* for that:

```java
ResultSet rs = session.execute("your query");
ByteBuffer pagingState = rs.getExecutionInfo().getPagingState();

// Finish processing the current page
while (rs.getAvailableWithoutFetching() > 0) {
  Row row = rs.one();
  // process the row
}

// Later:
SimpleStatement statement =
    SimpleStatement.builder("your query").setPagingState(pagingState).build();
session.execute(statement);
```

Note the loop to finish the current page after we extract the state. The new statement will start at
the beginning of the next page, so we want to make sure we don't leave a gap of unprocessed rows.  

The paging state can only be reused with the exact same statement (same query string, same
parameters). It is an opaque value that is only meant to be collected, stored and re-used. If you
try to modify its contents or reuse it with a different statement, the results are unpredictable.

If you want additional safety, the driver also provides a "safe" wrapper around the raw value:
[PagingState]. 

```java
PagingState pagingState = rs.getExecutionInfo().getSafePagingState();
```

It works in the exact same manner, except that it will throw an `IllegalStateException` if you try
to reinject it in the wrong statement. This allows you to detect the error early, without a
roundtrip to the server.

Note that, if you use a simple statement and one of the bound values requires a [custom
codec](../custom_codecs), you have to provide a reference to the session when reinjecting the paging
state:

```java
CustomType value = ...
SimpleStatement statement = SimpleStatement.newInstance("query", value);
// session required here, otherwise you will get a CodecNotFoundException:
statement = statement.setPagingState(pagingState, session);
```

This is a small corner case because checking the state requires encoding the values, and a simple
statement doesn't have a reference to the codec registry. If you don't use custom codecs, or if the
statement is a bound statement, you can use the regular `setPagingState(pagingState)`.

### Offset queries

Saving the paging state works well when you only let the user move from one page to the next. But in
most Web UIs and REST services, you need paginated results with random access, for example: "given a
page size of 20 elements, fetch page 5".

Cassandra does not support this natively (see
[CASSANDRA-6511](https://issues.apache.org/jira/browse/CASSANDRA-6511)), because such queries are
inherently linear: the database would have to restart from the beginning every time, and skip
unwanted rows until it reaches the desired offset.

However, random pagination is a real need for many applications, and linear performance can be a
reasonable trade-off if the cardinality stays low. The driver provides a utility to emulate offset
queries on the client side: [OffsetPager].

#### Performance considerations

For each page that you want to retrieve:

* you need to re-execute the query, in order to start with a fresh result set;
* you then pass the result to `OffsetPager`, which starts iterating from the beginning, and skips
  rows until it reaches the desired offset.

```java
String query = "SELECT ...";
OffsetPager pager = new OffsetPager(20);

// Get page 2: start from a fresh result set, throw away rows 1-20, then return rows 21-40
ResultSet rs = session.execute(query);
OffsetPager.Page<Row> page2 = pager.getPage(rs, 2);

// Get page 5: start from a fresh result set, throw away rows 1-80, then return rows 81-100
rs = session.execute(query);
OffsetPager.Page<Row> page5 = pager.getPage(rs, 5);
```

Note that `getPage` can also process the entity iterables returned by the [mapper](../../mapper/).

#### Establishing application-level guardrails

Linear performance should be fine for the values typically encountered in real-world applications:
for example, if the page size is 25 and users never go past page 10, the worst case is only 250
rows, which is a very small result set. However, we strongly recommend that you implement hard
limits in your application code: if the page number is exposed to the user (for example if it is
passed as a URL parameter), make sure it is properly validated and enforce a maximum, so that an
attacker can't inject a large value that could potentially fetch millions of rows.

#### Relation with protocol-level paging

Offset paging has no direct relation to `basic.request.page-size`. Protocol-level paging happens
under the hood, and is completely transparent for offset paging: `OffsetPager` will work the same no
matter how many network roundtrips were needed to fetch the result. You don't need to set the
protocol page size and the logical page size to the same value.

-----

The [driver examples] include two complete web service implementations demonstrating forward-only
and offset paging.

[ResultSet]:         https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/ResultSet.html
[AsyncResultSet]:    https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/AsyncResultSet.html
[AsyncPagingIterable.hasMorePages]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/AsyncPagingIterable.html#hasMorePages--
[AsyncPagingIterable.fetchNextPage]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/AsyncPagingIterable.html#fetchNextPage--
[OffsetPager]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/paging/OffsetPager.html
[PagingState]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/cql/PagingState.html

[CompletionStage]: https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletionStage.html

[driver examples]: https://github.com/datastax/java-driver/tree/4.x/examples/src/main/java/com/datastax/oss/driver/examples/paging
