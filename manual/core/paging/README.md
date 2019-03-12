## Paging

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

By default, the background fetch happens at the last moment, when there are no more "local" rows
available. If you need finer control, [ResultSet] provides the following methods:

* `getAvailableWithoutFetching()` and `isFullyFetched()` to check the current state;
* `fetchMoreResults()` to force a page fetch.

Here's how you could use these methods to pre-fetch the next page in advance, in order to avoid the
performance hit at the end of each page:

```java
ResultSet rs = session.execute("your query");
for (Row row : rs) {
  // Fetch when there's only half a page left:
  if (rs.getAvailableWithoutFetching() == 10 && !rs.isFullyFetched()) {
    rs.fetchMoreResults(); // this is asynchronous
  }
  // Process the row...
}
```


### Asynchronous paging

In previous versions of the driver, the synchronous and asynchronous APIs returned the same
`ResultSet` type. This made asynchronous paging very tricky, because it was very easy to
accidentally trigger background synchronous queries (which would defeat the whole purpose of async,
or potentially introduce deadlocks).

To avoid this problem, the driver's asynchronous API now returns a dedicated [AsyncResultSet];
iteration only yields the current page, and the next page must be explicitly fetched. Here's how
that translates to our example:

```java
CompletionStage<? extends AsyncResultSet> futureRs =
    session.executeAsync("SELECT * FROM myTable WHERE id = 1");
futureRs.whenComplete(this::processRows);

void processRows(AsyncResultSet rs, Throwable error) {
  if (error != null) {
    // The query failed, process the error
  } else {
    for (Row row : rs.currentPage()) {
      // Process the row...
    }
    if (rs.hasMorePages()) {
      rs.fetchNextPage().whenComplete(this::processRows);
    }
  }
}
```


### Saving and reusing the paging state

Sometimes it is convenient to interrupt paging and resume it later. For example, this could be
used for a stateless web service that displays a list of results with a link to the next page. When
the user clicks that link, we want to run the exact same query, except that the iteration should
start where we stopped the last time.

The driver exposes a *paging state* for that:

```java
ResultSet rs = session.execute("your query");
ByteBuffer pagingState = rs.getExecutionInfo().getPagingState();

// Later:
SimpleStatement statement =
    SimpleStatement.builder("your query").setPagingState(pagingState).build();
session.execute(statement);
```

The paging state can only be reused with the exact same statement (same query string, same
parameters). It is an opaque value that is only meant to be collected, stored and re-used. If you
try to modify its contents or reuse it with a different statement, the results are unpredictable.


### Offset queries

Saving the paging state works well when you only let the user move from one page to the next. But it
doesn't allow random jumps (like "go directly to page 10"), because you can't fetch a page unless
you have the paging state of the previous one. Such a feature would require *offset queries*, but
they are not natively supported by Cassandra (see
[CASSANDRA-6511](https://issues.apache.org/jira/browse/CASSANDRA-6511)). The rationale is that
offset queries are inherently inefficient (the performance will always be linear in the number of
rows skipped), so the Cassandra team doesn't want to encourage their use.

If you really want offset queries, you can emulate them client-side. You'll still get linear
performance, but maybe that's acceptable for your use case. For example, if each page holds 10 rows
and you show at most 20 pages, it means that in the worst case you'll fetch 190 extra rows, which is
probably not a big performance hit.

For example, if the page size is 10, the fetch size is 50, and the user asks for page 12 (rows 110
to 119):

* execute the statement a first time (the result set contains rows 0 to 49, but you're not going to
  use them, only the paging state);
* execute the statement a second time with the paging state from the first query;
* execute the statement a third time with the paging state from the second query. The result set now
  contains rows 100 to 149;
* skip the first 10 rows of the iterator. Read the next 10 rows and discard the remaining ones.

You'll want to experiment with the fetch size to find the best balance: too small means many
background queries; too big means bigger messages and too many unneeded rows returned (we picked 50
above for the sake of example, but it's probably too small -- the default is 5000).

Again, offset queries are inefficient by nature. Emulating them client-side is a compromise when you
think you can get away with the performance hit. We recommend that you:

* test your code at scale with the expected query patterns, to make sure that your assumptions are
  correct;
* set a hard limit on the highest possible page number, to prevent malicious clients from triggering
  queries that would skip a huge amount of rows.

[ResultSet]:         http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/ResultSet.html
[AsyncResultSet]:    http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/cql/AsyncResultSet.html
