## Paging

When a query returns many rows, it would be inefficient to return them
as a single response message. Instead, the driver breaks the results
into *pages* which get returned as they are needed.

### Setting the fetch size

The *fetch size* specifies how many rows will be returned at once by
Cassandra (in other words, it's the size of each page).

You can set a default fetch size globally for a `Cluster` instance:

```java
// At initialization:
Cluster cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .withQueryOptions(new QueryOptions().setFetchSize(2000))
    .build();

// Or at runtime:
cluster.getConfiguration().getQueryOptions().setFetchSize(2000);
```

The fetch size can also be set on a statement:

```java
Statement statement = new SimpleStatement("your query");
statement.setFetchSize(2000);
```

If the fetch size is set on a statement, it will take precedence;
otherwise, the cluster-wide value (which defaults to 5000) will be used.

Note that setting a fetch size doesn't mean that Cassandra will always
return the exact number of rows, it is possible that it returns slightly
more or less results.

### Result set iteration

The fetch size limits the number of results that are returned in one
page; if you iterate past that, the driver will run background queries
to fetch subsequent pages. Here's an example with a fetch size of 20:

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

By default, the background fetch happens at the last moment, when there
are no more "local" rows available. If you need finer control, the
[ResultSet][result_set] interface provides the following methods:

* `getAvailableWithoutFetching()` and `isFullyFetched()` to check the
  current state;
* `fetchMoreResults()` to force a page fetch.

Here's how you could use these methods to pre-fetch the next page in
advance, in order to avoid the performance hit at the end of each page:

```java
ResultSet rs = session.execute("your query");
for (Row row : rs) {
    if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched())
        rs.fetchMoreResults(); // this is asynchronous
    // Process the row ...
    System.out.println(row);
}
```

### Manual paging

Sometimes it is convenient to save the paging state in order to restore
it later. For example, consider a stateless web service that displays a
list of results with a link to the next page. When the user clicks that
link, we want to run the exact same query, except that the iteration
should start where we stopped on the previous page.

To do so, the driver exposes a [PagingState][paging_state] object that represents
where we were in the result set when the last page was fetched:

```java
ResultSet resultSet = session.execute("your query");
// iterate the result set...
PagingState pagingState = resultSet.getExecutionInfo().getPagingState();
```

This object can be serialized to a `String` or a byte array:

```java
String string = pagingState.toString();
byte[] bytes = pagingState.toBytes();
```

This serialized form can be saved in some form of persistent storage to
be reused later. In our web service example, we would probably save the
string version as a query parameter in the URL to the next page
(`http://myservice.com/results?page=<...>`). When that value is
retrieved later, we can deserialize it and reinject it in a statement:

```java
PagingState pagingState = PagingState.fromString(string);
Statement st = new SimpleStatement("your query");
st.setPagingState(pagingState);
ResultSet rs = session.execute(st);
```

Note that the paging state can only be reused with the exact same
statement (same query string, same parameters). Also, it is an opaque
value that is only meant to be collected, stored an re-used. If you try
to modify its contents or reuse it with a different statement, the
driver will raise an error.

Putting it all together, here's a more comprehensive example
implementation for our web service:

```java
final int RESULTS_PER_PAGE = 100;

Statement st = new SimpleStatement("your query");
st.setFetchSize(RESULTS_PER_PAGE);

String requestedPage = extractPagingStateStringFromURL();
// This will be absent for the first page
if (requestedPage != null) {
    st.setPagingState(
        PagingState.fromString(requestedPage));
}

ResultSet rs = session.execute(st);
PagingState nextPage = rs.getExecutionInfo().getPagingState();

// Note that we don't rely on RESULTS_PER_PAGE, since Cassandra might
// have not respected it, or we might be at the end of the result set
int remaining = rs.getAvailableWithoutFetching();
for (Row row : rs) {
    renderInResponse(row);
    if (--remaining == 0) {
        break;
    }
}

// This will be null if there are no more pages
if (nextPage != null) {
    renderNextPageLink(nextPage.toString());
}
```

[result_set]:http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/ResultSet.html
[paging_state]:http://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/PagingState.html