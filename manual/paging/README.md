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

If you use paging with the async API, you'll also want to use those
methods to avoid triggering synchronous fetches unintentionally; see
[async paging](../async/#async-paging).


### Saving and reusing the paging state

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

[result_set]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/ResultSet.html
[paging_state]:https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/PagingState.html


Due to internal implementation details, `PagingState` instances are not
portable across [native protocol](../native_protocol/) versions. This
could become a problem in the following scenario:

* you're using the driver 2.0.x and Cassandra 2.0.x, and therefore
  native protocol v2;
* a user bookmarks a link to your web service that contains a serialized
  paging state;
* you upgrade your server stack to use the driver 2.1.x and Cassandra
  2.1.x, so you're now using protocol v3;
* the user tries to reload their bookmark, but the paging state was
  serialized with protocol v2, so trying to reuse it will fail.

If this is not acceptable for you, you might want to consider the unsafe
API described in the next section.

Note: because of [CASSANDRA-10880], paging states are also incompatible
between Cassandra 2.2 and 3.0, even if they're both using native protocol v4.
This will manifest as the following error:

```
com.datastax.driver.core.exceptions.ProtocolError: An unexpected protocol error occurred on host xxx.
This is a bug in this library, please report: Invalid value for the paging state
```

The [Cassandra documentation](https://github.com/apache/cassandra/blob/cassandra-3.0/NEWS.txt#L334-L336)
recommends staying on protocol v3 during an upgrade between these two versions:

```
Clients must use the native protocol version 3 when upgrading from 2.2.X as
the native protocol version 4 is not compatible between 2.2.X and 3.Y. See
https://www.mail-archive.com/user@cassandra.apache.org/msg45381.html for details.
```

[CASSANDRA-10880]: https://issues.apache.org/jira/browse/CASSANDRA-10880

#### Unsafe API

As an alternative to the standard API, there are two methods that
manipulate a raw `byte[]` instead of a `PagingState` object:

* [ExecutionInfo#getPagingStateUnsafe()][gpsu]
* [Statement#setPagingStateUnsafe(byte[])][spsu]

These low-level methods perform no validation on their arguments;
therefore nothing protects you from reusing a paging state that was
generated from a different statement, or altered in any way. This could
result in sending a corrupt paging state to Cassandra, with
unpredictable consequences (ranging from wrong results to a query
failure).

There are two situations where you might want to use the unsafe API:

* you never expose the paging state to end users and you are confident
  that it won't get altered;
* you want portability across protocol versions and/or you prefer
  implementing your own validation logic (for example, signing the raw
  state with a private key).

[gpsu]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/ExecutionInfo.html#getPagingStateUnsafe--
[spsu]: https://docs.datastax.com/en/drivers/java/3.10/com/datastax/driver/core/Statement.html#setPagingStateUnsafe-byte:A-

### Offset queries

Saving the paging state works well when you only let the user move from
one page to the next. But it doesn't allow random jumps (like "go
directly to page 10"), because you can't fetch a page unless you have
the paging state of the previous one. Such a feature would require
*offset queries*, but they are not natively supported by Cassandra (see
[CASSANDRA-6511](https://issues.apache.org/jira/browse/CASSANDRA-6511)).
The rationale is that offset queries are inherently inefficient (the
performance will always be linear in the number of rows skipped), so the
Cassandra team doesn't want to encourage their use.

If you really want offset queries, you can emulate them client-side.
You'll still get linear performance, but maybe that's acceptable for
your use case. For example, if each page holds 10 rows and you show at
most 20 pages, this means you'll fetch at most 190 extra rows, which
doesn't sound like a big deal.

For example, if the page size is 10, the fetch size is 50, and the user
asks for page 12 (rows 110 to 119):

* execute the statement a first time (the result set contains rows 0 to
  49, but you're not going to use them, only the paging state);
* execute the statement a second time with the paging state from the
  first query;
* execute the statement a third time with the paging state from the
  second query. The result set now contains rows 100 to 149;
* skip the first 10 rows of the iterator. Read the next 10 rows and
  discard the remaining ones.

You'll want to experiment with the fetch size to find the best balance:
too small means many background queries; too big means bigger messages
and too many unneeded rows returned (we picked 50 above for the sake of
example, but it's probably too small -- the default is 5000).

Again, offset queries are inefficient by nature. Emulating them
client-side is a compromise when you think you can get away with the
performance hit. We recommend that you:

* test your code at scale with the expected query patterns, to make sure
  that your assumptions are correct;
* set a hard limit on the highest possible page number, to prevent
  malicious users from triggering queries that would skip a huge amount
  of rows.
