# Queries and Results
There are many resources such as [this post][planetCCqlLink] or [this post][dsBlogCqlLink] to learn
how to transform previous Thrift operations to CQL queries.
 
The *Java driver* executes CQL queries through the `Session`. 
The queries can either be simple *CQL* Strings or represented in the form of 
`Statement`s. The driver offers 4 kinds of statements, `SimpleStatement`, 
`Prepared/BoundStatement`, `BuiltStatement`, and `BatchStatement`. All necessary
information can be [found here](../../../manual/statements/) about the nature of the different
`Statement`s.

As explained in [the running section](../../../manual/#running-queries),
results of a *CQL* query will be in the form of *Rows* from *Tables*, composed 
of fixed set of columns, each with a type and a name. The driver exposes the 
set of *Rows* returned from a query as a ResultSet, thus containing *Rows* on 
which `getXXX()` can be called. Here are simple examples of translation from 
*Astyanax* to *Java driver* in querying and retrieving query results.

## Single column

*Astyanax*:

```java
ColumnFamily<String, String> CF_STANDARD1 = new ColumnFamily<String, String>("cf1", StringSerializer.get(), StringSerializer.get(). StringSerializer.get());

Column<String> result = keyspace.prepareQuery(CF_STANDARD1)
    .getKey("1")
    .getColumn("3")
    .execute().getResult();
String value = result.getStringValue();
```

*Java driver*:

```
Row row = session.execute("SELECT value FROM table1 WHERE key = '1' AND column1 = '3'").one();
String value = row.getString("value");
```

## All columns

*Astyanax*: 

```java
ColumnList<String> columns;
int pagesize = 10;
RowQuery<String, String> query = keyspace
       .prepareQuery(CF_STANDARD1)
       .getKey("1")
       .autoPaginate(true)
       .withColumnRange(new RangeBuilder().setLimit(pagesize).build());

while (!(columns = query.execute().getResult()).isEmpty()) {
   for (Column<String> c : columns) {
       String value = c.getStringValue();
   }
}
```

*Java driver*:

```java
ResultSet rs = session.execute("SELECT value FROM table1 WHERE key = '1'");
for (Row row : rs) {
   String value = row.getString("value");
}
```

## Column range

*Astyanax*:

```java
ColumnList<String> result;
result = keyspace.prepareQuery(CF_STANDARD1)
       .getKey("1")
       .withColumnRange(new RangeBuilder().setStart("3").setEnd("5").setMaxSize(100).build())
       .execute().getResult();

Iterator<Column<String>> it = result.iterator();
while (it.hasNext()) {
   Column<String> col = it.next();
   String value = col.getStringValue();
}
```

*Java driver*:

```java
ResultSet rs = session.execute("SELECT value FROM table1 WHERE key = '1'" +
       " AND column1 > '3'" +
       " AND column1 < '5'");
for (Row row : rs) {
   String value = row.getString("value");
}
```

## Async
The *Java driver* provides native support for asynchronous programming since it 
is built on top of an [asynchronous protocol](../../../manual/native_protocol/),
please see [this page](../../../manual/async/) for best practices regarding asynchronous programming
with the *Java driver*.


[planetCCqlLink]: http://www.planetcassandra.org/making-the-change-from-thrift-to-cql/
[dsBlogCqlLink]: http://www.datastax.com/dev/blog/thrift-to-cql3