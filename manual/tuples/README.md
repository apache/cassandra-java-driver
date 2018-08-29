## Using Tuples with the Java driver

Cassandra allows to use `tuple` data types [in tables and user-defined types](https://docs.datastax.com/en/cql/3.1/cql/cql_reference/tupleType.html):

```
CREATE TABLE ks.collect_things (
  pk int,
  ck1 text,
  ck2 text,
  v tuple<int, text, float>,
  PRIMARY KEY (pk, ck1, ck2)
);
```

### Fetching Tuples from Rows results

The DataStax Java driver exposes a special [`TupleValue`][TupleValue] class to handle such columns. 
[`TupleValue`][TupleValue] exposes getters allowing to extract from the tuple all the data types 
supported by Cassandra:

```java
Row row = session.execute("SELECT v FROM ks.collect_things WHERE pk = 1").one();

TupleValue tupleValue = row.getTupleValue("v");

int firstValueInTuple = tupleValue.getInt(0);

String secondValueInTuple = tupleValue.getString(1);

Float thirdValueInTuple = tupleValue.getFloat(2);
```

### Using tuples as statement parameters

A prepared statement may contain a Tuple as a query parameter. In such cases, users 
will need to create or gather a [`TupleType`][TupleType] first, in order to be able to create a [`TupleValue`][TupleValue] 
to bind:

```java
PreparedStatement ps = session.prepare("INSERT INTO ks.collect_things (pk, ck1, ck2, v) VALUES (:pk, :ck1, :ck2, :v)");

TupleType tupleType = cluster.getMetadata().newTupleType(DataType.cint(), DataType.text(), DataType.cfloat());

BoundStatement bs = ps.bind();
bs.setInt("pk", 1);
bs.setString("ck1", "1");
bs.setString("ck2", "1");
bs.setTupleValue("v", tupleType.newValue(1, "hello", 2.3f));

session.execute(bs);
```

The method [`newValue(Object...)`][newValueVararg] follows the same rules as `new SimpleStatement(String, Object...)`;
there can be ambiguities due to the fact that the driver will infer the data types from the values
given in parameters of the method, whereas the data types required may differ (numeric 
literals are always interpreted as `int`).

To avoid such ambiguities, a [`TupleValue`][TupleValue] returned by [`newValue()`][newValue] also exposes specific 
setters for all the existing Cassandra data types:

```java
TupleType tupleType = cluster.getMetadata().newTupleType(DataType.bigint(), DataType.text(), DataType.cfloat());

TupleValue value = tupleType.newValue().setLong(0, 2).setString(1, "hello").setDouble(2, 2.3f);
```

#### More use cases

Users can also define single-usage tuples in _SELECT_ queries with the `IN` keyword 
(called a "multi-column IN restriction"), usually for tables with composite clustering 
keys. In this case, a tuple will be usable the same way it was for prepared statements' parameters:

```java
TupleType oneTimeUsageTuple = cluster.getMetadata().newTupleType(DataType.text(), DataType.text());

PreparedStatement ps = session.prepare("SELECT * FROM ks.collect_things WHERE pk = 1 and (ck1, ck2) IN (:t)");

BoundStatement bs = ps.bind();
bs.setTupleValue("t", oneTimeUsageTuple.newValue("1", "1"));

session.execute(bs);
```

More generally, the `IN` keyword in a `SELECT` query will be used to define a *list* of 
desired values of the filtered clustering keys, those would simply be bound as a list of 
[`TupleValue`][TupleValue] with the Java driver:

```java
TupleType oneTimeUsageTuple = cluster.getMetadata().newTupleType(DataType.text(), DataType.text());

PreparedStatement ps = session.prepare("SELECT * FROM ks.collect_things WHERE pk = 1 AND (ck1, ck2) IN :l");

BoundStatement bs = ps.bind();
bs.setList("l", Arrays.asList(oneTimeUsageTuple.newValue("1", "1"), oneTimeUsageTuple.newValue("1", "2"), oneTimeUsageTuple.newValue("2", "1")));

session.execute(bs);
```

[TupleType]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/TupleType.html
[TupleValue]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/TupleValue.html
[newValueVararg]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/TupleType.html#newValue-java.lang.Object...-
[newValue]: http://docs.datastax.com/en/drivers/java/3.6/com/datastax/driver/core/TupleType.html#newValue--
