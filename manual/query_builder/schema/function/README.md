## Function

User-defined functions (UDF) enable users to create user code written in JSR-232 compliant scripting
languages that can be evaluated in CQL queries.  [SchemaBuilderDsl] offers API methods for creating
and dropping UDFs.

### Creating a Function (CREATE FUNCTION)

To start a `CREATE FUNCTION` query, use `createFunction` in [SchemaBuilderDsl]:

```java
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilderDsl.*;

CreateFunctionStart create = createFunction("log");
```

Like all other `CREATE` queries, one may supply `ifNotExists()` to require that the UDF should only
be created if it doesn't already exist, i.e.:

```java
CreateFunctionStart create = createFunction("cycling", "log").ifNotExists();
```

You may also specify that you would like to replace an existing function by the same signature if it
exists.  In this case, use `orReplace`:

```java
CreateFunctionStart create = createFunction("cycling", "log").orReplace();
```

One may also specify the parameters of a function using `withParameter`:

```
createFunction("cycling", "left")
    .withParameter("colName", DataTypes.TEXT)
    .withParameter("num", DataTypes.DOUBLE)
```

There are a number of steps that must be executed to complete a function:

* Specify whether the function is called on null input (`calledOnNull`) or if it should simply
  return null (`returnsNullOnNull`).
* Specify the return type of the function using `returnsType`
* Specify language of the function body using `withJavaLanguage`, `withJavaScriptLanguage`, or
  `withLanguage`
* Specify the function body with `as` or `asQuoted`

For example, the following defines a complete `CREATE FUNCTION` statement:

```java
createFunction("cycling", "log")
    .withParameter("input", DataTypes.DOUBLE)
    .calledOnNull()
    .returnsType(DataTypes.DOUBLE)
    .withJavaLanguage()
    .asQuoted("return Double.valueOf(Math.log(input.doubleValue()));");

// CREATE FUNCTION cycling.log (columnname text,num int) CALLED ON NULL INPUT RETURNS double LANGUAGE java
// AS 'return Double.valueOf(Math.log(input.doubleValue()));'
```

Note that when providing a function body, the `as` method does not implicitly quote your function
body.  If you would like to have the API handle this for you, use `asQuoted`.  This will surround
your function body in single quotes if the body itself does not contain a single quote, otherwise it
will surround your function body in two dollar signs (`$$`) mimicking a postgres-style string
literal, i.e.:

```java
createFunction("sayhi")
    .withParameter("input", DataTypes.TEXT)
    .returnsNullOnNull()
    .returnsType(DataTypes.TEXT)
    .withJavaScriptLanguage()
    .asQuoted("'hi ' + input;");
// CREATE FUNCTION sayhi (input text) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE javascript AS $$ 'hi ' + input; $$
```


### Dropping a Function (DROP FUNCTION)

To create a `DROP FUNCTION` query, use `dropFunction`:

```java
dropFunction("cycling", "log");
// DROP FUNCTION cycling.log
```

You may also specify `ifExists`:

```java
dropFunction("log").ifExists();
// DROP FUNCTION IF EXISTS log
```

[SchemaBuilderDsl]: http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/querybuilder/SchemaBuilderDsl.html
