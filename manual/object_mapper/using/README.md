## Using the mapper

First, create a [MappingManager]. It wraps an existing [Session]
instance:

```java
MappingManager manager = new MappingManager(session);
```

`MappingManager` is thread-safe and can be safely shared throughout your
application. You would typically create one instance at startup, right
after your `Session`.

Note that `MappingManager` will initialize the `Session` if not
previously done (this was not the case in previous driver versions; if
this is a problem for you, see `MappingManager(session, protocolVersion)`).

### Entity mappers

Each entity class (annotated with `@Table`) is managed by a dedicated
[Mapper] object. You obtain this object from the `MappingManager`:

```java
Mapper<User> mapper = manager.mapper(User.class);
```

`Mapper` objects are thread-safe. The manager caches them internally, so
calling `manager#mapper` more than once for the same class will return
the previously generated mapper.

[Mapper]:http://docs.datastax.com/en/drivers/java/3.7/com/datastax/driver/mapping/Mapper.html
[MappingManager]:http://docs.datastax.com/en/drivers/java/3.7/com/datastax/driver/mapping/MappingManager.html
[Session]:http://docs.datastax.com/en/drivers/java/3.7/com/datastax/driver/core/Session.html

#### Basic CRUD operations

To save an object, use `Mapper#save`:

```java
UUID userId = ...;
User u = new User(userId, "John Doe", new Address("street", 01000));
mapper.save(u);
```

--------------

To retrieve an object, use `Mapper#get`:

```java
UUID userId = ...;
User u = mapper.get(userId);
```

`get`'s arguments must match the primary key components (number of
arguments, their types, and order).

--------------

To delete a row in a table, use `Mapper#delete`. This method support
deleting a row given either its primary keys, or the object to delete:

```java
UUID userId = ...;
mapper.delete(userId);
mapper.delete(u);
```

--------------

All these CRUD operations are synchronous, but the `Mapper` provides
their asynchronous equivalents:

```java
ListenableFuture<Void> saveFuture = mapper.saveAsync(u);
ListenableFuture<User> userFuture = mapper.getAsync(userId);
ListenableFuture<Void> deleteFuture = mapper.deleteAsync(userId);
```

#### Mapper options

The basic CRUD operations accept additional options to customize the
underlying query:

- `ttl`: add a time-to-live value for the operation.
- `timestamp`: add a timestamp value for the operation.
- `consistencyLevel`: specify a consistency level.
- `tracing`: set tracing flag for the query.
- `saveNullFields`: if set to true, fields with value `null` in an
  instance that is to be persisted will be explicitly written as `null`
  in the query. If set to false, fields with null value won't be included
  in the write query (thus avoiding tombstones), or if using Protocol V4+ 
  unset() will be used for null values.  If not specified, the 
  default behavior is to persist `null` fields.
- `ifNotExists`: if set to true, adds an `IF NOT EXISTS` clause to the
  save operation (use `ifNotExists(false)` if you enabled the option by
  default and need to disable it for a specific operation).

To use options, add them to the mapper call after regular parameters:

```java
import static com.datastax.driver.mapping.Mapper.Option.*;

mapper.save(new User(userId, "helloworld"),
            timestamp(123456L), tracing(true), ttl(42));
```

Some options don't apply to all operations:

<table border="1" style="text-align:center; width:100%;margin-bottom:1em;">
    <tr> <td><b>Option</b></td>         <td><b>save/saveQuery</b></td> <td><b>get/getQuery</b></td> <td><b>delete/deleteQuery</b></td></tr>
    <tr> <td>Ttl</td>                   <td>yes</td>                   <td>no</td>                  <td>no</td> </tr>
    <tr> <td>Timestamp</td>             <td>yes</td>                   <td>no</td>                  <td>yes</td> </tr>
    <tr> <td>ConsistencyLevel</td>      <td>yes</td>                   <td>yes</td>                 <td>yes</td> </tr>
    <tr> <td>Tracing</td>               <td>yes</td>                   <td>yes</td>                 <td>yes</td> </tr>
    <tr> <td>SaveNullFields</td>        <td>yes</td>                   <td>no</td>                  <td>no</td> </tr>
    <tr> <td>IfNotExists</td>           <td>yes</td>                   <td>no</td>                  <td>no</td> </tr>
</table>

Note that `Option.consistencyLevel` is redundant with the consistency
level defined by [@Table](../creating/#creating-a-table-entity).
If both are defined, the option will take precedence over the
annotation.

Default options can be defined for each type of operation:

```java
mapper.setDefaultGetOption(tracing(true), consistencyLevel(QUORUM));
mapper.setDefaultSaveOption(saveNullFields(false));
mapper.setDefaultDeleteOption(consistencyLevel(ONE));

// Given the defaults above, this will use tracing(true), consistencyLevel(ONE)
mapper.get(uuid, consistencyLevel(ONE));
```

To reset default options, use the following methods:

```java
mapper.resetDefaultGetOption();
mapper.resetDefaultSaveOption();
mapper.resetDefaultDeleteOption();
```

#### Access to underlying `Statement`s

Instead of performing an operation directly, it's possible to ask the
`Mapper` to just return the corresponding `Statement` object. This gives
the client a chance to customize the statement before executing it.

- `Mapper.saveQuery(entity)`: returns a statement generated by the
  mapper to save `entity` into the database.
- `Mapper.getQuery(userId)`: returns a statement to select a row in the
  database, selected on the given `userId`, and matching the mapped
  object structure.
- `Mapper.deleteQuery(userID)`: returns a statement to delete a row in the
  database given the `userId` provided. This method can also accept a
  mapped object instance.

#### Manual mapping

`Mapper#map` provides a way to convert the results of a regular query:

```java
ResultSet results = session.execute("SELECT * FROM user");
Result<User> users = mapper.map(results);
for (User u : users) {
    System.out.println("User : " + u.getUserId());
}
```

This method will ignore:

* extra columns in the `ResultSet` that are not mapped for this entity.
* mapped fields that are not present in the `ResultSet` (setters won't
  be called so the value will be the one after invocation of the class's
  default constructor).

[Result] is similar to `ResultSet` but for a given mapped class.
It provides methods `one()`, `all()`, `iterator()`, `getExecutionInfo()`
and `isExhausted()`. Note that iterating the `Result` will consume the
`ResultSet`, and vice-versa.

[Result]: http://docs.datastax.com/en/drivers/java/3.7/com/datastax/driver/mapping/Result.html

### Accessors

`Accessor`s provide a way to map custom queries not supported by the
default entity mappers.

To create an accessor, define a Java interface and annotate each method
to provide the corresponding CQL query:

```java
@Accessor
public interface UserAccessor {
    @Query("SELECT * FROM user")
    Result<User> getAll();
}
```

The `MappingManager` can then process this interface and automatically generate an
implementation for it:

```java
UserAccessor userAccessor = manager.createAccessor(UserAccessor.class);
Result<User> users = userAccessor.getAll();
```

Like mappers, accessors are cached at the manager level and thus, are
thread-safe/sharable.

#### Parameters

A query can have bind markers, that will be set with the method's
arguments.

With unnamed markers, the order of the arguments must match the order of
the markers:

```java
@Query("insert into user (id, name) values (?, ?)")
ResultSet insert(UUID userId, String name);
```

With named markers, use [@Param][param] to indicate which parameter
corresponds to which marker:

```java
@Query("insert into user (userId, name) values (:u, :n)")
ResultSet insert(@Param("u") UUID userId, @Param("n") String name);
```

[param]:http://docs.datastax.com/en/drivers/java/3.7/com/datastax/driver/mapping/annotations/Param.html

If a method argument is a Java enumeration, it must be annotated with
`@Enumerated` to indicate how to convert it to a CQL type (the rules are
the same as in [mapping definition](../creating/#enumerations)):

```java
@Query("insert into user (key, gender) values (?,?)")
ResultSet addUser(int key, @Enumerated(EnumType.ORDINAL) Enum value);
```

#### Return type

The declared return type of each method affects how the query will get
executed:

<table border="1" style="text-align:center; width:100%;margin-bottom:1em;">
    <tr> <td><b>Return type</b></td>            <td><b>Effect</b></td> </tr>
    <tr>
      <td><code>void</code></td>
      <td>Synchronous execution, discards the results of the query.</td>
    </tr>
    <tr>
      <td><code>ResultSet</code></td>
      <td>Synchronous execution, returns unmapped results.</td>
    </tr>
    <tr>
      <td><code>T</code></td>
      <td><code>T</code> must be a mapped class.<br/>Synchronous execution, returns the first row (or <code>null</code> if there are no results).</td>
    </tr>
    <tr>
      <td><code>Result&lt;T&gt;</code></td>
      <td><code>T</code> must be a mapped class.<br/>Synchronous execution, returns a list of mapped objects.</td>
    </tr>
    <tr>
      <td><code>ResultSetFuture</code></td>
      <td>Asynchronous execution, returns unmapped results.</td>
    </tr>
    <tr>
      <td><code>ListenableFuture&lt;T&gt;</code></td>
      <td><code>T</code> must be a mapped class.<br/>Asynchronous execution, returns the first row (or <code>null</code> if there are no results).</td>
    </tr>
    <tr>
      <td><code>ListenableFuture&lt;Result&lt;T&gt;&gt;</code></td>
      <td><code>T</code> must be a mapped class.<br/>Asynchronous execution, returns a list of mapped objects.</td>
    </tr>
    <tr>
      <td><code>Statement</code></td>
      <td>Object mapper doesn't execute query, but returns an instance of <code>BoundStatement</code> that could be executed via <code>Session</code> object. </td>
    </tr>
</table>

Example:

```java
@Query("SELECT * FROM user")
public ListenableFuture<Result<User>> getAllAsync();
```


#### Customizing the statement

It is possible to customize query parameters to include in a `Accessor`
query with the annotation [@QueryParameters]. Then, options like
*consistency level*, *fetchsize* or *tracing* are settable:

```java
@Query("SELECT * FROM ks.users")
@QueryParameters(consistency="QUORUM")
public ListenableFuture<Result<User>> getAllAsync();
```

[@QueryParameters]: http://docs.datastax.com/en/drivers/java/3.7/com/datastax/driver/mapping/annotations/QueryParameters.html


### Mapping configuration

[MappingConfiguration] lets you configure low-level aspects of the object mapper. It is configured
when initializing the mapping manager:

```java
PropertyMapper propertyMapper = ... ; // see examples below
MappingConfiguration configuration = 
        MappingConfiguration.builder()
                .withPropertyMapper(propertyMapper)
                .build();
MappingManager manager = new MappingManager(session, configuration);
```

The main component in the configuration is [PropertyMapper], which controls how annotated classes
will relate to database objects. The best way to plug in specific behavior is to create an instance of
[DefaultPropertyMapper] and customize it.

For example, the mapper's default behavior is to try to map all the properties of your Java objects.
You might want to take the opposite approach and only map the ones that are specifically annotated
with `@Column` or `@Field`:

```java
PropertyMapper propertyMapper = new DefaultPropertyMapper()
        .setPropertyTransienceStrategy(PropertyTransienceStrategy.OPT_IN);
```

Another common need is to customize the way Cassandra column names are inferred. Out of the box, Java
property names are simply lowercased, so a `userName` property would be mapped to the `username` column.
To map to `user_name` instead, use the following:

```java
PropertyMapper propertyMapper = new DefaultPropertyMapper()
        .setNamingStrategy(new DefaultNamingStrategy(
                NamingConventions.LOWER_CAMEL_CASE, 
                NamingConventions.LOWER_SNAKE_CASE));
```

There is more to `DefaultPropertyMapper`; see the Javadocs and implementation for details.


[MappingConfiguration]: http://docs.datastax.com/en/drivers/java/3.7/com/datastax/driver/mapping/MappingConfiguration.html
[PropertyMapper]: http://docs.datastax.com/en/drivers/java/3.7/com/datastax/driver/mapping/PropertyMapper.html
[DefaultPropertyMapper]: http://docs.datastax.com/en/drivers/java/3.7/com/datastax/driver/mapping/DefaultPropertyMapper.html
