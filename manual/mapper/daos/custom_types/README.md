## Custom result types

The mapper supports a pre-defined set of built-in types for DAO method results. For example, a
[Select](../select/#return-type) method can return a single entity, an asynchronous
`CompletionStage`, a `ReactiveResultSet`, etc.

Sometimes it's convenient to use your own types. For example if you use a specific Reactive Streams
implementation (RxJava, Reactor, Mutiny...), you probably want your DAOs to return those types
directly, instead of having to wrap every call manually.

To achieve this, the mapper allows you to plug custom logic that will get invoked when an unknown
type is encountered.

In the rest of this page, we'll show a simple example that replaces Java's `CompletableFuture` with
Guava's `ListenableFuture`. Our goal is to have the mapper implement this interface:

```java
import com.google.common.util.concurrent.ListenableFuture;

@Dao
public interface ProductDao {
  @Select
  ListenableFuture<Product> select(UUID id);

  @Update
  ListenableFuture<Void> update(Product entity);

  @Insert
  ListenableFuture<Void> insert(Product entity);

  @Delete
  ListenableFuture<Void> delete(Product entity);
}
```

### Writing the producers

The basic component that encapsulates conversion logic is [MapperResultProducer]. Our DAO has two
different return types: `ListenableFuture<Void>` and `ListenableFuture<Product>`. So we're going to
write two producers:

#### Future of void

```java
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class FutureOfVoidProducer implements MapperResultProducer {
  private static final GenericType<ListenableFuture<Void>> PRODUCED_TYPE =
      new GenericType<ListenableFuture<Void>>() {};

  @Override
  public boolean canProduce(GenericType<?> resultType) {
    return resultType.equals(PRODUCED_TYPE);                                   // (1)
  }

  @Override
  public ListenableFuture<Void> execute(
      Statement<?> statement, MapperContext context, EntityHelper<?> entityHelper) {
    CqlSession session = context.getSession();                                 // (2)
    SettableFuture<Void> result = SettableFuture.create();                     // (3)
    session.executeAsync(statement).whenComplete(
        (resultSet, error) -> {
          if (error != null) {
            result.setException(error);
          } else { 
            result.set(null);
          }});
    return result;
  }

  @Override
  public ListenableFuture<Void> wrapError(Exception error) {
    return Futures.immediateFailedFuture(error);                               // (4)
  }
}
```

All the producer methods will be invoked at runtime, by the mapper-generated DAO implementation:

1. `canProduce()` is used to select a producer. All registered producers are tried in the order that
  they were added, the first one that returns `true` is used. The [GenericType] argument is a
  runtime representation of the static type. Here we know exactly the type we're looking for:
  `ListenableFuture<Void>`. So we can use simple equality.
2. `execute()` is invoked once the statement is ready to be sent. Note that the producer is not only
  responsible for converting the result, but also for invoking the appropriate execution method: to
  this effect, it receives the [MapperContext], which provides access to the session. The
  `entityHelper` argument is not used in this implementation (and in fact it happens to be `null`);
  see the next producer for more explanations.
3. We execute the statement asynchronously to obtain a `CompletionStage`, and then convert it into a
  `ListenableFuture`.
4. `wrapError()` handles any error thrown throughout the process (either while building the
  statement, or while invoking `execute()` in this class). Clients of asynchronous APIs generally
  expect to deal with exceptions in future callbacks rather than having to catch them directly, so
  we create a failed future.
  
Note that we specialized the return types of `execute()` and `wrapError()`, instead of using
`Object` as declared by the parent interface. This is not strictly necessary (the calling code only
knows the parent interface, so there *will* be an unchecked cast), but it makes the code a bit nicer
to read.

#### Future of entity

```java
public class FutureOfEntityProducer implements MapperResultProducer {
  @Override
  public boolean canProduce(GenericType<?> resultType) {
    return resultType.getRawType().equals(ListenableFuture.class);             // (1)
  }

  @Override
  public ListenableFuture<?> execute(
      Statement<?> statement, MapperContext context, EntityHelper<?> entityHelper) {
    assert entityHelper != null;
    SettableFuture<Object> result = SettableFuture.create();
    CqlSession session = context.getSession();
    session
        .executeAsync(statement)
        .whenComplete(
            (resultSet, error) -> {
              if (error != null) {
                result.setException(error);
              } else {
                Row row = resultSet.one();
                result.set((row == null) ? null : entityHelper.get(row));      // (2)
              }
            });
    return result;
  }

  @Override
  public ListenableFuture<?> wrapError(Exception error) {
    return Futures.immediateFailedFuture(error); // same as other producer
  }
}
```

1. We could use an exact match with `ListenableFuture<Product>` like the previous example, but
  that's not very scalable: in a real application, we'll probably have more than one entity, we
  don't want to write a separate producer every time. So instead we match `ListenableFuture<?>`.
  Note that this would also match `ListenableFuture<Void>`, so we'll have to be careful of the order
  of the producers (more on that in the "packaging" section below).
2. Whenever a return type references a mapped entity, the mapper processor will detect it and inject
  the corresponding [EntityHelper] in the `execute()` method. This is a general-purpose utility
  class used throughout the mapper, in this case the method we're more specifically interested in is
  `get()`: it allows us to convert CQL rows into entity instances.
  
At most one entity class is allowed in the return type.

#### Matching more complex types

The two examples above (exact match and matching the raw type) should cover the vast majority of
needs. Occasionally you may encounter cases with deeper level of parameterization, such as
`ListenableFuture<Optional<Product>>`. To match this you'll have to call `getType()` and switch to
the `java.lang.reflect` world: 

```java
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

// Matches ListenableFuture<Optional<?>>
public boolean canProduce(GenericType<?> genericType) {
  if (genericType.getRawType().equals(ListenableFuture.class)) {
    Type type = genericType.getType();
    if (type instanceof ParameterizedType) {
      Type[] arguments = ((ParameterizedType) type).getActualTypeArguments();
      if (arguments.length == 1) {
        Type argument = arguments[0];
        return argument instanceof ParameterizedType
            && ((ParameterizedType) argument).getRawType().equals(Optional.class);
      }
    }
  }
  return false;
}
```

### Packaging the producers in a service

Once all the producers are ready, we package them in a class that implements
[MapperResultProducerService]:

```java
public class GuavaFutureProducerService implements MapperResultProducerService {
  @Override
  public Iterable<MapperResultProducer> getProducers() {
    return Arrays.asList(
        // Order matters, the most specific must come first.
        new FutureOfVoidProducer(), new FutureOfEntityProducer());
  }  
}
```

As hinted previously, the order of the producers matter: they will be tried from left to right.
Since our "future of entity" producer would also match `Void`, it must come last.

The mapper uses the Java Service Provider mechanism to register producers: create a new file 
`META-INF/services/com.datastax.oss.driver.api.mapper.result.MapperResultProducerService`,
containing the name of the implementation:

```
some.package.name.GuavaFutureProducerService
```

You can put the producers, service and service descriptor directly in your application, or
distribute them as a standalone JAR if you intend to reuse them.

### Disabling custom types

Custom types are handled at runtime. This goes a bit against the philosophy of the rest of the
object mapper, where most of the work is done at compile time thanks to annotation processing. There
are ways to extend the mapper processor, but we feel that this would be too complicated for this use
case.

One downside is that validation can now only be done at runtime: if you use a return type that isn't
supported by any producer, you'll only find out when you call the method.

**If you don't use custom types at all**, you can disable the feature with an annotation processor
flag:

```xml
  <build>
    <plugins>
      <plugin>
        <artifactId>maven-compiler-plugin</artifactId>
        <configuration>
          <compilerArgument>-Acom.datastax.oss.driver.mapper.customResults.enabled=false</compilerArgument>
        </configuration>
      </plugin>
    </plugins>
  </build>
```

With this configuration, if a DAO method declares a non built-in return type, it will be surfaced as
a compiler error.

[EntityHelper]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/entity/EntityHelper.html
[GenericType]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/core/type/reflect/GenericType.html
[MapperContext]:  https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/MapperContext.html
[MapperResultProducer]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/result/MapperResultProducer.html
[MapperResultProducerService]: https://docs.datastax.com/en/drivers/java/4.13/com/datastax/oss/driver/api/mapper/result/MapperResultProducerService.html
