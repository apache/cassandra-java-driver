/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.mapper.processor.dao;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.dse.driver.api.mapper.reactive.MappedReactiveResultSet;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;

public class DefaultDaoReturnTypeParser implements DaoReturnTypeParser {

  /**
   * The return types that can be inferred directly from {@link TypeMirror#getKind()} (void and
   * primitives).
   */
  protected static final Map<TypeKind, DaoReturnType> DEFAULT_TYPE_KIND_MATCHES =
      ImmutableMap.<TypeKind, DaoReturnType>builder()
          .put(TypeKind.VOID, DaoReturnType.VOID)
          .put(TypeKind.BOOLEAN, DaoReturnType.BOOLEAN)
          .put(TypeKind.LONG, DaoReturnType.LONG)
          .build();

  /** The return types that correspond directly to a non-generic Java class. */
  protected static final Map<Class<?>, DaoReturnType> DEFAULT_CLASS_MATCHES =
      ImmutableMap.<Class<?>, DaoReturnType>builder()
          .put(Boolean.class, DaoReturnType.BOOLEAN)
          .put(Long.class, DaoReturnType.LONG)
          .put(Row.class, DaoReturnType.ROW)
          .put(ResultSet.class, DaoReturnType.RESULT_SET)
          .put(BoundStatement.class, DaoReturnType.BOUND_STATEMENT)
          .put(ReactiveResultSet.class, DaoReturnType.REACTIVE_RESULT_SET)
          .build();

  /**
   * The return types that correspond to a generic class with a single type parameter that is an
   * entity class.
   */
  protected static final Map<Class<?>, DaoReturnTypeKind> DEFAULT_ENTITY_CONTAINER_MATCHES =
      ImmutableMap.<Class<?>, DaoReturnTypeKind>builder()
          .put(Optional.class, DefaultDaoReturnTypeKind.OPTIONAL_ENTITY)
          .put(CompletionStage.class, DefaultDaoReturnTypeKind.FUTURE_OF_ENTITY)
          .put(CompletableFuture.class, DefaultDaoReturnTypeKind.FUTURE_OF_ENTITY)
          .put(PagingIterable.class, DefaultDaoReturnTypeKind.PAGING_ITERABLE)
          .put(Stream.class, DefaultDaoReturnTypeKind.STREAM)
          .put(MappedReactiveResultSet.class, DefaultDaoReturnTypeKind.MAPPED_REACTIVE_RESULT_SET)
          .build();

  /** The return types that correspond to a future of a non-generic Java class. */
  protected static final Map<Class<?>, DaoReturnType> DEFAULT_FUTURE_OF_CLASS_MATCHES =
      ImmutableMap.<Class<?>, DaoReturnType>builder()
          .put(Void.class, DaoReturnType.FUTURE_OF_VOID)
          .put(Boolean.class, DaoReturnType.FUTURE_OF_BOOLEAN)
          .put(Long.class, DaoReturnType.FUTURE_OF_LONG)
          .put(Row.class, DaoReturnType.FUTURE_OF_ROW)
          .put(AsyncResultSet.class, DaoReturnType.FUTURE_OF_ASYNC_RESULT_SET)
          .build();

  /**
   * The return types that correspond to a future of a generic class with a single type parameter
   * that is an entity class.
   */
  protected static final Map<Class<?>, DaoReturnTypeKind>
      DEFAULT_FUTURE_OF_ENTITY_CONTAINER_MATCHES =
          ImmutableMap.<Class<?>, DaoReturnTypeKind>builder()
              .put(Optional.class, DefaultDaoReturnTypeKind.FUTURE_OF_OPTIONAL_ENTITY)
              .put(
                  MappedAsyncPagingIterable.class,
                  DefaultDaoReturnTypeKind.FUTURE_OF_ASYNC_PAGING_ITERABLE)
              .put(Stream.class, DefaultDaoReturnTypeKind.FUTURE_OF_STREAM)
              .build();

  protected final ProcessorContext context;
  private final Map<TypeKind, DaoReturnType> typeKindMatches;
  private final Map<Class<?>, DaoReturnType> classMatches;
  private final Map<Class<?>, DaoReturnTypeKind> entityContainerMatches;
  private final Map<Class<?>, DaoReturnType> futureOfClassMatches;
  private final Map<Class<?>, DaoReturnTypeKind> futureOfEntityContainerMatches;

  public DefaultDaoReturnTypeParser(ProcessorContext context) {
    this(
        context,
        DEFAULT_TYPE_KIND_MATCHES,
        DEFAULT_CLASS_MATCHES,
        DEFAULT_ENTITY_CONTAINER_MATCHES,
        DEFAULT_FUTURE_OF_CLASS_MATCHES,
        DEFAULT_FUTURE_OF_ENTITY_CONTAINER_MATCHES);
  }

  protected DefaultDaoReturnTypeParser(
      ProcessorContext context,
      Map<TypeKind, DaoReturnType> typeKindMatches,
      Map<Class<?>, DaoReturnType> classMatches,
      Map<Class<?>, DaoReturnTypeKind> entityContainerMatches,
      Map<Class<?>, DaoReturnType> futureOfClassMatches,
      Map<Class<?>, DaoReturnTypeKind> futureOfEntityContainerMatches) {
    this.context = context;
    this.typeKindMatches = typeKindMatches;
    this.classMatches = classMatches;
    this.entityContainerMatches = entityContainerMatches;
    this.futureOfClassMatches = futureOfClassMatches;
    this.futureOfEntityContainerMatches = futureOfEntityContainerMatches;
  }

  @NonNull
  @Override
  public DaoReturnType parse(
      @NonNull TypeMirror returnTypeMirror, @NonNull Map<Name, TypeElement> typeParameters) {

    // void or a primitive?
    DaoReturnType match = typeKindMatches.get(returnTypeMirror.getKind());
    if (match != null) {
      return match;
    }

    if (returnTypeMirror.getKind() == TypeKind.DECLARED) {

      // entity class? e.g. Product
      TypeElement entityElement;
      if ((entityElement = EntityUtils.asEntityElement(returnTypeMirror, typeParameters)) != null) {
        return new DaoReturnType(DefaultDaoReturnTypeKind.ENTITY, entityElement);
      }

      // simple class? e.g. Boolean
      DeclaredType declaredReturnType = (DeclaredType) returnTypeMirror;
      for (Map.Entry<Class<?>, DaoReturnType> entry : classMatches.entrySet()) {
        Class<?> simpleClass = entry.getKey();
        if (context.getClassUtils().isSame(declaredReturnType, simpleClass)) {
          return entry.getValue();
        }
      }

      // entity container? e.g. Optional<Product>
      if (declaredReturnType.getTypeArguments().size() == 1
          && (entityElement =
                  EntityUtils.typeArgumentAsEntityElement(returnTypeMirror, typeParameters))
              != null) {
        Element returnElement = declaredReturnType.asElement();
        for (Map.Entry<Class<?>, DaoReturnTypeKind> entry : entityContainerMatches.entrySet()) {
          Class<?> containerClass = entry.getKey();
          if (context.getClassUtils().isSame(returnElement, containerClass)) {
            return new DaoReturnType(entry.getValue(), entityElement);
          }
        }
      }

      if (context.getClassUtils().isFuture(declaredReturnType)) {
        TypeMirror typeArgumentMirror = declaredReturnType.getTypeArguments().get(0);

        // future of a simple class? e.g. CompletableFuture<Boolean>
        for (Map.Entry<Class<?>, DaoReturnType> entry : futureOfClassMatches.entrySet()) {
          Class<?> simpleClassArgument = entry.getKey();
          if (context.getClassUtils().isSame(typeArgumentMirror, simpleClassArgument)) {
            return entry.getValue();
          }
        }

        // Note that futures of entities (e.g. CompletionStage<Product>) are already covered by the
        // "entity container" check above

        // future of entity container? e.g. CompletionStage<Optional<Product>>
        if (typeArgumentMirror.getKind() == TypeKind.DECLARED) {
          DeclaredType declaredTypeArgument = (DeclaredType) typeArgumentMirror;
          if (declaredTypeArgument.getTypeArguments().size() == 1
              && (entityElement =
                      EntityUtils.typeArgumentAsEntityElement(typeArgumentMirror, typeParameters))
                  != null) {
            Element typeArgumentElement = declaredTypeArgument.asElement();
            for (Map.Entry<Class<?>, DaoReturnTypeKind> entry :
                futureOfEntityContainerMatches.entrySet()) {
              Class<?> containerClass = entry.getKey();
              if (context.getClassUtils().isSame(typeArgumentElement, containerClass)) {
                return new DaoReturnType(entry.getValue(), entityElement);
              }
            }
          }
        }
      }

      // Otherwise assume a custom type. A MappedResultProducer will be looked up from the
      // MapperContext at runtime.
      if (context.areCustomResultsEnabled()) {
        return new DaoReturnType(
            DefaultDaoReturnTypeKind.CUSTOM,
            findEntityInCustomType(declaredReturnType, typeParameters, new ArrayList<>()));
      }
    }

    if (returnTypeMirror.getKind() == TypeKind.TYPEVAR) {

      // entity class? e.g. Product
      TypeElement entityElement;
      if ((entityElement = EntityUtils.asEntityElement(returnTypeMirror, typeParameters)) != null) {
        return new DaoReturnType(DefaultDaoReturnTypeKind.ENTITY, entityElement);
      }

      // simple class? e.g. Boolean
      TypeVariable typeVariable = ((TypeVariable) returnTypeMirror);
      Name name = typeVariable.asElement().getSimpleName();
      TypeElement element = typeParameters.get(name);
      if (element != null) {
        for (Map.Entry<Class<?>, DaoReturnType> entry : classMatches.entrySet()) {
          Class<?> simpleClass = entry.getKey();
          if (context.getClassUtils().isSame(element, simpleClass)) {
            return entry.getValue();
          }
        }
      }

      // DAO parameterization by more complex types (futures, containers...) is not supported
    }

    return DaoReturnType.UNSUPPORTED;
  }

  /**
   * If we're dealing with a {@link DefaultDaoReturnTypeKind#CUSTOM}, we allow one entity element to
   * appear at any level of nesting in the type, e.g. {@code MyCustomFuture<Optional<Entity>>}.
   */
  private TypeElement findEntityInCustomType(
      TypeMirror type,
      Map<Name, TypeElement> typeParameters,
      List<TypeMirror> alreadyCheckedTypes) {

    // Generic types can be recursive! e.g. Integer implements Comparable<Integer>. Avoid infinite
    // recursion:
    for (TypeMirror alreadyCheckedType : alreadyCheckedTypes) {
      if (context.getTypeUtils().isSameType(type, alreadyCheckedType)) {
        return null;
      }
    }
    alreadyCheckedTypes.add(type);

    TypeElement entityElement = EntityUtils.asEntityElement(type, typeParameters);
    if (entityElement != null) {
      return entityElement;
    } else if (type.getKind() == TypeKind.DECLARED) {
      // Check type arguments, e.g. `Foo<T>` where T = Product
      DeclaredType declaredType = (DeclaredType) type;
      for (TypeMirror typeArgument : declaredType.getTypeArguments()) {
        entityElement = findEntityInCustomType(typeArgument, typeParameters, alreadyCheckedTypes);
        if (entityElement != null) {
          return entityElement;
        }
      }
      Element element = declaredType.asElement();
      if (element.getKind() == ElementKind.CLASS || element.getKind() == ElementKind.INTERFACE) {
        // Check interfaces, e.g. `Foo implements Iterable<T>`, where T = Product
        TypeElement typeElement = (TypeElement) element;
        for (TypeMirror parentInterface : typeElement.getInterfaces()) {
          entityElement =
              findEntityInCustomType(parentInterface, typeParameters, alreadyCheckedTypes);
          if (entityElement != null) {
            return entityElement;
          }
        }
        // Check superclass (if there is none then the mirror has TypeKind.NONE and the recursive
        // call will return null).
        return findEntityInCustomType(
            typeElement.getSuperclass(), typeParameters, alreadyCheckedTypes);
      }
    }
    // null is a valid result even at the top level, a custom type may not contain any entity
    return null;
  }
}
