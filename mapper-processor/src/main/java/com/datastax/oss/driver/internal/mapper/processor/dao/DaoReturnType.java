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

import javax.lang.model.element.TypeElement;

/** Holds information about the return type of a DAO method. */
public class DaoReturnType {

  public static final DaoReturnType VOID = new DaoReturnType(DefaultDaoReturnTypeKind.VOID);
  public static final DaoReturnType BOOLEAN = new DaoReturnType(DefaultDaoReturnTypeKind.BOOLEAN);
  public static final DaoReturnType LONG = new DaoReturnType(DefaultDaoReturnTypeKind.LONG);
  public static final DaoReturnType ROW = new DaoReturnType(DefaultDaoReturnTypeKind.ROW);
  public static final DaoReturnType RESULT_SET =
      new DaoReturnType(DefaultDaoReturnTypeKind.RESULT_SET);
  public static final DaoReturnType BOUND_STATEMENT =
      new DaoReturnType(DefaultDaoReturnTypeKind.BOUND_STATEMENT);
  public static final DaoReturnType FUTURE_OF_VOID =
      new DaoReturnType(DefaultDaoReturnTypeKind.FUTURE_OF_VOID);
  public static final DaoReturnType FUTURE_OF_BOOLEAN =
      new DaoReturnType(DefaultDaoReturnTypeKind.FUTURE_OF_BOOLEAN);
  public static final DaoReturnType FUTURE_OF_LONG =
      new DaoReturnType(DefaultDaoReturnTypeKind.FUTURE_OF_LONG);
  public static final DaoReturnType FUTURE_OF_ROW =
      new DaoReturnType(DefaultDaoReturnTypeKind.FUTURE_OF_ROW);
  public static final DaoReturnType FUTURE_OF_ASYNC_RESULT_SET =
      new DaoReturnType(DefaultDaoReturnTypeKind.FUTURE_OF_ASYNC_RESULT_SET);
  public static final DaoReturnType REACTIVE_RESULT_SET =
      new DaoReturnType(DefaultDaoReturnTypeKind.REACTIVE_RESULT_SET);
  public static final DaoReturnType UNSUPPORTED =
      new DaoReturnType(DefaultDaoReturnTypeKind.UNSUPPORTED);

  private final DaoReturnTypeKind kind;
  private final TypeElement entityElement;

  public DaoReturnType(DaoReturnTypeKind kind, TypeElement entityElement) {
    this.kind = kind;
    this.entityElement = entityElement;
  }

  public DaoReturnType(DaoReturnTypeKind kind) {
    this(kind, null);
  }

  public DaoReturnTypeKind getKind() {
    return kind;
  }

  /**
   * If the type is parameterized by an entity-annotated class, return that entity.
   *
   * <p>For example {@code CompletionStage<Product>} => {@code Product}.
   */
  public TypeElement getEntityElement() {
    return entityElement;
  }

  /**
   * Whether this return type requires the Reactive Streams API.
   *
   * <p>If true, the generated DAO class will inherit from {@link
   * com.datastax.dse.driver.internal.mapper.reactive.ReactiveDaoBase}, otherwise it will inherit
   * from {@link com.datastax.oss.driver.internal.mapper.DaoBase}.
   */
  public boolean requiresReactive() {
    return kind.requiresReactive();
  }
}
