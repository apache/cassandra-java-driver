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
package com.datastax.oss.driver.api.mapper.annotations;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.Statement;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;

/**
 * A set of compile time parameters to use for mapped queries (this can be used for methods
 * annotated with {@link Delete}, {@link Insert}, {@link Query}, {@link Select} or {@link Update}).
 *
 * <p>If you decorate a DAO method with this, it will use those values when constructing the bound
 * statement. Note that the method can also take a function argument to modify the statement at
 * runtime; in that case, the values from this annotation will be applied first, and the function
 * second.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface StatementAttributes {
  /**
   * The name of the execution profile to use.
   *
   * @see Statement#setExecutionProfileName(String)
   */
  String executionProfileName() default "";

  /**
   * The page size to use.
   *
   * <p>If unset, the mapper won't set any value on the statement (letting it default to the value
   * defined in the configuration).
   *
   * @see Statement#setPageSize(int)
   */
  int pageSize() default Integer.MIN_VALUE;

  /**
   * Whether the request is idempotent; that is, whether applying the request twice leaves the
   * database in the same state.
   *
   * <p>If unset, the mapper won't set any value on the statement (letting it default to the value *
   * defined in the configuration).
   *
   * <p>Note that this attribute is an array only to allow an empty default; only the first element
   * will be considered.
   *
   * @see Statement#setIdempotent(Boolean)
   */
  boolean[] idempotence() default {};

  /**
   * The consistency level to use for the statement.
   *
   * <p>If unset, the mapper won't set any value on the statement (letting it default to the value
   * defined in the configuration).
   *
   * @see Statement#setConsistencyLevel(ConsistencyLevel)
   */
  String consistencyLevel() default "";

  /**
   * The serial consistency level to use for the statement.
   *
   * <p>If unset, the mapper won't set any value on the statement (letting it default to the value
   * defined in the configuration).
   *
   * @see Statement#setSerialConsistencyLevel(ConsistencyLevel)
   */
  String serialConsistencyLevel() default "";

  /**
   * How long to wait for this request to complete.
   *
   * <p>This expects a string in the format accepted by {@link Duration#parse(CharSequence)}.
   *
   * <p>If unset, the mapper won't set any value on the statement (letting it default to the value
   * defined in the configuration).
   *
   * @see Statement#setTimeout(Duration)
   */
  String timeout() default "";

  /**
   * The keyspace to use for token-aware routing.
   *
   * <p>If unset, the mapper won't set any value on the statement.
   *
   * @see Statement#setRoutingKeyspace(String)
   */
  String routingKeyspace() default "";
}
