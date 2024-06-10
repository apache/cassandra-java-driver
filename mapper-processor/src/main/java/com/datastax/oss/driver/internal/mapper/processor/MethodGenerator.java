/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.mapper.processor;

import com.squareup.javapoet.MethodSpec;
import java.util.Optional;

/** A component that generates a single method. */
public interface MethodGenerator {

  /**
   * @return empty if an error occurred during generation. In that case, the caller doesn't have
   *     anything to do: it's the generator's responsibility to report the error via {@link
   *     DecoratedMessager}.
   */
  Optional<MethodSpec> generate();

  /**
   * Whether the generated method requires the Reactive Streams API.
   *
   * <p>If true, the generated DAO class will inherit from {@link
   * com.datastax.dse.driver.internal.mapper.reactive.ReactiveDaoBase}, otherwise it will inherit
   * from {@link com.datastax.oss.driver.internal.mapper.DaoBase}.
   *
   * <p>Only generated methods returning {@link
   * com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet} and {@link
   * com.datastax.dse.driver.api.mapper.reactive.MappedReactiveResultSet} should return true here.
   */
  default boolean requiresReactive() {
    return false;
  }
}
