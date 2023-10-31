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
package com.datastax.dse.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import javax.annotation.Nonnull;

public interface CreateDseFunctionWithType {
  /**
   * Adds LANGUAGE to the create function specification. This is used to specify what language is
   * used in the function body.
   */
  @Nonnull
  CreateDseFunctionWithLanguage withLanguage(@Nonnull String language);

  /**
   * Adds "LANGUAGE java" to create function specification. Shortcut for {@link
   * #withLanguage(String) withLanguage("java")}.
   */
  @Nonnull
  default CreateDseFunctionWithLanguage withJavaLanguage() {
    return withLanguage("java");
  }

  /**
   * Adds "LANGUAGE javascript" to create function specification. Shortcut for {@link
   * #withLanguage(String) withLanguage("javascript")}.
   */
  @Nonnull
  default CreateDseFunctionWithLanguage withJavaScriptLanguage() {
    return withLanguage("javascript");
  }

  /**
   * Adds "DETERMINISTIC" to create function specification. This is used to specify that this
   * function always returns the same output for a given input.
   */
  @Nonnull
  CreateDseFunctionWithType deterministic();

  /**
   * Adds "MONOTONIC" to create function specification. This is used to specify that this function
   * is either entirely non-increasing, or entirely non-decreasing.
   */
  @Nonnull
  CreateDseFunctionWithType monotonic();

  /**
   * Adds "MONOTONIC ON" to create function specification. This is used to specify that this
   * function has only a single column that is monotonic. If the function is fully monotonic, use
   * {@link #monotonic()} instead.
   */
  @Nonnull
  CreateDseFunctionWithType monotonicOn(@Nonnull CqlIdentifier monotonicColumn);

  /**
   * Shortcut for {@link #monotonicOn(CqlIdentifier)
   * monotonicOn(CqlIdentifier.fromCql(monotonicColumn))}.
   */
  @Nonnull
  default CreateDseFunctionWithType monotonicOn(@Nonnull String monotonicColumn) {
    return monotonicOn(CqlIdentifier.fromCql(monotonicColumn));
  }
}
