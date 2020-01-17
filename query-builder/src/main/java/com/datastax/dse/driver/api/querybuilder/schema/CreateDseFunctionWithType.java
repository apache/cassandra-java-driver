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
package com.datastax.dse.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface CreateDseFunctionWithType {
  /**
   * Adds LANGUAGE to the create function specification. This is used to specify what language is
   * used in the function body.
   */
  @NonNull
  CreateDseFunctionWithLanguage withLanguage(@NonNull String language);

  /**
   * Adds "LANGUAGE java" to create function specification. Shortcut for {@link
   * #withLanguage(String) withLanguage("java")}.
   */
  @NonNull
  default CreateDseFunctionWithLanguage withJavaLanguage() {
    return withLanguage("java");
  }

  /**
   * Adds "LANGUAGE javascript" to create function specification. Shortcut for {@link
   * #withLanguage(String) withLanguage("javascript")}.
   */
  @NonNull
  default CreateDseFunctionWithLanguage withJavaScriptLanguage() {
    return withLanguage("javascript");
  }

  /**
   * Adds "DETERMINISTIC" to create function specification. This is used to specify that this
   * function always returns the same output for a given input.
   */
  @NonNull
  CreateDseFunctionWithType deterministic();

  /**
   * Adds "MONOTONIC" to create function specification. This is used to specify that this function
   * is either entirely non-increasing, or entirely non-decreasing.
   */
  @NonNull
  CreateDseFunctionWithType monotonic();

  /**
   * Adds "MONOTONIC ON" to create function specification. This is used to specify that this
   * function has only a single column that is monotonic. If the function is fully monotonic, use
   * {@link #monotonic()} instead.
   */
  @NonNull
  CreateDseFunctionWithType monotonicOn(@NonNull CqlIdentifier monotonicColumn);

  /**
   * Shortcut for {@link #monotonicOn(CqlIdentifier)
   * monotonicOn(CqlIdentifier.fromCql(monotonicColumn))}.
   */
  @NonNull
  default CreateDseFunctionWithType monotonicOn(@NonNull String monotonicColumn) {
    return monotonicOn(CqlIdentifier.fromCql(monotonicColumn));
  }
}
