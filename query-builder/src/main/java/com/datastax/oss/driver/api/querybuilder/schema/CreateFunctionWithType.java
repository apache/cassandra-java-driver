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
package com.datastax.oss.driver.api.querybuilder.schema;

import edu.umd.cs.findbugs.annotations.NonNull;

public interface CreateFunctionWithType {
  /**
   * Adds LANGUAGE to the create function specification. This is used to specify what language is
   * used in the function body.
   */
  @NonNull
  CreateFunctionWithLanguage withLanguage(@NonNull String language);

  /**
   * Adds "LANGUAGE java" to create function specification. Shortcut for {@link
   * #withLanguage(String) withLanguage("java")}.
   */
  @NonNull
  default CreateFunctionWithLanguage withJavaLanguage() {
    return withLanguage("java");
  }

  /**
   * Adds "LANGUAGE javascript" to create function specification. Shortcut for {@link
   * #withLanguage(String) withLanguage("javascript")}.
   */
  @NonNull
  default CreateFunctionWithLanguage withJavaScriptLanguage() {
    return withLanguage("javascript");
  }
}
