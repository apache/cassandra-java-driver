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
package com.datastax.oss.driver.api.querybuilder.schema;

import edu.umd.cs.findbugs.annotations.NonNull;

public interface CreateFunctionWithLanguage {

  /**
   * Adds AS to the create function specification. This is used to specify the body of the function.
   * Note that it is expected that the provided body is properly quoted as this method does not make
   * that decision for the user. For simple cases, one should wrap the input in single quotes, i.e.
   * <code>'myBody'</code>. If the body itself contains single quotes, one could use a
   * postgres-style string literal, which is surrounded in two dollar signs, i.e. <code>$$ myBody $$
   * </code>.
   */
  @NonNull
  CreateFunctionEnd as(@NonNull String functionBody);

  /**
   * Adds AS to the create function specification and quotes the function body. Assumes that if the
   * input body contains at least one single quote, to quote the body with two dollar signs, i.e.
   * <code>$$ myBody $$</code>, otherwise the body is quoted with single quotes, i.e. <code>
   * ' myBody '</code>. If the function body is already quoted {@link #as(String)} should be used
   * instead.
   */
  @NonNull
  default CreateFunctionEnd asQuoted(@NonNull String functionBody) {
    if (functionBody.contains("'")) {
      return as("$$ " + functionBody + " $$");
    } else {
      return as('\'' + functionBody + '\'');
    }
  }
}
