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

import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

public interface CreateIndex extends OptionProvider<CreateIndex>, BuildableQuery {

  /**
   * Convenience method for when {@link CreateIndexStart#usingSASI()} is used, provides SASI
   * specific options that are provided under the index 'OPTIONS' property. Is equivalent to {@link
   * #withOption(String, Object) withOption("OPTIONS", sasiOptions)}.
   */
  @NonNull
  default CreateIndex withSASIOptions(@NonNull Map<String, Object> sasiOptions) {
    return withOption("OPTIONS", sasiOptions);
  }
}
