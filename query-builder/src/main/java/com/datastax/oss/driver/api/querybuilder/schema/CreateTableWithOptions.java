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
import com.datastax.oss.driver.internal.querybuilder.schema.RawOptionsWrapper;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

public interface CreateTableWithOptions
    extends BuildableQuery, RelationStructure<CreateTableWithOptions> {

  /** Enables COMPACT STORAGE in the CREATE TABLE statement. */
  @NonNull
  CreateTableWithOptions withCompactStorage();

  /** Attaches custom metadata to CQL table definition. */
  @NonNull
  @CheckReturnValue
  default CreateTableWithOptions withExtensions(@NonNull Map<String, byte[]> extensions) {
    return withOption("extensions", Maps.transformValues(extensions, RawOptionsWrapper::of));
  }
}
