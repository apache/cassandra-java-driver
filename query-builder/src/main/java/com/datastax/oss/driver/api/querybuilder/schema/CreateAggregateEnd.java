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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface CreateAggregateEnd extends BuildableQuery {

  /**
   * Adds INITCOND to the aggregate query. Defines the initial condition, values, of the first
   * parameter in the SFUNC.
   */
  @NonNull
  CreateAggregateEnd withInitCond(@NonNull Term term);

  /**
   * Adds FINALFUNC to the create aggregate query. This is used to specify what type is returned
   * from the state function.
   */
  @NonNull
  CreateAggregateEnd withFinalFunc(@NonNull CqlIdentifier finalFunc);

  /**
   * Shortcut for {@link #withFinalFunc(CqlIdentifier)
   * withFinalFunc(CqlIdentifier.fromCql(finalFuncName))}.
   */
  @NonNull
  default CreateAggregateEnd withFinalFunc(@NonNull String finalFuncName) {
    return withFinalFunc(CqlIdentifier.fromCql(finalFuncName));
  }
}
