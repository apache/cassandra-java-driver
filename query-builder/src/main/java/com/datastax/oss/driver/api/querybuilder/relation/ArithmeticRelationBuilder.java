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
package com.datastax.oss.driver.api.querybuilder.relation;

import com.datastax.oss.driver.api.querybuilder.term.Term;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public interface ArithmeticRelationBuilder<ResultT> {

  /** Builds an '=' relation with the given term. */
  @NonNull
  default ResultT isEqualTo(@NonNull Term rightOperand) {
    return build("=", rightOperand);
  }

  /** Builds a '&lt;' relation with the given term. */
  @NonNull
  default ResultT isLessThan(@NonNull Term rightOperand) {
    return build("<", rightOperand);
  }

  /** Builds a '&lt;=' relation with the given term. */
  @NonNull
  default ResultT isLessThanOrEqualTo(@NonNull Term rightOperand) {
    return build("<=", rightOperand);
  }

  /** Builds a '&gt;' relation with the given term. */
  @NonNull
  default ResultT isGreaterThan(@NonNull Term rightOperand) {
    return build(">", rightOperand);
  }

  /** Builds a '&gt;=' relation with the given term. */
  @NonNull
  default ResultT isGreaterThanOrEqualTo(@NonNull Term rightOperand) {
    return build(">=", rightOperand);
  }

  /** Builds a '!=' relation with the given term. */
  @NonNull
  default ResultT isNotEqualTo(@NonNull Term rightOperand) {
    return build("!=", rightOperand);
  }

  @NonNull
  ResultT build(@NonNull String operator, @Nullable Term rightOperand);
}
