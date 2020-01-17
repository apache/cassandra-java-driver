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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.util.function.BiPredicate;

/**
 * An extension of TinkerPop's {@link BiPredicate} adding simple pre-condition checking methods that
 * have to be written in the implementations.
 */
public interface DsePredicate extends BiPredicate<Object, Object> {

  default void preEvaluate(Object condition) {
    Preconditions.checkArgument(
        this.isValidCondition(condition), "Invalid condition provided: %s", condition);
  }

  boolean isValidCondition(Object condition);
}
