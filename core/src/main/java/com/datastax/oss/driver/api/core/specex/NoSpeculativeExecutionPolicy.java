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
package com.datastax.oss.driver.api.core.specex;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;

/** A policy that never triggers speculative executions. */
public class NoSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {

  public NoSpeculativeExecutionPolicy(@SuppressWarnings("unused") DriverContext context) {
    // nothing to do
  }

  @Override
  @SuppressWarnings("unused")
  public long nextExecution(
      Node node, CqlIdentifier keyspace, Request request, int runningExecutions) {
    // never start speculative executions
    return -1;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
