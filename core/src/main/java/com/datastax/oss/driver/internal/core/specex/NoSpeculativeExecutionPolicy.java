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
package com.datastax.oss.driver.internal.core.specex;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.ThreadSafe;

/**
 * A policy that never triggers speculative executions.
 *
 * <p>To activate this policy, modify the {@code advanced.speculative-execution-policy} section in
 * the driver configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.speculative-execution-policy {
 *     class = NoSpeculativeExecutionPolicy
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
@ThreadSafe
public class NoSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {

  public NoSpeculativeExecutionPolicy(
      @SuppressWarnings("unused") DriverContext context,
      @SuppressWarnings("unused") String profileName) {
    // nothing to do
  }

  @Override
  @SuppressWarnings("unused")
  public long nextExecution(
      @NonNull Node node,
      @Nullable CqlIdentifier keyspace,
      @NonNull Request request,
      int runningExecutions) {
    // never start speculative executions
    return -1;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
