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
package com.datastax.oss.driver.api.core.session;

import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.DefaultSessionLifecycleManager;
import java.util.concurrent.CompletionStage;

/**
 * Provides extra methods for {@link Session} lifecycle. In a suspended state the session should not
 * keep any connections to cluster nodes open.
 */
public interface SessionLifecycleManager {
  /**
   * Creates a new manager for session lifecycle.
   *
   * @param session Session that should be managed.
   * @return The new session lifecycle manager.
   * @throws IllegalArgumentException if the session cannot be managed.
   */
  static SessionLifecycleManager of(Session session) {
    if (session instanceof DefaultSession) {
      return new DefaultSessionLifecycleManager((DefaultSession) session);
    } else {
      throw new IllegalArgumentException(session + " is not an instance of DefaultSession");
    }
  }

  /**
   * Terminates all connections to cluster nodes.
   *
   * @return Stage that completes when all connections are terminated.
   */
  CompletionStage<Void> suspendAsync();

  /** Helper method invoking {@link #suspendAsync()} in a synchronous way. */
  default void suspend() {
    suspendAsync().toCompletableFuture().join();
  }

  /**
   * Triggers reconnection to the cluster. This reconnection proceeds asynchronously; the invocation
   * does not wait for connections establishment.
   */
  void resume();

  /**
   * @return True if the session is {@link #suspendAsync()} was called, until {@link #resumeAsync()}
   *     is called.
   */
  boolean isSuspended();
}
