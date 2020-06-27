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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import java.util.concurrent.CompletionStage;

/**
 * An initially disconnected {@link CqlSession} that can be lazily initialized on demand.
 *
 * <p>instances of this interface are created uninitialized; initialization and connection is
 * triggered by the first call to {@link #init()}.
 */
public interface LazyCqlSession extends CqlSession {

  /**
   * Triggers an asynchronous session initialization on the first call to this method.
   *
   * <p>Subsequent calls to this method on the same instance are no-ops.
   *
   * @return A stage that completes when the session is fully initialized.
   */
  CompletionStage<Void> init();

  /** @return true if the session is initialized, false otherwise. */
  boolean isInitialized();
}
