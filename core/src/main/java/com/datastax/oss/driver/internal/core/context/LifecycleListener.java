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
package com.datastax.oss.driver.internal.core.context;

import com.datastax.oss.driver.api.core.session.SessionBuilder;

/** A component that gets notified of certain events in the session's lifecycle. */
public interface LifecycleListener extends AutoCloseable {

  /**
   * Invoked when the session is ready to process user requests.
   *
   * <p>This corresponds to the moment when the {@link SessionBuilder#build()} returns, or the
   * future returned by {@link SessionBuilder#buildAsync()} completes. If the session initialization
   * fails, this method will not get called.
   *
   * <p>This method is invoked on a driver thread, it should complete relatively quickly and not
   * block.
   */
  void onSessionReady();

  /**
   * Invoked when the session shuts down.
   *
   * <p>Implementations should perform any necessary cleanup, for example freeing resources or
   * cancelling scheduled tasks.
   *
   * <p>Note that this method gets called even if the shutdown results from a failed initialization.
   * In that case, implementations should be ready to handle a call to this method even though
   * {@link #onSessionReady()} hasn't been invoked.
   *
   * <p>This method is invoked on a driver thread, it should complete relatively quickly and not
   * block.
   */
  @Override
  void close() throws Exception;
}
