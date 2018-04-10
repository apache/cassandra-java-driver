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
package com.datastax.oss.driver.api.core.session;

/**
 * A listener that gets notified when a session is initialized or closed.
 *
 * <p>An implementation of this interface can be registered with {@link
 * SessionBuilder#addSessionLifecycleListeners(SessionLifecycleListener...)} or at runtime with
 * {@link Session#register(SessionLifecycleListener)}.
 *
 * <p>Note that the methods defined by this interface will be executed by internal driver threads,
 * and are therefore expected to have short execution times. If you need to perform long
 * computations or blocking calls in response to schema change events, it is strongly recommended to
 * schedule them asynchronously on a separate thread provided by your application code.
 */
public interface SessionLifecycleListener {

  /**
   * Invoked before the session initialization process starts.
   *
   * @param session the session being initialized.
   */
  default void beforeInit(Session session) {}

  /**
   * Invoked when the session initialization process completes successfully.
   *
   * @param session the session being initialized.
   */
  default void afterInit(Session session) {}

  /**
   * Invoked when the session initialization process fails.
   *
   * @param session the session being initialized.
   * @param error the error the caused the initialization to fail.
   */
  default void onInitFailed(Session session, Throwable error) {}

  /**
   * Invoked before the session shutdown process starts.
   *
   * @param session the session being shut down.
   */
  default void beforeClose(Session session) {}

  /**
   * Invoked after the session shutdown process completes successfully.
   *
   * @param session the session being shut down.
   */
  default void afterClose(Session session) {}

  /**
   * Invoked when the session shutdown process fails.
   *
   * @param session the session being shut down.
   * @param error the error the caused the shutdown to fail.
   */
  default void onCloseFailed(Session session, Throwable error) {}
}
