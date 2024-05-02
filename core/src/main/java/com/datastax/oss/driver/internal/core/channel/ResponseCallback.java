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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.protocol.internal.Frame;

/**
 * The outcome of a request sent to a Cassandra node.
 *
 * <p>This comes into play after the request has been successfully written to the channel.
 *
 * <p>Due to internal implementation constraints, different instances of this type must not be equal
 * to each other (they are stored in a {@code BiMap} in {@link InFlightHandler}); reference equality
 * should be appropriate in all cases.
 */
public interface ResponseCallback {

  /**
   * Invoked when the server replies (note that the response frame might contain an error message).
   */
  void onResponse(Frame responseFrame);

  /**
   * Invoked if we couldn't get the response.
   *
   * <p>This can be triggered in two cases:
   *
   * <ul>
   *   <li>the connection was closed (for example, because of a heartbeat failure) before the
   *       response was received;
   *   <li>the response was received but there was an error while decoding it.
   * </ul>
   */
  void onFailure(Throwable error);

  /**
   * Reports the stream id used for the request on the current connection.
   *
   * <p>This is called every time the request is written successfully to a connection (and therefore
   * might multiple times in case of retries). It is guaranteed to be invoked before any response to
   * the request on that connection is processed.
   *
   * <p>The default implementation does nothing. This only needs to be overridden for specialized
   * requests that hold the stream id across multiple responses.
   *
   * @see #isLastResponse(Frame)
   */
  default void onStreamIdAssigned(int streamId) {
    // nothing to do
  }

  /**
   * Whether the given frame is the last response to this request.
   *
   * <p>This is invoked for each response received by this callback; if it returns {@code true}, the
   * driver assumes that the server is no longer using this stream id, and that it can be safely
   * reused to send another request.
   *
   * <p>The default implementation always returns {@code true}: regular CQL requests only have one
   * response, and we can reuse the stream id as soon as we've received it. This only needs to be
   * overridden for specialized requests that hold the stream id across multiple responses.
   */
  default boolean isLastResponse(Frame responseFrame) {
    return true;
  }
}
