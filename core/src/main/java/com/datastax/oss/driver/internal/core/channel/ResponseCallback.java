/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
   * Whether to hold the stream id beyond the first response.
   *
   * <p>By default, this is false, and the channel will release the stream id (and make it available
   * for other requests) as soon as {@link #onResponse(Frame)} or {@link #onFailure(Throwable)} gets
   * invoked.
   *
   * <p>If this is true, the channel will keep the stream id assigned to this request, and {@code
   * onResponse} might be invoked multiple times. {@link #onStreamIdAssigned(int)} will be called to
   * notify the caller of the stream id, and it is the caller's responsibility to determine when the
   * request is over, and then call {@link DriverChannel#release(int)} to release the stream id.
   *
   * <p>This is intended to allow streaming requests, that would send multiple chunks of data in
   * response to a single request (this feature does not exist yet in Cassandra but might be
   * implemented in the future).
   */
  default boolean holdStreamId() {
    return false;
  }

  /**
   * Reports the stream id to the caller if {@link #holdStreamId()} is true.
   *
   * <p>By default, this will never get called.
   */
  default void onStreamIdAssigned(int streamId) {
    // nothing to do by default
  }
}
