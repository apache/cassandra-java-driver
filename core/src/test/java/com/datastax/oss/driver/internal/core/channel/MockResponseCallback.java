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
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Predicate;

class MockResponseCallback implements ResponseCallback {
  private final Queue<Object> responses = new ArrayDeque<>();
  private final Predicate<Frame> isLastResponse;

  volatile int streamId = -1;

  MockResponseCallback() {
    this(f -> true);
  }

  MockResponseCallback(Predicate<Frame> isLastResponse) {
    this.isLastResponse = isLastResponse;
  }

  @Override
  public void onResponse(Frame responseFrame) {
    responses.offer(responseFrame);
  }

  @Override
  public void onFailure(Throwable error) {
    responses.offer(error);
  }

  @Override
  public boolean isLastResponse(Frame responseFrame) {
    return isLastResponse.test(responseFrame);
  }

  @Override
  public void onStreamIdAssigned(int streamId) {
    this.streamId = streamId;
  }

  Frame getLastResponse() {
    return (Frame) responses.poll();
  }

  Throwable getFailure() {
    return (Throwable) responses.poll();
  }
}
