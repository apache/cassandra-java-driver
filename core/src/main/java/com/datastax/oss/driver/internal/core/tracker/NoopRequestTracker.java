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
package com.datastax.oss.driver.internal.core.tracker;

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class NoopRequestTracker implements RequestTracker {

  public NoopRequestTracker(@SuppressWarnings("unused") DriverContext context) {
    // nothing to do
  }

  @Override
  public void onSuccess(
      Request request, long latencyNanos, DriverConfigProfile configProfile, Node node) {
    // nothing to do
  }

  @Override
  public void onError(
      Request request,
      Throwable error,
      long latencyNanos,
      DriverConfigProfile configProfile,
      Node node) {
    // nothing to do
  }

  @Override
  public void close() throws Exception {
    // nothing to do
  }
}
