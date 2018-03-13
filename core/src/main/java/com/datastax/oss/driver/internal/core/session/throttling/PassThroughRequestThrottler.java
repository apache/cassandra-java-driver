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
package com.datastax.oss.driver.internal.core.session.throttling;

import com.datastax.oss.driver.api.core.context.DriverContext;
import java.io.IOException;

/**
 * A request throttler that does not enforce any kind of limitation: requests are always executed
 * immediately.
 */
public class PassThroughRequestThrottler implements RequestThrottler {

  @SuppressWarnings("unused")
  public PassThroughRequestThrottler(DriverContext context) {
    // nothing to do
  }

  @Override
  public void register(Throttled request) {
    request.onThrottleReady(false);
  }

  @Override
  public void signalSuccess(Throttled request) {
    // nothing to do
  }

  @Override
  public void signalError(Throttled request, Throwable error) {
    // nothing to do
  }

  @Override
  public void signalTimeout(Throttled request) {
    // nothing to do
  }

  @Override
  public void close() throws IOException {
    // nothing to do
  }
}
