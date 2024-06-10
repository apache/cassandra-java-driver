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
package com.datastax.oss.driver.internal.core.session.throttling;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import net.jcip.annotations.ThreadSafe;

/**
 * A request throttler that does not enforce any kind of limitation: requests are always executed
 * immediately.
 *
 * <p>To activate this throttler, modify the {@code advanced.throttler} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.throttler {
 *     class = PassThroughRequestThrottler
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
@ThreadSafe
public class PassThroughRequestThrottler implements RequestThrottler {

  @SuppressWarnings("unused")
  public PassThroughRequestThrottler(DriverContext context) {
    // nothing to do
  }

  @Override
  public void register(@NonNull Throttled request) {
    request.onThrottleReady(false);
  }

  @Override
  public void signalSuccess(@NonNull Throttled request) {
    // nothing to do
  }

  @Override
  public void signalError(@NonNull Throttled request, @NonNull Throwable error) {
    // nothing to do
  }

  @Override
  public void signalTimeout(@NonNull Throttled request) {
    // nothing to do
  }

  @Override
  public void close() throws IOException {
    // nothing to do
  }
}
