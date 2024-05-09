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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.cql.TraceEvent;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Date;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultTraceEvent implements TraceEvent {

  private final String activity;
  private final long timestamp;
  private final InetSocketAddress source;
  private final int sourceElapsedMicros;
  private final String threadName;

  public DefaultTraceEvent(
      String activity,
      long timestamp,
      InetSocketAddress source,
      int sourceElapsedMicros,
      String threadName) {
    this.activity = activity;
    // Convert the UUID timestamp to an epoch timestamp
    this.timestamp = (timestamp - 0x01b21dd213814000L) / 10000;
    this.source = source;
    this.sourceElapsedMicros = sourceElapsedMicros;
    this.threadName = threadName;
  }

  @Override
  public String getActivity() {
    return activity;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  @Deprecated
  public InetAddress getSource() {
    return source.getAddress();
  }

  @Override
  public InetSocketAddress getSourceAddress() {
    return source;
  }

  @Override
  public int getSourceElapsedMicros() {
    return sourceElapsedMicros;
  }

  @Override
  public String getThreadName() {
    return threadName;
  }

  @Override
  public String toString() {
    return String.format("%s on %s[%s] at %s", activity, source, threadName, new Date(timestamp));
  }
}
