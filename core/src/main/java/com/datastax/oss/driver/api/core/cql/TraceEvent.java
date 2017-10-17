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
package com.datastax.oss.driver.api.core.cql;

import java.net.InetAddress;

/** An event in a {@link QueryTrace}. */
public interface TraceEvent {

  /** Which activity this event corresponds to. */
  String getActivity();

  /** The server-side timestamp of the event. */
  long getTimestamp();

  /** The IP of the host having generated this event. */
  InetAddress getSource();

  /**
   * The number of microseconds elapsed on the source when this event occurred since the moment when
   * the source started handling the query.
   */
  int getSourceElapsedMicros();

  /** The name of the thread on which this event occurred. */
  String getThreadName();
}
