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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;

/** Options for the creation of a driver channel. */
public class DriverChannelOptions {

  /** No keyspace, no events, don't report available stream ids. */
  public static DriverChannelOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new Builder();
  }

  public final CqlIdentifier keyspace;
  /** Whether {@link DriverChannel#availableIds()} should be maintained */
  public final boolean reportAvailableIds;

  /**
   * What kind of protocol events to listen for.
   *
   * @see com.datastax.oss.protocol.internal.ProtocolConstants.EventType
   */
  public final List<String> eventTypes;

  public final EventCallback eventCallback;

  private DriverChannelOptions(
      CqlIdentifier keyspace,
      boolean reportAvailableIds,
      List<String> eventTypes,
      EventCallback eventCallback) {
    this.keyspace = keyspace;
    this.reportAvailableIds = reportAvailableIds;
    this.eventTypes = eventTypes;
    this.eventCallback = eventCallback;
  }

  public static class Builder {
    private CqlIdentifier keyspace = null;
    private boolean reportAvailableIds = false;
    private List<String> eventTypes = Collections.emptyList();
    private EventCallback eventCallback = null;

    public Builder withKeyspace(CqlIdentifier keyspace) {
      this.keyspace = keyspace;
      return this;
    }

    public Builder reportAvailableIds(boolean reportAvailableIds) {
      this.reportAvailableIds = reportAvailableIds;
      return this;
    }

    public Builder withEvents(List<String> eventTypes, EventCallback eventCallback) {
      Preconditions.checkArgument(eventTypes != null && !eventTypes.isEmpty());
      Preconditions.checkNotNull(eventCallback);
      this.eventTypes = eventTypes;
      this.eventCallback = eventCallback;
      return this;
    }

    public DriverChannelOptions build() {
      return new DriverChannelOptions(keyspace, reportAvailableIds, eventTypes, eventCallback);
    }
  }
}
