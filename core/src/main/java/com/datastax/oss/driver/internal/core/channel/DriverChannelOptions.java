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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import net.jcip.annotations.Immutable;

/** Options for the creation of a driver channel. */
@Immutable
public class DriverChannelOptions {

  /** No keyspace, no events, don't report available stream ids. */
  public static DriverChannelOptions DEFAULT = builder().build();

  public static Builder builder() {
    return new Builder();
  }

  public final CqlIdentifier keyspace;

  /**
   * What kind of protocol events to listen for.
   *
   * @see com.datastax.oss.protocol.internal.ProtocolConstants.EventType
   */
  public final List<String> eventTypes;

  public final EventCallback eventCallback;

  public final String ownerLogPrefix;

  private DriverChannelOptions(
      CqlIdentifier keyspace,
      List<String> eventTypes,
      EventCallback eventCallback,
      String ownerLogPrefix) {
    this.keyspace = keyspace;
    this.eventTypes = eventTypes;
    this.eventCallback = eventCallback;
    this.ownerLogPrefix = ownerLogPrefix;
  }

  public static class Builder {
    private CqlIdentifier keyspace = null;
    private List<String> eventTypes = Collections.emptyList();
    private EventCallback eventCallback = null;
    private String ownerLogPrefix = null;

    public Builder withKeyspace(CqlIdentifier keyspace) {
      this.keyspace = keyspace;
      return this;
    }

    public Builder withEvents(List<String> eventTypes, EventCallback eventCallback) {
      Preconditions.checkArgument(eventTypes != null && !eventTypes.isEmpty());
      Preconditions.checkNotNull(eventCallback);
      this.eventTypes = eventTypes;
      this.eventCallback = eventCallback;
      return this;
    }

    public Builder withOwnerLogPrefix(String ownerLogPrefix) {
      this.ownerLogPrefix = ownerLogPrefix;
      return this;
    }

    public DriverChannelOptions build() {
      return new DriverChannelOptions(keyspace, eventTypes, eventCallback, ownerLogPrefix);
    }
  }
}
