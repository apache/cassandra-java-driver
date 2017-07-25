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

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Handle options common to all statement builders.
 *
 * @see SimpleStatement#builder(String)
 * @see BatchStatement#builder(BatchType)
 * @see PreparedStatement#boundStatementBuilder()
 */
public abstract class StatementBuilder<T extends StatementBuilder<T, S>, S extends Statement<S>> {

  @SuppressWarnings("unchecked")
  private final T self = (T) this;

  protected String configProfileName;
  protected DriverConfigProfile configProfile;
  protected String keyspace;
  protected ImmutableMap.Builder<String, ByteBuffer> customPayloadBuilder;
  protected Boolean idempotent;
  protected boolean tracing;
  protected long timestamp = Long.MIN_VALUE;
  protected ByteBuffer pagingState;

  protected StatementBuilder() {
    // nothing to do
  }

  protected StatementBuilder(S template) {
    this.configProfileName = template.getConfigProfileName();
    this.configProfile = template.getConfigProfile();
    this.customPayloadBuilder =
        ImmutableMap.<String, ByteBuffer>builder().putAll(template.getCustomPayload());
    this.idempotent = template.isIdempotent();
    this.tracing = template.isTracing();
    this.timestamp = template.getTimestamp();
    this.pagingState = template.getPagingState();
  }

  /** @see Statement#setConfigProfileName(String) */
  public T withConfigProfileName(String configProfileName) {
    this.configProfileName = configProfileName;
    return self;
  }

  /** @see Statement#setConfigProfile(DriverConfigProfile) */
  public T withConfigProfile(DriverConfigProfile configProfile) {
    this.configProfile = configProfile;
    this.configProfileName = null;
    return self;
  }

  /** @see Statement#setCustomPayload(Map) */
  public T addCustomPayload(String key, ByteBuffer value) {
    if (customPayloadBuilder == null) {
      customPayloadBuilder = ImmutableMap.builder();
    }
    customPayloadBuilder.put(key, value);
    return self;
  }

  /** @see Statement#setCustomPayload(Map) */
  public T clearCustomPayload() {
    customPayloadBuilder = null;
    return self;
  }

  /** @see Statement#setIdempotent(Boolean) */
  public T withIdempotence(boolean idempotent) {
    this.idempotent = idempotent;
    return self;
  }

  /** @see Statement#setTracing(boolean) */
  public T withTracing() {
    this.tracing = true;
    return self;
  }

  /** @see Statement#setTimestamp(long) */
  public T withTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return self;
  }

  /** @see Statement#setPagingState(ByteBuffer) */
  public T withPagingState(ByteBuffer pagingState) {
    this.pagingState = pagingState;
    return self;
  }

  public abstract S build();
}
