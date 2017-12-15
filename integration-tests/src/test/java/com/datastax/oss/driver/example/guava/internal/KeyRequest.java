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
package com.datastax.oss.driver.example.guava.internal;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.session.Request;
import java.nio.ByteBuffer;
import java.util.Map;

/** A custom request that simply wraps an integer key and uses it as a parameter for a query. */
public class KeyRequest implements Request {

  private final int key;

  public KeyRequest(int key) {
    this.key = key;
  }

  public int getKey() {
    return key;
  }

  @Override
  public String getConfigProfileName() {
    return null;
  }

  @Override
  public DriverConfigProfile getConfigProfile() {
    return null;
  }

  @Override
  public CqlIdentifier getKeyspace() {
    return null;
  }

  @Override
  public CqlIdentifier getRoutingKeyspace() {
    return null;
  }

  @Override
  public ByteBuffer getRoutingKey() {
    return null;
  }

  @Override
  public Token getRoutingToken() {
    return null;
  }

  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return null;
  }

  @Override
  public Boolean isIdempotent() {
    return true;
  }

  @Override
  public boolean isTracing() {
    return false;
  }
}
