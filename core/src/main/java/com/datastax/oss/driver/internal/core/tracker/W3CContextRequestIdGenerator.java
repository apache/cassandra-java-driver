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
package com.datastax.oss.driver.internal.core.tracker;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestIdGenerator;
import com.datastax.oss.driver.shaded.guava.common.io.BaseEncoding;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.security.SecureRandom;
import java.util.Random;

public class W3CContextRequestIdGenerator implements RequestIdGenerator {

  private final Random random = new SecureRandom();
  private final BaseEncoding baseEncoding = BaseEncoding.base16().lowerCase();
  private final String payloadKey;

  public W3CContextRequestIdGenerator(DriverContext context) {
    payloadKey = RequestIdGenerator.super.getCustomPayloadKey();
  }

  public W3CContextRequestIdGenerator(String payloadKey) {
    this.payloadKey = payloadKey;
  }

  /** Random 16 bytes, e.g. "4bf92f3577b34da6a3ce929d0e0e4736" */
  @Override
  public String getSessionRequestId() {
    byte[] bytes = new byte[16];
    random.nextBytes(bytes);
    return baseEncoding.encode(bytes);
  }

  /**
   * Following the format of W3C "traceparent" spec,
   * https://www.w3.org/TR/trace-context/#traceparent-header-field-values e.g.
   * "00-4bf92f3577b34da6a3ce929d0e0e4736-a3ce929d0e0e4736-01" All node requests in the same session
   * request share the same "trace-id" field value
   */
  @Override
  public String getNodeRequestId(@NonNull Request statement, @NonNull String parentId) {
    byte[] bytes = new byte[8];
    random.nextBytes(bytes);
    return String.format("00-%s-%s-00", parentId, baseEncoding.encode(bytes));
  }

  @Override
  public String getCustomPayloadKey() {
    return this.payloadKey;
  }
}
