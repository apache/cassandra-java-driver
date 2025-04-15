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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.tracker.RequestIdGenerator;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Strict.class)
public class RequestIdGeneratorTest {
  @Mock private InternalDriverContext context;
  @Mock private Statement<?> statement;

  @Test
  public void uuid_generator_should_generate() {
    // given
    UuidRequestIdGenerator generator = new UuidRequestIdGenerator(context);
    // when
    String parentId = generator.getSessionRequestId();
    String requestId = generator.getNodeRequestId(statement, parentId);
    // then
    // e.g. "550e8400-e29b-41d4-a716-446655440000", which is 36 characters long
    assertThat(parentId.length()).isEqualTo(36);
    // e.g. "550e8400-e29b-41d4-a716-446655440000-550e8400-e29b-41d4-a716-446655440000", which is 73
    // characters long
    assertThat(requestId.length()).isEqualTo(73);
  }

  @Test
  public void w3c_generator_should_generate() {
    // given
    W3CContextRequestIdGenerator generator = new W3CContextRequestIdGenerator(context);
    // when
    String parentId = generator.getSessionRequestId();
    String requestId = generator.getNodeRequestId(statement, parentId);
    // then
    // e.g. "4bf92f3577b34da6a3ce929d0e0e4736", which is 32 characters long
    assertThat(parentId.length()).isEqualTo(32);
    // According to W3C "traceparent" spec,
    // https://www.w3.org/TR/trace-context/#traceparent-header-field-values
    // e.g. "00-4bf92f3577b34da6a3ce929d0e0e4736-a3ce929d0e0e4736-01", which 55 characters long
    assertThat(requestId.length()).isEqualTo(55);
  }

  @Test
  public void w3c_generator_default_payloadkey() {
    W3CContextRequestIdGenerator w3cGenerator = new W3CContextRequestIdGenerator(context);
    assertThat(w3cGenerator.getCustomPayloadKey())
        .isEqualTo(RequestIdGenerator.DEFAULT_PAYLOAD_KEY);
  }

  @Test
  public void w3c_generator_provided_payloadkey() {
    String someString = RandomStringUtils.random(12);
    W3CContextRequestIdGenerator w3cGenerator = new W3CContextRequestIdGenerator(someString);
    assertThat(w3cGenerator.getCustomPayloadKey()).isEqualTo(someString);
  }
}
