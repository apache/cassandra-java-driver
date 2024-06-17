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
package com.datastax.oss.driver.internal.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.context.MockedDriverContextFactory;
import java.net.InetSocketAddress;
import java.util.UUID;
import org.junit.Test;

public class DefaultNodeTest {

  private final String uuidStr = "1e4687e6-f94e-432e-a792-216f89ef265f";
  private final UUID hostId = UUID.fromString(uuidStr);
  private final EndPoint endPoint = new DefaultEndPoint(new InetSocketAddress("localhost", 9042));

  @Test
  public void should_have_expected_string_representation() {

    DefaultNode node = new DefaultNode(endPoint, MockedDriverContextFactory.defaultDriverContext());
    node.hostId = hostId;

    String expected =
        String.format(
            "Node(endPoint=localhost/127.0.0.1:9042, hostId=1e4687e6-f94e-432e-a792-216f89ef265f, hashCode=%x)",
            node.hashCode());
    assertThat(node.toString()).isEqualTo(expected);
  }

  @Test
  public void should_have_expected_string_representation_if_hostid_is_null() {

    DefaultNode node = new DefaultNode(endPoint, MockedDriverContextFactory.defaultDriverContext());
    node.hostId = null;

    String expected =
        String.format(
            "Node(endPoint=localhost/127.0.0.1:9042, hostId=null, hashCode=%x)", node.hashCode());
    assertThat(node.toString()).isEqualTo(expected);
  }
}
