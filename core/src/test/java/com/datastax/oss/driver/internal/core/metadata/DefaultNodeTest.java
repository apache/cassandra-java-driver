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
package com.datastax.oss.driver.internal.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.context.MockedDriverContextFactory;
import java.net.InetSocketAddress;
import java.util.UUID;
import org.junit.Test;

public class DefaultNodeTest {

  @Test
  public void should_have_expected_string_representation() {

    String uuidStr = "1e4687e6-f94e-432e-a792-216f89ef265f";
    UUID hostId = UUID.fromString(uuidStr);
    EndPoint endPoint = new DefaultEndPoint(new InetSocketAddress("localhost", 9042));
    DefaultNode node = new DefaultNode(endPoint, MockedDriverContextFactory.defaultDriverContext());
    node.hostId = hostId;

    String expected =
        String.format(
            "Node(endPoint=localhost/127.0.0.1:9042, hostId=%s, hashCode=%s)",
            uuidStr, Integer.toHexString(node.hashCode()));
    assertThat(node.toString()).isEqualTo(expected);
  }
}
