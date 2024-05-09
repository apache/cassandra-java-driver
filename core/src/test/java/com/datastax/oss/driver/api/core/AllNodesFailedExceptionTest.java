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
package com.datastax.oss.driver.api.core;

import static com.datastax.oss.driver.api.core.ConsistencyLevel.QUORUM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.MapEntry.entry;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AllNodesFailedExceptionTest {

  @Mock(name = "node1")
  private Node node1;

  @Mock(name = "node2")
  private Node node2;

  @SuppressWarnings("deprecation")
  @Test
  public void should_create_instance_from_map_of_first_errors() {
    // given
    UnavailableException e1 = new UnavailableException(node1, QUORUM, 2, 1);
    ReadTimeoutException e2 = new ReadTimeoutException(node2, QUORUM, 2, 1, false);
    Map<Node, Throwable> errors = ImmutableMap.of(node1, e1, node2, e2);
    // when
    AllNodesFailedException e = AllNodesFailedException.fromErrors(errors);
    // then
    assertThat(e)
        .hasMessage(
            "All 2 node(s) tried for the query failed "
                + "(showing first 2 nodes, use getAllErrors() for more): "
                + "node1: [%s], node2: [%s]",
            e1, e2);
    assertThat(e.getAllErrors())
        .hasEntrySatisfying(node1, list -> assertThat(list).containsExactly(e1));
    assertThat(e.getAllErrors())
        .hasEntrySatisfying(node2, list -> assertThat(list).containsExactly(e2));
    assertThat(e.getErrors()).containsEntry(node1, e1);
    assertThat(e.getErrors()).containsEntry(node2, e2);
    assertThat(e).hasSuppressedException(e1).hasSuppressedException(e2);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void should_create_instance_from_list_of_all_errors() {
    // given
    UnavailableException e1a = new UnavailableException(node1, QUORUM, 2, 1);
    ReadTimeoutException e1b = new ReadTimeoutException(node1, QUORUM, 2, 1, false);
    ReadTimeoutException e2a = new ReadTimeoutException(node2, QUORUM, 2, 1, false);
    List<Entry<Node, Throwable>> errors =
        ImmutableList.of(entry(node1, e1a), entry(node1, e1b), entry(node2, e2a));
    // when
    AllNodesFailedException e = AllNodesFailedException.fromErrors(errors);
    // then
    assertThat(e)
        .hasMessage(
            "All 2 node(s) tried for the query failed "
                + "(showing first 2 nodes, use getAllErrors() for more): "
                + "node1: [%s, %s], node2: [%s]",
            e1a, e1b, e2a);
    assertThat(e.getAllErrors())
        .hasEntrySatisfying(node1, list -> assertThat(list).containsExactly(e1a, e1b));
    assertThat(e.getAllErrors())
        .hasEntrySatisfying(node2, list -> assertThat(list).containsExactly(e2a));
    assertThat(e.getErrors()).containsEntry(node1, e1a);
    assertThat(e.getErrors()).containsEntry(node2, e2a);
    assertThat(e)
        .hasSuppressedException(e1a)
        .hasSuppressedException(e1b)
        .hasSuppressedException(e2a);
  }
}
