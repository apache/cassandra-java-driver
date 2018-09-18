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
package com.datastax.oss.driver.api.core;

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Thrown when a query failed on all the coordinators it was tried on. This exception may wrap
 * multiple errors, use {@link #getErrors()} to inspect the individual problem on each node.
 */
public class AllNodesFailedException extends DriverException {

  @NonNull
  public static AllNodesFailedException fromErrors(@Nullable Map<Node, Throwable> errors) {
    if (errors == null || errors.isEmpty()) {
      return new NoNodeAvailableException();
    } else {
      return new AllNodesFailedException(ImmutableMap.copyOf(errors));
    }
  }

  @NonNull
  public static AllNodesFailedException fromErrors(
      @Nullable List<Map.Entry<Node, Throwable>> errors) {
    Map<Node, Throwable> map;
    if (errors == null || errors.isEmpty()) {
      map = null;
    } else {
      ImmutableMap.Builder<Node, Throwable> builder = ImmutableMap.builder();
      for (Map.Entry<Node, Throwable> entry : errors) {
        builder.put(entry);
      }
      map = builder.build();
    }
    return fromErrors(map);
  }

  private final Map<Node, Throwable> errors;

  protected AllNodesFailedException(
      @NonNull String message,
      @Nullable ExecutionInfo executionInfo,
      @NonNull Map<Node, Throwable> errors) {
    super(message, executionInfo, null, true);
    this.errors = errors;
  }

  private AllNodesFailedException(Map<Node, Throwable> errors) {
    this(
        buildMessage(
            String.format("All %d node(s) tried for the query failed", errors.size()), errors),
        null,
        errors);
  }

  private static String buildMessage(String baseMessage, Map<Node, Throwable> errors) {
    int limit = Math.min(errors.size(), 3);
    String details =
        Joiner.on(", ").withKeyValueSeparator(": ").join(Iterables.limit(errors.entrySet(), limit));

    return String.format(
        baseMessage + " (showing first %d, use getErrors() for more: %s)", limit, details);
  }

  /** The details of the individual error on each node. */
  @NonNull
  public Map<Node, Throwable> getErrors() {
    return errors;
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new AllNodesFailedException(getMessage(), getExecutionInfo(), errors);
  }

  @NonNull
  public AllNodesFailedException reword(String newMessage) {
    return new AllNodesFailedException(
        buildMessage(newMessage, errors), getExecutionInfo(), errors);
  }
}
