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

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Thrown when a query failed on all the coordinators it was tried on. This exception may wrap
 * multiple errors, that are available either as {@linkplain #getSuppressed() suppressed
 * exceptions}, or via {@link #getAllErrors()} where they are grouped by node.
 */
public class AllNodesFailedException extends DriverException {

  /** @deprecated Use {@link #fromErrors(List)} instead. */
  @NonNull
  @Deprecated
  public static AllNodesFailedException fromErrors(@Nullable Map<Node, Throwable> errors) {
    if (errors == null || errors.isEmpty()) {
      return new NoNodeAvailableException();
    } else {
      return new AllNodesFailedException(groupByNode(errors));
    }
  }

  @NonNull
  public static AllNodesFailedException fromErrors(@Nullable List<Entry<Node, Throwable>> errors) {
    if (errors == null || errors.isEmpty()) {
      return new NoNodeAvailableException();
    } else {
      return new AllNodesFailedException(groupByNode(errors));
    }
  }

  private final Map<Node, List<Throwable>> errors;

  /** @deprecated Use {@link #AllNodesFailedException(String, ExecutionInfo, Iterable)} instead. */
  @Deprecated
  protected AllNodesFailedException(
      @NonNull String message,
      @Nullable ExecutionInfo executionInfo,
      @NonNull Map<Node, Throwable> errors) {
    super(message, executionInfo, null, true);
    this.errors = toDeepImmutableMap(groupByNode(errors));
    addSuppressedErrors();
  }

  protected AllNodesFailedException(
      @NonNull String message,
      @Nullable ExecutionInfo executionInfo,
      @NonNull Iterable<Entry<Node, List<Throwable>>> errors) {
    super(message, executionInfo, null, true);
    this.errors = toDeepImmutableMap(errors);
    addSuppressedErrors();
  }

  private void addSuppressedErrors() {
    for (List<Throwable> errors : this.errors.values()) {
      for (Throwable error : errors) {
        addSuppressed(error);
      }
    }
  }

  private AllNodesFailedException(Map<Node, List<Throwable>> errors) {
    this(
        buildMessage(
            String.format("All %d node(s) tried for the query failed", errors.size()), errors),
        null,
        errors.entrySet());
  }

  private static String buildMessage(String baseMessage, Map<Node, List<Throwable>> errors) {
    int limit = Math.min(errors.size(), 3);
    Iterator<Entry<Node, List<Throwable>>> iterator =
        Iterables.limit(errors.entrySet(), limit).iterator();
    StringBuilder details = new StringBuilder();
    while (iterator.hasNext()) {
      Entry<Node, List<Throwable>> entry = iterator.next();
      details.append(entry.getKey()).append(": ").append(entry.getValue());
      if (iterator.hasNext()) {
        details.append(", ");
      }
    }
    return String.format(
        "%s (showing first %d nodes, use getAllErrors() for more): %s",
        baseMessage, limit, details);
  }

  /**
   * An immutable map containing the first error on each tried node.
   *
   * @deprecated Use {@link #getAllErrors()} instead.
   */
  @NonNull
  @Deprecated
  public Map<Node, Throwable> getErrors() {
    ImmutableMap.Builder<Node, Throwable> builder = ImmutableMap.builder();
    for (Node node : errors.keySet()) {
      List<Throwable> nodeErrors = errors.get(node);
      if (!nodeErrors.isEmpty()) {
        builder.put(node, nodeErrors.get(0));
      }
    }
    return builder.build();
  }

  /** An immutable map containing all errors on each tried node. */
  @NonNull
  public Map<Node, List<Throwable>> getAllErrors() {
    return errors;
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new AllNodesFailedException(getMessage(), getExecutionInfo(), errors.entrySet());
  }

  @NonNull
  public AllNodesFailedException reword(String newMessage) {
    return new AllNodesFailedException(
        buildMessage(newMessage, errors), getExecutionInfo(), errors.entrySet());
  }

  private static Map<Node, List<Throwable>> groupByNode(Map<Node, Throwable> errors) {
    return groupByNode(errors.entrySet());
  }

  private static Map<Node, List<Throwable>> groupByNode(Iterable<Entry<Node, Throwable>> errors) {
    // no need for immutable collections here
    Map<Node, List<Throwable>> map = new LinkedHashMap<>();
    for (Entry<Node, Throwable> entry : errors) {
      Node node = entry.getKey();
      Throwable error = entry.getValue();
      map.compute(
          node,
          (k, v) -> {
            if (v == null) {
              v = new ArrayList<>();
            }
            v.add(error);
            return v;
          });
    }
    return map;
  }

  private static Map<Node, List<Throwable>> toDeepImmutableMap(Map<Node, List<Throwable>> errors) {
    return toDeepImmutableMap(errors.entrySet());
  }

  private static Map<Node, List<Throwable>> toDeepImmutableMap(
      Iterable<Entry<Node, List<Throwable>>> errors) {
    ImmutableMap.Builder<Node, List<Throwable>> builder = ImmutableMap.builder();
    for (Entry<Node, List<Throwable>> entry : errors) {
      builder.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
    }
    return builder.build();
  }
}
