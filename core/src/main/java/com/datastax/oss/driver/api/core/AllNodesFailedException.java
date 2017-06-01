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
package com.datastax.oss.driver.api.core;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.Map;

/**
 * Thrown when a query failed on all the coordinators it was tried on. This exception may wrap
 * multiple errors, use {@link #getErrors()} to inspect the individual problem on each node.
 */
public class AllNodesFailedException extends DriverException {
  public static AllNodesFailedException fromErrors(Map<Node, Throwable> errors) {
    if (errors == null || errors.size() == 0) {
      return new NoNodeAvailableException();
    } else {
      return new AllNodesFailedException(ImmutableMap.copyOf(errors));
    }
  }

  private final Map<Node, Throwable> errors;

  protected AllNodesFailedException(String message, Map<Node, Throwable> errors) {
    super(message, null, true);
    this.errors = errors;
  }

  private AllNodesFailedException(Map<Node, Throwable> errors) {
    this(buildMessage(errors), errors);
  }

  private static String buildMessage(Map<Node, Throwable> errors) {
    int limit = Math.min(errors.size(), 3);
    String details =
        Joiner.on(", ").withKeyValueSeparator(": ").join(Iterables.limit(errors.entrySet(), limit));

    return String.format(
        "All %d node tried for the query failed (showing first %d, use getErrors() for more: %s)",
        errors.size(), limit, details);
  }

  /** The details of the individual error on each node. */
  public Map<Node, Throwable> getErrors() {
    return errors;
  }

  @Override
  public DriverException copy() {
    return new AllNodesFailedException(getMessage(), errors);
  }
}
