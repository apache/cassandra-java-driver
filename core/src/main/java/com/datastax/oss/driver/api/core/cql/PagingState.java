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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.cql.DefaultPagingState;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;

/**
 * A safe wrapper around the paging state of a query.
 *
 * <p>This class performs additional checks to fail fast if the paging state is not reused on the
 * same query, and it provides utility methods for conversion to/from strings and byte arrays.
 *
 * <p>The serialized form returned by {@link #toBytes()} and {@link Object#toString()} is an opaque
 * sequence of bytes. Note however that it is <b>not cryptographically secure</b>: the contents are
 * not encrypted and the checks are performed with a simple MD5 checksum. If you need stronger
 * guarantees, you should build your own wrapper around {@link ExecutionInfo#getPagingState()}.
 */
public interface PagingState {

  /** Parses an instance from a string previously generated with {@code toString()}. */
  @NonNull
  static PagingState fromString(@NonNull String string) {
    return DefaultPagingState.fromString(string);
  }

  /** Parses an instance from a byte array previously generated with {@link #toBytes()}. */
  @NonNull
  static PagingState fromBytes(byte[] bytes) {
    return DefaultPagingState.fromBytes(bytes);
  }

  /** Returns a representation of this object as a byte array. */
  byte[] toBytes();

  /**
   * Checks if this paging state can be safely reused for the given statement. Specifically, the
   * query string and any bound values must match.
   *
   * <p>Note that, if {@code statement} is a {@link SimpleStatement} with bound values, those values
   * must be encoded in order to perform the check. This method uses the default codec registry and
   * default protocol version. This might fail if you use custom codecs; in that case, use {@link
   * #matches(Statement, Session)} instead.
   *
   * <p>If {@code statement} is a {@link BoundStatement}, it is always safe to call this method.
   */
  default boolean matches(@NonNull Statement<?> statement) {
    return matches(statement, null);
  }

  /**
   * Alternative to {@link #matches(Statement)} that specifies the session the statement will be
   * executed with. <b>You only need this for simple statements, and if you use custom codecs.</b>
   * Bound statements already know which session they are attached to.
   */
  boolean matches(@NonNull Statement<?> statement, @Nullable Session session);

  /**
   * Returns the underlying "unsafe" paging state (the equivalent of {@link
   * ExecutionInfo#getPagingState()}).
   */
  @NonNull
  ByteBuffer getRawPagingState();
}
