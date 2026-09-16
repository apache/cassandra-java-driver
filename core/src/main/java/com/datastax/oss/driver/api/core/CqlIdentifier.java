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

import com.datastax.oss.driver.internal.core.util.Strings;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Locale;
import net.jcip.annotations.Immutable;

/**
 * The identifier of CQL element (keyspace, table, column, etc).
 *
 * <p>It has two representations:
 *
 * <ul>
 *   <li>the "CQL" form, which is how you would type the identifier in a CQL query. It is
 *       case-insensitive unless enclosed in double quotation marks; in addition, identifiers that
 *       contain special characters (anything other than alphanumeric and underscore), or match CQL
 *       keywords, must be double-quoted (with inner double quotes escaped as {@code ""}).
 *   <li>the "internal" form, which is how the name is stored in Cassandra system tables. It is
 *       lower-case for case-sensitive identifiers, and in the exact case for case-sensitive
 *       identifiers.
 * </ul>
 *
 * Examples:
 *
 * <table summary="examples">
 *   <tr><th>Create statement</th><th>Case-sensitive?</th><th>CQL id</th><th>Internal id</th></tr>
 *   <tr><td>CREATE TABLE t(foo int PRIMARY KEY)</td><td>No</td><td>foo</td><td>foo</td></tr>
 *   <tr><td>CREATE TABLE t(Foo int PRIMARY KEY)</td><td>No</td><td>foo</td><td>foo</td></tr>
 *   <tr><td>CREATE TABLE t("Foo" int PRIMARY KEY)</td><td>Yes</td><td>"Foo"</td><td>Foo</td></tr>
 *   <tr><td>CREATE TABLE t("foo bar" int PRIMARY KEY)</td><td>Yes</td><td>"foo bar"</td><td>foo bar</td></tr>
 *   <tr><td>CREATE TABLE t("foo""bar" int PRIMARY KEY)</td><td>Yes</td><td>"foo""bar"</td><td>foo"bar</td></tr>
 *   <tr><td>CREATE TABLE t("create" int PRIMARY KEY)</td><td>Yes (reserved keyword)</td><td>"create"</td><td>create</td></tr>
 * </table>
 *
 * This class provides a common representation and avoids any ambiguity about which form the
 * identifier is in. Driver clients will generally want to create instances from the CQL form with
 * {@link #fromCql(String)}.
 *
 * <p>There is no internal caching; if you reuse the same identifiers often, consider caching them
 * in your application.
 */
@Immutable
public class CqlIdentifier implements Serializable {

  private static final long serialVersionUID = 1;

  // IMPLEMENTATION NOTES:
  // This is used internally, and for all API methods where the overhead of requiring the client to
  // create an instance is acceptable (metadata, statement.getKeyspace, etc.)
  // One exception is named getters, where we keep raw strings with the 3.x rules.

  /** Creates an identifier from its {@link CqlIdentifier CQL form}. */
  @NonNull
  public static CqlIdentifier fromCql(@NonNull String cql) {
    Preconditions.checkNotNull(cql, "cql must not be null");
    final String internal;
    if (Strings.isDoubleQuoted(cql)) {
      internal = Strings.unDoubleQuote(cql);
    } else {
      internal = cql.toLowerCase(Locale.ROOT);
      Preconditions.checkArgument(
          !Strings.needsDoubleQuotes(internal), "Invalid CQL form [%s]: needs double quotes", cql);
    }
    return fromInternal(internal);
  }

  /** Creates an identifier from its {@link CqlIdentifier internal form}. */
  @NonNull
  public static CqlIdentifier fromInternal(@NonNull String internal) {
    Preconditions.checkNotNull(internal, "internal must not be null");
    return new CqlIdentifier(internal);
  }

  /** @serial */
  private final String internal;

  protected CqlIdentifier(String internal) {
    this.internal = internal;
  }

  /**
   * Returns the identifier in the "internal" format.
   *
   * @return the identifier in its exact case, unquoted.
   */
  @NonNull
  public String asInternal() {
    return this.internal;
  }

  /**
   * Returns the identifier in a format appropriate for concatenation in a CQL query.
   *
   * @param pretty if {@code true}, use the shortest possible representation: if the identifier is
   *     case-insensitive, an unquoted, lower-case string, otherwise the double-quoted form. If
   *     {@code false}, always use the double-quoted form (this is slightly more efficient since we
   *     don't need to inspect the string).
   */
  @NonNull
  public String asCql(boolean pretty) {
    if (pretty) {
      return Strings.needsDoubleQuotes(internal) ? Strings.doubleQuote(internal) : internal;
    } else {
      return Strings.doubleQuote(internal);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof CqlIdentifier) {
      CqlIdentifier that = (CqlIdentifier) other;
      return this.internal.equals(that.internal);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return internal.hashCode();
  }

  @Override
  public String toString() {
    return internal;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    Preconditions.checkNotNull(internal, "internal must not be null");
  }
}
