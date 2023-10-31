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
package com.datastax.oss.driver.api.querybuilder;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.truncate.Truncate;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.internal.core.metadata.schema.ShallowUserDefinedType;
import com.datastax.oss.driver.internal.core.metadata.token.ByteOrderedToken;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.RandomToken;
import com.datastax.oss.driver.internal.querybuilder.ArithmeticOperator;
import com.datastax.oss.driver.internal.querybuilder.DefaultLiteral;
import com.datastax.oss.driver.internal.querybuilder.DefaultRaw;
import com.datastax.oss.driver.internal.querybuilder.delete.DefaultDelete;
import com.datastax.oss.driver.internal.querybuilder.insert.DefaultInsert;
import com.datastax.oss.driver.internal.querybuilder.select.DefaultBindMarker;
import com.datastax.oss.driver.internal.querybuilder.select.DefaultSelect;
import com.datastax.oss.driver.internal.querybuilder.term.BinaryArithmeticTerm;
import com.datastax.oss.driver.internal.querybuilder.term.FunctionTerm;
import com.datastax.oss.driver.internal.querybuilder.term.OppositeTerm;
import com.datastax.oss.driver.internal.querybuilder.term.TupleTerm;
import com.datastax.oss.driver.internal.querybuilder.term.TypeHintTerm;
import com.datastax.oss.driver.internal.querybuilder.truncate.DefaultTruncate;
import com.datastax.oss.driver.internal.querybuilder.update.DefaultUpdate;
import java.util.Arrays;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** A Domain-Specific Language to build CQL queries using Java code. */
public class QueryBuilder {

  /** Starts a SELECT query for a qualified table. */
  @Nonnull
  public static SelectFrom selectFrom(
      @Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier table) {
    return new DefaultSelect(keyspace, table);
  }

  /**
   * Shortcut for {@link #selectFrom(CqlIdentifier, CqlIdentifier)
   * selectFrom(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(table))}
   */
  @Nonnull
  public static SelectFrom selectFrom(@Nullable String keyspace, @Nonnull String table) {
    return selectFrom(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(table));
  }

  /** Starts a SELECT query for an unqualified table. */
  @Nonnull
  public static SelectFrom selectFrom(@Nonnull CqlIdentifier table) {
    return selectFrom(null, table);
  }

  /** Shortcut for {@link #selectFrom(CqlIdentifier) selectFrom(CqlIdentifier.fromCql(table))} */
  @Nonnull
  public static SelectFrom selectFrom(@Nonnull String table) {
    return selectFrom(CqlIdentifier.fromCql(table));
  }

  /** Starts an INSERT query for a qualified table. */
  @Nonnull
  public static InsertInto insertInto(
      @Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier table) {
    return new DefaultInsert(keyspace, table);
  }

  /**
   * Shortcut for {@link #insertInto(CqlIdentifier, CqlIdentifier)
   * insertInto(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(table))}.
   */
  @Nonnull
  public static InsertInto insertInto(@Nullable String keyspace, @Nonnull String table) {
    return insertInto(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(table));
  }

  /** Starts an INSERT query for an unqualified table. */
  @Nonnull
  public static InsertInto insertInto(@Nonnull CqlIdentifier table) {
    return insertInto(null, table);
  }

  /** Shortcut for {@link #insertInto(CqlIdentifier) insertInto(CqlIdentifier.fromCql(table))}. */
  @Nonnull
  public static InsertInto insertInto(@Nonnull String table) {
    return insertInto(CqlIdentifier.fromCql(table));
  }

  /** Starts an UPDATE query for a qualified table. */
  @Nonnull
  public static UpdateStart update(@Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier table) {
    return new DefaultUpdate(keyspace, table);
  }

  /**
   * Shortcut for {@link #update(CqlIdentifier, CqlIdentifier)
   * update(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(table))}
   */
  @Nonnull
  public static UpdateStart update(@Nullable String keyspace, @Nonnull String table) {
    return update(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(table));
  }

  /** Starts an UPDATE query for an unqualified table. */
  @Nonnull
  public static UpdateStart update(@Nonnull CqlIdentifier table) {
    return update(null, table);
  }

  /** Shortcut for {@link #update(CqlIdentifier) update(CqlIdentifier.fromCql(table))} */
  @Nonnull
  public static UpdateStart update(@Nonnull String table) {
    return update(CqlIdentifier.fromCql(table));
  }

  /** Starts a DELETE query for a qualified table. */
  @Nonnull
  public static DeleteSelection deleteFrom(
      @Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier table) {
    return new DefaultDelete(keyspace, table);
  }

  /**
   * Shortcut for {@link #deleteFrom(CqlIdentifier, CqlIdentifier)
   * deleteFrom(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(table))}
   */
  @Nonnull
  public static DeleteSelection deleteFrom(@Nullable String keyspace, @Nonnull String table) {
    return deleteFrom(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(table));
  }

  /** Starts a DELETE query for an unqualified table. */
  @Nonnull
  public static DeleteSelection deleteFrom(@Nonnull CqlIdentifier table) {
    return deleteFrom(null, table);
  }

  /** Shortcut for {@link #deleteFrom(CqlIdentifier) deleteFrom(CqlIdentifier.fromCql(table))} */
  @Nonnull
  public static DeleteSelection deleteFrom(@Nonnull String table) {
    return deleteFrom(CqlIdentifier.fromCql(table));
  }

  /**
   * An ordered set of anonymous terms, as in {@code WHERE (a, b) = (1, 2)} (on the right-hand
   * side).
   *
   * <p>For example, this can be used as the right operand of {@link Relation#columns(String...)}.
   */
  @Nonnull
  public static Term tuple(@Nonnull Iterable<? extends Term> components) {
    return new TupleTerm(components);
  }

  /** Var-arg equivalent of {@link #tuple(Iterable)}. */
  @Nonnull
  public static Term tuple(@Nonnull Term... components) {
    return tuple(Arrays.asList(components));
  }

  /** The sum of two terms, as in {@code WHERE k = left + right}. */
  @Nonnull
  public static Term add(@Nonnull Term left, @Nonnull Term right) {
    return new BinaryArithmeticTerm(ArithmeticOperator.SUM, left, right);
  }

  /** The difference of two terms, as in {@code WHERE k = left - right}. */
  @Nonnull
  public static Term subtract(@Nonnull Term left, @Nonnull Term right) {
    return new BinaryArithmeticTerm(ArithmeticOperator.DIFFERENCE, left, right);
  }

  /** The product of two terms, as in {@code WHERE k = left * right}. */
  @Nonnull
  public static Term multiply(@Nonnull Term left, @Nonnull Term right) {
    return new BinaryArithmeticTerm(ArithmeticOperator.PRODUCT, left, right);
  }

  /** The quotient of two terms, as in {@code WHERE k = left / right}. */
  @Nonnull
  public static Term divide(@Nonnull Term left, @Nonnull Term right) {
    return new BinaryArithmeticTerm(ArithmeticOperator.QUOTIENT, left, right);
  }

  /** The remainder of two terms, as in {@code WHERE k = left % right}. */
  @Nonnull
  public static Term remainder(@Nonnull Term left, @Nonnull Term right) {
    return new BinaryArithmeticTerm(ArithmeticOperator.REMAINDER, left, right);
  }

  /** The opposite of a term, as in {@code WHERE k = -argument}. */
  @Nonnull
  public static Term negate(@Nonnull Term argument) {
    return new OppositeTerm(argument);
  }

  /** A function call as a term, as in {@code WHERE k = f(arguments)}. */
  @Nonnull
  public static Term function(
      @Nonnull CqlIdentifier functionId, @Nonnull Iterable<Term> arguments) {
    return function(null, functionId, arguments);
  }

  /** Var-arg equivalent of {@link #function(CqlIdentifier, Iterable)}. */
  @Nonnull
  public static Term function(@Nonnull CqlIdentifier functionId, @Nonnull Term... arguments) {
    return function(functionId, Arrays.asList(arguments));
  }

  /**
   * Shortcut for {@link #function(CqlIdentifier, Iterable)
   * function(CqlIdentifier.fromCql(functionName), arguments)}.
   */
  @Nonnull
  public static Term function(@Nonnull String functionName, @Nonnull Iterable<Term> arguments) {
    return function(CqlIdentifier.fromCql(functionName), arguments);
  }

  /**
   * Shortcut for {@link #function(CqlIdentifier, Term...)
   * function(CqlIdentifier.fromCql(functionName), arguments)}.
   */
  @Nonnull
  public static Term function(@Nonnull String functionName, @Nonnull Term... arguments) {
    return function(CqlIdentifier.fromCql(functionName), arguments);
  }

  /** A function call as a term, as in {@code WHERE k = ks.f(arguments)}. */
  @Nonnull
  public static Term function(
      @Nullable CqlIdentifier keyspaceId,
      @Nonnull CqlIdentifier functionId,
      @Nonnull Iterable<Term> arguments) {
    return new FunctionTerm(keyspaceId, functionId, arguments);
  }

  /** Var-arg equivalent of {@link #function(CqlIdentifier, CqlIdentifier, Iterable)}. */
  @Nonnull
  public static Term function(
      @Nullable CqlIdentifier keyspaceId,
      @Nonnull CqlIdentifier functionId,
      @Nonnull Term... arguments) {
    return function(keyspaceId, functionId, Arrays.asList(arguments));
  }

  /**
   * Shortcut for {@link #function(CqlIdentifier, CqlIdentifier, Iterable)
   * function(CqlIdentifier.fromCql(keyspaceName), CqlIdentifier.fromCql(functionName), arguments)}.
   */
  @Nonnull
  public static Term function(
      @Nullable String keyspaceName,
      @Nonnull String functionName,
      @Nonnull Iterable<Term> arguments) {
    return function(
        keyspaceName == null ? null : CqlIdentifier.fromCql(keyspaceName),
        CqlIdentifier.fromCql(functionName),
        arguments);
  }

  /**
   * Shortcut for {@link #function(CqlIdentifier, CqlIdentifier, Term...)
   * function(CqlIdentifier.fromCql(keyspaceName), CqlIdentifier.fromCql(functionName), arguments)}.
   */
  @Nonnull
  public static Term function(
      @Nullable String keyspaceName, @Nonnull String functionName, @Nonnull Term... arguments) {
    return function(
        keyspaceName == null ? null : CqlIdentifier.fromCql(keyspaceName),
        CqlIdentifier.fromCql(functionName),
        arguments);
  }

  /**
   * Provides a type hint for an expression, as in {@code WHERE k = (double)1/3}.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link #udt(CqlIdentifier)}.
   */
  @Nonnull
  public static Term typeHint(@Nonnull Term term, @Nonnull DataType targetType) {
    return new TypeHintTerm(term, targetType);
  }

  /** A call to the built-in {@code now} function as a term. */
  @Nonnull
  public static Term now() {
    return function("now");
  }

  /** A call to the built-in {@code currentTimestamp} function as a term. */
  @Nonnull
  public static Term currentTimestamp() {
    return function("currenttimestamp");
  }

  /** A call to the built-in {@code currentDate} function as a term. */
  @Nonnull
  public static Term currentDate() {
    return function("currentdate");
  }

  /** A call to the built-in {@code currentTime} function as a term. */
  @Nonnull
  public static Term currentTime() {
    return function("currenttime");
  }

  /** A call to the built-in {@code currentTimeUuid} function as a term. */
  @Nonnull
  public static Term currentTimeUuid() {
    return function("currenttimeuuid");
  }

  /** A call to the built-in {@code minTimeUuid} function as a term. */
  @Nonnull
  public static Term minTimeUuid(@Nonnull Term argument) {
    return function("mintimeuuid", argument);
  }

  /** A call to the built-in {@code maxTimeUuid} function as a term. */
  @Nonnull
  public static Term maxTimeUuid(@Nonnull Term argument) {
    return function("maxtimeuuid", argument);
  }

  /** A call to the built-in {@code toDate} function as a term. */
  @Nonnull
  public static Term toDate(@Nonnull Term argument) {
    return function("todate", argument);
  }

  /** A call to the built-in {@code toTimestamp} function as a term. */
  @Nonnull
  public static Term toTimestamp(@Nonnull Term argument) {
    return function("totimestamp", argument);
  }

  /** A call to the built-in {@code toUnixTimestamp} function as a term. */
  @Nonnull
  public static Term toUnixTimestamp(@Nonnull Term argument) {
    return function("tounixtimestamp", argument);
  }

  /**
   * A literal term, as in {@code WHERE k = 1}.
   *
   * <p>This method can process any type for which there is a default Java to CQL mapping, namely:
   * primitive types ({@code Integer=>int, Long=>bigint, String=>text, etc.}), and collections,
   * tuples, and user defined types thereof.
   *
   * <p>A null argument will be rendered as {@code NULL}.
   *
   * <p>For custom mappings, use {@link #literal(Object, CodecRegistry)} or {@link #literal(Object,
   * TypeCodec)}.
   *
   * @throws CodecNotFoundException if there is no default CQL mapping for the Java type of {@code
   *     value}.
   */
  @Nonnull
  public static Literal literal(@Nullable Object value) {
    return literal(value, CodecRegistry.DEFAULT);
  }

  /**
   * A literal term, as in {@code WHERE k = 1}.
   *
   * <p>This is an alternative to {@link #literal(Object)} for custom type mappings. The provided
   * registry should contain a codec that can format the value. Typically, this will be your
   * session's registry, which is accessible via {@code session.getContext().getCodecRegistry()}.
   *
   * @see DriverContext#getCodecRegistry()
   * @throws CodecNotFoundException if {@code codecRegistry} does not contain any codec that can
   *     handle {@code value}.
   */
  @Nonnull
  public static Literal literal(@Nullable Object value, @Nonnull CodecRegistry codecRegistry) {
    if (value instanceof Murmur3Token) {
      value = ((Murmur3Token) value).getValue();
    } else if (value instanceof ByteOrderedToken) {
      value = ((ByteOrderedToken) value).getValue();
    } else if (value instanceof RandomToken) {
      value = ((RandomToken) value).getValue();
    } else if (value instanceof Token) {
      throw new IllegalArgumentException("Unsupported token type: " + value.getClass().getName());
    }
    try {
      return literal(value, (value == null) ? null : codecRegistry.codecFor(value));
    } catch (CodecNotFoundException e) {
      assert value != null;
      throw new IllegalArgumentException(
          String.format(
              "Could not inline literal of type %s. "
                  + "This happens because the driver doesn't know how to map it to a CQL type. "
                  + "Try passing a TypeCodec or CodecRegistry to literal().",
              value.getClass().getName()),
          e);
    }
  }

  /**
   * A literal term, as in {@code WHERE k = 1}.
   *
   * <p>This is an alternative to {@link #literal(Object)} for custom type mappings. The value will
   * be turned into a string with {@link TypeCodec#format(Object)}, and inlined in the query.
   */
  @Nonnull
  public static <T> Literal literal(@Nullable T value, @Nullable TypeCodec<T> codec) {
    // Don't handle Token here, if the user calls this directly we assume they passed a codec that
    // can handle the value
    return new DefaultLiteral<>(value, codec);
  }

  /**
   * A raw CQL snippet.
   *
   * <p>The contents will be appended to the query as-is, without any syntax checking or escaping.
   * This method should be used with caution, as it's possible to generate invalid CQL that will
   * fail at execution time; on the other hand, it can be used as a workaround to handle new CQL
   * features that are not yet covered by the query builder.
   */
  @Nonnull
  public static Raw raw(@Nonnull String raw) {
    return new DefaultRaw(raw);
  }

  /** Creates an anonymous bind marker, which appears as {@code ?} in the generated CQL. */
  @Nonnull
  public static BindMarker bindMarker() {
    return bindMarker((CqlIdentifier) null);
  }

  /** Creates a named bind marker, which appears as {@code :id} in the generated CQL. */
  @Nonnull
  public static BindMarker bindMarker(@Nullable CqlIdentifier id) {
    return new DefaultBindMarker(id);
  }

  /** Shortcut for {@link #bindMarker(CqlIdentifier) bindMarker(CqlIdentifier.fromCql(name))} */
  @Nonnull
  public static BindMarker bindMarker(@Nullable String name) {
    return bindMarker(name == null ? null : CqlIdentifier.fromCql(name));
  }

  /**
   * Shortcut to reference a UDT in methods that use a {@link DataType}, such as {@link
   * #typeHint(Term, DataType)} and {@link Selector#cast(Selector, DataType)}.
   */
  @Nonnull
  public static UserDefinedType udt(@Nonnull CqlIdentifier name) {
    return new ShallowUserDefinedType(null, name, false);
  }

  /** Shortcut for {@link #udt(CqlIdentifier) udt(CqlIdentifier.fromCql(name))}. */
  @Nonnull
  public static UserDefinedType udt(@Nonnull String name) {
    return udt(CqlIdentifier.fromCql(name));
  }

  /**
   * Creates a new {@code TRUNCATE} query.
   *
   * @param table the name of the table to truncate.
   * @return the truncation query.
   */
  public static Truncate truncate(@Nonnull CqlIdentifier table) {
    return truncate(null, table);
  }

  /**
   * Creates a new {@code TRUNCATE} query.
   *
   * <p>This is a shortcut for {@link #truncate(CqlIdentifier)
   * truncate(CqlIdentifier.fromCql(table))}.
   *
   * @param table the name of the table to truncate.
   * @return the truncation query.
   */
  public static Truncate truncate(@Nonnull String table) {
    return truncate(null, CqlIdentifier.fromCql(table));
  }

  /**
   * Creates a new {@code TRUNCATE} query.
   *
   * @param keyspace the name of the keyspace to use.
   * @param table the name of the table to truncate.
   * @return the truncation query.
   */
  public static Truncate truncate(@Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier table) {
    return new DefaultTruncate(keyspace, table);
  }

  /**
   * Creates a new {@code TRUNCATE} query.
   *
   * <p>This is a shortcut for {@link #truncate(CqlIdentifier, CqlIdentifier)
   * truncate(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(table))}.
   *
   * @param keyspace the name of the keyspace to use.
   * @param table the name of the table to truncate.
   * @return the truncation query.
   */
  public static Truncate truncate(@Nullable String keyspace, @Nonnull String table) {
    return truncate(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(table));
  }
}
