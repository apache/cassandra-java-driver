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
package com.datastax.driver.core;

import com.datastax.driver.core.Frame.Header;
import com.datastax.driver.core.Requests.QueryFlag;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.exceptions.UnsupportedProtocolVersionException;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A regular (non-prepared and non batched) CQL statement.
 *
 * <p>This class represents a query string along with query options (and optionally binary values,
 * see {@code getValues}). It can be extended but {@link SimpleStatement} is provided as a simple
 * implementation to build a {@code RegularStatement} directly from its query string.
 */
public abstract class RegularStatement extends Statement {

  /** Creates a new RegularStatement. */
  protected RegularStatement() {}

  /**
   * Returns the query string for this statement.
   *
   * <p>It is important to note that the query string is merely a CQL representation of this
   * statement, but it does <em>not</em> convey all the information stored in {@link Statement}
   * objects.
   *
   * <p>For example, {@link Statement} objects carry numerous protocol-level settings, such as the
   * {@link Statement#getConsistencyLevel() consistency level} to use, or the {@link
   * Statement#isIdempotent() idempotence flag}, among others. <em>None of these settings will be
   * included in the resulting query string.</em>
   *
   * <p>Similarly, if values have been set on this statement because it has bind markers, these
   * values will not appear in the resulting query string.
   *
   * <p>Note: the consistency level was conveyed at CQL level in older versions of the CQL grammar,
   * but since <a href="https://issues.apache.org/jira/browse/CASSANDRA-4734">CASSANDRA-4734</a> it
   * is now a protocol-level setting and consequently does not appear in the query string.
   *
   * @param codecRegistry the codec registry that will be used if the actual implementation needs to
   *     serialize Java objects in the process of generating the query. Note that it might be
   *     possible to use the no-arg {@link #getQueryString()} depending on the type of statement
   *     this is called on.
   * @return a valid CQL query string.
   * @see #getQueryString()
   */
  public abstract String getQueryString(CodecRegistry codecRegistry);

  /**
   * Returns the query string for this statement.
   *
   * <p>This method calls {@link #getQueryString(CodecRegistry)} with {@link
   * CodecRegistry#DEFAULT_INSTANCE}. Whether you should use this or the other variant depends on
   * the type of statement this is called on:
   *
   * <ul>
   *   <li>for a {@link SimpleStatement} or {@link SchemaStatement}, the codec registry isn't
   *       actually needed, so it's always safe to use this method;
   *   <li>for a {@link BuiltStatement} you can use this method if you use no custom codecs, or if
   *       your custom codecs are registered with the default registry. Otherwise, use the other
   *       method and provide the registry that contains your codecs (see {@link BuiltStatement} for
   *       more explanations on why this is so);
   *   <li>for a {@link BatchStatement}, use the first rule if it contains no built statements, or
   *       the second rule otherwise.
   * </ul>
   *
   * @return a valid CQL query string.
   */
  public String getQueryString() {
    return getQueryString(CodecRegistry.DEFAULT_INSTANCE);
  }

  /**
   * The positional values to use for this statement.
   *
   * <p>A statement can use either positional or named values, but not both. So if this method
   * returns a non-null result, {@link #getNamedValues(ProtocolVersion, CodecRegistry)} will return
   * {@code null}.
   *
   * <p>Values for a RegularStatement (i.e. if either method does not return {@code null}) are not
   * supported with the native protocol version 1: you will get an {@link
   * UnsupportedProtocolVersionException} when submitting one if version 1 of the protocol is in use
   * (i.e. if you've forced version 1 through {@link Cluster.Builder#withProtocolVersion} or you use
   * Cassandra 1.2).
   *
   * @param protocolVersion the protocol version that will be used to serialize the values.
   * @param codecRegistry the codec registry that will be used to serialize the values.
   * @throws InvalidTypeException if one of the values is not of a type that can be serialized to a
   *     CQL3 type
   * @see SimpleStatement#SimpleStatement(String, Object...)
   */
  public abstract ByteBuffer[] getValues(
      ProtocolVersion protocolVersion, CodecRegistry codecRegistry);

  /**
   * The named values to use for this statement.
   *
   * <p>A statement can use either positional or named values, but not both. So if this method
   * returns a non-null result, {@link #getValues(ProtocolVersion, CodecRegistry)} will return
   * {@code null}.
   *
   * <p>Values for a RegularStatement (i.e. if either method does not return {@code null}) are not
   * supported with the native protocol version 1: you will get an {@link
   * UnsupportedProtocolVersionException} when submitting one if version 1 of the protocol is in use
   * (i.e. if you've forced version 1 through {@link Cluster.Builder#withProtocolVersion} or you use
   * Cassandra 1.2).
   *
   * @param protocolVersion the protocol version that will be used to serialize the values.
   * @param codecRegistry the codec registry that will be used to serialize the values.
   * @return the named values.
   * @throws InvalidTypeException if one of the values is not of a type that can be serialized to a
   *     CQL3 type
   * @see SimpleStatement#SimpleStatement(String, Map)
   */
  public abstract Map<String, ByteBuffer> getNamedValues(
      ProtocolVersion protocolVersion, CodecRegistry codecRegistry);

  /**
   * Whether or not this statement has values, that is if {@code getValues} will return {@code null}
   * or not.
   *
   * @param codecRegistry the codec registry that will be used if the actual implementation needs to
   *     serialize Java objects in the process of determining if the query has values. Note that it
   *     might be possible to use the no-arg {@link #hasValues()} depending on the type of statement
   *     this is called on.
   * @return {@code false} if both {@link #getValues(ProtocolVersion, CodecRegistry)} and {@link
   *     #getNamedValues(ProtocolVersion, CodecRegistry)} return {@code null}, {@code true}
   *     otherwise.
   * @see #hasValues()
   */
  public abstract boolean hasValues(CodecRegistry codecRegistry);

  /**
   * Whether this statement uses named values.
   *
   * @return {@code false} if {@link #getNamedValues(ProtocolVersion, CodecRegistry)} returns {@code
   *     null}, {@code true} otherwise.
   */
  public abstract boolean usesNamedValues();

  /**
   * Whether or not this statement has values, that is if {@code getValues} will return {@code null}
   * or not.
   *
   * <p>This method calls {@link #hasValues(CodecRegistry)} with {@link
   * ProtocolVersion#NEWEST_SUPPORTED}. Whether you should use this or the other variant depends on
   * the type of statement this is called on:
   *
   * <ul>
   *   <li>for a {@link SimpleStatement} or {@link SchemaStatement}, the codec registry isn't
   *       actually needed, so it's always safe to use this method;
   *   <li>for a {@link BuiltStatement} you can use this method if you use no custom codecs, or if
   *       your custom codecs are registered with the default registry. Otherwise, use the other
   *       method and provide the registry that contains your codecs (see {@link BuiltStatement} for
   *       more explanations on why this is so);
   *   <li>for a {@link BatchStatement}, use the first rule if it contains no built statements, or
   *       the second rule otherwise.
   * </ul>
   *
   * @return {@code false} if {@link #getValues} returns {@code null}, {@code true} otherwise.
   */
  public boolean hasValues() {
    return hasValues(CodecRegistry.DEFAULT_INSTANCE);
  }

  @Override
  public int requestSizeInBytes(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    int size = Header.lengthFor(protocolVersion);
    try {
      size += CBUtil.sizeOfLongString(getQueryString(codecRegistry));
      switch (protocolVersion) {
        case V1:
          size += CBUtil.sizeOfConsistencyLevel(getConsistencyLevel());
          break;
        case V2:
        case V3:
        case V4:
        case V5:
          size += CBUtil.sizeOfConsistencyLevel(getConsistencyLevel());
          size += QueryFlag.serializedSize(protocolVersion);
          if (hasValues()) {
            if (usesNamedValues()) {
              size += CBUtil.sizeOfNamedValueList(getNamedValues(protocolVersion, codecRegistry));
            } else {
              size += CBUtil.sizeOfValueList(getValues(protocolVersion, codecRegistry));
            }
          }
          // Fetch size, serial CL and default timestamp also depend on session-level defaults
          // (QueryOptions).
          // We always count them to avoid having to inject QueryOptions here, at worst we
          // overestimate by a
          // few bytes.
          size += 4; // fetch size
          if (getPagingState() != null) {
            size += CBUtil.sizeOfValue(getPagingState());
          }
          size += CBUtil.sizeOfConsistencyLevel(getSerialConsistencyLevel());
          if (ProtocolFeature.CLIENT_TIMESTAMPS.isSupportedBy(protocolVersion)) {
            size += 8; // timestamp
          }
          if (ProtocolFeature.CUSTOM_PAYLOADS.isSupportedBy(protocolVersion)
              && getOutgoingPayload() != null) {
            size += CBUtil.sizeOfBytesMap(getOutgoingPayload());
          }
          break;
        default:
          throw protocolVersion.unsupported();
      }
    } catch (Exception e) {
      size = -1;
    }
    return size;
  }

  /**
   * Returns this statement as a CQL query string.
   *
   * <p>It is important to note that the query string is merely a CQL representation of this
   * statement, but it does <em>not</em> convey all the information stored in {@link Statement}
   * objects.
   *
   * <p>See the javadocs of {@link #getQueryString()} for more information.
   *
   * @return this statement as a CQL query string.
   * @see #getQueryString()
   */
  @Override
  public String toString() {
    return getQueryString();
  }
}
