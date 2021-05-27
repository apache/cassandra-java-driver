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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PagingState;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.data.ValuesHelper;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class DefaultPagingState implements PagingState {

  private final ByteBuffer rawPagingState;
  private final byte[] hash;
  private final int protocolVersion;

  public DefaultPagingState(
      ByteBuffer rawPagingState, Statement<?> statement, AttachmentPoint attachmentPoint) {
    this(
        rawPagingState,
        hash(statement, rawPagingState, attachmentPoint),
        attachmentPoint.getProtocolVersion().getCode());
  }

  private DefaultPagingState(ByteBuffer rawPagingState, byte[] hash, int protocolVersion) {
    this.rawPagingState = rawPagingState;
    this.hash = hash;
    this.protocolVersion = protocolVersion;
  }

  // Same serialized form as in driver 3:
  //     size of raw state|size of hash|raw state|hash|protocol version
  //
  // The protocol version might be absent, in which case it defaults to V2 (this is for backward
  // compatibility with 2.0.10 where it is always absent).
  public static DefaultPagingState fromBytes(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    short rawPagingStateLength = buffer.getShort();
    short hashLength = buffer.getShort();
    int length = rawPagingStateLength + hashLength + 2;
    int legacyLength = rawPagingStateLength + hashLength; // without protocol version
    if (buffer.remaining() != length && buffer.remaining() != legacyLength) {
      throw new IllegalArgumentException(
          "Cannot deserialize paging state, invalid format. The serialized form was corrupted, "
              + "or not initially generated from a PagingState object.");
    }
    byte[] rawPagingState = new byte[rawPagingStateLength];
    buffer.get(rawPagingState);
    byte[] hash = new byte[hashLength];
    buffer.get(hash);
    int protocolVersion = buffer.hasRemaining() ? buffer.getShort() : 2;
    return new DefaultPagingState(ByteBuffer.wrap(rawPagingState), hash, protocolVersion);
  }

  @Override
  public byte[] toBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(rawPagingState.remaining() + hash.length + 6);
    buffer.putShort((short) rawPagingState.remaining());
    buffer.putShort((short) hash.length);
    buffer.put(rawPagingState.duplicate());
    buffer.put(hash);
    buffer.putShort((short) protocolVersion);
    buffer.rewind();
    return buffer.array();
  }

  public static DefaultPagingState fromString(String string) {
    byte[] bytes = Bytes.getArray(Bytes.fromHexString("0x" + string));
    return fromBytes(bytes);
  }

  @Override
  public String toString() {
    return Bytes.toHexString(toBytes()).substring(2); // remove "0x" prefix
  }

  @Override
  public boolean matches(@NonNull Statement<?> statement, @Nullable Session session) {
    AttachmentPoint attachmentPoint =
        (session == null) ? AttachmentPoint.NONE : session.getContext();
    byte[] actual = hash(statement, rawPagingState, attachmentPoint);
    return Arrays.equals(actual, hash);
  }

  @NonNull
  @Override
  public ByteBuffer getRawPagingState() {
    return rawPagingState;
  }

  // Hashes a statement's query string and parameters. We also include the paging state itself in
  // the hash computation, to make the serialized form a bit more resistant to manual tampering.
  private static byte[] hash(
      @NonNull Statement<?> statement,
      ByteBuffer rawPagingState,
      @NonNull AttachmentPoint attachmentPoint) {
    // Batch statements don't have paging, the driver should never call this method for one
    assert !(statement instanceof BatchStatement);

    MessageDigest messageDigest;
    try {
      messageDigest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(
          "It looks like this JVM doesn't support MD5 digests, "
              + "can't use the rich paging state feature",
          e);
    }
    if (statement instanceof BoundStatement) {
      BoundStatement boundStatement = (BoundStatement) statement;
      String queryString = boundStatement.getPreparedStatement().getQuery();
      messageDigest.update(queryString.getBytes(Charset.defaultCharset()));
      for (ByteBuffer value : boundStatement.getValues()) {
        messageDigest.update(value.duplicate());
      }
    } else {
      SimpleStatement simpleStatement = (SimpleStatement) statement;
      String queryString = simpleStatement.getQuery();
      messageDigest.update(queryString.getBytes(Charset.defaultCharset()));
      for (Object value : simpleStatement.getPositionalValues()) {
        ByteBuffer encodedValue =
            ValuesHelper.encodeToDefaultCqlMapping(
                value, attachmentPoint.getCodecRegistry(), attachmentPoint.getProtocolVersion());
        messageDigest.update(encodedValue);
      }
      for (Object value : simpleStatement.getNamedValues().values()) {
        ByteBuffer encodedValue =
            ValuesHelper.encodeToDefaultCqlMapping(
                value, attachmentPoint.getCodecRegistry(), attachmentPoint.getProtocolVersion());
        messageDigest.update(encodedValue);
      }
    }
    messageDigest.update(rawPagingState.duplicate());
    return messageDigest.digest();
  }
}
