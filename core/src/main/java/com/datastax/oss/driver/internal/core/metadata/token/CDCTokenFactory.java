/*
 * Copyright (C) 2020 ScyllaDB
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

package com.datastax.oss.driver.internal.core.metadata.token;

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CDCTokenFactory implements TokenFactory {

  public static final String PARTITIONER_NAME = "com.scylladb.dht.CDCPartitioner";

  public static final CDCToken MIN_TOKEN = new CDCToken(Long.MIN_VALUE);
  public static final CDCToken MAX_TOKEN = new CDCToken(Long.MAX_VALUE);

  private static final Logger logger = LoggerFactory.getLogger(CDCTokenFactory.class);
  private static final int CDC_PARTITION_KEY_LENGTH = 16;
  private static final long VERSION_MASK = 0xF;
  private static final int MIN_SUPPORTED_VERSION = 1;
  private static final int MAX_SUPPORTED_VERSION = 1;

  @Override
  public String getPartitionerName() {
    return PARTITIONER_NAME;
  }

  @Override
  public Token hash(ByteBuffer partitionKey) {
    int offset = partitionKey.position();
    int length = partitionKey.remaining();

    if (length != CDC_PARTITION_KEY_LENGTH) {
      logger.warn(
          "CDC partition key has invalid length: expected {} bytes, but got {} bytes",
          CDC_PARTITION_KEY_LENGTH,
          length);
    }
    if (length < 8) {
      return MIN_TOKEN;
    }

    long upperDword = partitionKey.getLong(offset + 0);
    if (length != CDC_PARTITION_KEY_LENGTH) {
      // Use first 8 bytes as token and skip checking the version
      return new CDCToken(upperDword);
    }

    long lowerDword = partitionKey.getLong(offset + 8);
    long version = lowerDword & VERSION_MASK;
    if (version < MIN_SUPPORTED_VERSION || version > MAX_SUPPORTED_VERSION) {
      logger.warn("CDC partition key version {} is not supported", version);
    }

    return new CDCToken(upperDword);
  }

  @Override
  public Token parse(String tokenString) {
    return new CDCToken(Long.parseLong(tokenString));
  }

  @Override
  public String format(Token token) {
    Preconditions.checkArgument(token instanceof CDCToken, "Can only format CDCToken instances");
    return Long.toString(((CDCToken) token).getValue());
  }

  @Override
  public Token minToken() {
    return MIN_TOKEN;
  }

  @Override
  public TokenRange range(Token start, Token end) {
    Preconditions.checkArgument(
        start instanceof CDCToken && end instanceof CDCToken,
        "Can only build ranges of CDCToken instances");
    return new CDCTokenRange((CDCToken) start, (CDCToken) end);
  }
}
