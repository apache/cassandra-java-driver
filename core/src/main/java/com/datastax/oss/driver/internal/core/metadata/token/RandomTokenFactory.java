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
package com.datastax.oss.driver.internal.core.metadata.token;

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class RandomTokenFactory implements TokenFactory {

  public static final String PARTITIONER_NAME = "org.apache.cassandra.dht.RandomPartitioner";

  private static final BigInteger MIN_VALUE = BigInteger.ONE.negate();
  static final BigInteger MAX_VALUE = BigInteger.valueOf(2).pow(127);
  public static final RandomToken MIN_TOKEN = new RandomToken(MIN_VALUE);
  public static final RandomToken MAX_TOKEN = new RandomToken(MAX_VALUE);

  private final MessageDigest prototype;
  private final boolean supportsClone;

  public RandomTokenFactory() {
    prototype = createMessageDigest();
    boolean supportsClone;
    try {
      prototype.clone();
      supportsClone = true;
    } catch (CloneNotSupportedException e) {
      supportsClone = false;
    }
    this.supportsClone = supportsClone;
  }

  @Override
  public String getPartitionerName() {
    return PARTITIONER_NAME;
  }

  @Override
  public Token hash(ByteBuffer partitionKey) {
    return new RandomToken(md5(partitionKey));
  }

  @Override
  public Token parse(String tokenString) {
    return new RandomToken(new BigInteger(tokenString));
  }

  @Override
  public String format(Token token) {
    Preconditions.checkArgument(
        token instanceof RandomToken, "Can only format RandomToken instances");
    return ((RandomToken) token).getValue().toString();
  }

  @Override
  public Token minToken() {
    return MIN_TOKEN;
  }

  @Override
  public TokenRange range(Token start, Token end) {
    Preconditions.checkArgument(
        start instanceof RandomToken && end instanceof RandomToken,
        "Can only build ranges of RandomToken instances");
    return new RandomTokenRange((RandomToken) start, (RandomToken) end);
  }

  private static MessageDigest createMessageDigest() {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("MD5 doesn't seem to be available on this JVM", e);
    }
  }

  private BigInteger md5(ByteBuffer data) {
    MessageDigest digest = newMessageDigest();
    digest.update(data.duplicate());
    return new BigInteger(digest.digest()).abs();
  }

  private MessageDigest newMessageDigest() {
    if (supportsClone) {
      try {
        return (MessageDigest) prototype.clone();
      } catch (CloneNotSupportedException ignored) {
      }
    }
    return createMessageDigest();
  }
}
