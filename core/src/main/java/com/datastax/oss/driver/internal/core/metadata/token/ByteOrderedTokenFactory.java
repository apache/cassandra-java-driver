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
package com.datastax.oss.driver.internal.core.metadata.token;

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class ByteOrderedTokenFactory implements TokenFactory {

  public static final String PARTITIONER_NAME = "org.apache.cassandra.dht.ByteOrderedPartitioner";

  public static final ByteOrderedToken MIN_TOKEN = new ByteOrderedToken(ByteBuffer.allocate(0));

  @Override
  public String getPartitionerName() {
    return PARTITIONER_NAME;
  }

  @Override
  public Token hash(ByteBuffer partitionKey) {
    return new ByteOrderedToken(partitionKey);
  }

  @Override
  public Token parse(String tokenString) {
    // This method must be able to parse the contents of system.peers.tokens, which do not have the
    // "0x" prefix. On the other hand, OPPToken#toString has the "0x" because it should be usable in
    // a CQL query, and it's nice to have fromString and toString symmetrical. So handle both cases:
    if (!tokenString.startsWith("0x")) {
      String prefix = (tokenString.length() % 2 == 0) ? "0x" : "0x0";
      tokenString = prefix + tokenString;
    }
    ByteBuffer value = Bytes.fromHexString(tokenString);
    return new ByteOrderedToken(value);
  }

  @Override
  public String format(Token token) {
    Preconditions.checkArgument(
        token instanceof ByteOrderedToken, "Can only format ByteOrderedToken instances");
    return Bytes.toHexString(((ByteOrderedToken) token).getValue());
  }

  @Override
  public Token minToken() {
    return MIN_TOKEN;
  }

  @Override
  public TokenRange range(Token start, Token end) {
    Preconditions.checkArgument(
        start instanceof ByteOrderedToken && end instanceof ByteOrderedToken,
        "Can only build ranges of ByteOrderedToken instances");
    return new ByteOrderedTokenRange(((ByteOrderedToken) start), ((ByteOrderedToken) end));
  }
}
