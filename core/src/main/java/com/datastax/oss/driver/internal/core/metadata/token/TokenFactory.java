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
import java.nio.ByteBuffer;

/** Manages token instances for a partitioner implementation. */
public interface TokenFactory {

  String getPartitionerName();

  Token hash(ByteBuffer partitionKey);

  Token parse(String tokenString);

  String format(Token token);

  /**
   * The minimum token is a special value that no key ever hashes to, it's used both as lower and
   * upper bound.
   */
  Token minToken();

  TokenRange range(Token start, Token end);
}
