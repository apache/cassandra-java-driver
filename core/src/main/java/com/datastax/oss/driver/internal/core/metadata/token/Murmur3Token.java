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
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.primitives.Longs;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

/** A token generated by {@code Murmur3Partitioner}. */
@Immutable
public class Murmur3Token implements Token {

  private final long value;

  public Murmur3Token(long value) {
    this.value = value;
  }

  public long getValue() {
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof Murmur3Token) {
      Murmur3Token that = (Murmur3Token) other;
      return this.value == that.value;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return (int) (value ^ (value >>> 32));
  }

  @Override
  public int compareTo(@NonNull Token other) {
    Preconditions.checkArgument(
        other instanceof Murmur3Token, "Can only compare tokens of the same type");
    Murmur3Token that = (Murmur3Token) other;
    return Longs.compare(this.value, that.value);
  }

  @Override
  public String toString() {
    return "Murmur3Token(" + value + ")";
  }
}
