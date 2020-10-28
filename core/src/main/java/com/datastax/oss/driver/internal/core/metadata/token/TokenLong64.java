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
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.primitives.Longs;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

/** Tokens represented by a 64-bit integer. */
@Immutable
public abstract class TokenLong64 implements Token {
  protected final long value;

  protected TokenLong64(long value) {
    this.value = value;
  }

  public long getValue() {
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof TokenLong64) {
      TokenLong64 that = (TokenLong64) other;
      return this.getValue() == that.getValue();
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
        other instanceof TokenLong64, "Cannot compare with non-64-bit-integer token");
    TokenLong64 that = (TokenLong64) other;
    return Longs.compare(this.value, that.value);
  }
}
