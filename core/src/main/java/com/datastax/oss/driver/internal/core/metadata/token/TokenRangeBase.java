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
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import net.jcip.annotations.Immutable;

@Immutable
public abstract class TokenRangeBase implements TokenRange {

  private final Token start;
  private final Token end;
  private final Token minToken;

  protected TokenRangeBase(Token start, Token end, Token minToken) {
    this.start = start;
    this.end = end;
    this.minToken = minToken;
  }

  @NonNull
  @Override
  public Token getStart() {
    return start;
  }

  @NonNull
  @Override
  public Token getEnd() {
    return end;
  }

  @NonNull
  @Override
  public List<TokenRange> splitEvenly(int numberOfSplits) {
    if (numberOfSplits < 1)
      throw new IllegalArgumentException(
          String.format("numberOfSplits (%d) must be greater than 0.", numberOfSplits));
    if (isEmpty()) {
      throw new IllegalArgumentException("Can't split empty range " + this);
    }

    List<TokenRange> tokenRanges = new ArrayList<>();
    List<Token> splitPoints = split(start, end, numberOfSplits);
    Token splitStart = start;
    for (Token splitEnd : splitPoints) {
      tokenRanges.add(newTokenRange(splitStart, splitEnd));
      splitStart = splitEnd;
    }
    tokenRanges.add(newTokenRange(splitStart, end));
    return tokenRanges;
  }

  protected abstract List<Token> split(Token start, Token end, int numberOfSplits);

  /** This is used by {@link #split(Token, Token, int)} implementations. */
  protected List<BigInteger> split(
      BigInteger start,
      BigInteger range,
      BigInteger ringEnd,
      BigInteger ringLength,
      int numberOfSplits) {
    BigInteger[] tmp = range.divideAndRemainder(BigInteger.valueOf(numberOfSplits));
    BigInteger divider = tmp[0];
    int remainder = tmp[1].intValue();

    List<BigInteger> results = Lists.newArrayListWithExpectedSize(numberOfSplits - 1);
    BigInteger current = start;
    BigInteger dividerPlusOne =
        (remainder == 0)
            ? null // won't be used
            : divider.add(BigInteger.ONE);

    for (int i = 1; i < numberOfSplits; i++) {
      current = current.add(remainder-- > 0 ? dividerPlusOne : divider);
      if (ringEnd != null && current.compareTo(ringEnd) > 0) current = current.subtract(ringLength);
      results.add(current);
    }
    return results;
  }

  protected abstract TokenRange newTokenRange(Token start, Token end);

  @Override
  public boolean isEmpty() {
    return start.equals(end) && !start.equals(minToken);
  }

  @Override
  public boolean isWrappedAround() {
    return start.compareTo(end) > 0 && !end.equals(minToken);
  }

  @Override
  public boolean isFullRing() {
    return start.equals(minToken) && end.equals(minToken);
  }

  @NonNull
  @Override
  public List<TokenRange> unwrap() {
    if (isWrappedAround()) {
      return ImmutableList.of(newTokenRange(start, minToken), newTokenRange(minToken, end));
    } else {
      return ImmutableList.of(this);
    }
  }

  @Override
  public boolean intersects(@NonNull TokenRange that) {
    // Empty ranges never intersect any other range
    if (this.isEmpty() || that.isEmpty()) {
      return false;
    }

    return contains(this, that.getStart(), true)
        || contains(this, that.getEnd(), false)
        || contains(that, this.start, true)
        || contains(that, this.end, false);
  }

  @NonNull
  @Override
  public List<TokenRange> intersectWith(@NonNull TokenRange that) {
    if (!this.intersects(that)) {
      throw new IllegalArgumentException(
          "The two ranges do not intersect, use intersects() before calling this method");
    }

    List<TokenRange> intersected = Lists.newArrayList();

    // Compare the unwrapped ranges to one another.
    List<TokenRange> unwrappedForThis = this.unwrap();
    List<TokenRange> unwrappedForThat = that.unwrap();
    for (TokenRange t1 : unwrappedForThis) {
      for (TokenRange t2 : unwrappedForThat) {
        if (t1.intersects(t2)) {
          intersected.add(
              newTokenRange(
                  contains(t1, t2.getStart(), true) ? t2.getStart() : t1.getStart(),
                  contains(t1, t2.getEnd(), false) ? t2.getEnd() : t1.getEnd()));
        }
      }
    }

    // If two intersecting ranges were produced, merge them if they are adjacent.
    // This could happen in the case that two wrapped ranges intersected.
    if (intersected.size() == 2) {
      TokenRange t1 = intersected.get(0);
      TokenRange t2 = intersected.get(1);
      if (t1.getEnd().equals(t2.getStart()) || t2.getEnd().equals(t1.getStart())) {
        return ImmutableList.of(t1.mergeWith(t2));
      }
    }

    return intersected;
  }

  @Override
  public boolean contains(@NonNull Token token) {
    return contains(this, token, false);
  }

  // isStart handles the case where the token is the start of another range, for example:
  // * ]1,2] contains 2, but it does not contain the start of ]2,3]
  // * ]1,2] does not contain 1, but it contains the start of ]1,3]
  @VisibleForTesting
  boolean contains(TokenRange range, Token token, boolean isStart) {
    if (range.isEmpty()) {
      return false;
    }
    if (range.getEnd().equals(minToken)) {
      if (range.getStart().equals(minToken)) { // ]min, min] = full ring, contains everything
        return true;
      } else if (token.equals(minToken)) {
        return !isStart;
      } else {
        return isStart
            ? token.compareTo(range.getStart()) >= 0
            : token.compareTo(range.getStart()) > 0;
      }
    } else {
      boolean isAfterStart =
          isStart ? token.compareTo(range.getStart()) >= 0 : token.compareTo(range.getStart()) > 0;
      boolean isBeforeEnd =
          isStart ? token.compareTo(range.getEnd()) < 0 : token.compareTo(range.getEnd()) <= 0;
      return range.isWrappedAround()
          ? isAfterStart || isBeforeEnd //  ####]----]####
          : isAfterStart && isBeforeEnd; // ----]####]----
    }
  }

  @NonNull
  @Override
  public TokenRange mergeWith(@NonNull TokenRange that) {
    if (this.equals(that)) {
      return this;
    }

    if (!(this.intersects(that)
        || this.end.equals(that.getStart())
        || that.getEnd().equals(this.start))) {
      throw new IllegalArgumentException(
          String.format(
              "Can't merge %s with %s because they neither intersect nor are adjacent",
              this, that));
    }

    if (this.isEmpty()) {
      return that;
    }

    if (that.isEmpty()) {
      return this;
    }

    // That's actually "starts in or is adjacent to the end of"
    boolean thisStartsInThat = contains(that, this.start, true) || this.start.equals(that.getEnd());
    boolean thatStartsInThis =
        contains(this, that.getStart(), true) || that.getStart().equals(this.end);

    // This takes care of all the cases that return the full ring, so that we don't have to worry
    // about them below
    if (thisStartsInThat && thatStartsInThis) {
      return fullRing();
    }

    // Starting at this.start, see how far we can go while staying in at least one of the ranges.
    Token mergedEnd =
        (thatStartsInThis && !contains(this, that.getEnd(), false)) ? that.getEnd() : this.end;

    // Repeat in the other direction.
    Token mergedStart = thisStartsInThat ? that.getStart() : this.start;

    return newTokenRange(mergedStart, mergedEnd);
  }

  private TokenRange fullRing() {
    return newTokenRange(minToken, minToken);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof TokenRange) {
      TokenRange that = (TokenRange) other;
      return this.start.equals(that.getStart()) && this.end.equals(that.getEnd());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return 31 * start.hashCode() + end.hashCode();
  }

  @Override
  public int compareTo(@NonNull TokenRange that) {
    if (this.equals(that)) {
      return 0;
    } else {
      int compareStart = this.start.compareTo(that.getStart());
      return compareStart != 0 ? compareStart : this.end.compareTo(that.getEnd());
    }
  }

  @Override
  public String toString() {
    return String.format("%s(%s, %s)", getClass().getSimpleName(), start, end);
  }
}
