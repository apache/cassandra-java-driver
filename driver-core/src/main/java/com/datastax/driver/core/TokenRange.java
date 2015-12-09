/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * A range of tokens on the Cassandra ring.
 * <p/>
 * A range is start-exclusive and end-inclusive. It is empty when start and end are the same token, except if that is the minimum
 * token, in which case the range covers the whole ring (this is consistent with the behavior of CQL range queries).
 * <p/>
 * Note that CQL does not handle wrapping. To query all partitions in a range, see {@link #unwrap()}.
 */
public final class TokenRange implements Comparable<TokenRange> {
    private final Token start;
    private final Token end;
    @VisibleForTesting
    final Token.Factory factory;

    TokenRange(Token start, Token end, Token.Factory factory) {
        this.start = start;
        this.end = end;
        this.factory = factory;
    }

    /**
     * Return the start of the range.
     *
     * @return the start of the range (exclusive).
     */
    public Token getStart() {
        return start;
    }

    /**
     * Return the end of the range.
     *
     * @return the end of the range (inclusive).
     */
    public Token getEnd() {
        return end;
    }

    /**
     * Splits this range into a number of smaller ranges of equal "size" (referring to the number of tokens, not the actual amount of data).
     * <p/>
     * Splitting an empty range is not permitted. But note that, in edge cases, splitting a range might produce one or more empty ranges.
     *
     * @param numberOfSplits the number of splits to create.
     * @return the splits.
     * @throws IllegalArgumentException if the range is empty or if numberOfSplits < 1.
     */
    public List<TokenRange> splitEvenly(int numberOfSplits) {
        if (numberOfSplits < 1)
            throw new IllegalArgumentException(String.format("numberOfSplits (%d) must be greater than 0.", numberOfSplits));
        if (isEmpty())
            throw new IllegalArgumentException("Can't split empty range " + this);

        List<TokenRange> tokenRanges = new ArrayList<TokenRange>();
        List<Token> splitPoints = factory.split(start, end, numberOfSplits);
        Token splitStart = start;
        for (Token splitEnd : splitPoints) {
            tokenRanges.add(new TokenRange(splitStart, splitEnd, factory));
            splitStart = splitEnd;
        }
        tokenRanges.add(new TokenRange(splitStart, end, factory));
        return tokenRanges;
    }

    /**
     * Returns whether this range is empty.
     * <p/>
     * A range is empty when start and end are the same token, except if that is the minimum token,
     * in which case the range covers the whole ring (this is consistent with the behavior of CQL
     * range queries).
     *
     * @return whether the range is empty.
     */
    public boolean isEmpty() {
        return start.equals(end) && !start.equals(factory.minToken());
    }

    /**
     * Returns whether this range wraps around the end of the ring.
     *
     * @return whether this range wraps around.
     */
    public boolean isWrappedAround() {
        return start.compareTo(end) > 0 && !end.equals(factory.minToken());
    }

    /**
     * Splits this range into a list of two non-wrapping ranges. This will return the range itself if it is
     * non-wrapping, or two ranges otherwise.
     * <p/>
     * For example:
     * <ul>
     * <li>{@code ]1,10]} unwraps to itself;</li>
     * <li>{@code ]10,1]} unwraps to {@code ]10,min_token]} and {@code ]min_token,1]}.</li>
     * </ul>
     * <p/>
     * This is useful for CQL range queries, which do not handle wrapping:
     * <pre>
     * {@code
     * List<Row> rows = new ArrayList<Row>();
     * for (TokenRange subRange : range.unwrap()) {
     *     ResultSet rs = session.execute("SELECT * FROM mytable WHERE token(pk) > ? and token(pk) <= ?",
     *                                    subRange.getStart(), subRange.getEnd());
     *     rows.addAll(rs.all());
     * }
     * }</pre>
     *
     * @return the list of non-wrapping ranges.
     */
    public List<TokenRange> unwrap() {
        if (isWrappedAround()) {
            return ImmutableList.of(
                    new TokenRange(start, factory.minToken(), factory),
                    new TokenRange(factory.minToken(), end, factory));
        } else {
            return ImmutableList.of(this);
        }
    }

    /**
     * Returns whether this range intersects another one.
     * <p/>
     * For example:
     * <ul>
     * <li>{@code ]3,5]} intersects {@code ]1,4]}, {@code ]4,5]}...</li>
     * <li>{@code ]3,5]} does not intersect {@code ]1,2]}, {@code ]2,3]}, {@code ]5,7]}...</li>
     * </ul>
     *
     * @param that the other range.
     * @return whether they intersect.
     */
    public boolean intersects(TokenRange that) {
        // Empty ranges never intersect any other range
        if (this.isEmpty() || that.isEmpty())
            return false;

        return this.contains(that.start, true)
                || this.contains(that.end, false)
                || that.contains(this.start, true)
                || that.contains(this.end, false);
    }

    /**
     * Computes the intersection of this range with another one.
     * <p/>
     * If either of these ranges overlap the the ring, they are unwrapped and the unwrapped
     * tokens are compared with one another.
     * <p/>
     * This call will fail if the two ranges do not intersect, you must check by calling
     * {@link #intersects(TokenRange)} beforehand.
     *
     * @param that the other range.
     * @return the range(s) resulting from the intersection.
     * @throws IllegalArgumentException if the ranges do not intersect.
     */
    public List<TokenRange> intersectWith(TokenRange that) {
        if (!this.intersects(that))
            throw new IllegalArgumentException("The two ranges do not intersect, use intersects() before calling this method");

        List<TokenRange> intersected = Lists.newArrayList();

        // Compare the unwrapped ranges to one another.
        List<TokenRange> unwrappedForThis = this.unwrap();
        List<TokenRange> unwrappedForThat = that.unwrap();
        for (TokenRange t1 : unwrappedForThis) {
            for (TokenRange t2 : unwrappedForThat) {
                if (t1.intersects(t2)) {
                    intersected.add(new TokenRange(
                            (t1.contains(t2.start, true)) ? t2.start : t1.start,
                            (t1.contains(t2.end, false)) ? t2.end : t1.end,
                            factory));
                }
            }
        }

        // If two intersecting ranges were produced, merge them if they are adjacent.
        // This could happen in the case that two wrapped ranges intersected.
        if (intersected.size() == 2) {
            TokenRange t1 = intersected.get(0);
            TokenRange t2 = intersected.get(1);
            if (t1.end.equals(t2.start) || t2.end.equals(t1.start)) {
                return ImmutableList.of(t1.mergeWith(t2));
            }
        }

        return intersected;
    }

    /**
     * Checks whether this range contains a given token.
     *
     * @param token the token to check for.
     * @return whether this range contains the token, i.e. {@code range.start &lt; token &lt;= range.end}.
     */
    public boolean contains(Token token) {
        return contains(token, false);
    }

    // isStart handles the case where the token is the start of another range, for example:
    // * ]1,2] contains 2, but it does not contain the start of ]2,3]
    // * ]1,2] does not contain 1, but it contains the start of ]1,3]
    private boolean contains(Token token, boolean isStart) {
        boolean isAfterStart = isStart ? token.compareTo(start) >= 0 : token.compareTo(start) > 0;
        boolean isBeforeEnd = end.equals(factory.minToken()) ||
                (isStart ? token.compareTo(end) < 0 : token.compareTo(end) <= 0);
        return isWrappedAround()
                ? isAfterStart || isBeforeEnd
                : isAfterStart && isBeforeEnd;
    }

    /**
     * Merges this range with another one.
     * <p/>
     * The two ranges should either intersect or be adjacent; in other words, the merged range
     * should not include tokens that are in neither of the original ranges.
     * <p/>
     * For example:
     * <ul>
     * <li>merging {@code ]3,5]} with {@code ]4,7]} produces {@code ]3,7]};</li>
     * <li>merging {@code ]3,5]} with {@code ]4,5]} produces {@code ]3,5]};</li>
     * <li>merging {@code ]3,5]} with {@code ]5,8]} produces {@code ]3,8]};</li>
     * <li>merging {@code ]3,5]} with {@code ]6,8]} fails.</li>
     * </ul>
     *
     * @param that the other range.
     * @return the resulting range.
     * @throws IllegalArgumentException if the ranges neither intersect nor are adjacent.
     */
    public TokenRange mergeWith(TokenRange that) {
        if (this.equals(that))
            return this;

        if (!(this.intersects(that) || this.end.equals(that.start) || that.end.equals(this.start)))
            throw new IllegalArgumentException(String.format(
                    "Can't merge %s with %s because they neither intersect nor are adjacent",
                    this, that));

        if (this.isEmpty())
            return that;

        if (that.isEmpty())
            return this;

        // That's actually "starts in or is adjacent to the end of"
        boolean thisStartsInThat = that.contains(this.start, true) || this.start.equals(that.end);
        boolean thatStartsInThis = this.contains(that.start, true) || that.start.equals(this.end);

        // This takes care of all the cases that return the full ring, so that we don't have to worry about them below
        if (thisStartsInThat && thatStartsInThis)
            return fullRing();

        // Starting at this.start, see how far we can go while staying in at least one of the ranges.
        Token mergedEnd = (thatStartsInThis && !this.contains(that.end, false))
                ? that.end
                : this.end;

        // Repeat in the other direction.
        Token mergedStart = thisStartsInThat ? that.start : this.start;

        return new TokenRange(mergedStart, mergedEnd, factory);
    }

    private TokenRange fullRing() {
        return new TokenRange(factory.minToken(), factory.minToken(), factory);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this)
            return true;
        if (other instanceof TokenRange) {
            TokenRange that = (TokenRange) other;
            return Objects.equal(this.start, that.start) &&
                    Objects.equal(this.end, that.end);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(start, end);
    }

    @Override
    public String toString() {
        return String.format("]%s, %s]", start, end);
    }

    @Override
    public int compareTo(TokenRange other) {
        if (this.equals(other)) {
            return 0;
        } else {
            int compareStart = this.start.compareTo(other.start);
            return compareStart != 0 ? compareStart : this.end.compareTo(other.end);
        }
    }
}
