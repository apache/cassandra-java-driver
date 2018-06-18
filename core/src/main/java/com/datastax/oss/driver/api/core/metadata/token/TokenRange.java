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
package com.datastax.oss.driver.api.core.metadata.token;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;

/**
 * A range of tokens on the Cassandra ring.
 *
 * <p>A range is start-exclusive and end-inclusive. It is empty when start and end are the same
 * token, except if that is the minimum token, in which case the range covers the whole ring (this
 * is consistent with the behavior of CQL range queries).
 *
 * <p>Note that CQL does not handle wrapping. To query all partitions in a range, see {@link
 * #unwrap()}.
 */
public interface TokenRange extends Comparable<TokenRange> {

  /** The start of the range (exclusive). */
  @NonNull
  Token getStart();

  /** The end of the range (inclusive). */
  @NonNull
  Token getEnd();

  /**
   * Splits this range into a number of smaller ranges of equal "size" (referring to the number of
   * tokens, not the actual amount of data).
   *
   * <p>Splitting an empty range is not permitted. But note that, in edge cases, splitting a range
   * might produce one or more empty ranges.
   *
   * @throws IllegalArgumentException if the range is empty or if {@code numberOfSplits < 1}.
   */
  @NonNull
  List<TokenRange> splitEvenly(int numberOfSplits);

  /**
   * Whether this range is empty.
   *
   * <p>A range is empty when {@link #getStart()} and {@link #getEnd()} are the same token, except
   * if that is the minimum token, in which case the range covers the whole ring (this is consistent
   * with the behavior of CQL range queries).
   */
  boolean isEmpty();

  /** Whether this range wraps around the end of the ring. */
  boolean isWrappedAround();

  /** Whether this range represents the full ring. */
  boolean isFullRing();

  /**
   * Splits this range into a list of two non-wrapping ranges. This will return the range itself if
   * it is non-wrapping, or two ranges otherwise.
   *
   * <p>For example:
   *
   * <ul>
   *   <li>{@code ]1,10]} unwraps to itself;
   *   <li>{@code ]10,1]} unwraps to {@code ]10,min_token]} and {@code ]min_token,1]}.
   * </ul>
   *
   * <p>This is useful for CQL range queries, which do not handle wrapping:
   *
   * <pre>{@code
   * List<Row> rows = new ArrayList<Row>();
   * for (TokenRange subRange : range.unwrap()) {
   *     ResultSet rs = session.execute(
   *         "SELECT * FROM mytable WHERE token(pk) > ? and token(pk) <= ?",
   *         subRange.getStart(), subRange.getEnd());
   *     rows.addAll(rs.all());
   * }
   * }</pre>
   */
  @NonNull
  List<TokenRange> unwrap();

  /**
   * Whether this range intersects another one.
   *
   * <p>For example:
   *
   * <ul>
   *   <li>{@code ]3,5]} intersects {@code ]1,4]}, {@code ]4,5]}...
   *   <li>{@code ]3,5]} does not intersect {@code ]1,2]}, {@code ]2,3]}, {@code ]5,7]}...
   * </ul>
   */
  boolean intersects(@NonNull TokenRange that);

  /**
   * Computes the intersection of this range with another one, producing one or more ranges.
   *
   * <p>If either of these ranges overlap the the ring, they are unwrapped and the unwrapped ranges
   * are compared to one another.
   *
   * <p>This call will fail if the two ranges do not intersect, you must check by calling {@link
   * #intersects(TokenRange)} first.
   *
   * @param that the other range.
   * @return the range(s) resulting from the intersection.
   * @throws IllegalArgumentException if the ranges do not intersect.
   */
  @NonNull
  List<TokenRange> intersectWith(@NonNull TokenRange that);

  /**
   * Checks whether this range contains a given token, i.e. {@code range.start < token <=
   * range.end}.
   */
  boolean contains(@NonNull Token token);

  /**
   * Merges this range with another one.
   *
   * <p>The two ranges should either intersect or be adjacent; in other words, the merged range
   * should not include tokens that are in neither of the original ranges.
   *
   * <p>For example:
   *
   * <ul>
   *   <li>merging {@code ]3,5]} with {@code ]4,7]} produces {@code ]3,7]};
   *   <li>merging {@code ]3,5]} with {@code ]4,5]} produces {@code ]3,5]};
   *   <li>merging {@code ]3,5]} with {@code ]5,8]} produces {@code ]3,8]};
   *   <li>merging {@code ]3,5]} with {@code ]6,8]} fails.
   * </ul>
   *
   * @throws IllegalArgumentException if the ranges neither intersect nor are adjacent.
   */
  @NonNull
  TokenRange mergeWith(@NonNull TokenRange that);
}
