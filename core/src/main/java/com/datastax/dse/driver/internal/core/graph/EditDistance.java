/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.oss.driver.shaded.guava.common.base.Objects;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.io.Serializable;
import net.jcip.annotations.Immutable;

/**
 * A container for a term and maximum edit distance.
 *
 * <p>The context in which this is used determines the semantics of the edit distance. For instance,
 * it might indicate single-character edits if used with fuzzy search queries or whole word
 * movements if used with phrase proximity queries.
 */
@Immutable
public class EditDistance implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final int DEFAULT_EDIT_DISTANCE = 0;

  public final String query;
  public final int distance;

  public EditDistance(String query) {
    this(query, DEFAULT_EDIT_DISTANCE);
  }

  public EditDistance(String query, int distance) {
    Preconditions.checkNotNull(query, "Query cannot be null.");
    Preconditions.checkArgument(distance >= 0, "Edit distance cannot be negative.");
    this.query = query;
    this.distance = distance;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EditDistance)) {
      return false;
    }
    EditDistance that = (EditDistance) o;
    return distance == that.distance && Objects.equal(query, that.query);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(query, distance);
  }

  @Override
  public String toString() {
    return "EditDistance{" + "query='" + query + '\'' + ", distance=" + distance + '}';
  }
}
