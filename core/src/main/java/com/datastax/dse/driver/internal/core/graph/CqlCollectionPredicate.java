/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.javatuples.Pair;

/** Predicates that can be used on CQL Collections. */
public enum CqlCollectionPredicate implements DsePredicate {
  contains {
    @Override
    public boolean test(Object value, Object condition) {
      preEvaluate(condition);
      Preconditions.checkArgument(value instanceof Collection);
      return ((Collection) value).contains(condition);
    }
  },

  containsKey {
    @Override
    public boolean test(Object value, Object condition) {
      preEvaluate(condition);
      Preconditions.checkArgument(value instanceof Map);
      return ((Map) value).containsKey(condition);
    }
  },

  containsValue {
    @Override
    public boolean test(Object value, Object condition) {
      preEvaluate(condition);
      Preconditions.checkArgument(value instanceof Map);
      return ((Map) value).containsValue(condition);
    }
  },

  entryEq {
    @Override
    public boolean test(Object value, Object condition) {
      preEvaluate(condition);
      Preconditions.checkArgument(condition instanceof Pair);
      Preconditions.checkArgument(value instanceof Map);
      Pair pair = (Pair) condition;
      Map map = (Map) value;
      return Objects.equals(map.get(pair.getValue0()), pair.getValue1());
    }
  };

  @Override
  public boolean isValidCondition(Object condition) {
    if (condition instanceof Pair) {
      Pair pair = (Pair) condition;
      return pair.getValue0() != null && pair.getValue1() != null;
    }
    return condition != null;
  }
}
