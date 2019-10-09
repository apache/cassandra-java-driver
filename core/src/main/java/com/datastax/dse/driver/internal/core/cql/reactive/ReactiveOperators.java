/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.reactive;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.atomic.AtomicLong;

final class ReactiveOperators {

  /**
   * Atomically adds the given value to the given AtomicLong, bound to Long.MAX_VALUE.
   *
   * @param current the current value.
   * @param toAdd the delta to add.
   */
  static void addCap(@NonNull AtomicLong current, long toAdd) {
    long r, u;
    do {
      r = current.get();
      if (r == Long.MAX_VALUE) {
        return;
      }
      u = r + toAdd;
      if (u < 0L) {
        u = Long.MAX_VALUE;
      }
    } while (!current.compareAndSet(r, u));
  }

  /**
   * Atomically subtracts the given value from the given AtomicLong, bound to 0.
   *
   * @param current the current value.
   * @param toSub the delta to subtract.
   */
  static void subCap(@NonNull AtomicLong current, long toSub) {
    long r, u;
    do {
      r = current.get();
      if (r == 0 || r == Long.MAX_VALUE) {
        return;
      }
      u = Math.max(r - toSub, 0);
    } while (!current.compareAndSet(r, u));
  }
}
