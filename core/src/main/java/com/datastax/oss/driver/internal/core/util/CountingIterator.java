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
package com.datastax.oss.driver.internal.core.util;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.util.Iterator;
import java.util.NoSuchElementException;
import net.jcip.annotations.NotThreadSafe;

/**
 * An iterator that knows in advance how many elements it will return, and maintains a counter as
 * elements get returned.
 */
@NotThreadSafe
public abstract class CountingIterator<ElementT> implements Iterator<ElementT> {

  protected int remaining;

  public CountingIterator(int remaining) {
    this.remaining = remaining;
  }

  public int remaining() {
    return remaining;
  }

  /*
   * The rest of this class was adapted from Guava's `AbstractIterator` (which we can't extend
   *  because its `next` method is final). Guava copyright notice follows:
   *
   * Copyright (C) 2007 The Guava Authors
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

  private enum State {
    READY,
    NOT_READY,
    DONE,
    FAILED,
  }

  private State state = State.NOT_READY;
  private ElementT next;

  protected abstract ElementT computeNext();

  protected final ElementT endOfData() {
    state = State.DONE;
    return null;
  }

  @Override
  public final boolean hasNext() {
    Preconditions.checkState(state != State.FAILED);
    switch (state) {
      case DONE:
        return false;
      case READY:
        return true;
      default:
    }
    return tryToComputeNext();
  }

  private boolean tryToComputeNext() {
    state = State.FAILED; // temporary pessimism
    next = computeNext();
    if (state != State.DONE) {
      state = State.READY;
      return true;
    }
    return false;
  }

  @Override
  public final ElementT next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    state = State.NOT_READY;
    ElementT result = next;
    next = null;
    // Added to original Guava code: decrement counter when we return an element
    remaining -= 1;
    return result;
  }

  public final ElementT peek() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return next;
  }
}
