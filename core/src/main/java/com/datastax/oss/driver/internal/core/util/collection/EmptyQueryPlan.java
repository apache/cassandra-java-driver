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
package com.datastax.oss.driver.internal.core.util.collection;

import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.AbstractQueue;
import java.util.Collections;
import java.util.Iterator;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
class EmptyQueryPlan extends AbstractQueue<Node> implements QueryPlan {

  @Override
  public Node poll() {
    return null;
  }

  @NonNull
  @Override
  public Iterator<Node> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public int size() {
    return 0;
  }
}
