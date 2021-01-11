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
package com.datastax.oss.driver.internal.core.loadbalancing.nodeset;

import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DcAgnosticNodeSet implements NodeSet {

  private final Set<Node> nodes = new CopyOnWriteArraySet<>();

  @Override
  public boolean add(@NonNull Node node) {
    return nodes.add(node);
  }

  @Override
  public boolean remove(@NonNull Node node) {
    return nodes.remove(node);
  }

  @Override
  @NonNull
  public Set<Node> dc(@Nullable String dc) {
    return nodes;
  }

  @Override
  public Set<String> dcs() {
    return Collections.emptySet();
  }
}
