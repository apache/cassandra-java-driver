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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Iterator;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class SinglePageGraphResultSet implements GraphResultSet {

  private final AsyncGraphResultSet onlyPage;

  public SinglePageGraphResultSet(AsyncGraphResultSet onlyPage) {
    this.onlyPage = onlyPage;
    assert !onlyPage.hasMorePages();
  }

  @NonNull
  @Override
  public ExecutionInfo getRequestExecutionInfo() {
    return onlyPage.getRequestExecutionInfo();
  }

  @NonNull
  @Override
  @Deprecated
  public com.datastax.dse.driver.api.core.graph.GraphExecutionInfo getExecutionInfo() {
    return onlyPage.getExecutionInfo();
  }

  @NonNull
  @Override
  public Iterator<GraphNode> iterator() {
    return onlyPage.currentPage().iterator();
  }

  @Override
  public void cancel() {
    onlyPage.cancel();
  }
}
