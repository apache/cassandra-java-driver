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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.data.GettableById;
import com.datastax.oss.driver.api.core.data.GettableByIndex;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.detach.Detachable;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A row from a CQL table.
 *
 * <p>The default implementation returned by the driver is immutable and serializable. If you write
 * your own implementation, it should at least be thread-safe; serializability is not mandatory, but
 * recommended for use with some 3rd-party tools like Apache Spark &trade;.
 */
public interface Row extends GettableByIndex, GettableByName, GettableById, Detachable {

  /** @return the column definitions contained in this result set. */
  @NonNull
  ColumnDefinitions getColumnDefinitions();
}
