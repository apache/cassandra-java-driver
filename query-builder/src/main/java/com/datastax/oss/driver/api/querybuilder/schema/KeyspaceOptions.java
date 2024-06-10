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
package com.datastax.oss.driver.api.querybuilder.schema;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface KeyspaceOptions<SelfT extends KeyspaceOptions<SelfT>>
    extends OptionProvider<SelfT> {

  /**
   * Adjusts durable writes configuration for this keyspace. If set to false, data written to the
   * keyspace will bypass the commit log.
   */
  @NonNull
  @CheckReturnValue
  default SelfT withDurableWrites(boolean durableWrites) {
    return withOption("durable_writes", durableWrites);
  }
}
