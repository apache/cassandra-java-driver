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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.util.Collections;
import java.util.List;

/**
 * Any update to the driver's metadata. It produces a new metadata instance, and may also trigger
 * events.
 *
 * <p>This is modelled as a separate type for modularity, and because we can't send the events while
 * we are doing the refresh (by contract, the new copy of the metadata needs to be visible before
 * the events are sent). This also makes unit testing very easy.
 *
 * <p>This is only instantiated and called from {@link MetadataManager}'s admin thread, therefore
 * implementations don't need to be thread-safe.
 *
 * @see Session#getMetadata()
 */
public interface MetadataRefresh {

  Result compute(
      DefaultMetadata oldMetadata, boolean tokenMapEnabled, InternalDriverContext context);

  class Result {
    public final DefaultMetadata newMetadata;
    public final List<Object> events;

    public Result(DefaultMetadata newMetadata, List<Object> events) {
      this.newMetadata = newMetadata;
      this.events = events;
    }

    public Result(DefaultMetadata newMetadata) {
      this(newMetadata, Collections.emptyList());
    }
  }
}
