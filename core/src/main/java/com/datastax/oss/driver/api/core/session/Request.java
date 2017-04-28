/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.session;

/**
 * A request executed by a {@link Session}.
 *
 * <p>This is a high-level abstraction, agnostic to the actual language (e.g. CQL). A request can be
 * anything that can be converted to a protocol message, provided that you register a request
 * processor with the driver to do that conversion.
 *
 * @param <SyncResultT> the type of response when this request is executed synchronously.
 * @param <AsyncResultT> the type of response when this request is executed asynchronously.
 */
public interface Request<SyncResultT, AsyncResultT> {

  /** The name of the configuration profile that will be used for execution. */
  String getConfigProfile();
}
