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
package com.datastax.oss.driver.api.core.cql;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A prepared statement in its executable form, with values bound to the variables.
 *
 * <p>The default implementation returned by the driver is <b>immutable</b> and <b>thread-safe</b>.
 * All mutating methods return a new instance.
 */
public interface BoundStatement
    extends BatchableStatement<BoundStatement>, Bindable<BoundStatement> {

  /** The prepared statement that was used to create this statement. */
  PreparedStatement getPreparedStatement();

  /** The values to bind, in their serialized form. */
  List<ByteBuffer> getValues();
}
