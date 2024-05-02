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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.time.ServerSideTimestampGenerator;
import com.datastax.oss.driver.internal.core.util.Sizes;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.request.query.Values;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
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
  @NonNull
  PreparedStatement getPreparedStatement();

  /** The values to bind, in their serialized form. */
  @NonNull
  List<ByteBuffer> getValues();

  /**
   * Always returns {@code null} (bound statements can't have a per-request keyspace, they always
   * inherit the one of the statement that was initially prepared).
   */
  @Override
  @Nullable
  default CqlIdentifier getKeyspace() {
    return null;
  }

  @Override
  default int computeSizeInBytes(@NonNull DriverContext context) {
    int size = Sizes.minimumStatementSize(this, context);

    // BoundStatement's additional elements to take into account are:
    // - prepared ID
    // - result metadata ID
    // - parameters
    // - page size
    // - paging state
    // - timestamp

    // prepared ID
    size += PrimitiveSizes.sizeOfShortBytes(getPreparedStatement().getId());

    // result metadata ID
    if (getPreparedStatement().getResultMetadataId() != null) {
      size += PrimitiveSizes.sizeOfShortBytes(getPreparedStatement().getResultMetadataId());
    }

    // parameters (always sent as positional values for bound statements)
    size += Values.sizeOfPositionalValues(getValues());

    // page size
    size += PrimitiveSizes.INT;

    // paging state
    if (getPagingState() != null) {
      size += PrimitiveSizes.sizeOfBytes(getPagingState());
    }

    // timestamp
    if (!(context.getTimestampGenerator() instanceof ServerSideTimestampGenerator)
        || getQueryTimestamp() != Statement.NO_DEFAULT_TIMESTAMP) {
      size += PrimitiveSizes.LONG;
    }

    return size;
  }
}
