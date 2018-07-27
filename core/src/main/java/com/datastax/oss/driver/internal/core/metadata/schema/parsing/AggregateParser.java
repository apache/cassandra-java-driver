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
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultAggregateMetadata;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class AggregateParser {
  private final DataTypeParser dataTypeParser;
  private final InternalDriverContext context;

  public AggregateParser(DataTypeParser dataTypeParser, InternalDriverContext context) {
    this.dataTypeParser = dataTypeParser;
    this.context = context;
  }

  public AggregateMetadata parseAggregate(
      AdminRow row,
      CqlIdentifier keyspaceId,
      Map<CqlIdentifier, UserDefinedType> userDefinedTypes) {
    // Cassandra < 3.0:
    // CREATE TABLE system.schema_aggregates (
    //     keyspace_name text,
    //     aggregate_name text,
    //     signature frozen<list<text>>,
    //     argument_types list<text>,
    //     final_func text,
    //     initcond blob,
    //     return_type text,
    //     state_func text,
    //     state_type text,
    //     PRIMARY KEY (keyspace_name, aggregate_name, signature)
    // ) WITH CLUSTERING ORDER BY (aggregate_name ASC, signature ASC)
    //
    // Cassandra >= 3.0:
    // CREATE TABLE system.schema_aggregates (
    //     keyspace_name text,
    //     aggregate_name text,
    //     argument_types frozen<list<text>>,
    //     final_func text,
    //     initcond text,
    //     return_type text,
    //     state_func text,
    //     state_type text,
    //     PRIMARY KEY (keyspace_name, aggregate_name, argument_types)
    // ) WITH CLUSTERING ORDER BY (aggregate_name ASC, argument_types ASC)
    String simpleName = row.getString("aggregate_name");
    List<String> argumentTypes = row.getListOfString("argument_types");
    FunctionSignature signature =
        new FunctionSignature(
            CqlIdentifier.fromInternal(simpleName),
            dataTypeParser.parse(keyspaceId, argumentTypes, userDefinedTypes, context));

    DataType stateType =
        dataTypeParser.parse(keyspaceId, row.getString("state_type"), userDefinedTypes, context);
    TypeCodec<Object> stateTypeCodec = context.getCodecRegistry().codecFor(stateType);

    String stateFuncSimpleName = row.getString("state_func");
    FunctionSignature stateFuncSignature =
        new FunctionSignature(
            CqlIdentifier.fromInternal(stateFuncSimpleName),
            ImmutableList.<DataType>builder()
                .add(stateType)
                .addAll(signature.getParameterTypes())
                .build());

    String finalFuncSimpleName = row.getString("final_func");
    FunctionSignature finalFuncSignature =
        (finalFuncSimpleName == null)
            ? null
            : new FunctionSignature(CqlIdentifier.fromInternal(finalFuncSimpleName), stateType);

    DataType returnType =
        dataTypeParser.parse(keyspaceId, row.getString("return_type"), userDefinedTypes, context);

    Object initCond;
    if (row.isString("initcond")) { // Cassandra 3
      String initCondString = row.getString("initcond");
      initCond = (initCondString == null) ? null : stateTypeCodec.parse(initCondString);
    } else { // Cassandra 2.2
      ByteBuffer initCondBlob = row.getByteBuffer("initcond");
      initCond =
          (initCondBlob == null)
              ? null
              : stateTypeCodec.decode(initCondBlob, context.getProtocolVersion());
    }
    return new DefaultAggregateMetadata(
        keyspaceId,
        signature,
        finalFuncSignature,
        initCond,
        returnType,
        stateFuncSignature,
        stateType,
        stateTypeCodec);
  }
}
