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
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultFunctionMetadata;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class FunctionParser {

  private static final Logger LOG = LoggerFactory.getLogger(FunctionParser.class);

  private final DataTypeParser dataTypeParser;
  private final InternalDriverContext context;
  private final String logPrefix;

  public FunctionParser(DataTypeParser dataTypeParser, InternalDriverContext context) {
    this.dataTypeParser = dataTypeParser;
    this.context = context;
    this.logPrefix = context.getSessionName();
  }

  public FunctionMetadata parseFunction(
      AdminRow row,
      CqlIdentifier keyspaceId,
      Map<CqlIdentifier, UserDefinedType> userDefinedTypes) {
    // Cassandra < 3.0:
    // CREATE TABLE system.schema_functions (
    //     keyspace_name text,
    //     function_name text,
    //     signature frozen<list<text>>,
    //     argument_names list<text>,
    //     argument_types list<text>,
    //     body text,
    //     called_on_null_input boolean,
    //     language text,
    //     return_type text,
    //     PRIMARY KEY (keyspace_name, function_name, signature)
    // ) WITH CLUSTERING ORDER BY (function_name ASC, signature ASC)
    //
    // Cassandra >= 3.0:
    // CREATE TABLE system_schema.functions (
    //     keyspace_name text,
    //     function_name text,
    //     argument_names frozen<list<text>>,
    //     argument_types frozen<list<text>>,
    //     body text,
    //     called_on_null_input boolean,
    //     language text,
    //     return_type text,
    //     PRIMARY KEY (keyspace_name, function_name, argument_types)
    // ) WITH CLUSTERING ORDER BY (function_name ASC, argument_types ASC)
    String simpleName = row.getString("function_name");
    List<CqlIdentifier> argumentNames =
        ImmutableList.copyOf(
            Lists.transform(row.getListOfString("argument_names"), CqlIdentifier::fromInternal));
    List<String> argumentTypes = row.getListOfString("argument_types");
    if (argumentNames.size() != argumentTypes.size()) {
      LOG.warn(
          "[{}] Error parsing system row for function {}.{}, "
              + "number of argument names and types don't match (got {} and {}).",
          logPrefix,
          keyspaceId.asInternal(),
          simpleName,
          argumentNames.size(),
          argumentTypes.size());
      return null;
    }
    FunctionSignature signature =
        new FunctionSignature(
            CqlIdentifier.fromInternal(simpleName),
            dataTypeParser.parse(keyspaceId, argumentTypes, userDefinedTypes, context));
    String body = row.getString("body");
    Boolean calledOnNullInput = row.getBoolean("called_on_null_input");
    String language = row.getString("language");
    DataType returnType =
        dataTypeParser.parse(keyspaceId, row.getString("return_type"), userDefinedTypes, context);

    return new DefaultFunctionMetadata(
        keyspaceId, signature, argumentNames, body, calledOnNullInput, language, returnType);
  }
}
