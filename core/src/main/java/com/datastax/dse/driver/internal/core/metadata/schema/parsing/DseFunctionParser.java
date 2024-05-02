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
package com.datastax.dse.driver.internal.core.metadata.schema.parsing;

import com.datastax.dse.driver.api.core.metadata.schema.DseFunctionMetadata;
import com.datastax.dse.driver.internal.core.metadata.schema.DefaultDseFunctionMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DataTypeParser;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.FunctionParser;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DseFunctionParser {

  private final FunctionParser functionParser;

  public DseFunctionParser(DataTypeParser dataTypeParser, InternalDriverContext context) {
    this.functionParser = new FunctionParser(dataTypeParser, context);
  }

  public DseFunctionMetadata parseFunction(
      AdminRow row,
      CqlIdentifier keyspaceId,
      Map<CqlIdentifier, UserDefinedType> userDefinedTypes) {
    FunctionMetadata function = functionParser.parseFunction(row, keyspaceId, userDefinedTypes);
    // parse the DSE extended columns
    final Boolean deterministic =
        row.contains("deterministic") ? row.getBoolean("deterministic") : null;
    final Boolean monotonic = row.contains("monotonic") ? row.getBoolean("monotonic") : null;
    // stream the list of strings into a list of CqlIdentifiers
    final List<CqlIdentifier> monotonicOn =
        row.contains("monotonic_on")
            ? row.getListOfString("monotonic_on").stream()
                .map(CqlIdentifier::fromInternal)
                .collect(Collectors.toList())
            : Collections.emptyList();

    return new DefaultDseFunctionMetadata(
        function.getKeyspace(),
        function.getSignature(),
        function.getParameterNames(),
        function.getBody(),
        function.isCalledOnNullInput(),
        function.getLanguage(),
        function.getReturnType(),
        deterministic,
        monotonic,
        monotonicOn);
  }
}
