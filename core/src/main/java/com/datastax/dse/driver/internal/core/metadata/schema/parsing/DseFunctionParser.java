/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
    final boolean deterministic =
        row.contains("deterministic") ? row.getBoolean("deterministic") : false;
    final boolean monotonic = row.contains("monotonic") ? row.getBoolean("monotonic") : false;
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
