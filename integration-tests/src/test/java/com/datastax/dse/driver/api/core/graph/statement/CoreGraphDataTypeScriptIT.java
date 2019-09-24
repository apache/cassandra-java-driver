/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph.statement;

import com.datastax.dse.driver.api.core.graph.CoreGraphDataTypeITBase;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatementBuilder;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.util.Map;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@DseRequirement(min = "6.8.0", description = "DSE 6.8.0 required for Core graph support")
@RunWith(DataProviderRunner.class)
@Category(IsolatedTests.class)
public class CoreGraphDataTypeScriptIT extends CoreGraphDataTypeITBase {

  @Override
  protected Map<Object, Object> insertVertexThenReadProperties(
      Map<String, Object> properties, int vertexID, String vertexLabel) {
    StringBuilder insert = new StringBuilder("g.addV(vertexLabel).property('id', vertexID)");

    ScriptGraphStatementBuilder statementBuilder =
        new ScriptGraphStatementBuilder()
            .setQueryParam("vertexID", vertexID)
            .setQueryParam("vertexLabel", vertexLabel);

    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String typeDefinition = entry.getKey();
      String propName = formatPropertyName(typeDefinition);
      Object value = entry.getValue();

      insert.append(String.format(".property('%s', %s)", propName, propName));
      statementBuilder = statementBuilder.setQueryParam(propName, value);
    }

    session().execute(statementBuilder.setScript(insert.toString()).build());

    return session()
        .execute(
            ScriptGraphStatement.newInstance(
                    "g.V().has(vertexLabel, 'id', vertexID).valueMap().by(unfold())")
                .setQueryParam("vertexID", vertexID)
                .setQueryParam("vertexLabel", vertexLabel))
        .one()
        .asMap();
  }
}
