/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

public class SampleGraphScripts {

  public static final String MAKE_STRICT =
      "schema.config().option('graph.schema_mode').set('production');\n";

  public static final String MAKE_NOT_STRICT =
      "schema.config().option('graph.schema_mode').set('development');\n";

  public static final String ALLOW_SCANS =
      "schema.config().option('graph.allow_scan').set('true');\n";

  public static final String MODERN_SCHEMA =
      "schema.propertyKey('name').Text().ifNotExists().create();\n"
          + "schema.propertyKey('age').Int().ifNotExists().create();\n"
          + "schema.propertyKey('lang').Text().ifNotExists().create();\n"
          + "schema.propertyKey('weight').Float().ifNotExists().create();\n"
          + "schema.vertexLabel('person').properties('name', 'age').ifNotExists().create();\n"
          + "schema.vertexLabel('software').properties('name', 'lang').ifNotExists().create();\n"
          + "schema.edgeLabel('created').properties('weight').connection('person', 'software').ifNotExists().create();\n"
          + "schema.edgeLabel('knows').properties('weight').connection('person', 'person').ifNotExists().create();\n";

  public static String MODERN_GRAPH =
      MODERN_SCHEMA
          + "marko = g.addV('person').property('name', 'marko').property('age', 29).next();\n"
          + "vadas = g.addV('person').property('name', 'vadas').property('age', 27).next();\n"
          + "josh = g.addV('person').property('name', 'josh').property('age', 32).next();\n"
          + "peter = g.addV('person').property('name', 'peter').property('age', 35).next();\n"
          + "lop = g.addV('software').property('name', 'lop').property('lang', 'java').next();\n"
          + "ripple = g.addV('software').property('name', 'ripple').property('lang', 'java').next();\n"
          + "g.addE('knows').from(marko).to(vadas).property('weight', 0.5f).next();\n"
          + "g.addE('knows').from(marko).to(josh).property('weight', 1.0f).next();\n"
          + "g.addE('created').from(marko).to(lop).property('weight', 0.4f).next();\n"
          + "g.addE('created').from(josh).to(ripple).property('weight', 1.0f).next();\n"
          + "g.addE('created').from(josh).to(lop).property('weight', 0.4f).next();\n"
          + "g.addE('created').from(peter).to(lop).property('weight', 0.2f);";
}
