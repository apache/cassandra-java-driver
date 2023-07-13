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
package com.datastax.oss.driver.internal.core.util;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;

/**
 * A set of driver optional dependencies and a common mechanism to test the presence of such
 * dependencies on the application's classpath.
 */
public enum DependencyCheck {
  SNAPPY("org.xerial.snappy.Snappy"),
  LZ4("net.jpountz.lz4.LZ4Compressor"),
  ESRI("com.esri.core.geometry.ogc.OGCGeometry"),
  TINKERPOP(
      // gremlin-core
      "org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal",
      // tinkergraph-gremlin
      "org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV3d0"),
  REACTIVE_STREAMS("org.reactivestreams.Publisher"),
  JACKSON(
      // jackson-core
      "com.fasterxml.jackson.core.JsonParser",
      // jackson-databind
      "com.fasterxml.jackson.databind.ObjectMapper"),
  ;

  /**
   * The fully-qualified name of classes that must exist for the dependency to work properly; we use
   * them to test the presence of the whole dependency on the classpath, including its transitive
   * dependencies if applicable. This assumes that if these classes are present, then the entire
   * library is present and functional, and vice versa.
   *
   * <p>Note: some of the libraries declared here may be shaded; in these cases the shade plugin
   * will replace the package names listed above with names starting with {@code
   * com.datastax.oss.driver.shaded.*}, but the presence check would still work as expected.
   */
  @SuppressWarnings("ImmutableEnumChecker")
  private final ImmutableSet<String> fqcns;

  DependencyCheck(String... fqcns) {
    this.fqcns = ImmutableSet.copyOf(fqcns);
  }

  /**
   * Checks if the dependency is present on the application's classpath and is loadable.
   *
   * @return true if the dependency is present and loadable, false otherwise.
   */
  public boolean isPresent() {
    for (String fqcn : fqcns) {
      // Always use the driver class loader, assuming that the driver classes and
      // the dependency classes are either being loaded by the same class loader,
      // or – as in OSGi deployments – by two distinct, but compatible class loaders.
      if (Reflection.loadClass(null, fqcn) == null) {
        return false;
      }
    }
    return true;
  }
}
