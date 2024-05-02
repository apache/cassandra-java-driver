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
package com.datastax.oss.driver.api.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.internal.core.util.Dependency;
import com.datastax.oss.driver.internal.core.util.Reflection;
import org.junit.Test;

public class OptionalDependencyTest {
  @Test
  public void should_not_include_snappy_jar() {
    Dependency.SNAPPY
        .classes()
        .forEach(clazz -> assertThat(Reflection.loadClass(null, clazz)).isNull());
  }

  @Test
  public void should_not_include_l4z_jar() {
    Dependency.LZ4
        .classes()
        .forEach(clazz -> assertThat(Reflection.loadClass(null, clazz)).isNull());
  }

  @Test
  public void should_not_include_esri_jar() {
    Dependency.ESRI
        .classes()
        .forEach(clazz -> assertThat(Reflection.loadClass(null, clazz)).isNull());
  }

  @Test
  public void should_not_include_tinkerpop_jar() {
    Dependency.TINKERPOP
        .classes()
        .forEach(clazz -> assertThat(Reflection.loadClass(null, clazz)).isNull());
  }
}
