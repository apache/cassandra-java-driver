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

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.mapper.MapperBuilder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.internal.core.util.Reflection;
import com.datastax.oss.driver.internal.mapper.processor.MapperProcessor;
import com.datastax.oss.driver.internal.metrics.micrometer.MicrometerMetricsFactory;
import com.datastax.oss.driver.internal.metrics.microprofile.MicroProfileMetricsFactory;
import org.junit.Test;

public class DriverDependencyTest {
  @Test
  public void should_include_core_jar() {
    assertThat(Reflection.loadClass(null, "com.datastax.oss.driver.api.core.session.Session"))
        .isEqualTo(Session.class);
  }

  @Test
  public void should_include_query_builder_jar() {
    assertThat(Reflection.loadClass(null, "com.datastax.oss.driver.api.querybuilder.QueryBuilder"))
        .isEqualTo(QueryBuilder.class);
  }

  @Test
  public void should_include_mapper_processor_jar() {
    assertThat(
            Reflection.loadClass(
                null, "com.datastax.oss.driver.internal.mapper.processor.MapperProcessor"))
        .isEqualTo(MapperProcessor.class);
  }

  @Test
  public void should_include_mapper_runtime_jar() {
    assertThat(Reflection.loadClass(null, "com.datastax.oss.driver.api.mapper.MapperBuilder"))
        .isEqualTo(MapperBuilder.class);
  }

  @Test
  public void should_include_metrics_micrometer_jar() {
    assertThat(
            Reflection.loadClass(
                null,
                "com.datastax.oss.driver.internal.metrics.micrometer.MicrometerMetricsFactory"))
        .isEqualTo(MicrometerMetricsFactory.class);
  }

  @Test
  public void should_include_metrics_microprofile_jar() {
    assertThat(
            Reflection.loadClass(
                null,
                "com.datastax.oss.driver.internal.metrics.microprofile.MicroProfileMetricsFactory"))
        .isEqualTo(MicroProfileMetricsFactory.class);
  }

  @Test
  public void should_include_test_infra_jar() {
    assertThat(
            Reflection.loadClass(
                null, "com.datastax.oss.driver.api.testinfra.CassandraResourceRule"))
        .isEqualTo(CassandraResourceRule.class);
  }
}
