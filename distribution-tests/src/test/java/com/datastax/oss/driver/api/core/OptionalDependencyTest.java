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

import com.datastax.oss.driver.internal.core.util.DependencyCheck;
import org.junit.Test;

public class OptionalDependencyTest {
  @Test
  public void should_not_include_snappy_jar() {
    assertThat(DependencyCheck.SNAPPY.isPresent()).isFalse();
  }

  @Test
  public void should_not_include_l4z_jar() {
    assertThat(DependencyCheck.LZ4.isPresent()).isFalse();
  }
}
