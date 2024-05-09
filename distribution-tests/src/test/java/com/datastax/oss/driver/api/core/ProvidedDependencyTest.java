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

import com.datastax.oss.driver.internal.core.util.Reflection;
import org.junit.Test;

public class ProvidedDependencyTest {
  @Test
  public void should_not_include_graal_sdk_jar() {
    assertThat(Reflection.loadClass(null, "org.graalvm.nativeimage.VMRuntime")).isNull();
  }

  @Test
  public void should_not_include_spotbugs_annotations_jar() {
    assertThat(Reflection.loadClass(null, "edu.umd.cs.findbugs.annotations.NonNull")).isNull();
  }

  @Test
  public void should_not_include_jicp_annotations_jar() {
    assertThat(Reflection.loadClass(null, "net.jcip.annotations.ThreadSafe")).isNull();
  }

  @Test
  public void should_not_include_blockhound_jar() {
    assertThat(Reflection.loadClass(null, "reactor.blockhound.BlockHoundRuntime")).isNull();
  }
}
