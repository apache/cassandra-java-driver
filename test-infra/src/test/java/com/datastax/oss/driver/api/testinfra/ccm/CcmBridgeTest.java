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
package com.datastax.oss.driver.api.testinfra.ccm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.Test;

public class CcmBridgeTest {

  @Test
  public void should_parse_system_jvm() {
    assertThat(CcmBridge.getCurrentJvmMajorVersion()).isGreaterThan(0);
  }

  @Test
  public void should_parse_jvm_8_like() {
    assertThat(CcmBridge.getJvmMajorVersion("1.8.0_181")).isEqualTo(8);
  }

  @Test
  public void should_parse_jvm_8_like_with_trailing_alphabetic() {
    assertThat(CcmBridge.getJvmMajorVersion("1.8.0_181_b1")).isEqualTo(8);
  }

  @Test
  public void should_parse_jvm_11_like() {
    assertThat(CcmBridge.getJvmMajorVersion("11.0.20.1")).isEqualTo(11);
  }

  @Test
  public void should_parse_jvm_17_like() {
    assertThat(CcmBridge.getJvmMajorVersion("17.0.8.1")).isEqualTo(17);
  }

  @Test
  public void should_parse_jvm_21_like() {
    assertThat(CcmBridge.getJvmMajorVersion("21.0.1")).isEqualTo(21);
  }

  @Test
  public void should_fail_null_version() {
    assertThatCode(() -> CcmBridge.getJvmMajorVersion(null))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void should_fail_empty_version() {
    assertThatCode(() -> CcmBridge.getJvmMajorVersion(""))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void should_fail_non_number() {
    assertThatCode(() -> CcmBridge.getJvmMajorVersion("abc"))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void should_fail_not_versioned() {
    assertThatCode(() -> CcmBridge.getJvmMajorVersion("8"))
        .isInstanceOf(IllegalStateException.class);
  }
}
