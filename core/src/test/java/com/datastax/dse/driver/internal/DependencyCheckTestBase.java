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
package com.datastax.dse.driver.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.Test;

public abstract class DependencyCheckTestBase {

  private String baseResourcePath;

  protected DependencyCheckTestBase() {
    Properties projectProperties = new Properties();
    try (InputStream is = this.getClass().getResourceAsStream("/project.properties")) {
      projectProperties.load(is);
      baseResourcePath = projectProperties.getProperty("project.basedir");
    } catch (IOException ioe) {
      throw new AssertionError(
          "Error retrieving \"project.basedir\" value from \"/project.properties\". Please check test resources in this project.",
          ioe);
    }
    assert baseResourcePath != null;
  }

  @Test
  public void should_generate_deps_txt() {
    assertThat(getDepsTxtPath()).exists();
  }

  protected final String getBaseResourcePathString() {
    return baseResourcePath;
  }

  protected abstract Path getDepsTxtPath();
}
