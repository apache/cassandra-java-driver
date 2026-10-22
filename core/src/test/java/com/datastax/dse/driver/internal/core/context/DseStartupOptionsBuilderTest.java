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
package com.datastax.dse.driver.internal.core.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.StartupOptionsBuilder;
import com.datastax.oss.protocol.internal.request.Startup;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

@RunWith(DataProviderRunner.class)
public class DseStartupOptionsBuilderTest {

  private DefaultDriverContext driverContext;

  // Mocks for instantiating the DSE driver context
  @Mock private DriverConfigLoader configLoader;
  @Mock private DriverConfig driverConfig;
  @Mock private DriverExecutionProfile defaultProfile;

  @Before
  public void before() {
    initMocks(this);
    when(configLoader.getInitialConfig()).thenReturn(driverConfig);
    when(driverConfig.getDefaultProfile()).thenReturn(defaultProfile);
    when(defaultProfile.isDefined(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE)).thenReturn(true);
  }

  private void buildContext() {
    this.driverContext =
        new DefaultDriverContext(configLoader, ProgrammaticArguments.builder().build());
  }

  private void assertDefaultStartupOptions(Startup startup) {
    assertThat(startup.options).containsEntry(Startup.CQL_VERSION_KEY, "3.0.0");
    assertThat(startup.options)
        .containsEntry(
            StartupOptionsBuilder.DRIVER_NAME_KEY, Session.OSS_DRIVER_COORDINATES.getName());
    assertThat(startup.options).containsKey(StartupOptionsBuilder.DRIVER_VERSION_KEY);
    Version version = Version.parse(startup.options.get(StartupOptionsBuilder.DRIVER_VERSION_KEY));
    assertThat(version).isEqualTo(Session.OSS_DRIVER_COORDINATES.getVersion());
  }

  @Test
  public void should_build_startup_options_with_no_compression_if_undefined() {
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn("none");
    buildContext();
    Startup startup = new Startup(driverContext.getStartupOptions());
    assertThat(startup.options).doesNotContainKey(Startup.COMPRESSION_KEY);
    assertDefaultStartupOptions(startup);
  }

  @Test
  @DataProvider({"lz4", "snappy"})
  public void should_build_startup_options_with_compression(String compression) {
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn(compression);
    buildContext();
    Startup startup = new Startup(driverContext.getStartupOptions());
    // assert the compression option is present
    assertThat(startup.options).containsEntry(Startup.COMPRESSION_KEY, compression);
    assertDefaultStartupOptions(startup);
  }

  @Test
  public void should_fail_to_build_startup_options_with_invalid_compression() {
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn("foobar");
    buildContext();
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new Startup(driverContext.getStartupOptions()));
  }

  @Test
  public void should_build_startup_options_with_all_options() {
    // mock config to specify "snappy" compression
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn("snappy");

    buildContext();
    Startup startup = new Startup(driverContext.getStartupOptions());
    assertThat(startup.options).containsEntry(Startup.COMPRESSION_KEY, "snappy");
    assertDefaultStartupOptions(startup);
  }
}
