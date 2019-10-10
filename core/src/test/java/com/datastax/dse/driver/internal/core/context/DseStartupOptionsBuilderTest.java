/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.StartupOptionsBuilder;
import com.datastax.oss.protocol.internal.request.Startup;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.util.UUID;
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

  private void buildContext(UUID clientId, String applicationName, String applicationVersion) {
    this.driverContext =
        new DefaultDriverContext(
            configLoader,
            ProgrammaticArguments.builder()
                .withStartupClientId(clientId)
                .withStartupApplicationName(applicationName)
                .withStartupApplicationVersion(applicationVersion)
                .build());
  }

  private void assertDefaultStartupOptions(Startup startup) {
    assertThat(startup.options).containsEntry(Startup.CQL_VERSION_KEY, "3.0.0");
    assertThat(startup.options)
        .containsEntry(
            StartupOptionsBuilder.DRIVER_NAME_KEY, Session.OSS_DRIVER_COORDINATES.getName());
    assertThat(startup.options).containsKey(StartupOptionsBuilder.DRIVER_VERSION_KEY);
    Version version = Version.parse(startup.options.get(StartupOptionsBuilder.DRIVER_VERSION_KEY));
    assertThat(version).isEqualTo(Session.OSS_DRIVER_COORDINATES.getVersion());
    assertThat(startup.options).containsKey(StartupOptionsBuilder.CLIENT_ID_KEY);
  }

  @Test
  public void should_build_startup_options_with_no_compression_if_undefined() {
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn("none");
    buildContext(null, null, null);
    Startup startup = new Startup(driverContext.getStartupOptions());
    assertThat(startup.options).doesNotContainKey(Startup.COMPRESSION_KEY);
    assertDefaultStartupOptions(startup);
  }

  @Test
  @DataProvider({"lz4", "snappy"})
  public void should_build_startup_options_with_compression(String compression) {
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn(compression);
    buildContext(null, null, null);
    Startup startup = new Startup(driverContext.getStartupOptions());
    // assert the compression option is present
    assertThat(startup.options).containsEntry(Startup.COMPRESSION_KEY, compression);
    assertThat(startup.options).doesNotContainKey(StartupOptionsBuilder.APPLICATION_NAME_KEY);
    assertThat(startup.options).doesNotContainKey(StartupOptionsBuilder.APPLICATION_VERSION_KEY);
    assertDefaultStartupOptions(startup);
  }

  @Test
  public void should_fail_to_build_startup_options_with_invalid_compression() {
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn("foobar");
    buildContext(null, null, null);
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new Startup(driverContext.getStartupOptions()));
  }

  @Test
  public void should_build_startup_options_with_client_id() {
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn("none");
    UUID customClientId = Uuids.random();
    buildContext(customClientId, null, null);
    Startup startup = new Startup(driverContext.getStartupOptions());
    // assert the client id is present
    assertThat(startup.options)
        .containsEntry(StartupOptionsBuilder.CLIENT_ID_KEY, customClientId.toString());
    assertThat(startup.options).doesNotContainKey(Startup.COMPRESSION_KEY);
    assertThat(startup.options).doesNotContainKey(StartupOptionsBuilder.APPLICATION_NAME_KEY);
    assertThat(startup.options).doesNotContainKey(StartupOptionsBuilder.APPLICATION_VERSION_KEY);
    assertDefaultStartupOptions(startup);
  }

  @Test
  public void should_build_startup_options_with_application_version_and_name() {
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn("none");
    buildContext(null, "Custom_App_Name", "Custom_App_Version");
    Startup startup = new Startup(driverContext.getStartupOptions());
    // assert the app name and version are present
    assertThat(startup.options)
        .containsEntry(StartupOptionsBuilder.APPLICATION_NAME_KEY, "Custom_App_Name");
    assertThat(startup.options)
        .containsEntry(StartupOptionsBuilder.APPLICATION_VERSION_KEY, "Custom_App_Version");
    assertThat(startup.options).doesNotContainKey(Startup.COMPRESSION_KEY);
    assertDefaultStartupOptions(startup);
  }

  @Test
  public void should_build_startup_options_with_all_options() {
    // mock config to specify "snappy" compression
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn("snappy");

    UUID customClientId = Uuids.random();

    buildContext(customClientId, "Custom_App_Name", "Custom_App_Version");
    Startup startup = new Startup(driverContext.getStartupOptions());
    assertThat(startup.options)
        .containsEntry(StartupOptionsBuilder.CLIENT_ID_KEY, customClientId.toString())
        .containsEntry(StartupOptionsBuilder.APPLICATION_NAME_KEY, "Custom_App_Name")
        .containsEntry(StartupOptionsBuilder.APPLICATION_VERSION_KEY, "Custom_App_Version");
    assertThat(startup.options).containsEntry(Startup.COMPRESSION_KEY, "snappy");
    assertDefaultStartupOptions(startup);
  }

  @Test
  public void should_use_configuration_when_no_programmatic_values_provided() {
    when(defaultProfile.getString(DseDriverOption.APPLICATION_NAME, null))
        .thenReturn("Config_App_Name");
    when(defaultProfile.getString(DseDriverOption.APPLICATION_VERSION, null))
        .thenReturn("Config_App_Version");
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn("none");

    buildContext(null, null, null);
    Startup startup = new Startup(driverContext.getStartupOptions());

    assertThat(startup.options)
        .containsEntry(StartupOptionsBuilder.APPLICATION_NAME_KEY, "Config_App_Name")
        .containsEntry(StartupOptionsBuilder.APPLICATION_VERSION_KEY, "Config_App_Version");
  }

  @Test
  public void should_ignore_configuration_when_programmatic_values_provided() {
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn("none");

    buildContext(null, "Custom_App_Name", "Custom_App_Version");
    Startup startup = new Startup(driverContext.getStartupOptions());

    assertThat(startup.options)
        .containsEntry(StartupOptionsBuilder.APPLICATION_NAME_KEY, "Custom_App_Name")
        .containsEntry(StartupOptionsBuilder.APPLICATION_VERSION_KEY, "Custom_App_Version");
  }
}
