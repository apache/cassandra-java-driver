/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import java.io.File;
import java.net.URL;
import java.time.Duration;
import org.junit.Test;

public class DseDriverConfigLoaderTest {

  @Test
  public void should_load_from_other_classpath_resource() {
    DriverConfigLoader loader = DseDriverConfigLoader.fromClasspath("config/customApplication");
    DriverExecutionProfile config = loader.getInitialConfig().getDefaultProfile();
    // From customApplication.conf:
    assertThat(config.getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
        .isEqualTo(Duration.ofMillis(500));
    assertThat(config.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES)).isEqualTo(10);
    // From customApplication.json:
    assertThat(config.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE)).isEqualTo(2000);
    assertThat(config.getInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE)).isEqualTo(2000);
    // From customApplication.properties:
    assertThat(config.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
        .isEqualTo(DefaultConsistencyLevel.ONE.name());
    assertThat(config.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES)).isEqualTo(8);
    // From reference.conf:
    assertThat(config.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .isEqualTo(DefaultConsistencyLevel.SERIAL.name());
    // From dse-reference.conf:
    assertThat(config.getDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE))
        .isEqualTo(Duration.ofSeconds(2));
  }

  @Test
  public void should_load_from_file() {
    File file = new File("src/test/resources/config/customApplication.conf");
    assertThat(file).exists();
    DriverConfigLoader loader = DseDriverConfigLoader.fromFile(file);
    DriverExecutionProfile config = loader.getInitialConfig().getDefaultProfile();
    // From customApplication.conf:
    assertThat(config.getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
        .isEqualTo(Duration.ofMillis(500));
    assertThat(config.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES)).isEqualTo(10);
    // From reference.conf:
    assertThat(config.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .isEqualTo(DefaultConsistencyLevel.SERIAL.name());
    // From dse-reference.conf:
    assertThat(config.getDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE))
        .isEqualTo(Duration.ofSeconds(2));
  }

  @Test
  public void should_load_from_url() throws Exception {
    URL url = new File("src/test/resources/config/customApplication.conf").toURI().toURL();
    DriverConfigLoader loader = DseDriverConfigLoader.fromUrl(url);
    DriverExecutionProfile config = loader.getInitialConfig().getDefaultProfile();
    // From customApplication.conf:
    assertThat(config.getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
        .isEqualTo(Duration.ofMillis(500));
    assertThat(config.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES)).isEqualTo(10);
    // From reference.conf:
    assertThat(config.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .isEqualTo(DefaultConsistencyLevel.SERIAL.name());
    // From dse-reference.conf:
    assertThat(config.getDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE))
        .isEqualTo(Duration.ofSeconds(2));
  }

  @Test
  public void should_build_programmatically() {
    DriverConfigLoader loader =
        DseDriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(500))
            .withInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES, 10)
            .startProfile("slow")
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .build();
    DriverConfig config = loader.getInitialConfig();

    DriverExecutionProfile defaultProfile = config.getDefaultProfile();
    // From programmatic overrides:
    assertThat(defaultProfile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
        .isEqualTo(Duration.ofMillis(500));
    assertThat(defaultProfile.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES)).isEqualTo(10);
    // From reference.conf:
    assertThat(defaultProfile.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .isEqualTo(DefaultConsistencyLevel.SERIAL.name());
    // From dse-reference.conf:
    assertThat(defaultProfile.getDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE))
        .isEqualTo(Duration.ofSeconds(2));

    DriverExecutionProfile slowProfile = config.getProfile("slow");
    // From programmatic override:
    assertThat(slowProfile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT))
        .isEqualTo(Duration.ofSeconds(30));
    // Inherited from the default profile (where the option was overridden programmatically)
    assertThat(slowProfile.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES)).isEqualTo(10);
    // Inherited from the default profile (where the option was pulled from reference.conf)
    assertThat(slowProfile.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
        .isEqualTo(DefaultConsistencyLevel.SERIAL.name());
    // Inherited from the default profile (where the option was pulled from dse-reference.conf)
    assertThat(slowProfile.getDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE))
        .isEqualTo(Duration.ofSeconds(2));
  }
}
