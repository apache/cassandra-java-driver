package com.datastax.oss.driver.internal.core.protocol;

import java.util.List;
import java.util.Map;

public class TabletInfo {
  private static final String SCYLLA_TABLETS_STARTUP_OPTION_KEY = "TABLETS_ROUTING_V1";
  private static final String SCYLLA_TABLETS_STARTUP_OPTION_VALUE = "";
  public static final String TABLETS_ROUTING_V1_CUSTOM_PAYLOAD_KEY = "tablets-routing-v1";

  private boolean enabled = false;

  private TabletInfo(boolean enabled) {
    this.enabled = enabled;
  }

  // Currently pertains only to TABLETS_ROUTING_V1
  public boolean isEnabled() {
    return enabled;
  }

  public static TabletInfo parseTabletInfo(Map<String, List<String>> supported) {
    List<String> values = supported.get(SCYLLA_TABLETS_STARTUP_OPTION_KEY);
    return new TabletInfo(
        values != null
            && values.size() == 1
            && values.get(0).equals(SCYLLA_TABLETS_STARTUP_OPTION_VALUE));
  }

  public static void addOption(Map<String, String> options) {
    options.put(SCYLLA_TABLETS_STARTUP_OPTION_KEY, SCYLLA_TABLETS_STARTUP_OPTION_VALUE);
  }
}
