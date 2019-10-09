/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.auth;

import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import java.util.ArrayList;
import java.util.List;

public class AuthUtils {
  /**
   * Utility function that checks for the existence of settings and throws an exception if they
   * aren't present
   *
   * @param config Current working driver configuration
   * @param authenticatorName name of authenticator for logging purposes
   * @param endPoint the host we are attempting to authenticate to
   * @param options a list of DriverOptions to check to see if they are present
   */
  public static void validateConfigPresent(
      DriverExecutionProfile config,
      String authenticatorName,
      EndPoint endPoint,
      DriverOption... options) {
    List<DriverOption> missingOptions = new ArrayList<>();
    for (DriverOption option : options) {

      if (!config.isDefined(option)) {
        missingOptions.add(option);
      }
      if (missingOptions.size() > 0) {
        String message =
            "Missing required configuration options for authenticator " + authenticatorName + ":";
        for (DriverOption missingOption : missingOptions) {
          message = message + " " + missingOption.getPath();
        }
        throw new AuthenticationException(endPoint, message);
      }
    }
  }
}
