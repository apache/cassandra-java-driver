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
