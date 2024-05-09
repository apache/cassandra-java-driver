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
package com.datastax.dse.driver.internal.core.insights;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_HOSTNAME_VALIDATION;

import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.util.HashMap;
import java.util.Map;

class ConfigAntiPatternsFinder {
  Map<String, String> findAntiPatterns(InternalDriverContext driverContext) {
    Map<String, String> antiPatterns = new HashMap<>();
    findSslAntiPattern(driverContext, antiPatterns);
    return antiPatterns;
  }

  private void findSslAntiPattern(
      InternalDriverContext driverContext, Map<String, String> antiPatterns) {
    boolean isSslDefined =
        driverContext.getConfig().getDefaultProfile().isDefined(SSL_ENGINE_FACTORY_CLASS);
    boolean certValidation =
        driverContext.getConfig().getDefaultProfile().getBoolean(SSL_HOSTNAME_VALIDATION, false);
    if (isSslDefined && !certValidation) {
      antiPatterns.put(
          "sslWithoutCertValidation",
          "Client-to-node encryption is enabled but server certificate validation is disabled");
    }
  }
}
