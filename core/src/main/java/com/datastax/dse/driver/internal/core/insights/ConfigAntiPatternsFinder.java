/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.insights;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_HOSTNAME_VALIDATION;

import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import java.util.HashMap;
import java.util.Map;

class ConfigAntiPatternsFinder {
  Map<String, String> findAntiPatterns(DseDriverContext driverContext) {
    Map<String, String> antiPatterns = new HashMap<>();
    findSslAntiPattern(driverContext, antiPatterns);
    return antiPatterns;
  }

  private void findSslAntiPattern(
      DseDriverContext driverContext, Map<String, String> antiPatterns) {
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
