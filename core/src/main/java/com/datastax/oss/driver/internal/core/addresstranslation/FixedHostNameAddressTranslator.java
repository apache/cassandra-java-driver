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
package com.datastax.oss.driver.internal.core.addresstranslation;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This translator always returns same hostname, no matter what IP address a node has but still
 * using its native transport port.
 *
 * <p>The translator can be used for scenarios when all nodes are behind some kind of proxy, and it
 * is not tailored for one concrete use case. One can use this, for example, for AWS PrivateLink as
 * all nodes would be exposed to consumer - behind one hostname pointing to AWS Endpoint.
 */
public class FixedHostNameAddressTranslator implements AddressTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(FixedHostNameAddressTranslator.class);

  public static final String ADDRESS_TRANSLATOR_ADVERTISED_HOSTNAME =
      "advanced.address-translator.advertised-hostname";

  public static DriverOption ADDRESS_TRANSLATOR_ADVERTISED_HOSTNAME_OPTION =
      new DriverOption() {
        @NonNull
        @Override
        public String getPath() {
          return ADDRESS_TRANSLATOR_ADVERTISED_HOSTNAME;
        }
      };

  private final String advertisedHostname;
  private final String logPrefix;

  public FixedHostNameAddressTranslator(@NonNull DriverContext context) {
    logPrefix = context.getSessionName();
    advertisedHostname =
        context
            .getConfig()
            .getDefaultProfile()
            .getString(ADDRESS_TRANSLATOR_ADVERTISED_HOSTNAME_OPTION);
  }

  @NonNull
  @Override
  public InetSocketAddress translate(@NonNull InetSocketAddress address) {
    final int port = address.getPort();
    LOG.debug("[{}] Resolved {}:{} to {}:{}", logPrefix, address, port, advertisedHostname, port);
    return new InetSocketAddress(advertisedHostname, port);
  }

  @Override
  public void close() {}
}
