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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.MockedDriverContextFactory;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.junit.Test;

public class FixedHostNameAddressTranslatorTest {

  @Test
  public void should_translate_address() {
    DriverExecutionProfile defaultProfile = mock(DriverExecutionProfile.class);
    when(defaultProfile.getString(
            FixedHostNameAddressTranslator.ADDRESS_TRANSLATOR_ADVERTISED_HOSTNAME_OPTION))
        .thenReturn("myaddress");
    DefaultDriverContext defaultDriverContext =
        MockedDriverContextFactory.defaultDriverContext(Optional.of(defaultProfile));

    FixedHostNameAddressTranslator translator =
        new FixedHostNameAddressTranslator(defaultDriverContext);
    InetSocketAddress address = new InetSocketAddress("192.0.2.5", 6061);

    assertThat(translator.translate(address)).isEqualTo(new InetSocketAddress("myaddress", 6061));
  }
}
