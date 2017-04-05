/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core.policies;

import org.testng.annotations.Test;

import javax.naming.NamingException;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.InitialDirContext;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class EC2MultiRegionAddressTranslatorTest {

    @Test(groups = "unit")
    public void should_return_same_address_when_no_entry_found() throws Exception {
        InitialDirContext mock = mock(InitialDirContext.class);
        when(mock.getAttributes(anyString(), any(String[].class)))
                .thenReturn(new BasicAttributes());
        EC2MultiRegionAddressTranslator translator = new EC2MultiRegionAddressTranslator(mock);

        InetSocketAddress address = new InetSocketAddress("192.0.2.5", 9042);
        assertThat(translator.translate(address)).isEqualTo(address);
    }

    @Test(groups = "unit")
    public void should_return_same_address_when_exception_encountered() throws Exception {
        InitialDirContext mock = mock(InitialDirContext.class);
        when(mock.getAttributes(anyString(), any(String[].class)))
                .thenThrow(new NamingException("Problem resolving address (not really)."));
        EC2MultiRegionAddressTranslator translator = new EC2MultiRegionAddressTranslator(mock);

        InetSocketAddress address = new InetSocketAddress("192.0.2.5", 9042);
        assertThat(translator.translate(address)).isEqualTo(address);
    }

    @Test(groups = "unit")
    public void should_return_new_address_when_match_found() throws Exception {
        InetSocketAddress expectedAddress = new InetSocketAddress("54.32.55.66", 9042);

        InitialDirContext mock = mock(InitialDirContext.class);
        when(mock.getAttributes("5.2.0.192.in-addr.arpa", new String[]{"PTR"}))
                .thenReturn(new BasicAttributes("PTR", expectedAddress.getHostName()));
        EC2MultiRegionAddressTranslator translator = new EC2MultiRegionAddressTranslator(mock);

        InetSocketAddress address = new InetSocketAddress("192.0.2.5", 9042);
        assertThat(translator.translate(address)).isEqualTo(expectedAddress);
    }

    @Test(groups = "unit")
    public void should_close_context_when_closed() throws Exception {
        InitialDirContext mock = mock(InitialDirContext.class);
        EC2MultiRegionAddressTranslator translator = new EC2MultiRegionAddressTranslator(mock);

        // ensure close has not been called to this point.
        verify(mock, times(0)).close();
        translator.close();
        // ensure close is closed.
        verify(mock).close();
    }

    @Test(groups = "unit")
    public void should_build_reversed_domain_name_for_ip_v4() throws Exception {
        InetAddress address = InetAddress.getByName("192.0.2.5");
        assertThat(
                EC2MultiRegionAddressTranslator.reverse(address)
        ).isEqualTo(
                "5.2.0.192.in-addr.arpa"
        );
    }

    @Test(groups = "unit")
    public void should_build_reversed_domain_name_for_ip_v6() throws Exception {
        InetAddress address = InetAddress.getByName("2001:db8::567:89ab");
        assertThat(
                EC2MultiRegionAddressTranslator.reverse(address)
        ).isEqualTo(
                "b.a.9.8.7.6.5.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.8.b.d.0.1.0.0.2.ip6.arpa"
        );
    }
}