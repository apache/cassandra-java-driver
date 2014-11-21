package com.datastax.driver.core.policies;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EC2MultiRegionAddressTranslaterTest {
    @Test(groups = "unit")
    public void should_build_reversed_domain_name_for_ip_v4() throws Exception {
        InetAddress address = InetAddress.getByName("192.0.2.5");
        assertThat(
            EC2MultiRegionAddressTranslater.reverse(address)
        ).isEqualTo(
            "5.2.0.192.in-addr.arpa"
        );
    }

    @Test(groups = "unit")
    public void should_build_reversed_domain_name_for_ip_v6() throws Exception {
        InetAddress address = InetAddress.getByName("2001:db8::567:89ab");
        assertThat(
            EC2MultiRegionAddressTranslater.reverse(address)
        ).isEqualTo(
            "b.a.9.8.7.6.5.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.8.b.d.0.1.0.0.2.ip6.arpa"
        );
    }
}