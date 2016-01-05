/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Enumeration;
import java.util.Hashtable;

/**
 * {@link AddressTranslator} implementation for a multi-region EC2 deployment <b>where clients are also deployed in EC2</b>.
 * <p/>
 * Its distinctive feature is that it translates addresses according to the location of the Cassandra host:
 * <ul>
 * <li>addresses in different EC2 regions (than the client) are unchanged;</li>
 * <li>addresses in the same EC2 region are <b>translated to private IPs</b>.</li>
 * </ul>
 * This optimizes network costs, because Amazon charges more for communication over public IPs.
 * <p/>
 * <p/>
 * Implementation note: this class performs a reverse DNS lookup of the origin address, to find the domain name of the target
 * instance. Then it performs a forward DNS lookup of the domain name; the EC2 DNS does the private/public switch automatically
 * based on location.
 */
public class EC2MultiRegionAddressTranslator implements AddressTranslator {

    private static final Logger logger = LoggerFactory.getLogger(EC2MultiRegionAddressTranslator.class);

    // TODO when we switch to Netty 4.1, we can replace this with the Netty built-in DNS client
    private final DirContext ctx;

    public EC2MultiRegionAddressTranslator() {
        Hashtable<Object, Object> env = new Hashtable<Object, Object>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
        try {
            ctx = new InitialDirContext(env);
        } catch (NamingException e) {
            throw new DriverException("Could not create translator", e);
        }
    }

    @VisibleForTesting
    EC2MultiRegionAddressTranslator(DirContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void init(Cluster cluster) {
        // nothing to do
    }

    @Override
    public InetSocketAddress translate(InetSocketAddress socketAddress) {
        InetAddress address = socketAddress.getAddress();
        try {
            // InetAddress#getHostName() is supposed to perform a reverse DNS lookup, but for some reason it doesn't work
            // within the same EC2 region (it returns the IP address itself).
            // We use an alternate implementation:
            String domainName = lookupPtrRecord(reverse(address));
            if (domainName == null) {
                logger.warn("Found no domain name for {}, returning it as-is", address);
                return socketAddress;
            }

            InetAddress translatedAddress = InetAddress.getByName(domainName);
            logger.debug("Resolved {} to {}", address, translatedAddress);
            return new InetSocketAddress(translatedAddress, socketAddress.getPort());
        } catch (Exception e) {
            logger.warn("Error resolving " + address + ", returning it as-is", e);
            return socketAddress;
        }
    }

    private String lookupPtrRecord(String reversedDomain) throws Exception {
        Attributes attrs = ctx.getAttributes(reversedDomain, new String[]{"PTR"});
        for (NamingEnumeration ae = attrs.getAll(); ae.hasMoreElements(); ) {
            Attribute attr = (Attribute) ae.next();
            for (Enumeration<?> vals = attr.getAll(); vals.hasMoreElements(); )
                return vals.nextElement().toString();
        }
        return null;
    }

    @Override
    public void close() {
        try {
            ctx.close();
        } catch (NamingException e) {
            logger.warn("Error closing translator", e);
        }
    }

    // Builds the "reversed" domain name in the ARPA domain to perform the reverse lookup
    @VisibleForTesting
    static String reverse(InetAddress address) {
        byte[] bytes = address.getAddress();
        if (bytes.length == 4)
            return reverseIpv4(bytes);
        else
            return reverseIpv6(bytes);
    }

    private static String reverseIpv4(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for (int i = bytes.length - 1; i >= 0; i--) {
            builder.append(bytes[i] & 0xFF).append('.');
        }
        builder.append("in-addr.arpa");
        return builder.toString();
    }

    private static String reverseIpv6(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for (int i = bytes.length - 1; i >= 0; i--) {
            byte b = bytes[i];
            int lowNibble = b & 0x0F;
            int highNibble = b >> 4 & 0x0F;
            builder.append(Integer.toHexString(lowNibble)).append('.')
                    .append(Integer.toHexString(highNibble)).append('.');
        }
        builder.append("ip6.arpa");
        return builder.toString();
    }
}
