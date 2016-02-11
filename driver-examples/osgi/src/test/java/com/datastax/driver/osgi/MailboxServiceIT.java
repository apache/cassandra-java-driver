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
package com.datastax.driver.osgi;

import com.datastax.driver.core.TestUtils;
import com.datastax.driver.osgi.api.MailboxException;
import com.datastax.driver.osgi.api.MailboxMessage;
import com.datastax.driver.osgi.api.MailboxService;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.options.CompositeOption;
import org.ops4j.pax.exam.options.MavenArtifactProvisionOption;
import org.ops4j.pax.exam.options.UrlProvisionOption;
import org.ops4j.pax.exam.testng.listener.PaxExam;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static com.datastax.driver.osgi.VersionProvider.projectVersion;
import static org.ops4j.pax.exam.CoreOptions.*;
import static org.testng.Assert.assertEquals;

@Listeners({CCMBridgeListener.class, PaxExam.class})
@Test(groups = "short")
public class MailboxServiceIT {
    @Inject
    MailboxService service;

    private UrlProvisionOption driverBundle() {
        return driverBundle(false);
    }

    private UrlProvisionOption driverBundle(boolean useShaded) {
        String classifier = useShaded ? "-shaded" : "";
        return bundle("reference:file:../../driver-core/target/cassandra-driver-core-" + projectVersion() + classifier + ".jar");
    }

    private MavenArtifactProvisionOption guavaBundle() {
        return mavenBundle("com.google.guava", "guava", "14.0.1");
    }

    private CompositeOption nettyBundles() {
        final String nettyVersion = "4.0.27.Final";
        return new CompositeOption() {

            @Override
            public Option[] getOptions() {
                return options(
                        mavenBundle("io.netty", "netty-buffer", nettyVersion),
                        mavenBundle("io.netty", "netty-codec", nettyVersion),
                        mavenBundle("io.netty", "netty-common", nettyVersion),
                        mavenBundle("io.netty", "netty-handler", nettyVersion),
                        mavenBundle("io.netty", "netty-transport", nettyVersion)
                );
            }
        };
    }

    private CompositeOption defaultOptions() {
        return new CompositeOption() {

            @Override
            public Option[] getOptions() {
                return options(
                        systemProperty("cassandra.contactpoints").value(TestUtils.IP_PREFIX + 1),
                        bundle("reference:file:target/classes"),
                        mavenBundle("com.codahale.metrics", "metrics-core", "3.0.2"),
                        mavenBundle("org.slf4j", "slf4j-api", "1.7.5"),
                        mavenBundle("org.slf4j", "slf4j-simple", "1.7.5").noStart(),
                        systemPackages("org.testng", "org.junit", "org.junit.runner", "org.junit.runner.manipulation",
                                "org.junit.runner.notification", "com.jcabi.manifests")
                );
            }
        };
    }

    @Configuration
    public Option[] shadedConfig() {
        return options(
                driverBundle(true),
                guavaBundle(),
                defaultOptions()
        );
    }

    @Configuration
    public Option[] defaultConfig() {
        return options(
                driverBundle(),
                guavaBundle(),
                nettyBundles(),
                defaultOptions()
        );
    }

    @Configuration
    public Option[] guava15Config() {
        return options(
                driverBundle(),
                nettyBundles(),
                guavaBundle().version("15.0"),
                defaultOptions()
        );
    }

    @Configuration
    public Option[] guava16Config() {
        return options(
                driverBundle(),
                nettyBundles(),
                guavaBundle().version("16.0.1"),
                defaultOptions()
        );
    }

    @Configuration
    public Option[] guava17Config() {
        return options(
                driverBundle(),
                nettyBundles(),
                guavaBundle().version("17.0"),
                defaultOptions()
        );
    }

    @Configuration
    public Option[] guava18Config() {
        return options(
                driverBundle(),
                nettyBundles(),
                guavaBundle().version("18.0"),
                defaultOptions()
        );
    }

    /**
     * <p>
     * Exercises a 'mailbox' service provided by an OSGi bundle that depends on the driver.  Ensures that
     * queries can be made through the service with the current given configuration.
     * </p>
     * <p/>
     * The following configurations are tried (defined via methods with the @Configuration annotation):
     * <ol>
     * <li>Default bundle (Driver with all of it's dependencies)</li>
     * <li>Shaded bundle (Driver with netty shaded)</li>
     * <li>With Guava 15</li>
     * <li>With Guava 16</li>
     * <li>With Guava 17</li>
     * <li>With Guava 18</li>
     * </ol>
     *
     * @test_category packaging
     * @expected_result Can create, retrieve and delete data using the mailbox service.
     * @jira_ticket JAVA-620
     * @since 2.0.10, 2.1.5
     */
    public void service_api_functional() throws MailboxException {
        // Insert some data into mailbox for a particular user.
        String recipient = "user@datastax.com";

        try {
            Collection<MailboxMessage> inMessages = new ArrayList<MailboxMessage>();
            for (int i = 0; i < 30; i++) {
                MailboxMessage message = new MailboxMessage(recipient, recipient, "" + i);
                UUID time = service.sendMessage(message);
                message.setDate(time);
                inMessages.add(message);
            }

            Collection<MailboxMessage> messages = service.getMessages(recipient);

            assertEquals(messages, inMessages);
        } finally {
            service.clearMailbox(recipient);
        }
    }
}
