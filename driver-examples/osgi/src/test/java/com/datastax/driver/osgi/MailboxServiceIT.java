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

import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;

import javax.inject.Inject;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.testng.listener.PaxExam;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import static org.ops4j.pax.exam.CoreOptions.options;
import static org.testng.Assert.assertEquals;

import com.datastax.driver.osgi.api.MailboxException;
import com.datastax.driver.osgi.api.MailboxMessage;
import com.datastax.driver.osgi.api.MailboxService;

import static com.datastax.driver.osgi.BundleOptions.*;

@Listeners({CCMBridgeListener.class, PaxExam.class})
@Test(groups="short")
public class MailboxServiceIT {
    @Inject MailboxService service;

    @Configuration
    public Option[] shadedConfig() {
        return options(
            driverBundle(true),
            mailboxBundle(),
            guavaBundle(),
            defaultOptions()
        );
    }

    @Configuration
    public Option[] defaultConfig() {
        return options(
            driverBundle(),
            mailboxBundle(),
            guavaBundle(),
            nettyBundles(),
            defaultOptions()
        );
    }

    @Configuration
    public Option[] guava17Config() {
        return options(
            driverBundle(),
            mailboxBundle(),
            nettyBundles(),
            guavaBundle().version("17.0"),
            defaultOptions()
        );
    }

    @Configuration
    public Option[] guava18Config() {
        return options(
            driverBundle(),
            mailboxBundle(),
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
     *
     * The following configurations are tried (defined via methods with the @Configuration annotation):
     * <ol>
     *   <li>Default bundle (Driver with all of it's dependencies)</li>
     *   <li>Shaded bundle (Driver with netty shaded)</li>
     *   <li>With Guava 16.0.1</li>
     *   <li>With Guava 17</li>
     *   <li>With Guava 18</li>
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
                MailboxMessage message = new MailboxMessage(recipient, new GregorianCalendar(2015, 1, i).getTime(), recipient, "" + i);
                inMessages.add(message);
                service.sendMessage(message);
            }

            Collection<MailboxMessage> messages = service.getMessages(recipient);

            assertEquals(messages, inMessages);
        } finally {
            service.clearMailbox(recipient);
        }
    }
}
