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
package com.datastax.driver.examples.rcp.mailbox.tests;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.osgi.framework.ServiceReference;

import com.datastax.driver.examples.rcp.mailbox.MailboxActivator;
import com.datastax.driver.examples.rcp.mailbox.MailboxMessage;
import com.datastax.driver.examples.rcp.mailbox.MailboxService;

/**
 * This test requires an OSGi container.
 * Under Eclipse, run it as a "JUnit Plugin Test".
 */
public class MailboxServiceTest {

    @ClassRule
    public static CassandraRule ccm = new CassandraRule();

    private MailboxService service;

    @Before
    public void setUp() {
        // Mailbox host bundle should have been activated when we get here
        ServiceReference<?> reference = MailboxActivator.bundleContext.getServiceReference(MailboxService.class.getName());
        service = (MailboxService) MailboxActivator.bundleContext.getService(reference);
    }

    /**
     * <p>
     * Global integration test for the 'mailbox' service. Ensures that queries can be made through the service with the current given configuration.
     * </p>
     */
    @Test
    public void service_api_functional() throws Exception {
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
