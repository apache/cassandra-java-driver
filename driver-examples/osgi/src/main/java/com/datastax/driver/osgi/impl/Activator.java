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
package com.datastax.driver.osgi.impl;

import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.osgi.api.MailboxService;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

import java.util.Hashtable;

public class Activator implements BundleActivator {

    private Cluster cluster;

    @Override
    public void start(BundleContext context) throws Exception {
        String contactPointsStr = context.getProperty("cassandra.contactpoints");
        if (contactPointsStr == null) {
            contactPointsStr = "127.0.0.1";
        }
        String[] contactPoints = contactPointsStr.split(",");

        String keyspace = context.getProperty("cassandra.keyspace");
        if (keyspace == null) {
            keyspace = "mailbox";
        }

        cluster = Cluster.builder().addContactPoints(contactPoints)
                .withTimestampGenerator(new AtomicMonotonicTimestampGenerator())
                .build();
        Session session = cluster.connect();

        MailboxImpl mailbox = new MailboxImpl(session, keyspace);
        mailbox.init();

        context.registerService(MailboxService.class.getName(), mailbox, new Hashtable<String, String>());
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        if (cluster != null) {
            cluster.close();
        }
    }
}
