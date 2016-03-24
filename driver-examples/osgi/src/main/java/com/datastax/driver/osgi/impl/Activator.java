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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.extras.codecs.date.SimpleTimestampCodec;
import com.datastax.driver.osgi.api.MailboxService;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

import java.util.Hashtable;

import static com.datastax.driver.osgi.api.MailboxMessage.TABLE;

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
        keyspace = Metadata.quote(keyspace);

        cluster = Cluster.builder()
                .addContactPoints(contactPoints)
                .withCodecRegistry(new CodecRegistry().register(SimpleTimestampCodec.instance))
                .build();

        Session session;
        try {
            session = cluster.connect(keyspace);
        } catch (InvalidQueryException e) {
            // Create the schema if it does not exist.
            session = cluster.connect();
            session.execute("CREATE KEYSPACE " + keyspace +
                    " with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}");
            session.execute("CREATE TABLE " + keyspace + "." + TABLE + " (" +
                    "recipient text," +
                    "time timestamp," +
                    "sender text," +
                    "body text," +
                    "PRIMARY KEY (recipient, time))");
            session.execute("USE " + keyspace);
        }

        MailboxImpl mailbox = new MailboxImpl(session, keyspace);
        mailbox.init();

        context.registerService(MailboxService.class.getName(), mailbox, new Hashtable<String, String>());
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        if (cluster != null) {
            cluster.close();
            /*
            Allow Netty ThreadDeathWatcher to terminate;
            unfortunately we can't explicitly call ThreadDeathWatcher.awaitInactivity()
            because Netty could be shaded.
            If this thread isn't terminated when this bundle is closed,
            we could get exceptions such as this one:
            Exception in thread "threadDeathWatcher-2-1" java.lang.NoClassDefFoundError: xxx
            Caused by: java.lang.ClassNotFoundException: Unable to load class 'xxx' because the bundle wiring for xxx is no longer valid.
            Although ugly, they are harmless and can be safely ignored.
            */
            Thread.sleep(10000);
        }
    }
}
