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
package com.datastax.driver.osgi.impl;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.policies.PercentileSpeculativeExecutionPolicy;
import com.datastax.driver.extras.codecs.date.SimpleTimestampCodec;
import com.datastax.driver.osgi.api.MailboxService;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;

import static com.datastax.driver.core.ProtocolOptions.Compression.LZ4;
import static com.datastax.driver.osgi.api.MailboxMessage.TABLE;

public class Activator implements BundleActivator {

    private static final Logger LOGGER = LoggerFactory.getLogger(Activator.class);

    private Cluster cluster;

    @Override
    public void start(BundleContext context) throws Exception {

        VersionNumber ver = VersionNumber.parse(context.getProperty("cassandra.version"));
        LOGGER.info("C* version: {}", ver);

        String contactPointsStr = context.getProperty("cassandra.contactpoints");
        if (contactPointsStr == null) {
            contactPointsStr = "127.0.0.1";
        }
        LOGGER.info("Contact points: {}", contactPointsStr);
        String[] contactPoints = contactPointsStr.split(",");

        String keyspace = context.getProperty("cassandra.keyspace");
        if (keyspace == null) {
            keyspace = "mailbox";
        }
        LOGGER.info("Keyspace: {}", keyspace);
        keyspace = Metadata.quote(keyspace);

        Cluster.Builder builder = Cluster.builder()
                .addContactPoints(contactPoints)
                .withCodecRegistry(new CodecRegistry().register(SimpleTimestampCodec.instance));

        String compression = context.getProperty("cassandra.compression");
        if (compression != null) {
            if (ver.getMajor() < 2 && compression.equals(LZ4.name())) {
                LOGGER.warn("Requested LZ4 compression but C* version is not compatible, disabling");
            } else {
                LOGGER.info("Compression: {}", compression);
                builder.withCompression(ProtocolOptions.Compression.valueOf(compression));
            }
        } else {
            LOGGER.info("Compression: NONE");
        }

        String usePercentileSpeculativeExecutionPolicy = context.getProperty("cassandra.usePercentileSpeculativeExecutionPolicy");
        if ("true".equals(usePercentileSpeculativeExecutionPolicy)) {
            PerHostPercentileTracker perHostPercentileTracker = PerHostPercentileTracker.builder(15000).build();
            builder.withSpeculativeExecutionPolicy(new PercentileSpeculativeExecutionPolicy(perHostPercentileTracker, 99, 1));
            LOGGER.info("Use PercentileSpeculativeExecutionPolicy: YES");
        } else {
            LOGGER.info("Use PercentileSpeculativeExecutionPolicy: NO");
        }

        cluster = builder.build();

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
        LOGGER.info("Mailbox Service successfully initialized");
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
            Thread.sleep(1000);
        }
    }
}
