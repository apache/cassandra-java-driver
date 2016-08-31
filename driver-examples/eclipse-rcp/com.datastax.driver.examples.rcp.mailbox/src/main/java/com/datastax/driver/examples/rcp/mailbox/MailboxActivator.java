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
package com.datastax.driver.examples.rcp.mailbox;

import java.util.Hashtable;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.examples.rcp.mailbox.impl.MailboxServiceImpl;

public class MailboxActivator implements BundleActivator {

    public static BundleContext bundleContext;

    private Cluster cluster;

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#start(org.osgi.framework.BundleContext)
     */
    @Override
    public void start(BundleContext context) throws Exception {
        bundleContext = context;
        String contactPointsStr = context.getProperty("cassandra.contactpoints");
        if (contactPointsStr == null) {
            contactPointsStr = "127.0.0.1";
        }
        String[] contactPoints = contactPointsStr.split(",");
        String keyspace = context.getProperty("cassandra.keyspace");
        if (keyspace == null) {
            keyspace = "mailbox";
        }
        cluster = Cluster.builder().addContactPoints(contactPoints).build();
        Session session = cluster.connect();
        MailboxServiceImpl mailbox = new MailboxServiceImpl(session, keyspace);
        mailbox.init();
        // register the service
        context.registerService(MailboxService.class.getName(), mailbox, new Hashtable<String, Object>());
    }

    /*
     * (non-Javadoc)
     * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
     */
    @Override
    public void stop(BundleContext context) throws Exception {
        if (cluster != null) {
            cluster.close();
        }
    }

}
