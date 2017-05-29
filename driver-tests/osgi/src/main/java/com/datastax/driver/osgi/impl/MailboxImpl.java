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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.osgi.api.MailboxException;
import com.datastax.driver.osgi.api.MailboxMessage;
import com.datastax.driver.osgi.api.MailboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.driver.osgi.api.MailboxMessage.TABLE;

public class MailboxImpl implements MailboxService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MailboxImpl.class);

    private final Session session;

    private final String keyspace;

    private volatile boolean initialized = false;

    private PreparedStatement retrieveStatement;

    private PreparedStatement deleteStatement;

    private Mapper<MailboxMessage> mapper;

    public MailboxImpl(Session session, String keyspace) {
        this.session = session;
        this.keyspace = keyspace;
    }

    public synchronized void init() {
        if (initialized)
            return;

        retrieveStatement = session.prepare(select()
                .from(keyspace, TABLE)
                .where(eq("recipient", bindMarker())));

        deleteStatement = session.prepare(delete().from(keyspace, TABLE)
                .where(eq("recipient", bindMarker())));

        MappingManager mappingManager = new MappingManager(session);

        mapper = mappingManager.mapper(MailboxMessage.class);

        // Exercise metrics
        LOGGER.info("Number of requests: {}", session.getCluster().getMetrics().getRequestsTimer().getCount());

        initialized = true;
    }

    @Override
    public Iterable<MailboxMessage> getMessages(String recipient) throws MailboxException {
        try {
            BoundStatement statement = new BoundStatement(retrieveStatement);
            statement.setString(0, recipient);
            return mapper.map(session.execute(statement));
        } catch (Exception e) {
            throw new MailboxException(e);
        }
    }

    @Override
    public long sendMessage(MailboxMessage message) throws MailboxException {
        try {
            mapper.save(message);
            return message.getDate();
        } catch (Exception e) {
            throw new MailboxException(e);
        }
    }

    @Override
    public void clearMailbox(String recipient) throws MailboxException {
        try {
            BoundStatement statement = new BoundStatement(deleteStatement);
            statement.setString(0, recipient);
            session.execute(statement);
        } catch (Exception e) {
            throw new MailboxException(e);
        }
    }
}
