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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.osgi.api.MailboxException;
import com.datastax.driver.osgi.api.MailboxMessage;
import com.datastax.driver.osgi.api.MailboxService;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

public class MailboxImpl implements MailboxService {

    private static final String TABLE = "mailbox";

    private final Session session;

    private final String keyspace;

    private volatile boolean initialized = false;

    private PreparedStatement retrieveStatement;

    private PreparedStatement insertStatement;

    private PreparedStatement deleteStatement;

    public MailboxImpl(Session session, String keyspace) {
        this.session = session;
        this.keyspace = keyspace;
    }

    public synchronized void init() {
        if(initialized)
            return;

        // Create the schema if it does not exist.
        try {
            session.execute("USE " + keyspace);
        } catch (InvalidQueryException e) {
            session.execute("CREATE KEYSPACE " + keyspace +
                " with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}");

            session.execute("CREATE TABLE " + keyspace + "." + TABLE + " (" +
                "recipient text," +
                "time timeuuid," +
                "sender text," +
                "body text," +
                "PRIMARY KEY (recipient, time))");
        }

        retrieveStatement = session.prepare(select()
            .from(keyspace, TABLE)
            .where(eq("recipient", bindMarker("recipient"))));

        insertStatement = session.prepare(insertInto(keyspace, TABLE)
            .value("recipient", bindMarker("recipient"))
            .value("time", bindMarker("time"))
            .value("sender", bindMarker("sender"))
            .value("body", bindMarker("body")));

        deleteStatement = session.prepare(delete().from(keyspace, TABLE)
            .where(eq("recipient", bindMarker("recipient"))));

        initialized = true;
    }

    @Override public Collection<MailboxMessage> getMessages(String recipient) throws MailboxException {
        try {
            BoundStatement statement = new BoundStatement(retrieveStatement);
            statement.setString("recipient", recipient);
            ResultSet result = session.execute(statement);

            Collection<MailboxMessage> messages = new ArrayList<MailboxMessage>();
            for(Row input : result) {
                Date date = new Date(UUIDs.unixTimestamp(input.getUUID("time")));
                messages.add(new MailboxMessage(input.getString("recipient"),
                    date,
                    input.getString("sender"),
                    input.getString("body")));
            }
            return messages;
        } catch(Exception e) {
            throw new MailboxException(e);
        }
    }

    @Override public UUID sendMessage(MailboxMessage message) throws MailboxException {
        try {
            UUID time = UUIDs.startOf(message.getDate().getTime());

            BoundStatement statement = new BoundStatement(insertStatement);
            statement.setString("recipient", message.getRecipient());
            statement.setUUID("time", time);
            statement.setString("sender", message.getSender());
            statement.setString("body", message.getBody());

            session.execute(statement);
            return time;
        } catch(Exception e) {
            throw new MailboxException(e);
        }
    }

    @Override public void clearMailbox(String recipient) throws MailboxException {
        try {
            BoundStatement statement = new BoundStatement(deleteStatement);
            statement.setString("recipient", recipient);
            session.execute(statement);
        } catch(Exception e) {
            throw new MailboxException(e);
        }
    }
}
