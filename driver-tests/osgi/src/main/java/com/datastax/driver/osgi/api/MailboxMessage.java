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
package com.datastax.driver.osgi.api;

import com.datastax.driver.core.utils.MoreObjects;
import com.datastax.driver.extras.codecs.date.SimpleTimestampCodec;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import static com.datastax.driver.osgi.api.MailboxMessage.TABLE;

/**
 * A mailbox message entity mapped to the table <code>{@value #TABLE}</code>.
 */
@SuppressWarnings("unused")
@Table(name = TABLE)
public class MailboxMessage {

    public static final String TABLE = "mailbox";

    @PartitionKey
    private String recipient;

    @ClusteringColumn
    @Column(name = "time", codec = SimpleTimestampCodec.class)
    private long date;

    @Column
    private String sender;

    @Column
    private String body;

    public MailboxMessage() {
    }

    public MailboxMessage(String recipient, long date, String sender, String body) {
        this.recipient = recipient;
        this.date = date;
        this.sender = sender;
        this.body = body;
    }

    public String getRecipient() {
        return recipient;
    }

    public void setRecipient(String recipient) {
        this.recipient = recipient;
    }

    public long getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = date;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MailboxMessage that = (MailboxMessage) o;
        return date == that.date &&
                MoreObjects.equal(recipient, that.recipient) &&
                MoreObjects.equal(sender, that.sender) &&
                MoreObjects.equal(body, that.body);
    }

    @Override
    public int hashCode() {
        return MoreObjects.hashCode(recipient, date, sender, body);
    }
}
