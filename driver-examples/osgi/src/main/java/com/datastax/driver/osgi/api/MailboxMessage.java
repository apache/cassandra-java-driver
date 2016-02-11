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
package com.datastax.driver.osgi.api;

import java.util.UUID;

public class MailboxMessage {
    private String recipient;
    private String sender;
    private String body;
    private UUID date;

    public MailboxMessage(String recipient, String sender, String body) {
        this(recipient, sender, body, null);
    }

    public MailboxMessage(String recipient, String sender, String body, UUID date) {
        this.recipient = recipient;
        this.sender = sender;
        this.body = body;
        this.date = date;
    }

    public String getRecipient() {
        return recipient;
    }

    public void setDate(UUID date) {
        this.date = date;
    }

    public UUID getDate() {
        return date;
    }

    public String getSender() {
        return sender;
    }

    public String getBody() {
        return body;
    }

    @Override
    public boolean equals(Object that) {
        if (that instanceof MailboxMessage) {
            MailboxMessage thatM = (MailboxMessage) that;
            return recipient.equals(thatM.getRecipient()) &&
                    date.equals(thatM.getDate()) &&
                    sender.equals(thatM.getSender()) &&
                    body.equals(thatM.getBody());
        } else {
            return false;
        }
    }
}
