/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.osgi.service;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Instant;
import java.util.Objects;

@Entity
@CqlName("messages_by_recipient")
public class MailboxMessage {

  public static final CqlIdentifier MAILBOX_TABLE =
      CqlIdentifier.fromInternal("messages_by_recipient");

  @PartitionKey private String recipient;

  @ClusteringColumn private Instant timestamp;

  private String sender;

  private String body;

  public MailboxMessage() {}

  public MailboxMessage(
      @NonNull String recipient,
      @NonNull Instant timestamp,
      @NonNull String sender,
      @NonNull String body) {
    this.recipient = recipient;
    this.timestamp = timestamp;
    this.sender = sender;
    this.body = body;
  }

  public String getRecipient() {
    return recipient;
  }

  public void setRecipient(String recipient) {
    this.recipient = recipient;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Instant timestamp) {
    this.timestamp = timestamp;
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
    if (this == o) {
      return true;
    }
    if (!(o instanceof MailboxMessage)) {
      return false;
    }
    MailboxMessage that = (MailboxMessage) o;
    return Objects.equals(recipient, that.recipient)
        && Objects.equals(timestamp, that.timestamp)
        && Objects.equals(sender, that.sender)
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(recipient, timestamp, sender, body);
  }
}
