/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.internal.osgi.checks;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.osgi.service.MailboxException;
import com.datastax.oss.driver.api.osgi.service.MailboxMessage;
import com.datastax.oss.driver.api.osgi.service.graph.GraphMailboxService;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class GraphServiceChecks {

  public static void checkGraphService(GraphMailboxService service) throws MailboxException {
    // Insert some data into mailbox for a particular user.
    String recipient = "user@datastax.com";
    List<MailboxMessage> insertedMessages = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      Instant timestamp = Instant.ofEpochMilli(i);
      MailboxMessage message = new MailboxMessage(recipient, timestamp, "sender" + i, "body" + i);
      insertedMessages.add(message);
      service.sendGraphMessage(message);
    }
    Iterable<MailboxMessage> retrievedMessages = service.getGraphMessages(recipient);
    assertThat(retrievedMessages).containsExactlyElementsOf(insertedMessages);
  }
}
