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

import com.datastax.dse.driver.api.mapper.reactive.MappedReactiveResultSet;
import com.datastax.oss.driver.api.osgi.service.MailboxException;
import com.datastax.oss.driver.api.osgi.service.MailboxMessage;
import com.datastax.oss.driver.api.osgi.service.reactive.ReactiveMailboxService;
import io.reactivex.Flowable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ReactiveServiceChecks {

  public static void checkServiceReactive(ReactiveMailboxService service) throws MailboxException {
    // Insert some data into mailbox for a particular user.
    String recipient = "user@datastax.com";
    try {
      List<MailboxMessage> insertedMessages = new ArrayList<>();
      for (int i = 0; i < 30; i++) {
        Instant timestamp = Instant.ofEpochMilli(i);
        MailboxMessage message = new MailboxMessage(recipient, timestamp, "sender" + i, "body" + i);
        insertedMessages.add(message);
        service.sendMessage(message);
      }
      MappedReactiveResultSet<MailboxMessage> retrievedMessages =
          service.getMessagesReactive(recipient);
      List<MailboxMessage> messageList =
          Flowable.fromPublisher(retrievedMessages).toList().blockingGet();
      assertThat(messageList).containsExactlyElementsOf(insertedMessages);
    } finally {
      service.clearMailbox(recipient);
    }
  }
}
