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

import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.oss.driver.api.osgi.service.geo.GeoMailboxMessage;
import com.datastax.oss.driver.api.osgi.service.geo.GeoMailboxService;
import java.util.ArrayList;
import java.util.List;

public class GeoServiceChecks {

  public static void checkServiceGeo(GeoMailboxService service) throws Exception {
    // Insert some data into mailbox for a particular user.
    String recipient = "user@datastax.com";
    try {
      List<GeoMailboxMessage> insertedMessages = new ArrayList<>();
      for (int i = 0; i < 30; i++) {
        Point location = Point.fromCoordinates(i, i);
        GeoMailboxMessage message =
            new GeoMailboxMessage(recipient, location, "sender" + i, "body" + i);
        insertedMessages.add(message);
        service.sendGeoMessage(message);
      }
      Iterable<GeoMailboxMessage> retrievedMessages = service.getGeoMessages(recipient);
      assertThat(retrievedMessages).containsExactlyInAnyOrderElementsOf(insertedMessages);
    } finally {
      service.clearGeoMailbox(recipient);
    }
  }
}
