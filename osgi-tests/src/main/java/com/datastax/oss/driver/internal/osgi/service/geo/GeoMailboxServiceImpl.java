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
package com.datastax.oss.driver.internal.osgi.service.geo;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.relation.Relation.column;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.osgi.service.MailboxException;
import com.datastax.oss.driver.api.osgi.service.geo.GeoMailboxMessage;
import com.datastax.oss.driver.api.osgi.service.geo.GeoMailboxService;
import com.datastax.oss.driver.internal.osgi.service.MailboxServiceImpl;

public class GeoMailboxServiceImpl extends MailboxServiceImpl implements GeoMailboxService {

  private PreparedStatement deleteGeoStatement;
  private GeoMailboxMessageDao geoDao;

  public GeoMailboxServiceImpl(CqlSession session, CqlIdentifier keyspace) {
    super(session, keyspace);
  }

  @Override
  protected void createSchema() {
    super.createSchema();
    session.execute(
        "CREATE TABLE "
            + keyspace
            + "."
            + GeoMailboxMessage.MAILBOX_TABLE
            + " ("
            + "recipient text,"
            + "location 'PointType',"
            + "sender text,"
            + "body text,"
            + "PRIMARY KEY (recipient, location))");
  }

  @Override
  protected void prepareStatements() {
    super.prepareStatements();
    deleteGeoStatement =
        session.prepare(
            deleteFrom(keyspace, GeoMailboxMessage.MAILBOX_TABLE)
                .where(column("recipient").isEqualTo(bindMarker()))
                .build());
  }

  @Override
  protected void createDaos() {
    super.createDaos();
    GeoMailboxMapper mapper = new GeoMailboxMapperBuilder(session).build();
    geoDao = mapper.mailboxMessageDao(keyspace);
  }

  @Override
  public void sendGeoMessage(GeoMailboxMessage message) throws MailboxException {
    try {
      geoDao.save(message);
    } catch (Exception e) {
      throw new MailboxException(e);
    }
  }

  @Override
  public Iterable<GeoMailboxMessage> getGeoMessages(String recipient) throws MailboxException {
    try {
      return geoDao.findGeoByRecipient(recipient);
    } catch (Exception e) {
      throw new MailboxException(e);
    }
  }

  @Override
  public void clearGeoMailbox(String recipient) throws MailboxException {
    try {
      BoundStatement statement = deleteGeoStatement.bind(recipient);
      session.execute(statement);
    } catch (Exception e) {
      throw new MailboxException(e);
    }
  }
}
