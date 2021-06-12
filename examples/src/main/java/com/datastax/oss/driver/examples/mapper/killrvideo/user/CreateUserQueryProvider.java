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
package com.datastax.oss.driver.examples.mapper.killrvideo.user;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Provides the implementation of {@link UserDao#create}.
 *
 * <p>Package-private visibility is sufficient, this will be called only from the generated DAO
 * implementation.
 */
class CreateUserQueryProvider {

  private final CqlSession session;
  private final EntityHelper<User> userHelper;
  private final EntityHelper<UserCredentials> credentialsHelper;
  private final PreparedStatement preparedInsertCredentials;
  private final PreparedStatement preparedInsertUser;
  private final PreparedStatement preparedDeleteCredentials;
  private final PreparedStatement preparedDeleteUser;

  CreateUserQueryProvider(
      MapperContext context,
      EntityHelper<User> userHelper,
      EntityHelper<UserCredentials> credentialsHelper) {

    this.session = context.getSession();

    this.userHelper = userHelper;
    this.credentialsHelper = credentialsHelper;

    this.preparedInsertCredentials =
        session.prepare(credentialsHelper.insert().ifNotExists().asCql());
    this.preparedInsertUser = session.prepare(userHelper.insert().asCql());
    this.preparedDeleteCredentials =
        session.prepare(
            credentialsHelper
                .deleteByPrimaryKey()
                .ifColumn("userid")
                .isEqualTo(bindMarker("userid"))
                .builder()
                .setConsistencyLevel(DefaultConsistencyLevel.ANY)
                .build());
    this.preparedDeleteUser =
        session.prepare(
            userHelper
                .deleteByPrimaryKey()
                .ifExists()
                .builder()
                .setConsistencyLevel(DefaultConsistencyLevel.ANY)
                .build());
  }

  boolean create(User user, char[] password) {
    Objects.requireNonNull(user.getUserid());
    Objects.requireNonNull(user.getEmail());
    if (user.getCreatedDate() == null) {
      user.setCreatedDate(Instant.now());
    }

    try {
      // Insert the user first: otherwise there would be a short window where we have credentials
      // without a corresponding user in the database, and this is considered an error state in
      // LoginQueryProvider
      insertUser(user);
      if (!insertCredentialsIfNotExists(user.getEmail(), password, user.getUserid())) {
        // email already exists
        session.execute(preparedDeleteUser.bind(user.getUserid()));
        return false;
      }
      return true;
    } catch (Exception insertException) {
      // Clean up and rethrow
      try {
        session.execute(preparedDeleteUser.bind(user.getUserid()));
      } catch (Exception e) {
        insertException.addSuppressed(e);
      }
      try {
        session.execute(preparedDeleteCredentials.bind(user.getEmail(), user.getUserid()));
      } catch (Exception e) {
        insertException.addSuppressed(e);
      }
      throw insertException;
    }
  }

  private boolean insertCredentialsIfNotExists(String email, char[] password, UUID userId) {
    String passwordHash = PasswordHashing.hash(Objects.requireNonNull(password));
    UserCredentials credentials =
        new UserCredentials(Objects.requireNonNull(email), passwordHash, userId);
    BoundStatementBuilder insertCredentials = preparedInsertCredentials.boundStatementBuilder();
    credentialsHelper.set(credentials, insertCredentials, NullSavingStrategy.DO_NOT_SET, false);
    ResultSet resultSet = session.execute(insertCredentials.build());
    return resultSet.wasApplied();
  }

  private void insertUser(User user) {
    BoundStatementBuilder insertUser = preparedInsertUser.boundStatementBuilder();
    userHelper.set(user, insertUser, NullSavingStrategy.DO_NOT_SET, false);
    session.execute(insertUser.build());
  }
}
