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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import java.util.Optional;
import java.util.UUID;

/**
 * Provides the implementation of {@link UserDao#login}.
 *
 * <p>Package-private visibility is sufficient, this will be called only from the generated DAO
 * implementation.
 */
class LoginQueryProvider {

  private final CqlSession session;
  private final EntityHelper<User> userHelper;
  private final PreparedStatement preparedSelectCredentials;
  private final PreparedStatement preparedSelectUser;

  LoginQueryProvider(
      MapperContext context,
      EntityHelper<User> userHelper,
      EntityHelper<UserCredentials> credentialsHelper) {

    this.session = context.getSession();

    this.userHelper = userHelper;

    this.preparedSelectCredentials =
        session.prepare(credentialsHelper.selectByPrimaryKey().asCql());
    this.preparedSelectUser = session.prepare(userHelper.selectByPrimaryKey().asCql());
  }

  Optional<User> login(String email, char[] password) {
    return Optional.ofNullable(session.execute(preparedSelectCredentials.bind(email)).one())
        .flatMap(
            credentialsRow -> {
              String hashedPassword = credentialsRow.getString("password");
              if (PasswordHashing.matches(password, hashedPassword)) {
                UUID userid = credentialsRow.getUuid("userid");
                Row userRow = session.execute(preparedSelectUser.bind(userid)).one();
                if (userRow == null) {
                  throw new IllegalStateException(
                      "Should have found matching row for userid " + userid);
                } else {
                  return Optional.of(userHelper.get(userRow, false));
                }
              } else {
                return Optional.empty();
              }
            });
  }
}
