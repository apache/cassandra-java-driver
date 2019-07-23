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

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import java.util.Optional;
import java.util.UUID;

@Dao
public interface UserDao {

  /** Simple selection by full primary key. */
  @Select
  User get(UUID userid);

  @Select
  UserCredentials getCredentials(String email);

  /**
   * An alternative to query providers is default methods that call other methods on the DAO.
   *
   * <p>The only drawback is that those other methods have to be part of the DAO's public API.
   */
  default User getByEmail(String email) {
    UserCredentials credentials = getCredentials(email);
    return (credentials == null) ? null : get(credentials.getUserid());
  }

  /**
   * Creating a user is more than a single insert: we have to update two different tables, check
   * that the email is not used already, and handle password encryption.
   *
   * <p>We use a query provider to wrap everything into a single method.
   *
   * <p>Note that you could opt for a more layered approach: only expose basic operations on the DAO
   * (insertCredentialsIfNotExists, insertUser...) and add a service layer on top for more complex
   * logic. Both designs are valid, this is a matter of personal choice.
   *
   * @return {@code true} if the new user was created, or {@code false} if this email address was
   *     already taken.
   */
  @QueryProvider(
      providerClass = CreateUserQueryProvider.class,
      entityHelpers = {User.class, UserCredentials.class})
  boolean create(User user, char[] password);

  /**
   * Similar to {@link #create}, this encapsulates encryption so we use a query provider.
   *
   * @return the authenticated user, or {@link Optional#empty()} if the credentials are invalid.
   */
  @QueryProvider(
      providerClass = LoginQueryProvider.class,
      entityHelpers = {User.class, UserCredentials.class})
  Optional<User> login(String email, char[] password);
}
