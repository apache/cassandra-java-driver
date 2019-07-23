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

import at.favre.lib.crypto.bcrypt.BCrypt;

/**
 * Utility methods to safely store passwords in the database.
 *
 * <p>We rely on a third-party implementation of the bcrypt password hash function.
 *
 * @see <a href="https://github.com/patrickfav/bcrypt">patrickfav/bcrypt</a>
 */
public class PasswordHashing {

  public static String hash(char[] password) {
    return BCrypt.withDefaults().hashToString(12, password);
  }

  public static boolean matches(char[] password, String hash) {
    BCrypt.Result result = BCrypt.verifyer().verify(password, hash);
    return result.verified;
  }
}
