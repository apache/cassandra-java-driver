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
package com.datastax.oss.driver.examples.mapper.killrvideo.user;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import java.time.Instant;
import java.util.StringJoiner;
import java.util.UUID;

@Entity
@CqlName("users")
public class User {

  @PartitionKey private UUID userid;
  private String firstname;
  private String lastname;
  private String email;
  private Instant createdDate;

  public User(UUID userid, String firstname, String lastname, String email, Instant createdDate) {
    this.userid = userid;
    this.firstname = firstname;
    this.lastname = lastname;
    this.email = email;
    this.createdDate = createdDate;
  }

  public User() {}

  public UUID getUserid() {
    return userid;
  }

  public void setUserid(UUID userid) {
    this.userid = userid;
  }

  public String getFirstname() {
    return firstname;
  }

  public void setFirstname(String firstname) {
    this.firstname = firstname;
  }

  public String getLastname() {
    return lastname;
  }

  public void setLastname(String lastname) {
    this.lastname = lastname;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public Instant getCreatedDate() {
    return createdDate;
  }

  public void setCreatedDate(Instant createdDate) {
    this.createdDate = createdDate;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", User.class.getSimpleName() + "[", "]")
        .add("userid=" + userid)
        .add("firstname='" + firstname + "'")
        .add("lastname='" + lastname + "'")
        .add("email='" + email + "'")
        .add("createdDate=" + createdDate)
        .toString();
  }
}
