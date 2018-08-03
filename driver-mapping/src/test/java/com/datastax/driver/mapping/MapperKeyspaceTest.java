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
package com.datastax.driver.mapping;

import static com.datastax.driver.core.Assertions.assertThat;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.UDT;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@CassandraVersion("2.1")
public class MapperKeyspaceTest extends CCMTestsSupport {

  @Override
  public void onTestContextInitialized() {
    execute(
        "CREATE KEYSPACE IF NOT EXISTS MapperKeyspaceTest WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "CREATE TYPE IF NOT EXISTS MapperKeyspaceTest.address(street text)",
        "CREATE TABLE IF NOT EXISTS MapperKeyspaceTest.user(name text PRIMARY KEY, address frozen<address>)",
        "CREATE KEYSPACE IF NOT EXISTS MapperKeyspaceTest2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "CREATE TYPE IF NOT EXISTS MapperKeyspaceTest2.address(street text)",
        "CREATE TABLE IF NOT EXISTS MapperKeyspaceTest2.user(name text PRIMARY KEY, address frozen<address>)");
  }

  @BeforeMethod(groups = "short")
  public void setup() {
    execute("TRUNCATE MapperKeyspaceTest.user", "TRUNCATE MapperKeyspaceTest2.user");
  }

  @Test(
      groups = "short",
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".* you must provide a keyspace name .*")
  public void should_fail_if_no_keyspace_provided() {
    new TestScenario<UserWithoutKeyspace>(UserWithoutKeyspace.class).check();
  }

  @Test(groups = "short")
  public void should_use_session_keyspace() {
    // Only works if it's not defined by the annotation
    new TestScenario<UserWithoutKeyspace>(UserWithoutKeyspace.class)
        .withSessionKeyspace("MapperKeyspaceTest")
        .withExpectedKeyspace("MapperKeyspaceTest")
        .check();
  }

  @Test(groups = "short")
  public void should_use_annotation_keyspace() {
    new TestScenario<UserWithKeyspace>(UserWithKeyspace.class)
        .withExpectedKeyspace("MapperKeyspaceTest")
        .check();
    // Annotation takes precedence over session keyspace:
    new TestScenario<UserWithKeyspace>(UserWithKeyspace.class)
        .withSessionKeyspace("MapperKeyspaceTest2")
        .withExpectedKeyspace("MapperKeyspaceTest")
        .check();
  }

  @Test(groups = "short")
  public void should_use_keyspace_provided_when_creating_mapper() {
    // Provided nowhere except in the mapper call:
    new TestScenario<UserWithoutKeyspace>(UserWithoutKeyspace.class)
        .withMapperKeyspace("MapperKeyspaceTest")
        .withExpectedKeyspace("MapperKeyspaceTest")
        .check();
    // Mapper takes precedence over session:
    new TestScenario<UserWithoutKeyspace>(UserWithoutKeyspace.class)
        .withSessionKeyspace("MapperKeyspaceTest")
        .withMapperKeyspace("MapperKeyspaceTest2")
        .withExpectedKeyspace("MapperKeyspaceTest2")
        .check();
    // Mapper takes precedence over annotation:
    new TestScenario<UserWithKeyspace>(UserWithKeyspace.class)
        .withMapperKeyspace("MapperKeyspaceTest2")
        .withExpectedKeyspace("MapperKeyspaceTest2")
        .check();
  }

  private class TestScenario<UserT extends User> {

    private final Class<UserT> userClass;
    private final UserT sampleUser;

    private String sessionKeyspace;
    private String mapperKeyspace;
    private String expectedKeyspace;

    TestScenario(Class<UserT> userClass) {
      this.userClass = userClass;
      this.sampleUser = buildSampleUser(userClass);
    }

    @SuppressWarnings("unchecked")
    private UserT buildSampleUser(Class<UserT> userClass) {
      // Hacky but we know the subclasses so no need to be more fancy
      if (userClass == UserWithKeyspace.class) {
        return (UserT) new UserWithKeyspace("user1", new AddressWithKeyspace("street"));
      } else if (userClass == UserWithoutKeyspace.class) {
        return (UserT) new UserWithoutKeyspace("user1", new AddressWithoutKeyspace("street"));
      } else {
        throw new AssertionError("Unsupported user class " + userClass);
      }
    }

    /** The keyspace passed to Cluster.connect() */
    TestScenario withSessionKeyspace(String sessionKeyspace) {
      this.sessionKeyspace = sessionKeyspace;
      return this;
    }

    /** The keyspace passed to MappingManager.mapper() */
    TestScenario withMapperKeyspace(String mapperKeyspace) {
      this.mapperKeyspace = mapperKeyspace;
      return this;
    }

    /** The keyspace where we expect data to be saved */
    TestScenario withExpectedKeyspace(String expectedKeyspace) {
      this.expectedKeyspace = expectedKeyspace;
      return this;
    }

    void check() {
      Session session = cluster().connect(sessionKeyspace);
      MappingManager manager = new MappingManager(session);
      Mapper<UserT> mapper = manager.mapper(userClass, mapperKeyspace);

      mapper.save(sampleUser);
      assertThat(expectedKeyspace).isNotNull();
      Row row =
          session()
              .execute(
                  String.format("SELECT * FROM %s.user WHERE name = ?", expectedKeyspace),
                  sampleUser.getName())
              .one();
      assertThat(row).isNotNull();

      UserT loadedUser = mapper.get(sampleUser.getName());
      assertThat(loadedUser.getName()).isEqualTo(sampleUser.getName());
      assertThat(loadedUser.getAddress().getStreet())
          .isEqualTo(sampleUser.getAddress().getStreet());

      mapper.delete(sampleUser);
      row =
          session()
              .execute(
                  String.format("SELECT * FROM %s.user WHERE name = ?", expectedKeyspace),
                  sampleUser.getName())
              .one();
      assertThat(row).isNull();
    }
  }

  @SuppressWarnings("unused")
  public static class Address {
    private String street;

    public Address() {}

    public Address(String street) {
      this.street = street;
    }

    public String getStreet() {
      return street;
    }

    public void setStreet(String street) {
      this.street = street;
    }
  }

  @SuppressWarnings("unused")
  @UDT(keyspace = "MapperKeyspaceTest", name = "address")
  public static class AddressWithKeyspace extends Address {
    public AddressWithKeyspace() {}

    public AddressWithKeyspace(String street) {
      super(street);
    }
  }

  @SuppressWarnings("unused")
  @UDT(name = "address")
  public static class AddressWithoutKeyspace extends Address {
    public AddressWithoutKeyspace() {}

    public AddressWithoutKeyspace(String street) {
      super(street);
    }
  }

  @SuppressWarnings("unused")
  public static class User<AddressT extends Address> {
    @PartitionKey private String name;
    private AddressT address;

    public User() {}

    public User(String name, AddressT address) {
      this.name = name;
      this.address = address;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public AddressT getAddress() {
      return address;
    }

    public void setAddress(AddressT address) {
      this.address = address;
    }
  }

  @SuppressWarnings("unused")
  @Table(keyspace = "MapperKeyspaceTest", name = "user")
  public static class UserWithKeyspace extends User<AddressWithKeyspace> {
    public UserWithKeyspace() {}

    public UserWithKeyspace(String name, AddressWithKeyspace address) {
      super(name, address);
    }

    @Override
    public AddressWithKeyspace getAddress() {
      return super.getAddress();
    }
  }

  @SuppressWarnings("unused")
  @Table(name = "user")
  public static class UserWithoutKeyspace extends User<AddressWithoutKeyspace> {
    public UserWithoutKeyspace() {}

    public UserWithoutKeyspace(String name, AddressWithoutKeyspace address) {
      super(name, address);
    }

    @Override
    public AddressWithoutKeyspace getAddress() {
      return super.getAddress();
    }
  }
}
