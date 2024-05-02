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
package com.datastax.oss.driver.mapper;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class UdtKeyIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static RecordDao dao;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();
    for (String ddlQuery :
        ImmutableList.of(
            "CREATE TYPE key (value int)",
            "CREATE TABLE record(key frozen<key> PRIMARY KEY, value int)",
            "CREATE TABLE multi_key_record(key frozen<list<key>> PRIMARY KEY, value int)")) {
      session.execute(
          SimpleStatement.builder(ddlQuery)
              .setExecutionProfile(SESSION_RULE.slowProfile())
              .build());
    }

    TestMapper mapper = new UdtKeyIT_TestMapperBuilder(SESSION_RULE.session()).build();
    dao = mapper.recordDao(SESSION_RULE.keyspace());
  }

  @Test
  public void should_save_and_retrieve_entity_with_udt_pk() {
    // Given
    Key key = new Key(1);
    dao.save(new Record(key, 42));

    // When
    Record record = dao.findByKey(key);

    // Then
    assertThat(record.getValue()).isEqualTo(42);
  }

  @Test
  public void should_save_and_retrieve_entity_with_udt_collection_pk() {
    // Given
    List<Key> key = ImmutableList.of(new Key(1), new Key(2));
    dao.saveMulti(new MultiKeyRecord(key, 42));

    // When
    MultiKeyRecord record = dao.findMultiByKey(key);

    // Then
    assertThat(record.getValue()).isEqualTo(42);
  }

  @Entity
  public static class Key {
    private int value;

    public Key() {}

    public Key(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }
  }

  @Entity
  public static class Record {
    @PartitionKey private Key key;
    private int value;

    public Record() {}

    public Record(Key key, int value) {
      this.key = key;
      this.value = value;
    }

    public Key getKey() {
      return key;
    }

    public void setKey(Key key) {
      this.key = key;
    }

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }
  }

  @Entity
  public static class MultiKeyRecord {
    @PartitionKey private List<Key> key;
    private int value;

    public MultiKeyRecord() {}

    public MultiKeyRecord(List<Key> key, int value) {
      this.key = key;
      this.value = value;
    }

    public List<Key> getKey() {
      return key;
    }

    public void setKey(List<Key> key) {
      this.key = key;
    }

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  interface RecordDao {
    @Select
    Record findByKey(Key key);

    @Insert
    void save(Record record);

    @Select
    MultiKeyRecord findMultiByKey(List<Key> key);

    @Insert
    void saveMulti(MultiKeyRecord record);
  }

  @Mapper
  interface TestMapper {
    @DaoFactory
    RecordDao recordDao(@DaoKeyspace CqlIdentifier keyspace);
  }
}
