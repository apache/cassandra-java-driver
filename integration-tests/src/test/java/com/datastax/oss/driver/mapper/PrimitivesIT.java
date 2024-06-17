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

import static com.datastax.oss.driver.assertions.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.Objects;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@BackendRequirement(
    type = BackendType.CASSANDRA,
    minInclusive = "2.2",
    description = "smallint is a reserved keyword in 2.1")
public class PrimitivesIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static TestMapper mapper;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();
    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE primitives_entity("
                    + "id int PRIMARY KEY, "
                    + "boolean_col boolean, "
                    + "byte_col tinyint, "
                    + "short_col smallint, "
                    + "long_col bigint, "
                    + "float_col float,"
                    + "double_col double)")
            .setExecutionProfile(SESSION_RULE.slowProfile())
            .build());
    mapper = new PrimitivesIT_TestMapperBuilder(session).build();
  }

  @Test
  public void should_not_include_computed_values_in_insert() {
    PrimitivesDao primitivesDao = mapper.primitivesDao(SESSION_RULE.keyspace());

    PrimitivesEntity expected = new PrimitivesEntity(0, true, (byte) 2, (short) 3, 4L, 5.0f, 6.0d);
    primitivesDao.save(expected);

    PrimitivesEntity actual = primitivesDao.findById(0);
    assertThat(actual).isEqualTo(expected);
  }

  @Entity
  public static class PrimitivesEntity {

    @PartitionKey private int id;

    private boolean booleanCol;

    private byte byteCol;

    private short shortCol;

    private long longCol;

    private float floatCol;

    private double doubleCol;

    public PrimitivesEntity() {}

    public PrimitivesEntity(
        int id,
        boolean booleanCol,
        byte byteCol,
        short shortCol,
        long longCol,
        float floatCol,
        double doubleCol) {
      this.id = id;
      this.booleanCol = booleanCol;
      this.byteCol = byteCol;
      this.shortCol = shortCol;
      this.longCol = longCol;
      this.floatCol = floatCol;
      this.doubleCol = doubleCol;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public boolean isBooleanCol() {
      return booleanCol;
    }

    public void setBooleanCol(boolean booleanCol) {
      this.booleanCol = booleanCol;
    }

    public byte getByteCol() {
      return byteCol;
    }

    public void setByteCol(byte byteCol) {
      this.byteCol = byteCol;
    }

    public short getShortCol() {
      return shortCol;
    }

    public void setShortCol(short shortCol) {
      this.shortCol = shortCol;
    }

    public long getLongCol() {
      return longCol;
    }

    public void setLongCol(long longCol) {
      this.longCol = longCol;
    }

    public float getFloatCol() {
      return floatCol;
    }

    public void setFloatCol(float floatCol) {
      this.floatCol = floatCol;
    }

    public double getDoubleCol() {
      return doubleCol;
    }

    public void setDoubleCol(double doubleCol) {
      this.doubleCol = doubleCol;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PrimitivesEntity)) {
        return false;
      }
      PrimitivesEntity that = (PrimitivesEntity) o;
      return this.id == that.id
          && this.booleanCol == that.booleanCol
          && this.byteCol == that.byteCol
          && this.shortCol == that.shortCol
          && this.longCol == that.longCol
          && Float.compare(this.floatCol, that.floatCol) == 0
          && Double.compare(this.doubleCol, that.doubleCol) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, booleanCol, byteCol, shortCol, longCol, floatCol, doubleCol);
    }
  }

  @Dao
  public interface PrimitivesDao {

    @Select
    PrimitivesEntity findById(int id);

    @Insert(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void save(PrimitivesEntity entity);
  }

  @Mapper
  public interface TestMapper {
    @DaoFactory
    PrimitivesDao primitivesDao(@DaoKeyspace CqlIdentifier keyspace);
  }
}
