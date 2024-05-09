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
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.MapperBuilder;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Computed;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
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
    minInclusive = "3.6",
    description = "Uses PER PARTITION LIMIT")
public class SelectOtherClausesIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();
  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static SimpleDao dao;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    for (String query :
        ImmutableList.of("CREATE TABLE simple (k int, cc int, v int, PRIMARY KEY (k, cc))")) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(SESSION_RULE.slowProfile()).build());
    }

    TestMapper mapper = TestMapper.builder(session).build();
    dao = mapper.simpleDao(SESSION_RULE.keyspace());

    for (int k = 0; k < 2; k++) {
      for (int cc = 0; cc < 10; cc++) {
        dao.insert(new Simple(k, cc, 1));
      }
    }
  }

  @Test
  public void should_select_with_limit() {
    PagingIterable<Simple> elements = dao.selectWithLimit(10);
    assertThat(elements.isFullyFetched()).isTrue();
    assertThat(elements.getAvailableWithoutFetching()).isEqualTo(10);

    elements = dao.selectWithLimit(0, 5);
    assertThat(elements.isFullyFetched()).isTrue();
    assertThat(elements.getAvailableWithoutFetching()).isEqualTo(5);

    elements = dao.selectWithLimit(0, 0, 1);
    assertThat(elements.isFullyFetched()).isTrue();
    assertThat(elements.getAvailableWithoutFetching()).isEqualTo(1);
  }

  @Test
  public void should_select_with_per_partition_limit() {
    PagingIterable<Simple> elements = dao.selectWithPerPartitionLimit(5);
    assertThat(elements.isFullyFetched()).isTrue();
    assertThat(elements.getAvailableWithoutFetching()).isEqualTo(10);

    Map<Integer, Integer> elementCountPerPartition = new HashMap<>();
    for (Simple element : elements) {
      elementCountPerPartition.compute(element.getK(), (k, v) -> (v == null) ? 1 : v + 1);
    }
    assertThat(elementCountPerPartition).hasSize(2).containsEntry(0, 5).containsEntry(1, 5);
  }

  @Test
  public void should_select_with_order_by() {
    PagingIterable<Simple> elements = dao.selectByCcDesc(0);
    int previousCc = Integer.MAX_VALUE;
    for (Simple element : elements) {
      assertThat(element.getCc()).isLessThan(previousCc);
      previousCc = element.getCc();
    }
  }

  @Test
  public void should_select_with_group_by() {
    PagingIterable<Sum> sums = dao.selectSumByK();
    assertThat(sums.all()).hasSize(2).containsOnly(new Sum(0, 10), new Sum(1, 10));
  }

  @Test
  public void should_select_with_allow_filtering() {
    PagingIterable<Simple> elements = dao.selectByCc(1);
    assertThat(elements.all()).hasSize(2).containsOnly(new Simple(0, 1, 1), new Simple(1, 1, 1));
  }

  @Mapper
  public interface TestMapper {
    @DaoFactory
    SimpleDao simpleDao(@DaoKeyspace CqlIdentifier keyspace);

    static MapperBuilder<TestMapper> builder(CqlSession session) {
      return new SelectOtherClausesIT_TestMapperBuilder(session);
    }
  }

  @Dao
  public interface SimpleDao {
    @Insert
    void insert(Simple simple);

    @Select(limit = ":l")
    PagingIterable<Simple> selectWithLimit(@CqlName("l") int l);

    @Select(limit = ":l")
    PagingIterable<Simple> selectWithLimit(int k, @CqlName("l") int l);

    /**
     * Contrived since the query will return at most a single row, but this is just to check that
     * {@code l} doesn't need an explicit name when the full primary key is provided.
     */
    @Select(limit = ":l")
    PagingIterable<Simple> selectWithLimit(int k, int cc, int l);

    @Select(perPartitionLimit = ":perPartitionLimit")
    PagingIterable<Simple> selectWithPerPartitionLimit(
        @CqlName("perPartitionLimit") int perPartitionLimit);

    @Select(orderBy = "cc DESC")
    PagingIterable<Simple> selectByCcDesc(int k);

    @Select(groupBy = "k")
    PagingIterable<Sum> selectSumByK();

    @Select(customWhereClause = "cc = :cc", allowFiltering = true)
    PagingIterable<Simple> selectByCc(int cc);
  }

  @Entity
  public static class Simple {
    @PartitionKey private int k;
    @ClusteringColumn private int cc;
    private int v;

    public Simple() {}

    public Simple(int k, int cc, int v) {
      this.k = k;
      this.cc = cc;
      this.v = v;
    }

    public int getK() {
      return k;
    }

    public void setK(int k) {
      this.k = k;
    }

    public int getCc() {
      return cc;
    }

    public void setCc(int cc) {
      this.cc = cc;
    }

    public int getV() {
      return v;
    }

    public void setV(int v) {
      this.v = v;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof Simple) {
        Simple that = (Simple) other;
        return this.k == that.k && this.cc == that.cc && this.v == that.v;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(k, cc, v);
    }

    @Override
    public String toString() {
      return String.format("Simple(%d, %d, %d)", k, cc, v);
    }
  }

  @Entity
  @CqlName("simple")
  public static class Sum {
    private int k;

    @Computed("sum(v)")
    private int value;

    public Sum() {}

    public Sum(int k, int value) {
      this.k = k;
      this.value = value;
    }

    public int getK() {
      return k;
    }

    public void setK(int k) {
      this.k = k;
    }

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof Sum) {
        Sum that = (Sum) other;
        return this.k == that.k && this.value == that.value;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(k, value);
    }

    @Override
    public String toString() {
      return String.format("Sum(%d, %d)", k, value);
    }
  }
}
