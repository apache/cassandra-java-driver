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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class QueryProviderIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();
  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  // Dummy counter to exercize the "custom state" feature: it gets incremented each time the query
  // provider is called.
  private static AtomicInteger executionCount = new AtomicInteger();

  private static SensorDao dao;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE sensor_reading(id int, month int, day int, value double, "
                    + "PRIMARY KEY (id, month, day)) "
                    + "WITH CLUSTERING ORDER BY (month DESC, day DESC)")
            .setExecutionProfile(SESSION_RULE.slowProfile())
            .build());

    SensorMapper mapper =
        new QueryProviderIT_SensorMapperBuilder(session)
            .withCustomState("executionCount", executionCount)
            .build();
    dao = mapper.sensorDao(SESSION_RULE.keyspace());
  }

  @Test
  public void should_invoke_query_provider() {
    SensorReading readingFeb3 = new SensorReading(1, 2, 3, 9.3);
    SensorReading readingFeb2 = new SensorReading(1, 2, 2, 8.6);
    SensorReading readingFeb1 = new SensorReading(1, 2, 1, 8.7);
    SensorReading readingJan31 = new SensorReading(1, 1, 31, 8.2);
    dao.save(readingFeb3);
    dao.save(readingFeb2);
    dao.save(readingFeb1);
    dao.save(readingJan31);

    assertThat(executionCount.get()).isEqualTo(0);

    assertThat(dao.findSlice(1, null, null).all())
        .containsExactly(readingFeb3, readingFeb2, readingFeb1, readingJan31);
    assertThat(dao.findSlice(1, 2, null).all())
        .containsExactly(readingFeb3, readingFeb2, readingFeb1);
    assertThat(dao.findSlice(1, 2, 3).all()).containsExactly(readingFeb3);

    assertThat(executionCount.get()).isEqualTo(3);
  }

  @Mapper
  public interface SensorMapper {
    @DaoFactory
    SensorDao sensorDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface SensorDao {

    @QueryProvider(providerClass = FindSliceProvider.class, entityHelpers = SensorReading.class)
    PagingIterable<SensorReading> findSlice(int id, Integer month, Integer day);

    @QueryProvider(
        providerClass = FindSliceStreamProvider.class,
        entityHelpers = SensorReading.class)
    Stream<SensorReading> findSliceAsStream(int id, Integer month, Integer day);

    @Insert
    void save(SensorReading reading);
  }

  public static class FindSliceProvider {
    private final CqlSession session;
    private final AtomicInteger executionCount;
    private final EntityHelper<SensorReading> sensorReadingHelper;
    private final Select selectStart;

    public FindSliceProvider(
        MapperContext context, EntityHelper<SensorReading> sensorReadingHelper) {
      this.session = context.getSession();
      this.executionCount = ((AtomicInteger) context.getCustomState().get("executionCount"));
      this.sensorReadingHelper = sensorReadingHelper;
      this.selectStart =
          sensorReadingHelper.selectStart().whereColumn("id").isEqualTo(bindMarker());
    }

    public PagingIterable<SensorReading> findSlice(int id, Integer month, Integer day) {
      if (month == null && day != null) {
        throw new IllegalArgumentException("Can't specify day if month is null");
      }

      executionCount.incrementAndGet();

      Select select = this.selectStart;
      if (month != null) {
        select = select.whereColumn("month").isEqualTo(bindMarker());
        if (day != null) {
          select = select.whereColumn("day").isEqualTo(bindMarker());
        }
      }
      PreparedStatement preparedStatement = session.prepare(select.build());
      BoundStatementBuilder boundStatementBuilder =
          preparedStatement.boundStatementBuilder().setInt("id", id);
      if (month != null) {
        boundStatementBuilder = boundStatementBuilder.setInt("month", month);
        if (day != null) {
          boundStatementBuilder = boundStatementBuilder.setInt("day", day);
        }
      }
      return session
          .execute(boundStatementBuilder.build())
          .map(row -> sensorReadingHelper.get(row, false));
    }
  }

  public static class FindSliceStreamProvider extends FindSliceProvider {

    public FindSliceStreamProvider(
        MapperContext context, EntityHelper<SensorReading> sensorReadingHelper) {
      super(context, sensorReadingHelper);
    }

    public Stream<SensorReading> findSliceAsStream(int id, Integer month, Integer day) {
      return StreamSupport.stream(findSlice(id, month, day).spliterator(), false);
    }
  }

  @Entity
  public static class SensorReading {
    @PartitionKey private int id;

    @ClusteringColumn(1)
    private int month;

    @ClusteringColumn(2)
    private int day;

    private double value;

    public SensorReading() {}

    public SensorReading(int id, int month, int day, double value) {
      this.id = id;
      this.month = month;
      this.day = day;
      this.value = value;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public int getMonth() {
      return month;
    }

    public void setMonth(int month) {
      this.month = month;
    }

    public int getDay() {
      return day;
    }

    public void setDay(int day) {
      this.day = day;
    }

    public double getValue() {
      return value;
    }

    public void setValue(double value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof SensorReading) {
        SensorReading that = (SensorReading) other;
        return this.id == that.id
            && this.month == that.month
            && this.day == that.day
            && this.value == that.value;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, month, day, value);
    }

    @Override
    public String toString() {
      return String.format("%d %d/%d %f", id, month, day, value);
    }
  }
}
