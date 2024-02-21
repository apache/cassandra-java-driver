package com.datastax.oss.driver.mapper;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.testinfra.CassandraSkip;
import com.datastax.oss.driver.api.testinfra.ScyllaRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@CassandraSkip(description = "USING TIMEOUT is a ScyllaDB CQL Extension")
@ScyllaRequirement(
    minOSS = "4.4.0",
    minEnterprise = "2022.1.0",
    description = "According to labels attached to 39afe14ad4")
public class UsingTimeoutIT {
  private static final CcmRule CCM_RULE = CcmRule.getInstance();
  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static TimeoutTestEntityDao dao;
  private static TimeoutTestMapper mapper;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    session.execute(
        SimpleStatement.newInstance(
                "CREATE TABLE timeout_test_entity(id int, col int, PRIMARY KEY (id))")
            .setExecutionProfile(SESSION_RULE.slowProfile()));

    mapper = new UsingTimeoutIT_TimeoutTestMapperBuilder(session).build();
    dao = mapper.timeoutTestEntityDao(SESSION_RULE.keyspace());
  }

  @Test
  public void should_throw_when_negative_timeouts() {
    assertThrows(
        "Timeout values must be non-negative",
        InvalidQueryException.class,
        () -> dao.saveNegative(new TimeoutTestEntity(1, 1)));
    assertThrows(
        "Timeout values must be non-negative",
        InvalidQueryException.class,
        () -> dao.findByIdNegative(1));
    assertThrows(
        "Timeout values must be non-negative",
        InvalidQueryException.class,
        () -> dao.updateNegative(new TimeoutTestEntity(1, 2)));
  }

  @Test
  public void should_pass_when_valid_timeouts() {
    ResultSet saveResult = dao.save(new TimeoutTestEntity(2, 1));
    assertTrue(saveResult.getExecutionInfo().getErrors().isEmpty());
    assertTrue(saveResult.getExecutionInfo().getWarnings().isEmpty());

    PagingIterable<TimeoutTestEntity> selectResult = dao.findById(2);
    assertTrue(selectResult.getExecutionInfo().getErrors().isEmpty());
    assertTrue(selectResult.getExecutionInfo().getWarnings().isEmpty());

    ResultSet updateResult = dao.update(new TimeoutTestEntity(2, 2));
    assertTrue(updateResult.getExecutionInfo().getErrors().isEmpty());
    assertTrue(updateResult.getExecutionInfo().getWarnings().isEmpty());
  }

  @Test
  public void should_time_out_when_set_to_millisecond() {
    for (int i = 0; i < 10004; i++) {
      dao.save(new TimeoutTestEntity(i, 1));
    }
    assertThrows(ReadTimeoutException.class, () -> dao.allInMillisecond());
  }

  @Test
  public void should_fail_with_too_low_granularity() {
    assertThrows(
        "Timeout values cannot have granularity finer than milliseconds",
        InvalidQueryException.class,
        () -> dao.allInNanosecond());
    assertThrows(
        "Timeout values cannot have granularity finer than milliseconds",
        InvalidQueryException.class,
        () -> dao.allInMicrosecond());
  }

  @Entity
  public static class TimeoutTestEntity {
    @PartitionKey private Integer id;
    private Integer col;

    public TimeoutTestEntity() {}

    public TimeoutTestEntity(Integer id, Integer col) {
      this.id = id;
      this.col = col;
    }

    public Integer getId() {
      return id;
    }

    public void setId(Integer id) {
      this.id = id;
    }

    public Integer getCol() {
      return col;
    }

    public void setCol(Integer col) {
      this.col = col;
    }
  }

  @Dao
  public interface TimeoutTestEntityDao {
    @Select(usingTimeout = "-5s")
    PagingIterable<TimeoutTestEntity> findByIdNegative(Integer id);

    @Insert(usingTimeout = "-5s")
    ResultSet saveNegative(TimeoutTestEntity timeoutTestEntity);

    @Update(usingTimeout = "-5s")
    ResultSet updateNegative(TimeoutTestEntity timeoutTestEntity);

    @Select(usingTimeout = "5m")
    PagingIterable<TimeoutTestEntity> findById(Integer id);

    @Insert(usingTimeout = "5m")
    ResultSet save(TimeoutTestEntity timeoutTestEntity);

    @Update(usingTimeout = "5m")
    ResultSet update(TimeoutTestEntity timeoutTestEntity);

    @Select(usingTimeout = "1ns")
    PagingIterable<TimeoutTestEntity> allInNanosecond();

    @Select(usingTimeout = "1us")
    PagingIterable<TimeoutTestEntity> allInMicrosecond();

    @Select(usingTimeout = "1ms")
    PagingIterable<TimeoutTestEntity> allInMillisecond();
  }

  @Mapper
  public interface TimeoutTestMapper {
    @DaoFactory
    TimeoutTestEntityDao timeoutTestEntityDao(@DaoKeyspace CqlIdentifier keyspace);
  }
}
