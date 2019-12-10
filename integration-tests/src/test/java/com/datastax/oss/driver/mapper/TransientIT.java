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
package com.datastax.oss.driver.mapper;

import static com.datastax.oss.driver.assertions.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.datastax.oss.driver.api.mapper.annotations.TransientProperties;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class TransientIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();
  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static TestMapper mapper;

  private static final AtomicInteger keyProvider = new AtomicInteger(0);

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    session.execute(
        SimpleStatement.builder("CREATE TABLE entity(id int primary key, v int)")
            .setExecutionProfile(SESSION_RULE.slowProfile())
            .build());

    mapper = new TransientIT_TestMapperBuilder(session).build();
  }

  @Test
  public void should_ignore_field_with_transient_annotated_field() {
    EntityWithTransientAnnotatedFieldDao dao =
        mapper.entityWithTransientAnnotatedFieldDao(
            SESSION_RULE.keyspace(), CqlIdentifier.fromCql("entity"));

    int key = keyProvider.incrementAndGet();
    EntityWithTransientAnnotatedField entity = new EntityWithTransientAnnotatedField(key, 1, 7);
    dao.save(entity);

    EntityWithTransientAnnotatedField retrievedEntity = dao.findById(key);
    assertThat(retrievedEntity.getId()).isEqualTo(key);
    assertThat(retrievedEntity.getV()).isEqualTo(1);
    // column should not have been set since field was @Transient-annotated
    assertThat(retrievedEntity.getNotAColumn()).isNull();
  }

  @Test
  public void should_ignore_field_with_transient_annotated_getter() {
    EntityWithTransientAnnotatedGetterDao dao =
        mapper.entityWithTransientAnnotatedGetterDao(
            SESSION_RULE.keyspace(), CqlIdentifier.fromCql("entity"));

    int key = keyProvider.incrementAndGet();
    EntityWithTransientAnnotatedGetter entity = new EntityWithTransientAnnotatedGetter(key, 1, 7);
    dao.save(entity);

    EntityWithTransientAnnotatedGetter retrievedEntity = dao.findById(key);
    assertThat(retrievedEntity.getId()).isEqualTo(key);
    assertThat(retrievedEntity.getV()).isEqualTo(1);
    // column should not have been set since getter was @Transient-annotated
    assertThat(retrievedEntity.getNotAColumn()).isNull();
  }

  @Test
  public void should_ignore_field_with_transient_keyword() {
    EntityWithTransientKeywordDao dao =
        mapper.entityWithTransientKeywordDao(
            SESSION_RULE.keyspace(), CqlIdentifier.fromCql("entity"));

    int key = keyProvider.incrementAndGet();
    EntityWithTransientKeyword entity = new EntityWithTransientKeyword(key, 1, 7);
    dao.save(entity);

    EntityWithTransientKeyword retrievedEntity = dao.findById(key);
    assertThat(retrievedEntity.getId()).isEqualTo(key);
    assertThat(retrievedEntity.getV()).isEqualTo(1);
    // column should not have been set since field had transient keyword
    assertThat(retrievedEntity.getNotAColumn()).isNull();
  }

  @Test
  public void should_ignore_properties_included_in_transient_properties_keyword() {
    EntityWithTransientPropertiesAnnotationDao dao =
        mapper.entityWithTransientPropertiesAnnotation(
            SESSION_RULE.keyspace(), CqlIdentifier.fromCql("entity"));

    int key = keyProvider.incrementAndGet();
    EntityWithTransientPropertiesAnnotation entity =
        new EntityWithTransientPropertiesAnnotation(key, 1, 7, 10L);
    dao.save(entity);

    EntityWithTransientPropertiesAnnotation retrievedEntity = dao.findById(key);
    assertThat(retrievedEntity.getId()).isEqualTo(key);
    assertThat(retrievedEntity.getV()).isEqualTo(1);
    // columns should not have been set since field was @Transient-annotated
    assertThat(retrievedEntity.getNotAColumn()).isNull();
    assertThat(retrievedEntity.getAlsoNotAColumn()).isNull();
  }

  @Entity
  public static class EntityWithTransientAnnotatedField {

    @PartitionKey private int id;

    private int v;

    @Transient private Integer notAColumn;

    EntityWithTransientAnnotatedField() {}

    EntityWithTransientAnnotatedField(int id, int v, Integer notAColumn) {
      this.id = id;
      this.v = v;
      this.notAColumn = notAColumn;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public int getV() {
      return v;
    }

    public void setV(int v) {
      this.v = v;
    }

    @SuppressWarnings("WeakerAccess")
    public Integer getNotAColumn() {
      return notAColumn;
    }

    @SuppressWarnings("unused")
    public void setNotAColumn(Integer notAColumn) {
      this.notAColumn = notAColumn;
    }
  }

  @Entity
  public static class EntityWithTransientAnnotatedGetter {

    @PartitionKey private int id;

    private int v;

    private Integer notAColumn;

    EntityWithTransientAnnotatedGetter() {}

    EntityWithTransientAnnotatedGetter(int id, int v, Integer notAColumn) {
      this.id = id;
      this.v = v;
      this.notAColumn = notAColumn;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public int getV() {
      return v;
    }

    public void setV(int v) {
      this.v = v;
    }

    @Transient
    @SuppressWarnings("WeakerAccess")
    public Integer getNotAColumn() {
      return notAColumn;
    }

    @SuppressWarnings("unused")
    public void setNotAColumn(Integer notAColumn) {
      this.notAColumn = notAColumn;
    }
  }

  @Entity
  public static class EntityWithTransientKeyword {

    @PartitionKey private int id;

    private int v;

    private transient Integer notAColumn;

    EntityWithTransientKeyword() {}

    EntityWithTransientKeyword(int id, int v, Integer notAColumn) {
      this.id = id;
      this.v = v;
      this.notAColumn = notAColumn;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public int getV() {
      return v;
    }

    public void setV(int v) {
      this.v = v;
    }

    @SuppressWarnings("WeakerAccess")
    public Integer getNotAColumn() {
      return notAColumn;
    }

    @SuppressWarnings("unused")
    public void setNotAColumn(Integer notAColumn) {
      this.notAColumn = notAColumn;
    }
  }

  @TransientProperties({"notAColumn", "alsoNotAColumn"})
  @Entity
  public static class EntityWithTransientPropertiesAnnotation {

    @PartitionKey private int id;

    private int v;

    private transient Integer notAColumn;

    private transient Long alsoNotAColumn;

    EntityWithTransientPropertiesAnnotation() {}

    EntityWithTransientPropertiesAnnotation(
        int id, int v, Integer notAColumn, Long alsoNotAColumn) {
      this.id = id;
      this.v = v;
      this.notAColumn = notAColumn;
      this.alsoNotAColumn = alsoNotAColumn;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public int getV() {
      return v;
    }

    public void setV(int v) {
      this.v = v;
    }

    @SuppressWarnings("WeakerAccess")
    public Integer getNotAColumn() {
      return notAColumn;
    }

    @SuppressWarnings("unused")
    public void setNotAColumn(Integer notAColumn) {
      this.notAColumn = notAColumn;
    }

    @SuppressWarnings("WeakerAccess")
    public Long getAlsoNotAColumn() {
      return alsoNotAColumn;
    }

    @SuppressWarnings("unused")
    public void setAlsoNotAColumn(Long alsoNotAColumn) {
      this.alsoNotAColumn = alsoNotAColumn;
    }
  }

  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  interface BaseDao {}

  @Dao
  public interface EntityWithTransientAnnotatedFieldDao extends BaseDao {
    @Select
    EntityWithTransientAnnotatedField findById(int id);

    @Insert
    void save(EntityWithTransientAnnotatedField entity);
  }

  @Dao
  public interface EntityWithTransientAnnotatedGetterDao extends BaseDao {
    @Select
    EntityWithTransientAnnotatedGetter findById(int id);

    @Insert
    void save(EntityWithTransientAnnotatedGetter entity);
  }

  @Dao
  public interface EntityWithTransientKeywordDao extends BaseDao {
    @Select
    EntityWithTransientKeyword findById(int id);

    @Insert
    void save(EntityWithTransientKeyword entity);
  }

  @Dao
  public interface EntityWithTransientPropertiesAnnotationDao extends BaseDao {
    @Select
    EntityWithTransientPropertiesAnnotation findById(int id);

    @Insert
    void save(EntityWithTransientPropertiesAnnotation entity);
  }

  @Mapper
  public interface TestMapper {
    @DaoFactory
    EntityWithTransientAnnotatedFieldDao entityWithTransientAnnotatedFieldDao(
        @DaoKeyspace CqlIdentifier keyspace, @DaoTable CqlIdentifier table);

    @DaoFactory
    EntityWithTransientAnnotatedGetterDao entityWithTransientAnnotatedGetterDao(
        @DaoKeyspace CqlIdentifier keyspace, @DaoTable CqlIdentifier table);

    @DaoFactory
    EntityWithTransientKeywordDao entityWithTransientKeywordDao(
        @DaoKeyspace CqlIdentifier keyspace, @DaoTable CqlIdentifier table);

    @DaoFactory
    EntityWithTransientPropertiesAnnotationDao entityWithTransientPropertiesAnnotation(
        @DaoKeyspace CqlIdentifier keyspace, @DaoTable CqlIdentifier table);
  }
}
