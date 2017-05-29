/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.extras.codecs.jdk8;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.Lists;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static com.datastax.driver.core.TypeCodec.*;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;

public class OptionalCodecTest extends CCMTestsSupport {

    private final OptionalCodec<List<String>> optionalCodec = new OptionalCodec<List<String>>(list(varchar()));

    private final CodecRegistry registry = new CodecRegistry().register(optionalCodec);

    private BuiltStatement insertStmt;

    private BuiltStatement selectStmt;

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE foo (c1 text, c2 text, c3 list<text>, c4 bigint, c5 decimal, PRIMARY KEY (c1, c2))");
    }

    @Override
    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder().withCodecRegistry(registry);
    }

    @BeforeMethod(groups = "short")
    public void createBuiltStatements() throws Exception {
        insertStmt = insertInto("foo").value("c1", bindMarker()).value("c2", bindMarker()).value("c3", bindMarker());
        selectStmt = select("c2", "c3").from("foo").where(eq("c1", bindMarker()));
    }

    /**
     * <p>
     * Validates that if a column is unset, that retrieving the value using {@link OptionalCodec} should return
     * an {@link Optional#empty()} value.   Since CQL Lists can't differentiate between null and empty lists, the
     * OptionalCodec should be smart enough to map an empty list to empty.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result an empty value.
     * @jira_ticket JAVA-606
     * @since 2.2.0
     */
    @Test(groups = "short")
    @CassandraVersion("2.2.0")
    public void should_map_unset_value_to_empty() {
        PreparedStatement insertPrep = session().prepare(this.insertStmt);
        PreparedStatement selectPrep = session().prepare(this.selectStmt);

        BoundStatement bs = insertPrep.bind();
        bs.setString(0, "should_map_unset_value_to_empty");
        bs.setString(1, "1");
        session().execute(bs);

        ResultSet results = session().execute(selectPrep.bind("should_map_unset_value_to_empty"));
        assertThat(results.getAvailableWithoutFetching()).isEqualTo(1);

        Row row = results.one();
        assertThat(row.getString("c2")).isEqualTo("1");
        assertThat(row.get("c3", optionalCodec.getJavaType())).isEqualTo(Optional.empty());
    }

    /**
     * <p>
     * Validates that if a column is set to {@link Optional#empty()} using {@link OptionalCodec} that it should be
     * stored as null and that retrieving it should return {@link Optional#empty()} using {@link OptionalCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result an empty value.
     * @jira_ticket JAVA-606
     * @since 2.2.0
     */
    @Test(groups = "short")
    public void should_map_absent_null_value_to_empty() {
        PreparedStatement insertPrep = session().prepare(this.insertStmt);
        PreparedStatement selectPrep = session().prepare(this.selectStmt);

        BoundStatement bs = insertPrep.bind();
        bs.setString(0, "should_map_absent_null_value_to_empty");
        bs.setString(1, "1");
        bs.set(2, Optional.<List<String>>empty(), optionalCodec.getJavaType());
        session().execute(bs);

        ResultSet results = session().execute(selectPrep.bind("should_map_absent_null_value_to_empty"));
        assertThat(results.getAvailableWithoutFetching()).isEqualTo(1);

        Row row = results.one();
        assertThat(row.getString("c2")).isEqualTo("1");
        assertThat(row.getList("c3", String.class)).isEmpty();
        assertThat(row.get("c3", optionalCodec.getJavaType())).isEqualTo(Optional.empty());
    }

    /**
     * <p>
     * Validates that if a column is set to an {@link Optional} value using {@link OptionalCodec} that it should be
     * stored as the option's value and that retrieving it should return an {@link Optional} using {@link OptionalCodec}
     * and its actual value without using it.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result The options value is stored appropriately and is retrievable with and without OptionalCodec.
     * @jira_ticket JAVA-606
     * @since 2.2.0
     */
    @Test(groups = "short")
    public void should_map_some_back_to_itself() {
        PreparedStatement insertPrep = session().prepare(this.insertStmt);
        PreparedStatement selectPrep = session().prepare(this.selectStmt);

        List<String> data = Lists.newArrayList("1", "2", "3");

        BoundStatement bs = insertPrep.bind();
        bs.setString(0, "should_map_some_back_to_itself");
        bs.setString(1, "1");
        bs.set(2, Optional.of(data), optionalCodec.getJavaType());
        session().execute(bs);

        ResultSet results = session().execute(selectPrep.bind("should_map_some_back_to_itself"));
        assertThat(results.getAvailableWithoutFetching()).isEqualTo(1);

        Row row = results.one();
        assertThat(row.getString("c2")).isEqualTo("1");

        // Ensure data stored correctly.
        assertThat(row.getList("c3", String.class)).isEqualTo(data);

        // Ensure data retrievable using Option codec.
        Optional<List<String>> returnData = row.get("c3", optionalCodec.getJavaType());
        assertThat(returnData.isPresent()).isTrue();
        assertThat(returnData.get()).isEqualTo(data);
    }

    @Test(groups = "short")
    public void should_map_a_primitive_type_to_empty() {
        OptionalCodec<Long> optionalLongCodec = new OptionalCodec<Long>(bigint());
        cluster().getConfiguration().getCodecRegistry().register(optionalLongCodec);

        PreparedStatement stmt = session().prepare("insert into foo (c1, c2, c4) values (?,?,?)");

        BoundStatement bs = stmt.bind();
        bs.setString(0, "should_map_a_primitive_type_to_empty");
        bs.setString(1, "1");
        bs.set(2, Optional.<Long>empty(), optionalLongCodec.getJavaType());
        session().execute(bs);

        PreparedStatement selectBigint = session().prepare("select c1, c4 from foo where c1=?");
        ResultSet results = session().execute(selectBigint.bind("should_map_a_primitive_type_to_empty"));

        assertThat(results.getAvailableWithoutFetching()).isEqualTo(1);

        Row row = results.one();
        assertThat(row.get("c4", optionalLongCodec.getJavaType())).isEqualTo(Optional.<Long>empty());
        assertThat(row.getLong("c4")).isEqualTo(0L); // This will return a 0L since it returns the primitive value.
        assertThat(row.get("c4", Long.class)).isNull();
    }

    @Test(groups = "short")
    public void should_map_a_nullable_type_to_empty() {
        OptionalCodec<BigDecimal> optionalDecimalCodec = new OptionalCodec<BigDecimal>(decimal());
        cluster().getConfiguration().getCodecRegistry().register(optionalDecimalCodec);

        PreparedStatement stmt = session().prepare("insert into foo (c1, c2, c5) values (?,?,?)");

        BoundStatement bs = stmt.bind();
        bs.setString(0, "should_map_a_nullable_type_to_empty");
        bs.setString(1, "1");
        bs.set(2, Optional.<BigDecimal>empty(), optionalDecimalCodec.getJavaType());
        session().execute(bs);

        PreparedStatement selectDecimal = session().prepare("select c1, c5 from foo where c1=?");
        ResultSet results = session().execute(selectDecimal.bind("should_map_a_nullable_type_to_empty"));

        assertThat(results.getAvailableWithoutFetching()).isEqualTo(1);

        Row row = results.one();
        assertThat(row.get("c5", optionalDecimalCodec.getJavaType())).isEqualTo(Optional.<BigDecimal>empty());
        assertThat(row.getDecimal("c5")).isNull(); // Since BigDecimal is not a primitive it is nullable so expect null.
    }

}
