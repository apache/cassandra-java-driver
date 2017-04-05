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
package com.datastax.driver.extras.codecs.date;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;

@CassandraVersion("2.2.0")
public class SimpleDateCodecsTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TABLE IF NOT EXISTS foo ("
                        + "c1 text PRIMARY KEY, "
                        + "cdate date, "
                        + "ctimestamp timestamp, "
                        + "cdates frozen<list<date>>, "
                        + "ctimestamps frozen<map<text,timestamp>> "
                        + ")");
    }

    @BeforeClass(groups = "short")
    public void registerCodecs() throws Exception {
        CodecRegistry codecRegistry = cluster().getConfiguration().getCodecRegistry();
        codecRegistry
                .register(SimpleDateCodec.instance)
                .register(SimpleTimestampCodec.instance);
    }


    /**
     * <p>
     * Validates that a <code>date</code> column can be mapped to an int by using
     * {@link SimpleDateCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result properly maps.
     * @since 2.2.0
     */
    @Test(groups = "short")
    public void should_map_date_to_days_since_epoch() {
        // given
        int days = 12345;
        LocalDate localDate = LocalDate.fromDaysSinceEpoch(days);
        // when
        // note: these codecs cannot work with simple statements!
        BoundStatement bs = session().prepare("insert into foo (c1, cdate) values (?, ?)").bind("should_map_date_to_days_since_epoch", days);
        session().execute(bs);
        ResultSet result = session().execute("select cdate from foo where c1=?", "should_map_date_to_days_since_epoch");
        // then
        assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);
        Row row = result.one();
        assertThat(row.getInt("cdate")).isEqualTo(days);
        assertThat(row.get("cdate", Integer.class)).isEqualTo(days);
        assertThat(row.getDate("cdate")).isEqualTo(localDate);
        assertThat(row.get("cdate", LocalDate.class)).isEqualTo(localDate);
    }

    /**
     * <p>
     * Validates that a <code>timestamp</code> column can be mapped to a long by using
     * {@link SimpleTimestampCodec}.
     * </p>
     *
     * @test_category data_types:serialization
     * @expected_result properly maps.
     * @since 2.2.0
     */
    @Test(groups = "short")
    public void should_map_timestamp_to_millis_since_epoch() {
        // given
        long millis = new Date().getTime();
        Date date = new Date(millis);
        // when
        // note: these codecs cannot work with simple statements!
        BoundStatement bs = session().prepare("insert into foo (c1, ctimestamp) values (?, ?)").bind("should_map_timestamp_to_millis_since_epoch", millis);
        session().execute(bs);
        ResultSet result = session().execute("select ctimestamp from foo where c1=?", "should_map_timestamp_to_millis_since_epoch");
        // then
        assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);
        Row row = result.one();
        assertThat(row.getLong("ctimestamp")).isEqualTo(millis);
        assertThat(row.get("ctimestamp", Long.class)).isEqualTo(millis);
        assertThat(row.getTimestamp("ctimestamp")).isEqualTo(date);
        assertThat(row.get("ctimestamp", Date.class)).isEqualTo(date);
    }

    @Test(groups = "short")
    public void should_use_mapper_to_store_and_retrieve_values_with_simple_date_codecs() {
        // given
        MappingManager manager = new MappingManager(session());
        Mapper<Mapped> mapper = manager.mapper(Mapped.class);
        // when
        Mapped pojo = new Mapped();
        mapper.save(pojo);
        Mapped actual = mapper.get("mapper");
        // then
        assertThat(actual).isEqualToComparingFieldByField(pojo);
    }

    @SuppressWarnings("unused")
    @Table(name = "foo")
    public static class Mapped {

        @PartitionKey
        private String c1;

        @Column(name = "ctimestamp")
        private long millis;

        @Column(name = "cdate")
        private int days;

        @Column(name = "ctimestamps")
        private Map<String, Long> mapOfMillis;

        @Column(name = "cdates")
        private List<Integer> listOfDays;

        public Mapped() {
            c1 = "mapper";
            millis = 123456;
            days = 123;
            mapOfMillis = ImmutableMap.of("foo", 123456L);
            listOfDays = newArrayList(123, 456);
        }

        public String getC1() {
            return c1;
        }

        public void setC1(String c1) {
            this.c1 = c1;
        }

        public long getMillis() {
            return millis;
        }

        public void setMillis(long millis) {
            this.millis = millis;
        }

        public int getDays() {
            return days;
        }

        public void setDays(int days) {
            this.days = days;
        }

        public Map<String, Long> getMapOfMillis() {
            return mapOfMillis;
        }

        public void setMapOfMillis(Map<String, Long> mapOfMillis) {
            this.mapOfMillis = mapOfMillis;
        }

        public List<Integer> getListOfDays() {
            return listOfDays;
        }

        public void setListOfDays(List<Integer> listOfDays) {
            this.listOfDays = listOfDays;
        }
    }

}
