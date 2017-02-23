/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.configuration.MapperConfiguration;
import com.datastax.driver.mapping.configuration.naming.CommonNamingConventions;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for JAVA-1316 - validate ability to automatically translate property names
 * across different naming conventions.
 */
@SuppressWarnings("unused")
public class PropertyNamingStrategyTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        createTable("camel_case_table", "myXmlParser", "other");
        createTable("snake_case_table", "my_xml_parser", "other");
        createTable("lower_case_table", "myxmlparser", "other");
    }

    private void createTable(String tableName, String propertyOne, String propertyTwo) {
        execute(String.format("CREATE TABLE %s (\"%s\" int primary key, \"%s\" int)", tableName, propertyOne, propertyTwo));
        execute(String.format("INSERT INTO %s (\"%s\", \"%s\") VALUES (1, 2)", tableName, propertyOne, propertyTwo));
    }

    @Test(groups = "short")
    public void camel_case_to_camel_case() {
        MapperConfiguration conf = new MapperConfiguration();
        conf.getNamingStrategy()
                .setJavaConvention(new CommonNamingConventions.LowerCamelCase())
                .setCassandraConvention(new CommonNamingConventions.LowerCamelCase());
        MappingManager mappingManager = new MappingManager(session(), conf);
        Mapper<CamelCaseToCamelCase> mapper = mappingManager.mapper(CamelCaseToCamelCase.class);
        assertThat(mapper.get(1).getMyXmlParser()).isEqualTo(1);
        assertThat(mapper.get(1).getOther()).isEqualTo(2);
    }

    @Table(name = "camel_case_table")
    public static class CamelCaseToCamelCase {

        @PartitionKey
        private int myXmlParser;

        private int other;

        public int getMyXmlParser() {
            return myXmlParser;
        }

        public void setMyXmlParser(int myXmlParser) {
            this.myXmlParser = myXmlParser;
        }

        public int getOther() {
            return other;
        }

        public void setOther(int other) {
            this.other = other;
        }

    }

    @Test(groups = "short")
    public void camel_case_to_snake_case() {
        MapperConfiguration conf = new MapperConfiguration();
        conf.getNamingStrategy()
                .setJavaConvention(new CommonNamingConventions.LowerCamelCase())
                .setCassandraConvention(new CommonNamingConventions.LowerSnakeCase());
        MappingManager mappingManager = new MappingManager(session(), conf);
        Mapper<CamelCaseToSnakeCase> mapper = mappingManager.mapper(CamelCaseToSnakeCase.class);
        assertThat(mapper.get(1).getMyXmlParser()).isEqualTo(1);
        assertThat(mapper.get(1).getOther()).isEqualTo(2);
    }

    @Table(name = "snake_case_table")
    public static class CamelCaseToSnakeCase {

        @PartitionKey
        private int myXmlParser;

        private int other;

        public int getMyXmlParser() {
            return myXmlParser;
        }

        public void setMyXmlParser(int myXmlParser) {
            this.myXmlParser = myXmlParser;
        }

        public int getOther() {
            return other;
        }

        public void setOther(int other) {
            this.other = other;
        }

    }

    @Test(groups = "short")
    public void camel_case_to_lower_case() {
        MapperConfiguration conf = new MapperConfiguration();
        conf.getNamingStrategy()
                .setJavaConvention(new CommonNamingConventions.LowerCamelCase())
                .setCassandraConvention(new CommonNamingConventions.LowerCase());
        MappingManager mappingManager = new MappingManager(session(), conf);
        Mapper<CamelCaseToLowerCase> mapper = mappingManager.mapper(CamelCaseToLowerCase.class);
        assertThat(mapper.get(1).getMyXmlParser()).isEqualTo(1);
        assertThat(mapper.get(1).getOther()).isEqualTo(2);
    }

    @Table(name = "lower_case_table")
    public static class CamelCaseToLowerCase {

        @PartitionKey
        private int myXmlParser;

        private int other;

        public int getMyXmlParser() {
            return myXmlParser;
        }

        public void setMyXmlParser(int myXmlParser) {
            this.myXmlParser = myXmlParser;
        }

        public int getOther() {
            return other;
        }

        public void setOther(int other) {
            this.other = other;
        }

    }

    @Test(groups = "short")
    public void camel_case_abbreviation_to_camel_case() {
        MapperConfiguration conf = new MapperConfiguration();
        conf.getNamingStrategy()
                .setJavaConvention(new CommonNamingConventions.LowerCamelCase(true))
                .setCassandraConvention(new CommonNamingConventions.LowerCamelCase());
        MappingManager mappingManager = new MappingManager(session(), conf);
        Mapper<CamelCaseAbbreviationToCamelCase> mapper = mappingManager.mapper(CamelCaseAbbreviationToCamelCase.class);
        assertThat(mapper.get(1).getMyXMLParser()).isEqualTo(1);
        assertThat(mapper.get(1).getOther()).isEqualTo(2);
    }

    @Table(name = "camel_case_table")
    public static class CamelCaseAbbreviationToCamelCase {

        @PartitionKey
        private int myXMLParser;

        private int other;

        public int getMyXMLParser() {
            return myXMLParser;
        }

        public void setMyXMLParser(int myXMLParser) {
            this.myXMLParser = myXMLParser;
        }

        public int getOther() {
            return other;
        }

        public void setOther(int other) {
            this.other = other;
        }

    }

    @Test(groups = "short")
    public void snake_case_to_camel_case() {
        MapperConfiguration conf = new MapperConfiguration();
        conf.getNamingStrategy()
                .setJavaConvention(new CommonNamingConventions.LowerSnakeCase())
                .setCassandraConvention(new CommonNamingConventions.LowerCamelCase());
        MappingManager mappingManager = new MappingManager(session(), conf);
        Mapper<SnakeCaseToCamelCase> mapper = mappingManager.mapper(SnakeCaseToCamelCase.class);
        assertThat(mapper.get(1).getMy_xml_parser()).isEqualTo(1);
        assertThat(mapper.get(1).getOther()).isEqualTo(2);
    }

    @Table(name = "camel_case_table")
    public static class SnakeCaseToCamelCase {

        @PartitionKey
        private int my_xml_parser;

        private int other;

        public int getMy_xml_parser() {
            return my_xml_parser;
        }

        public void setMy_xml_parser(int my_xml_parser) {
            this.my_xml_parser = my_xml_parser;
        }

        public int getOther() {
            return other;
        }

        public void setOther(int other) {
            this.other = other;
        }

    }

    @Test(groups = "short")
    public void test_overrides() {
        MapperConfiguration conf = new MapperConfiguration();
        conf.getNamingStrategy()
                .setJavaConvention(new CommonNamingConventions.LowerSnakeCase())
                .setCassandraConvention(new CommonNamingConventions.LowerCamelCase());
        MappingManager mappingManager = new MappingManager(session(), conf);
        Mapper<TestOverrides> mapper = mappingManager.mapper(TestOverrides.class);
        assertThat(mapper.get(1).getMyXmlParser()).isEqualTo(1);
        assertThat(mapper.get(1).getGoo()).isEqualTo(2);
    }

    @Table(name = "camel_case_table")
    public static class TestOverrides {

        @PartitionKey
        @Column(caseSensitive = true)
        private int myXmlParser;

        @Column(name = "other")
        private int goo;

        public int getMyXmlParser() {
            return myXmlParser;
        }

        public void setMyXmlParser(int myXmlParser) {
            this.myXmlParser = myXmlParser;
        }

        public int getGoo() {
            return goo;
        }

        public void setGoo(int goo) {
            this.goo = goo;
        }

    }@Test(groups = "short")

    public void test_recommended_settings() {
        MapperConfiguration conf = new MapperConfiguration();
        conf.getNamingStrategy()
                .setJavaConvention(new CommonNamingConventions.LowerCamelCase("m", "_"))
                .setCassandraConvention(new CommonNamingConventions.LowerSnakeCase());
        MappingManager mappingManager = new MappingManager(session(), conf);
        Mapper<TestRecommendedSettings> mapper = mappingManager.mapper(TestRecommendedSettings.class);
        assertThat(mapper.get(1).getmMyXmlParser()).isEqualTo(1);
        assertThat(mapper.get(1).get_other()).isEqualTo(2);
    }

    @Table(name = "snake_case_table")
    public static class TestRecommendedSettings {

        @PartitionKey
        private int mMyXmlParser;

        private int _other;

        public int getmMyXmlParser() {
            return mMyXmlParser;
        }

        public void setmMyXmlParser(int mMyXmlParser) {
            this.mMyXmlParser = mMyXmlParser;
        }

        public int get_other() {
            return _other;
        }

        public void set_other(int _other) {
            this._other = _other;
        }

    }

}
