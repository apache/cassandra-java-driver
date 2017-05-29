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
package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

@CassandraVersion("2.1.0")
public class QueryBuilder21ExecutionTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        // Taken from http://www.datastax.com/dev/blog/cql-in-2-1
        execute(
                "CREATE TABLE products (id int PRIMARY KEY, description text, price int, categories set<text>, buyers list<int>, features_keys map<text, text>, features_values map<text, text>)",
                "CREATE INDEX cat_index ON products(categories)",
                "CREATE INDEX buyers_index ON products(buyers)",
                "CREATE INDEX feat_index ON products(features_values)",
                "CREATE INDEX feat_key_index ON products(KEYS(features_keys))",
                "INSERT INTO products(id, description, price, categories, buyers, features_keys, features_values) " +
                        "VALUES (34134, '120-inch 1080p 3D plasma TV', 9999, {'tv', '3D', 'hdtv'}, [1], {'screen' : '120-inch', 'refresh-rate' : '400hz', 'techno' : 'plasma'}, {'screen' : '120-inch', 'refresh-rate' : '400hz', 'techno' : 'plasma'})",
                "INSERT INTO products(id, description, price, categories, buyers, features_keys, features_values) " +
                        "VALUES (29412, '32-inch LED HDTV (black)', 929, {'tv', 'hdtv'}, [1,2,3], {'screen' : '32-inch', 'techno' : 'LED'}, {'screen' : '32-inch', 'techno' : 'LED'})",
                "INSERT INTO products(id, description, price, categories, buyers, features_keys, features_values) " +
                        "VALUES (38471, '32-inch LCD TV', 110, {'tv', 'used'}, [2,4], {'screen' : '32-inch', 'techno' : 'LCD'}, {'screen' : '32-inch', 'techno' : 'LCD'})"
        );
    }

    @Test(groups = "short")
    public void should_handle_contains_on_set_with_index() {
        PreparedStatement byCategory = session().prepare(select("id", "description", "categories")
                .from("products")
                .where(contains("categories", bindMarker("category"))));

        ResultSet results = session().execute(byCategory.bind().setString("category", "hdtv"));

        assertThat(results.getAvailableWithoutFetching()).isEqualTo(2);
        for (Row row : results) {
            assertThat(row.getSet("categories", String.class)).contains("hdtv");
        }
    }

    @Test(groups = "short")
    public void should_handle_contains_on_list_with_index() {
        PreparedStatement byBuyer = session().prepare(select("id", "description", "buyers")
                .from("products")
                .where(contains("buyers", bindMarker("buyer"))));

        ResultSet results = session().execute(byBuyer.bind().setInt("buyer", 4));

        Row row = results.one();
        assertThat(row).isNotNull();
        assertThat(row.getInt("id")).isEqualTo(38471);
        assertThat(row.getList("buyers", Integer.class)).contains(4);
    }

    @Test(groups = "short")
    public void should_handle_contains_on_map_with_index() {
        PreparedStatement byFeatures = session().prepare(select("id", "description", "features_values")
                .from("products")
                .where(contains("features_values", bindMarker("feature"))));

        ResultSet results = session().execute(byFeatures.bind().setString("feature", "LED"));

        Row row = results.one();
        assertThat(row).isNotNull();
        assertThat(row.getInt("id")).isEqualTo(29412);
        assertThat(row.getMap("features_values", String.class, String.class)).containsEntry("techno", "LED");
    }


    @Test(groups = "short")
    public void should_handle_contains_key_on_map_with_index() {
        PreparedStatement byFeatures = session().prepare(select("id", "description", "features_keys")
                .from("products")
                .where(containsKey("features_keys", bindMarker("feature"))));

        ResultSet results = session().execute(byFeatures.bind().setString("feature", "refresh-rate"));

        Row row = results.one();
        assertThat(row).isNotNull();
        assertThat(row.getInt("id")).isEqualTo(34134);
        assertThat(row.getMap("features_keys", String.class, String.class)).containsEntry("refresh-rate", "400hz");
    }

}
