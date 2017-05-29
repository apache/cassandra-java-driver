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
package com.datastax.driver.core;

import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This tests that the CodecRegistry is able to create codecs on the fly
 * for nested UDTs and tuples received from a ResultSet, i.e. the first time
 * the registry encounters the UDT or tuple, it's coming from a ResultSet,
 * not from the Cluster's metadata.
 *
 * @jira_ticket JAVA-847
 */
@CassandraVersion("2.1.0")
public class TypeCodecNestedUDTAndTupleIntegrationTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TYPE IF NOT EXISTS \"udt3\" (f3 text)",
                "CREATE TYPE IF NOT EXISTS \"udt2\" (f2 frozen<udt3>)",
                "CREATE TYPE IF NOT EXISTS \"udt1\" (f1 frozen<udt2>)",
                "CREATE TABLE IF NOT EXISTS \"t1\" (pk int PRIMARY KEY, "
                        + "c1 frozen<udt1>, "
                        + "c2 frozen<tuple<tuple<tuple<text>>>>, "
                        + "c3 frozen<tuple<tuple<tuple<udt1>>>>"
                        + ")",
                // it's important to insert values using CQL literals
                // so that the CodecRegistry will not be required until
                // we receive a ResultSet
                "INSERT INTO t1 (pk, c1) VALUES (1, {f1:{f2:{f3:'foo'}}})",
                "INSERT INTO t1 (pk, c2) VALUES (2, ((('foo'))))",
                "INSERT INTO t1 (pk, c3) VALUES (3, ((({f1:{f2:{f3:'foo'}}}))))"
        );
    }

    @Test(groups = "short")
    public void should_set_registry_on_nested_udts() {
        ResultSet rows = session().execute("SELECT c1 FROM t1 WHERE pk = 1");
        Row row = rows.one();
        // here the CodecRegistry will create a codec on-the-fly using the UserType received from the resultset metadata
        UDTValue udt1 = row.getUDTValue("c1");
        assertThat(udt1.getCodecRegistry()).isSameAs(cluster().getConfiguration().getCodecRegistry());
        UDTValue udt2 = udt1.getUDTValue("f1");
        assertThat(udt2.getCodecRegistry()).isSameAs(cluster().getConfiguration().getCodecRegistry());
        UDTValue udt3 = udt2.getUDTValue("f2");
        assertThat(udt3.getCodecRegistry()).isSameAs(cluster().getConfiguration().getCodecRegistry());
        String f3 = udt3.getString("f3");
        assertThat(f3).isEqualTo("foo");
    }

    @Test(groups = "short")
    public void should_set_registry_on_nested_tuples() {
        ResultSet rows = session().execute("SELECT c2 FROM t1 WHERE pk = 2");
        Row row = rows.one();
        // here the CodecRegistry will create a codec on-the-fly using the TupleType received from the resultset metadata
        TupleValue tuple1 = row.getTupleValue("c2");
        assertThat(tuple1.getCodecRegistry()).isSameAs(cluster().getConfiguration().getCodecRegistry());
        TupleValue tuple2 = tuple1.getTupleValue(0);
        assertThat(tuple2.getCodecRegistry()).isSameAs(cluster().getConfiguration().getCodecRegistry());
        TupleValue tuple3 = tuple2.getTupleValue(0);
        assertThat(tuple3.getCodecRegistry()).isSameAs(cluster().getConfiguration().getCodecRegistry());
        String s = tuple3.getString(0);
        assertThat(s).isEqualTo("foo");
    }

    @Test(groups = "short")
    public void should_set_registry_on_nested_tuples_and_udts() {
        ResultSet rows = session().execute("SELECT c3 FROM t1 WHERE pk = 3");
        Row row = rows.one();
        // here the CodecRegistry will create a codec on-the-fly using the TupleType received from the resultset metadata
        TupleValue tuple1 = row.getTupleValue("c3");
        assertThat(tuple1.getCodecRegistry()).isSameAs(cluster().getConfiguration().getCodecRegistry());
        TupleValue tuple2 = tuple1.getTupleValue(0);
        assertThat(tuple2.getCodecRegistry()).isSameAs(cluster().getConfiguration().getCodecRegistry());
        TupleValue tuple3 = tuple2.getTupleValue(0);
        assertThat(tuple3.getCodecRegistry()).isSameAs(cluster().getConfiguration().getCodecRegistry());
        UDTValue udt1 = tuple3.getUDTValue(0);
        assertThat(udt1.getCodecRegistry()).isSameAs(cluster().getConfiguration().getCodecRegistry());
        UDTValue udt2 = udt1.getUDTValue("f1");
        assertThat(udt2.getCodecRegistry()).isSameAs(cluster().getConfiguration().getCodecRegistry());
        UDTValue udt3 = udt2.getUDTValue("f2");
        assertThat(udt3.getCodecRegistry()).isSameAs(cluster().getConfiguration().getCodecRegistry());
        String f3 = udt3.getString("f3");
        assertThat(f3).isEqualTo("foo");
    }


}
