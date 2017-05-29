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

import java.util.UUID;

import static com.datastax.driver.core.DataType.cfloat;
import static org.assertj.core.api.Assertions.assertThat;

@CassandraVersion("2.1.0")
public class TypeCodecTupleIntegrationTest extends CCMTestsSupport {

    private final String insertQuery = "INSERT INTO users (id, name, location) VALUES (?, ?, ?)";
    private final String selectQuery = "SELECT id, name, location FROM users WHERE id = ?";

    private final UUID uuid = UUID.randomUUID();

    private TupleType locationType;
    private TupleValue locationValue;
    private TupleValue partialLocationValueInserted;
    private Location location;
    private Location partialLocation;
    private TupleValue partialLocationValueRetrieved;

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TABLE IF NOT EXISTS \"users\" (id uuid PRIMARY KEY, name text, location frozen<tuple<float,float>>)"
        );
    }

    @Test(groups = "short")
    public void should_handle_tuples_with_default_codecs() {
        setUpTupleTypes(cluster());
        // simple statement
        session().execute(insertQuery, uuid, "John Doe", locationValue);
        ResultSet rows = session().execute(selectQuery, uuid);
        Row row = rows.one();
        assertRow(row);
        // prepared + values
        PreparedStatement ps = session().prepare(insertQuery);
        session().execute(ps.bind(uuid, "John Doe", locationValue));
        rows = session().execute(selectQuery, uuid);
        row = rows.one();
        assertRow(row);
        // bound with setTupleValue
        session().execute(ps.bind().setUUID(0, uuid).setString(1, "John Doe").setTupleValue("location", locationValue));
        rows = session().execute(selectQuery, uuid);
        row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_handle_partial_tuples_with_default_codecs() {
        setUpTupleTypes(cluster());
        // simple statement
        session().execute(insertQuery, uuid, "John Doe", partialLocationValueInserted);
        ResultSet rows = session().execute(selectQuery, uuid);
        Row row = rows.one();
        assertPartialRow(row);
        // prepared + values
        PreparedStatement ps = session().prepare(insertQuery);
        session().execute(ps.bind(uuid, "John Doe", partialLocationValueInserted));
        rows = session().execute(selectQuery, uuid);
        row = rows.one();
        assertPartialRow(row);
        // bound with setTupleValue
        session().execute(ps.bind().setUUID(0, uuid).setString(1, "John Doe").setTupleValue("location", partialLocationValueInserted));
        rows = session().execute(selectQuery, uuid);
        row = rows.one();
        assertPartialRow(row);
    }

    @Test(groups = "short")
    public void should_handle_tuples_with_custom_codecs() {
        CodecRegistry codecRegistry = new CodecRegistry();
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withCodecRegistry(codecRegistry)
                .build());
        Session session = cluster.connect(keyspace);
        setUpTupleTypes(cluster);
        codecRegistry.register(new LocationCodec(TypeCodec.tuple(locationType)));
        session.execute(insertQuery, uuid, "John Doe", locationValue);
        ResultSet rows = session.execute(selectQuery, uuid);
        Row row = rows.one();
        assertThat(row.getUUID(0)).isEqualTo(uuid);
        assertThat(row.getObject(0)).isEqualTo(uuid);
        assertThat(row.get(0, UUID.class)).isEqualTo(uuid);
        assertThat(row.getString(1)).isEqualTo("John Doe");
        assertThat(row.getObject(1)).isEqualTo("John Doe");
        assertThat(row.get(1, String.class)).isEqualTo("John Doe");
        assertThat(row.getTupleValue(2)).isEqualTo(locationValue);
        // edge case: getObject should use default codecs;
        // but tuple and udt codecs are registered on the fly;
        // so if we have another manually-registered codec
        // that one will be picked up
        assertThat(row.getObject(2)).isEqualTo(location);
        assertThat(row.get(2, TupleValue.class)).isEqualTo(locationValue);
        assertThat(row.get(2, Location.class)).isEqualTo(location);
    }

    @Test(groups = "short")
    public void should_handle_partial_tuples_with_custom_codecs() {
        CodecRegistry codecRegistry = new CodecRegistry();
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withCodecRegistry(codecRegistry)
                .build());
        Session session = cluster.connect(keyspace);
        setUpTupleTypes(cluster);
        codecRegistry.register(new LocationCodec(TypeCodec.tuple(locationType)));
        session.execute(insertQuery, uuid, "John Doe", partialLocationValueInserted);
        ResultSet rows = session.execute(selectQuery, uuid);
        Row row = rows.one();
        assertThat(row.getUUID(0)).isEqualTo(uuid);
        assertThat(row.getObject(0)).isEqualTo(uuid);
        assertThat(row.get(0, UUID.class)).isEqualTo(uuid);
        assertThat(row.getString(1)).isEqualTo("John Doe");
        assertThat(row.getObject(1)).isEqualTo("John Doe");
        assertThat(row.get(1, String.class)).isEqualTo("John Doe");
        assertThat(row.getTupleValue(2)).isEqualTo(locationType.newValue(37.387224f, null));
        // corner case: getObject should use default codecs;
        // but tuple and udt codecs are registered on the fly;
        // so if we have another manually-registered codec
        // that one will be picked up :(
        assertThat(row.getObject(2)).isEqualTo(partialLocation);
        assertThat(row.get(2, TupleValue.class)).isEqualTo(locationType.newValue(37.387224f, null));
        assertThat(row.get(2, Location.class)).isEqualTo(partialLocation);
    }

    private void assertRow(Row row) {
        assertThat(row.getUUID(0)).isEqualTo(uuid);
        assertThat(row.getObject(0)).isEqualTo(uuid);
        assertThat(row.get(0, UUID.class)).isEqualTo(uuid);
        assertThat(row.getString(1)).isEqualTo("John Doe");
        assertThat(row.getObject(1)).isEqualTo("John Doe");
        assertThat(row.get(1, String.class)).isEqualTo("John Doe");
        assertThat(row.getTupleValue(2)).isEqualTo(locationValue);
        assertThat(row.getObject(2)).isEqualTo(locationValue);
        assertThat(row.get(2, TupleValue.class)).isEqualTo(locationValue);
    }

    private void assertPartialRow(Row row) {
        assertThat(row.getUUID(0)).isEqualTo(uuid);
        assertThat(row.getObject(0)).isEqualTo(uuid);
        assertThat(row.get(0, UUID.class)).isEqualTo(uuid);
        assertThat(row.getString(1)).isEqualTo("John Doe");
        assertThat(row.getObject(1)).isEqualTo("John Doe");
        assertThat(row.get(1, String.class)).isEqualTo("John Doe");
        assertThat(row.getTupleValue(2)).isEqualTo(partialLocationValueRetrieved);
        assertThat(row.getObject(2)).isEqualTo(partialLocationValueRetrieved);
        assertThat(row.get(2, TupleValue.class)).isEqualTo(partialLocationValueRetrieved);
    }

    private void setUpTupleTypes(Cluster cluster) {
        locationType = cluster.getMetadata().newTupleType(cfloat(), cfloat());
        locationValue = locationType.newValue()
                .setFloat(0, 37.387224f)
                .setFloat(1, -121.9733837f);
        // insert a tuple of a different dimension
        partialLocationValueInserted = cluster.getMetadata().newTupleType(cfloat()).newValue().setFloat(0, 37.387224f);
        // retrieve the partial tuple with null missing values
        partialLocationValueRetrieved = locationType.newValue(37.387224f, null);
        location = new Location(37.387224f, -121.9733837f);
        partialLocation = new Location(37.387224f, 0.0f);
    }

    static class LocationCodec extends MappingCodec<Location, TupleValue> {

        private final TupleType tupleType;

        public LocationCodec(TypeCodec<TupleValue> innerCodec) {
            super(innerCodec, Location.class);
            tupleType = (TupleType) innerCodec.getCqlType();
        }

        @Override
        protected Location deserialize(TupleValue value) {
            return value == null ? null : new Location(value.getFloat(0), value.getFloat(1));
        }

        @Override
        protected TupleValue serialize(Location value) {
            return value == null ? null : tupleType.newValue().setFloat(0, value.latitude).setFloat(1, value.longitude);
        }
    }

    static class Location {

        float latitude;

        float longitude;

        public Location(float latitude, float longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Location location = (Location) o;

            return Float.compare(location.latitude, latitude) == 0 && Float.compare(location.longitude, longitude) == 0;

        }

        @Override
        public int hashCode() {
            int result = (latitude != +0.0f ? Float.floatToIntBits(latitude) : 0);
            result = 31 * result + (longitude != +0.0f ? Float.floatToIntBits(longitude) : 0);
            return result;
        }

        @Override
        public String toString() {
            return "[" + latitude + ", " + longitude + "]";
        }
    }

}
