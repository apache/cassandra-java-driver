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
package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import com.google.common.reflect.TypeToken;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.datastax.driver.core.CodecFactory.DefaultCodecFactory;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.list;

public class TypeCodecOverridingIntegrationTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final String query = "INSERT INTO \"myTable\" (c_int, l_int) VALUES (?, ?)";

    private CodecRegistry registry;

    private MockIntCodec codec;

    private DefaultCodecFactory factory;

    private TypeCodec.ListCodec<Integer> listCodec;

    private PreparedStatement ps;

    private ProtocolVersion protocolVersion;

    @Override
    protected Collection<String> getTableDefinitions() {
        return newArrayList(
            "CREATE TABLE \"myTable\" ("
                + "c_int int PRIMARY KEY, "
                + "l_int list<int> "
                + ")"
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Cluster.Builder configure(Cluster.Builder builder) {
        codec = new MockIntCodec();
        listCodec = spy(new TypeCodec.ListCodec<Integer>(codec));
        factory = spy((DefaultCodecFactory)DefaultCodecFactory.DEFAULT_INSTANCE);
        registry = new CodecRegistry(factory).register(codec);
        return builder.withCodecRegistry(registry);
    }

    @BeforeClass(groups = "short")
    public void recordProtocolVersion(){
        protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    }

    @BeforeMethod(groups = "short")
    public void prepareStatements() {
        ps = session.prepare(query);
    }

    @BeforeMethod(groups = "short")
    public void prepareMocks() {
        doReturn(listCodec).when(factory).newListCodec(codec);
    }

    @AfterMethod(groups = "short", alwaysRun = true)
    @SuppressWarnings("unchecked")
    public void resetMocks() {
        codec.reset();
        reset(factory);
        reset(listCodec);
    }

    @Test(groups = "short")
    public void should_use_overriding_codecs_with_simple_statements() {
        session.execute(query,
            42,
            newArrayList(42)
        );
        assertMocksInvoked();
    }

    @Test(groups = "short")
    public void should_use_overriding_codecs_with_prepared_statements_1() {
        session.execute(
            ps.bind()
                .setInt(0, 42)
                .setList(1, newArrayList(42))
        );
        assertMocksInvoked();
    }

    @Test(groups = "short")
    public void should_use_overriding_codecs_with_prepared_statements_2() {
        session.execute(
            ps.bind()
                .setObject(0, 42)
                .setObject(1, newArrayList(42))
        );
        assertMocksInvoked();
    }

    private void assertMocksInvoked() {
        assertThat(codec.actualValue).isEqualTo(42);
        assertThat(registry.codecFor(cint(), TypeToken.of(Integer.class))).isSameAs(codec);
        assertThat(registry.codecFor(list(cint()), new TypeToken<List<Integer>>(){})).isSameAs(listCodec);
        verify(listCodec).serialize(newArrayList(42), protocolVersion);
        verify(factory).newListCodec(codec);
    }

    // can't spy or mock this as we need the original equals() method to be invoked
    private class MockIntCodec extends TypeCodec<Integer> implements TypeCodec.PrimitiveIntCodec {

        private Integer actualValue = null;

        private MockIntCodec() {
            super(cint(), Integer.class);
        }

        private void reset() {
            actualValue = null;
        }

        @Override
        public ByteBuffer serializeNoBoxing(int v, ProtocolVersion protocolVersion) {
            actualValue = v;
            return IntCodec.instance.serializeNoBoxing(v, protocolVersion);
        }

        @Override
        public int deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion) {
            return IntCodec.instance.deserializeNoBoxing(v, protocolVersion);
        }

        @Override
        public ByteBuffer serialize(Integer value, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public Integer deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
            return deserializeNoBoxing(bytes, protocolVersion);
        }

        @Override
        public Integer parse(String value) throws InvalidTypeException {
            return IntCodec.instance.parse(value);
        }

        @Override
        public String format(Integer value) throws InvalidTypeException {
            return IntCodec.instance.format(value);
        }
    }

}
