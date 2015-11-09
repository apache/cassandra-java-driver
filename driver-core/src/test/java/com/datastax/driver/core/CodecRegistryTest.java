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

import java.util.List;

import com.google.common.reflect.TypeToken;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.datastax.driver.core.Assertions.assertThat;

public class CodecRegistryTest {

    static final TypeToken<List<Integer>> LIST_OF_INT_TOKEN = new TypeToken<List<Integer>>(){};
    static final DataType.CollectionType LIST_OF_INT_DATATYPE = DataType.list(DataType.cint());

    @Test(groups = "unit")
    public void should_find_codec_by_cql_type() {
        CodecRegistry registry = new CodecRegistry();
        assertThat(registry.codecFor(DataType.cint())).isSameAs(TypeCodec.cint());
        assertThat(registry.codecFor(LIST_OF_INT_DATATYPE)).accepts(LIST_OF_INT_TOKEN);
        assertThat(registry.codecFor(LIST_OF_INT_DATATYPE)).accepts(newArrayList(1, 2 ,3));
    }

    @Test(groups = "unit")
    public void should_find_codec_by_java_type() {
        CodecRegistry registry = new CodecRegistry();
        assertThat(registry.codecFor(Integer.class)).isSameAs(TypeCodec.cint());
        assertThat(registry.codecFor(LIST_OF_INT_TOKEN)).accepts(LIST_OF_INT_DATATYPE);
        assertThat(registry.codecFor(LIST_OF_INT_TOKEN)).accepts(newArrayList(1, 2 ,3));
    }

    @Test(groups = "unit")
    public void should_find_codec_by_value() {
        CodecRegistry registry = new CodecRegistry();
        assertThat(registry.codecFor(Integer.class)).isSameAs(TypeCodec.cint());
        assertThat(registry.codecFor(newArrayList(1, 2 ,3))).accepts(LIST_OF_INT_DATATYPE);
        assertThat(registry.codecFor(newArrayList(1, 2 ,3))).accepts(LIST_OF_INT_TOKEN);
    }

    @Test(groups = "unit")
    public void should_ignore_codec_colliding_with_already_registered_codec() {
        MemoryAppender logs = startCapturingLogs();

        CodecRegistry registry = new CodecRegistry();

        TypeCodec newCodec = mock(TypeCodec.class);
        when(newCodec.getCqlType()).thenReturn(DataType.cint());
        when(newCodec.getJavaType()).thenReturn(TypeToken.of(Integer.class));
        when(newCodec.toString()).thenReturn("newCodec");

        registry.register(newCodec);

        assertThat(logs.getNext()).contains("Ignoring codec newCodec");
        assertThat(
            registry.codecFor(DataType.cint(), Integer.class)
        ).isNotSameAs(newCodec);

        stopCapturingLogs(logs);
    }

    @Test(groups = "unit")
    public void should_ignore_codec_colliding_with_already_generated_codec() {
        MemoryAppender logs = startCapturingLogs();

        CodecRegistry registry = new CodecRegistry();

        // Force generation of a list token from the default token
        registry.codecFor(LIST_OF_INT_DATATYPE, LIST_OF_INT_TOKEN);

        TypeCodec newCodec = mock(TypeCodec.class);
        when(newCodec.getCqlType()).thenReturn(LIST_OF_INT_DATATYPE);
        when(newCodec.getJavaType()).thenReturn(LIST_OF_INT_TOKEN);
        when(newCodec.toString()).thenReturn("newCodec");

        registry.register(newCodec);

        assertThat(logs.getNext()).contains("Ignoring codec newCodec");
        assertThat(
            registry.codecFor(LIST_OF_INT_DATATYPE, LIST_OF_INT_TOKEN)
        ).isNotSameAs(newCodec);

        stopCapturingLogs(logs);
    }

    private MemoryAppender startCapturingLogs() {
        Logger registryLogger = Logger.getLogger(CodecRegistry.class);
        registryLogger.setLevel(Level.WARN);
        MemoryAppender logs = new MemoryAppender();
        registryLogger.addAppender(logs);
        return logs;
    }

    private void stopCapturingLogs(MemoryAppender logs) {
        Logger registryLogger = Logger.getLogger(CodecRegistry.class);
        registryLogger.setLevel(null);
        registryLogger.removeAppender(logs);
    }
}
