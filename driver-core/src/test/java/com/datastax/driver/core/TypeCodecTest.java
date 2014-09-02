/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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
import java.util.Map;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.base.Strings;

import org.testng.Assert;
import org.testng.annotations.Test;

import static com.datastax.driver.core.DataType.text;

public class TypeCodecTest {

    public static final DataType CUSTOM_FOO = DataType.custom("com.example.FooBar");

    @Test(groups = "unit")
    public void testCustomList() throws Exception {
        TypeCodec<?> listType = TypeCodec.listOf(CUSTOM_FOO, ProtocolVersion.V2);
        Assert.assertNotNull(listType);
    }

    @Test(groups = "unit")
    public void testCustomSet() throws Exception {
        TypeCodec<?> setType = TypeCodec.setOf(CUSTOM_FOO, ProtocolVersion.V2);
        Assert.assertNotNull(setType);
    }

    @Test(groups = "unit")
    public void testCustomKeyMap() throws Exception {
        TypeCodec<Map<ByteBuffer, String>> mapType = TypeCodec.mapOf(CUSTOM_FOO, text(), ProtocolVersion.V2);
        Assert.assertNotNull(mapType);
    }

    @Test(groups = "unit")
    public void testCustomValueMap() throws Exception {
        TypeCodec<Map<String, ByteBuffer>> mapType = TypeCodec.mapOf(text(), CUSTOM_FOO, ProtocolVersion.V2);
        Assert.assertNotNull(mapType);
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void collectionTooLargeTest() throws Exception {
        TypeCodec<List<Integer>> listType = TypeCodec.listOf(DataType.cint(), 2);
        List<Integer> list = Collections.nCopies(65536, 1);

        listType.serialize(list);
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void collectionElementTooLargeTest() throws Exception {
        TypeCodec<List<String>> listType = TypeCodec.listOf(DataType.text(), 2);
        List<String> list = Lists.newArrayList(Strings.repeat("a", 65536));

        listType.serialize(list);
    }
}
