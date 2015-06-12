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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.reflect.TypeToken;
import org.testng.annotations.Test;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.reflect.TypeToken.of;

import com.datastax.driver.core.TypeCodec.DoubleCodec;
import com.datastax.driver.core.TypeCodec.ListCodec;
import com.datastax.driver.core.TypeCodec.VarcharCodec;
import com.datastax.driver.core.exceptions.CodecNotFoundException;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.Assertions.fail;
import static com.datastax.driver.core.CodecUtils.listOf;
import static com.datastax.driver.core.CodecUtils.setOf;
import static com.datastax.driver.core.DataType.*;

public class CodecRegistryTest {

    @Test(groups = "unit")
    public void should_retrieve_codec_by_java_and_cql_when_simple_type(){
        CodecRegistry registry = CodecRegistry.builder().withCodecs(VarcharCodec.instance).build();
        assertThat(registry.codecFor(varchar(), String.class)).isSameAs(VarcharCodec.instance);
        assertThat(registry.codecFor(text(), String.class)).isSameAs(VarcharCodec.instance); // just an alias
    }

    @Test(groups = "unit")
    public void should_retrieve_codec_by_java_and_cql_when_parameterized_type(){
        ListCodec<Double> codec = new ListCodec<Double>(DoubleCodec.instance);
        CodecRegistry registry = CodecRegistry.builder().withCodecs(codec).build();
        assertThat(registry.codecFor(list(cdouble()), listOf(Double.class))).isSameAs(codec);
    }

    @Test(groups = "unit")
    public void should_override_codec_when_overlapping_scope(){
        TypeCodec<?> codec1 = new FakeCodec<Integer>(bigint(), of(Integer.class));
        TypeCodec<?> codec2 = new FakeCodec<Integer>(cint(), of(Integer.class));
        TypeCodec<?> codec3 = new FakeCodec<Integer>(cint(), of(Integer.class));
        TypeCodec<?> codec4 = new FakeCodec<BigInteger>(cint(), of(BigInteger.class));
        CodecRegistry registry = CodecRegistry.builder().withCodecs(codec1, codec2, codec3, codec4).build();
        assertThat(registry.codecFor(cint())).isSameAs(codec2); // first wins
        assertThat(registry.codecFor(1234)).isSameAs(codec1); // first wins
        assertThat(registry.codecFor(cint(), of(Integer.class))).isSameAs(codec2); // first exact match wins
        assertThat(registry.codecFor(cint(), of(BigInteger.class))).isSameAs(codec4); // only matching codec
    }

    @Test(groups = "unit")
    public void should_override_codec_per_column(){
        TypeCodec<?> codec1 = new FakeCodec<Integer>(cint(), of(Integer.class));
        TypeCodec<?> codec2 = new FakeCodec<Integer>(cint(), of(Integer.class));
        CodecRegistry registry = CodecRegistry.builder()
            .withCodecs(codec1)
            .withOverridingCodec("myKeyspace", "myTable", "myColumn", codec2)
            .build();
        assertThat(registry.codecFor(cint(), of(Integer.class))).isSameAs(codec1);
        assertThat(registry.codecFor(cint(), of(Integer.class), "myKeyspace", "myTable", "myColumn")).isSameAs(codec2);
    }

    @Test(groups = "unit")
    public void test_raw_type_covariance(){
        CodecRegistry registry = CodecRegistry.builder().withCodecs(new NumberCodec()).build();
        assertThat(registry.codecFor(decimal())).isInstanceOf(NumberCodec.class);
        assertThat(registry.codecFor(decimal(), Integer.class)).isInstanceOf(NumberCodec.class);
        assertThat(registry.codecFor(1234)).isInstanceOf(NumberCodec.class);
    }

    @Test(groups = "unit", expectedExceptions = CodecNotFoundException.class)
    public void test_parameterized_type_covariance(){
        // List<? extends Number>
        TypeCodec<?> codec = new AnyNumberListCodec();
        CodecRegistry registry = CodecRegistry.builder().withCodecs(codec).build();
        assertThat(registry.codecFor(list(decimal()))).isSameAs(codec);
        assertThat(registry.codecFor(list(decimal()), listOf(Integer.class))).isSameAs(codec); // List<Integer> instance of List<? extends Number>
        // List<Number>
        codec = new NumberListCodec();
        registry = CodecRegistry.builder().withCodecs(codec).build();
        assertThat(registry.codecFor(list(decimal()))).isSameAs(codec);
        registry.codecFor(list(decimal()), listOf(Integer.class)); // List<Integer> not instance of List<Number> -> CodecNotFoundException
    }

    @Test(groups = "unit", expectedExceptions = CodecNotFoundException.class)
    public void test_interface_vs_implementation(){
        // List<String>
        TypeCodec<?> codec = new ListCodec<String>(VarcharCodec.instance);
        CodecRegistry registry = CodecRegistry.builder().withCodecs(codec).build();
        assertThat(registry.codecFor(list(text()))).isSameAs(codec);
        assertThat(registry.codecFor(newArrayList("a", "b", "c"))).isSameAs(codec);
        assertThat(registry.codecFor(list(text()), new TypeToken<ArrayList<String>>(){})).isSameAs(codec); // ArrayList<String> instance of List<String>
        // ArrayList<String>
        codec = new ArrayListOfStringsCodec();
        registry = CodecRegistry.builder().withCodecs(codec).build();
        assertThat(registry.codecFor(list(text()))).isSameAs(codec);
        assertThat(registry.codecFor(newArrayList("a", "b", "c"))).isSameAs(codec);
        registry.codecFor(list(text()), listOf(String.class)); // List<String> not instance of ArrayList<String>
    }

    @Test(groups = "unit", expectedExceptions = CodecNotFoundException.class)
    public void native_to_collection(){
        TypeCodec<?> codec = new TextToListOfStringsCodec();
        CodecRegistry registry = CodecRegistry.builder().withCodecs(codec).build();
        assertThat(registry.codecFor(text())).isSameAs(codec);
        assertThat(registry.codecFor(text(), listOf(String.class))).isSameAs(codec);
        assertThat(registry.codecFor(newArrayList("a", "b", "c"))).isSameAs(codec);
    }

    @Test(groups = "unit")
    public void collection_to_native(){
        TypeCodec<?> codec = new ListOfIntsToStringCodec();
        CodecRegistry registry = CodecRegistry.builder().withCodecs(codec).build();
        assertThat(registry.codecFor(list(cint()))).isSameAs(codec);
        assertThat(registry.codecFor(list(cint()), String.class)).isSameAs(codec);
        assertThat(registry.codecFor("123")).isSameAs(codec);
    }

    @Test(groups = "unit")
    public void collection_to_nested_collection(){
        TypeCodec<?> codec = new ListOfVarcharsToSetOfListOfIntegersCodec();
        CodecRegistry registry = CodecRegistry.builder().withCodecs(codec).build();
        assertThat(registry.codecFor(list(varchar()))).isSameAs(codec);
        assertThat(registry.codecFor(list(varchar()), setOf(listOf(Integer.class)))).isSameAs(codec);
        try {
            registry.codecFor(list(varchar()), listOf(listOf(Integer.class)));
            fail("should not have found codec for List<List<Integer>");
        } catch (CodecNotFoundException e) {
            //ok
        }
        try {
            assertThat(registry.codecFor(newArrayList(newArrayList(1, 2, 3)))).isSameAs(codec);
            fail("should not have found codec for [[1,2,3]]");
        } catch (CodecNotFoundException e) {
            //ok
        }
    }

    private class FakeCodec<T> extends TypeCodec<T> {

        protected FakeCodec(DataType cqlType, TypeToken<T> javaType) {
            super(cqlType, javaType);
        }

        @Override
        public ByteBuffer serialize(T value) {
            return null;
        }

        @Override
        public T deserialize(ByteBuffer bytes) {
            return null;
        }

        @Override
        public T parse(String value) {
            return null;
        }

        @Override
        public String format(T value) {
            return null;
        }
    }

    private class ArrayListOfStringsCodec extends TypeCodec.CollectionCodec<String, ArrayList<String>> {

        public ArrayListOfStringsCodec() {
            super(list(text()), new TypeToken<ArrayList<String>>(){}, VarcharCodec.instance);
        }

        @Override
        protected ArrayList<String> newInstance(int capacity) {
            return new ArrayList<String>(capacity);
        }

        @Override
        protected char getOpeningChar() {
            return '[';
        }

        @Override
        protected char getClosingChar() {
            return ']';
        }

    }

    private class NumberCodec extends FakeCodec<Number> {

        protected NumberCodec() {
            super(decimal(), of(Number.class));
        }

    }

    private class TextToListOfStringsCodec extends FakeCodec<List<String>> {

        protected TextToListOfStringsCodec() {
            super(text(), listOf(String.class));
        }

    }

    private class ListOfIntsToStringCodec extends FakeCodec<String> {

        protected ListOfIntsToStringCodec() {
            super(list(cint()), of(String.class));
        }

    }

    private class NumberListCodec extends ListCodec<Number> {

        protected NumberListCodec() {
            super(new NumberCodec());
        }

    }

    private class AnyNumberListCodec extends FakeCodec<List<? extends Number>> {

        protected AnyNumberListCodec() {
            super(list(decimal()), new TypeToken<List<? extends Number>>(){});
        }

    }

    private class ListOfVarcharsToSetOfListOfIntegersCodec extends FakeCodec<Set<List<Integer>>> {

        protected ListOfVarcharsToSetOfListOfIntegersCodec() {
            super(list(varchar()), setOf(listOf(Integer.class)));
        }

    }

}
