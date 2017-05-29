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
package com.datastax.driver.mapping;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.annotations.*;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

@SuppressWarnings({"unused", "WeakerAccess"})
public class MapperInvalidAnnotationsTest {

    MappingManager mappingManager;
    MappingConfiguration mappingConfiguration;

    @BeforeClass(groups = "unit")
    public void setup() {
        mappingManager = mock(MappingManager.class);
        mappingConfiguration = MappingConfiguration.builder().build();
        Session session = mock(Session.class);
        when(mappingManager.getSession()).thenReturn(session);
        when(mappingManager.getConfiguration()).thenReturn(mappingConfiguration);
        Cluster cluster = mock(Cluster.class);
        when(session.getCluster()).thenReturn(cluster);
        Metadata metadata = mock(Metadata.class);
        when(cluster.getMetadata()).thenReturn(metadata);
        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        when(metadata.getKeyspace(anyString())).thenReturn(keyspace);

        UserType userType = mock(UserType.class);
        when(keyspace.getUserType(anyString())).thenReturn(userType);
        when(userType.contains(anyString())).thenReturn(true);

        TableMetadata table = mock(TableMetadata.class);
        when(keyspace.getTable(anyString())).thenReturn(table);
        ColumnMetadata column = mock(ColumnMetadata.class);
        when(table.getColumn(anyString())).thenReturn(column);
    }

    static class Invalid1 {
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "@Table annotation was not found on class " +
                            "com.datastax.driver.mapping.MapperInvalidAnnotationsTest\\$Invalid1")
    public void should_throw_IAE_when_Table_annotation_not_found_on_entity_class() throws Exception {
        AnnotationParser.parseEntity(Invalid1.class, mappingManager);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "@UDT annotation was not found on class " +
                            "com.datastax.driver.mapping.MapperInvalidAnnotationsTest\\$Invalid1")
    public void should_throw_IAE_when_UDT_annotation_not_found_on_udt_class() throws Exception {
        AnnotationParser.parseUDT(Invalid1.class, mappingManager);
    }

    @Table(name = "foo")
    @UDT(name = "foo")
    static class Invalid2 {
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "Cannot have both @Table and @UDT on class " +
                            "com.datastax.driver.mapping.MapperInvalidAnnotationsTest\\$Invalid2")
    public void should_throw_IAE_when_UDT_annotation_found_on_entity_class() throws Exception {
        AnnotationParser.parseEntity(Invalid2.class, mappingManager);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "Cannot have both @UDT and @Table on class " +
                            "com.datastax.driver.mapping.MapperInvalidAnnotationsTest\\$Invalid2")
    public void should_throw_IAE_when_Table_annotation_found_on_udt_class() throws Exception {
        AnnotationParser.parseUDT(Invalid2.class, mappingManager);
    }

    @Table(name = "foo")
    @Accessor
    static class Invalid3 {
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "Cannot have both @Table and @Accessor on class " +
                            "com.datastax.driver.mapping.MapperInvalidAnnotationsTest\\$Invalid3")
    public void should_throw_IAE_when_Accessor_annotation_found_on_entity_class() throws Exception {
        AnnotationParser.parseEntity(Invalid3.class, mappingManager);
    }

    @UDT(name = "foo")
    @Accessor
    static class Invalid4 {
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "Cannot have both @UDT and @Accessor on class " +
                            "com.datastax.driver.mapping.MapperInvalidAnnotationsTest\\$Invalid4")
    public void should_throw_IAE_when_Accessor_annotation_found_on_udt_class() throws Exception {
        AnnotationParser.parseUDT(Invalid4.class, mappingManager);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "@Accessor annotation is only allowed on interfaces, got class " +
                            "com.datastax.driver.mapping.MapperInvalidAnnotationsTest\\$Invalid4")
    public void should_throw_IAE_when_Accessor_annotation_found_on_concrete_class() throws Exception {
        AnnotationParser.parseAccessor(Invalid4.class, mappingManager);
    }

    interface Invalid5 {
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "@Accessor annotation was not found on interface " +
                            "com.datastax.driver.mapping.MapperInvalidAnnotationsTest\\$Invalid5")
    public void should_throw_IAE_when_Accessor_annotation_not_found_on_accessor_class() throws Exception {
        AnnotationParser.parseAccessor(Invalid5.class, mappingManager);
    }

    @Table(name = "foo")
    @Accessor
    interface Invalid6 {
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "Cannot have both @Accessor and @Table on interface " +
                            "com.datastax.driver.mapping.MapperInvalidAnnotationsTest\\$Invalid6")
    public void should_throw_IAE_when_Table_annotation_found_on_accessor_class() throws Exception {
        AnnotationParser.parseAccessor(Invalid6.class, mappingManager);
    }

    @UDT(name = "foo")
    @Accessor
    interface Invalid7 {
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "Cannot have both @Accessor and @UDT on interface " +
                            "com.datastax.driver.mapping.MapperInvalidAnnotationsTest\\$Invalid7")
    public void should_throw_IAE_when_UDT_annotation_found_on_accessor_class() throws Exception {
        AnnotationParser.parseAccessor(Invalid7.class, mappingManager);
    }

    @Table(name = "foo", keyspace = "ks")
    static class Invalid8 {
        @Field
        int invalid;
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "Annotation @Field is not allowed on property 'invalid'")
    public void should_throw_IAE_when_Field_annotation_found_on_entity_class_field() throws Exception {
        AnnotationParser.parseEntity(Invalid8.class, mappingManager);
    }

    @UDT(name = "foo", keyspace = "ks")
    static class Invalid9 {

        int invalid;

        @Column
        public int getInvalid() {
            return invalid;
        }
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "Annotation @Column is not allowed on property 'invalid'")
    public void should_throw_IAE_when_Column_annotation_found_on_udt_class_field() throws Exception {
        AnnotationParser.parseUDT(Invalid9.class, mappingManager);
    }

    @Table(name = "foo", keyspace = "ks")
    static class Invalid10 {

        @PartitionKey
        @ClusteringColumn
        int invalid;

    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "Property 'invalid' cannot be annotated with both @PartitionKey and @ClusteringColumn")
    public void should_throw_IAE_when_PartitionKey_and_ClusteringColumn_on_same_property() throws Exception {
        AnnotationParser.parseEntity(Invalid10.class, mappingManager);
    }

    @Table(name = "foo", keyspace = "ks")
    static class Invalid11 {

        @Computed("foo")
        @Column
        int invalid;

    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "Property 'invalid' cannot be annotated with both @Column and @Computed")
    public void should_throw_IAE_when_Computed_and_Column_on_same_property() throws Exception {
        AnnotationParser.parseEntity(Invalid11.class, mappingManager);
    }

    @Table(name = "foo", keyspace = "ks")
    static class Invalid12 {

        @Computed("")
        int invalid;

    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "Property 'invalid': attribute 'value' of annotation @Computed is mandatory for computed properties")
    public void should_throw_IAE_when_Computed_with_empty_value() throws Exception {
        AnnotationParser.parseEntity(Invalid12.class, mappingManager);
    }

    @Table(name = "foo", keyspace = "ks")
    static class Invalid13 {

        @PartitionKey(-1)
        int invalid;

    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "Invalid ordering value -1 for annotation @PartitionKey of property 'invalid', was expecting 0")
    public void should_throw_IAE_when_PartitionKey_with_wrong_order() throws Exception {
        AnnotationParser.parseEntity(Invalid13.class, mappingManager);
    }

    @Table(name = "foo", keyspace = "ks")
    static class Invalid14 {

        public void setNotReadable(int i) {
        }

    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "Property 'notReadable' is not readable")
    public void should_throw_IAE_when_unreadable_property() throws Exception {
        AnnotationParser.parseEntity(Invalid14.class, mappingManager);
    }

    @Table(name = "foo", keyspace = "ks")
    static class Invalid15 {

        public int getNotWritable() {
            return 0;
        }

    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp =
                    "Property 'notWritable' is not writable")
    public void should_throw_IAE_when_unwritable_property() throws Exception {
        AnnotationParser.parseEntity(Invalid15.class, mappingManager);
    }

}
