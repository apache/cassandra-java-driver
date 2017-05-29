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

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.io.Closeables;
import org.objectweb.asm.*;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;

import static org.testng.Assert.assertEquals;

@SuppressWarnings("unused")
public class SyntheticFieldsMapperTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE synthetic_fields (id int PRIMARY KEY)");
    }

    /**
     * Test that synthetic fields are ignored by the {@code AnnotationParser} (JAVA-465).
     * <p/>
     * This test creates a modified version of this class where {@code futureSynthetic} is marked as synthetic, and
     * therefore ignored by the {@code AnnotationParser}. If the test throws an error "Cannot find matching getter and
     * setter for field 'futureSynthetic'", it means that the field is not ignored by the mapped. However,
     * if it succeed, we know that the field was ignored and that the fix does the right job.
     * <p/>
     * If the class {@code ClassWithSyntheticField} was used as-is, the {@code Mapper} would complain that there are
     * no getter and setter for the field {@code futureSynthetic}.
     * <p/>
     * Note that we could also have used not-static inner classes, but the {@code Mapper} is never able to instantiate
     * those as they require a reference to their enclosing class that is not provided at instantiation time.
     */
    @Test(groups = "short")
    public void should_ignore_synthetic_fields() {
        Class<?> classWithSyntheticFields = makeTestClassWithSyntheticFields();
        Object instance = instantiateNewClass(classWithSyntheticFields, 42);

        // Here we cannot use ClassWithSyntheticField since it is not the one the source file was compiled against
        // Instead, it is a different Class (identity + ClassLoader), a modified version of ClassWithSyntheticField
        // Nice ClassCastException will be thrown if we try to cast it to ClassWithSyntheticField
        @SuppressWarnings("unchecked")
        Mapper<Object> m = (Mapper<Object>) new MappingManager(session()).mapper(classWithSyntheticFields);
        m.save(instance);

        assertEquals(m.get(42), instance);
    }

    private Class<?> makeTestClassWithSyntheticFields() {
        InputStream stream = null;
        try {
            // Get class bytes
            Class<ClassWithSyntheticField> c = ClassWithSyntheticField.class;
            String classAsPath = c.getName().replace('.', '/') + ".class";
            stream = c.getClassLoader().getResourceAsStream(classAsPath);

            // Make "futureSynthetic" field actually synthetic
            ClassWriter cw = new ClassWriter(0);
            ClassVisitor cv = new SyntheticFieldCreator(Opcodes.ASM5, cw);
            ClassReader cr = new ClassReader(stream);
            cr.accept(cv, 0);
            byte[] updatedClassBytes = cw.toByteArray();

            // Build the new class
            return new InterceptingClassLoader().defineClass(
                    ClassWithSyntheticField.class.getName(),
                    updatedClassBytes);
        } catch (IOException e) {
            throw new RuntimeException("Could not read Class bytes", e);
        } finally {
            try {
                Closeables.close(stream, true);
            } catch (IOException ignored) {
            }
        }
    }

    private Object instantiateNewClass(Class<?> classWithSyntheticFields, int id) {
        try {
            Constructor<?> declaredConstructor = classWithSyntheticFields.getDeclaredConstructor(int.class);
            return declaredConstructor.newInstance(id);
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate Class", e);
        }
    }

    @Table(name = "synthetic_fields")
    public static class ClassWithSyntheticField {
        @PartitionKey
        private int id;

        // Intentionally broken class: there is no getter/setter for this field
        private int futureSynthetic;

        // This constructor will be used by the Mapper
        @SuppressWarnings("unused")
        public ClassWithSyntheticField() {
        }

        // This constructor is invoked using reflection
        @SuppressWarnings("unused")
        public ClassWithSyntheticField(int id) {
            this.id = id;
        }

        // Getters & Setters used by the Mapper
        @SuppressWarnings("unused")
        public int getId() {
            return id;
        }

        // Getters & Setters used by the Mapper
        @SuppressWarnings("unused")
        public void setId(int id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ClassWithSyntheticField that = (ClassWithSyntheticField) o;

            return id == that.id;

        }

        @Override
        public int hashCode() {
            return id;
        }
    }

    private static class SyntheticFieldCreator extends ClassVisitor {
        public SyntheticFieldCreator(int api, ClassVisitor cv) {
            super(api, cv);
        }

        @Override
        public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
            int newAccesses = access;
            if ("futureSynthetic".equals(name)) {
                newAccesses = access + Opcodes.ACC_SYNTHETIC;
            }
            return super.visitField(newAccesses, name, desc, signature, value);
        }
    }

    private static class InterceptingClassLoader extends ClassLoader {
        public Class defineClass(String name, byte[] b) {
            return defineClass(name, b, 0, b.length);
        }
    }
}
