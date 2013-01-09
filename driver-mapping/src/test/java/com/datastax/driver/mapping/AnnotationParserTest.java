package com.datastax.driver.mapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.datastax.driver.mapping.EntityDefinition.ColumnDefinition;

public class AnnotationParserTest {

    @Test
    public void testBasicEntity() {

        EntityDefinition entityDef = AnnotationParser.parseEntity(A.class);
        assertNotNull(entityDef);
        assertEquals("a", entityDef.tableName);
        assertEquals("test", entityDef.keyspaceName);

        assertEquals(1, entityDef.columns.size());

        ColumnDefinition columnDef = entityDef.columns.get(0);
        assertEquals("p1", columnDef.fieldName);
        assertEquals("c1", columnDef.columnName);
        assertEquals("getP1", columnDef.readMethod.getName());
        assertEquals("setP1", columnDef.writeMethod.getName());
    }


    @Test
    public void testInheritance() {

        EntityDefinition entityDef = AnnotationParser.parseEntity(Product.class);
        assertNotNull(entityDef);
        assertEquals("product", entityDef.tableName);
        assertEquals("product_type", entityDef.inheritanceColumn);

        assertEquals(3, entityDef.subEntities.size());
    }
}
