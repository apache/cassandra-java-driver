package com.datastax.driver.mapping;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

import com.datastax.driver.mapping.EntityDefinition.ColumnDefinition;

public class AnnotationParserTest {

    @Test
    public void testBasicEntity() {

        EntityDefinition<A> entityDef = AnnotationParser.parseEntity(A.class);
        assertNotNull(entityDef);
        assertEquals(entityDef.tableName, "a");
        assertEquals(entityDef.keyspaceName, "ks");

        assertEquals(1, entityDef.columns.size());

        ColumnDefinition columnDef = entityDef.columns.get(0);
        assertEquals(columnDef.fieldName, "p1");
        assertEquals(columnDef.columnName, "c1");
        assertEquals(columnDef.readMethod.getName(), "getP1");
        assertEquals(columnDef.writeMethod.getName(), "setP1");
    }

    @Test
    public void testInheritance() {

        EntityDefinition<Product> entityDef = AnnotationParser.parseEntity(Product.class);
        assertNotNull(entityDef);
        assertEquals(entityDef.tableName, "product");
        assertEquals(entityDef.inheritanceColumn, "product_type");

        assertEquals(entityDef.subEntities.size(), 3);
    }
}
