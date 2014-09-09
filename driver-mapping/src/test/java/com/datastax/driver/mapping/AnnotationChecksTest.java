package com.datastax.driver.mapping;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.FrozenValue;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;

public class AnnotationChecksTest {
    // Dummy UDT class:
    public class User {
    }

    // Dummy fields to run our checks on:
    String string;
    @Frozen
    String frozenString;
    List<String> listOfStrings;
    User unfrozenUser;
    @Frozen
    User frozenUser;
    List<User> listOfUnfrozenUsers;
    @FrozenValue
    List<User> listOfFrozenUsers;
    Map<String, Map<String, User>> deeplyNestedUnfrozenUser;
    @Frozen("map<text, map<text, frozen<user>>>")
    Map<String, Map<String, User>> deeplyNestedFrozenUser;

    @Test(groups = "unit")
    public void checkFrozenTypesTest() throws Exception {
        checkFrozenTypesTest("string", true);
        checkFrozenTypesTest("frozenString", false);
        checkFrozenTypesTest("listOfStrings", true);
        checkFrozenTypesTest("unfrozenUser", false);
        checkFrozenTypesTest("frozenUser", true);
        checkFrozenTypesTest("listOfUnfrozenUsers", false);
        checkFrozenTypesTest("listOfFrozenUsers", true);
        checkFrozenTypesTest("deeplyNestedUnfrozenUser", false);
        checkFrozenTypesTest("deeplyNestedFrozenUser", true);
    }

    private void checkFrozenTypesTest(String fieldName, boolean expectSuccess) throws Exception {
        Field field = AnnotationChecksTest.class.getDeclaredField(fieldName);
        Exception exception = null;
        try {
            AnnotationChecks.checkFrozenTypes(field);
        } catch (IllegalArgumentException e) {
            exception = e;
        }
        if (expectSuccess && exception != null)
            fail("expected check to succeed but got exception " + exception.getMessage());
        else if (!expectSuccess && exception == null)
            fail("expected check to fail but got no exception");
    }
}
