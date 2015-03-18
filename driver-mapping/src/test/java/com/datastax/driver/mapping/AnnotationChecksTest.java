package com.datastax.driver.mapping;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.FrozenValue;
import com.datastax.driver.mapping.annotations.UDT;

import org.testng.annotations.Test;

import static org.testng.Assert.fail;

public class AnnotationChecksTest {
    // Dummy UDT class:
    @UDT(name = "user")
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
    @Frozen("map<text, frozen<map<text, frozen<user>>>>")
    Map<String, Map<String, User>> deeplyNestedFrozenUser;

    Map<String, List<Integer>> mapOfUnfrozenListOfInt;
    @FrozenValue
    Map<String, List<Integer>> mapOfFrozenListOfInt;
    @FrozenValue
    Map<Integer, List<Set<Integer>>> deeplyNestedUnfrozenSet;
    @Frozen("map<int, frozen<list<set<int>>>>")
    Map<Integer, List<Set<Integer>>> deeplyNestedUnfrozenSet2;
    @Frozen("map<int, frozen<list<frozen<set<int>>>>>")
    Map<Integer, List<Set<Integer>>> deeplyNestedFrozenSet;

    @Test(groups = "unit")
    public void should_fail_if_udt_not_frozen() throws Exception {
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

    @Test(groups = "unit")
    public void should_fail_if_nested_collection_not_frozen() throws Exception {
        checkFrozenTypesTest("mapOfUnfrozenListOfInt", false);
        checkFrozenTypesTest("mapOfFrozenListOfInt", true);
        checkFrozenTypesTest("deeplyNestedUnfrozenSet", false);
        checkFrozenTypesTest("deeplyNestedUnfrozenSet2", false);
        checkFrozenTypesTest("deeplyNestedFrozenSet", true);
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
