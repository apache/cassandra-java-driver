package com.datastax.driver.mapping;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.CoreHooks;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.UserType;
import com.datastax.driver.mapping.annotations.UDT;

import static com.datastax.driver.core.Assertions.assertThat;

public class InferredCQLTypeTest {
    MappingManager manager;
    UDTMapper mockMapper;
    UserType mockUDTType = CoreHooks.MOCK_USER_TYPE;

    @BeforeClass(groups = "unit")
    @SuppressWarnings("unchecked")
    public void setup() {
        mockMapper = mock(UDTMapper.class);
        when(mockMapper.getUserType()).thenReturn(mockUDTType);

        manager = mock(MappingManager.class);
        when(manager.udtMapper(MockUDT.class)).thenReturn(mockMapper);
    }

    @Test(groups = "unit")
    public void should_parse_collections_of_primitives() throws Exception {
        InferredCQLType instance = newInstanceForField("listOfIntegers");
        assertThat(instance.dataType).isEqualTo(DataType.list(DataType.cint()));
        assertThat(instance.containsMappedUDT).isFalse();
        assertThat(instance.udtMapper).isNull();

        instance = newInstanceForField("setOfStrings");
        assertThat(instance.dataType).isEqualTo(DataType.set(DataType.text()));
        assertThat(instance.containsMappedUDT).isFalse();
        assertThat(instance.udtMapper).isNull();

        instance = newInstanceForField("mapOfIntegerToString");
        assertThat(instance.dataType).isEqualTo(DataType.map(DataType.cint(), DataType.text()));
        assertThat(instance.containsMappedUDT).isFalse();
        assertThat(instance.udtMapper).isNull();
    }

    @Test(groups = "unit")
    public void should_parse_collection_containing_UDT() throws Exception {
        InferredCQLType instance = newInstanceForField("listOfUDT");
        assertThat(instance.dataType).isEqualTo(DataType.list(mockUDTType));
        assertThat(instance.containsMappedUDT).isTrue();
        assertThat(instance.udtMapper).isNull();
        assertThat(instance.childTypes.get(0).udtMapper).isEqualTo(mockMapper);

        instance = newInstanceForField("complexMap");
        assertThat(instance.dataType).isEqualTo(DataType.map(
            DataType.list(DataType.cint()),
            DataType.map(DataType.text(), DataType.set(mockUDTType))
        ));
        assertThat(instance.containsMappedUDT).isTrue();
        assertThat(instance.udtMapper).isNull();
        InferredCQLType key = instance.childTypes.get(0);
        assertThat(key.containsMappedUDT).isFalse();
        assertThat(key.udtMapper).isNull();
        InferredCQLType value = instance.childTypes.get(1);
        assertThat(value.containsMappedUDT).isTrue();
        assertThat(value.udtMapper).isNull();
        assertThat(value.childTypes.get(0).containsMappedUDT).isFalse();
        assertThat(value.childTypes.get(1).containsMappedUDT).isTrue();
        assertThat(value.childTypes.get(1).childTypes.get(0).udtMapper).isEqualTo(mockMapper);
    }

    private InferredCQLType newInstanceForField(String name) throws NoSuchFieldException {
        Field field = InferredCQLTypeTest.class.getDeclaredField(name);
        return InferredCQLType.from(field, manager);
    }

    @UDT(name = "mock")
    static class MockUDT {
    }

    List<Integer> listOfIntegers;
    Set<String> setOfStrings;
    Map<Integer, String> mapOfIntegerToString;
    List<MockUDT> listOfUDT;
    Map<List<Integer>, Map<String, Set<MockUDT>>> complexMap;
}