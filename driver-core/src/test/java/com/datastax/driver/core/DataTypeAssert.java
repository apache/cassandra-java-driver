package com.datastax.driver.core;

import com.google.common.reflect.TypeToken;
import org.assertj.core.api.AbstractAssert;

import static com.datastax.driver.core.Assertions.assertThat;

public class DataTypeAssert extends AbstractAssert<DataTypeAssert, DataType> {
    public DataTypeAssert(DataType actual) {
        super(actual, DataTypeAssert.class);
    }

    public DataTypeAssert hasName(DataType.Name name) {
        assertThat(actual.name).isEqualTo(name);
        return this;
    }

    public DataTypeAssert isFrozen() {
        assertThat(actual.isFrozen()).isTrue();
        return this;
    }

    public DataTypeAssert isNotFrozen() {
        assertThat(actual.isFrozen()).isFalse();
        return this;
    }

    public DataTypeAssert canBeDeserializedAs(TypeToken typeToken) {
        assertThat(actual.canBeDeserializedAs(typeToken)).isTrue();
        return this;
    }

    public DataTypeAssert cannotBeDeserializedAs(TypeToken typeToken) {
        assertThat(actual.canBeDeserializedAs(typeToken)).isFalse();
        return this;
    }

    public DataTypeAssert hasTypeArgument(int position, DataType expected) {
        assertThat(actual.getTypeArguments().get(position)).isEqualTo(expected);
        return this;
    }

    public DataTypeAssert hasTypeArguments(DataType... expected) {
        assertThat(actual.getTypeArguments()).containsExactly(expected);
        return this;
    }
}
