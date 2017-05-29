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
package com.datastax.driver.stress;

import com.datastax.driver.core.ConsistencyLevel;
import joptsimple.ValueConverter;

/**
 * Created by Alram.Lechner on 03.03.2016.
 */
public class ConsistencyLevelConverter implements ValueConverter<ConsistencyLevel> {

    @Override
    public ConsistencyLevel convert(String value) {
        return ConsistencyLevel.valueOf(value);
    }

    @Override
    public Class<ConsistencyLevel> valueType() {
        return ConsistencyLevel.class;
    }

    @Override
    public String valuePattern() {
        StringBuilder pattern = new StringBuilder();
        for (ConsistencyLevel level : ConsistencyLevel.values()) {
            pattern.append("(").append(level.name()).append(")|");
        }
        pattern.setLength(pattern.length() - 1);
        return pattern.toString();
    }
}
