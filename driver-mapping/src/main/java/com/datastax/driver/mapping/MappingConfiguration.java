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

/**
 * The configuration to use for the mappers.
 */
public class MappingConfiguration {

    /**
     * Returns a new {@link Builder} instance.
     *
     * @return a new {@link Builder} instance.
     */
    public static MappingConfiguration.Builder builder() {
        return new MappingConfiguration.Builder();
    }

    /**
     * Builder for {@link MappingConfiguration} instances.
     */
    public static class Builder {

        private PropertyMapper propertyMapper = new DefaultPropertyMapper();

        /**
         * Sets the {@link PropertyMapper property access strategy} to use.
         *
         * @param propertyMapper the {@link PropertyMapper property access strategy} to use.
         * @return this {@link Builder} instance (to allow for fluent builder pattern).
         */
        public Builder withPropertyMapper(PropertyMapper propertyMapper) {
            this.propertyMapper = propertyMapper;
            return this;
        }

        /**
         * Builds a new instance of {@link MappingConfiguration} with this builder's
         * settings.
         *
         * @return a new instance of {@link MappingConfiguration}
         */
        public MappingConfiguration build() {
            return new MappingConfiguration(propertyMapper);
        }
    }

    private final PropertyMapper propertyMapper;

    private MappingConfiguration(PropertyMapper propertyMapper) {
        this.propertyMapper = propertyMapper;
    }

    /**
     * Returns the {@link PropertyMapper}.
     *
     * @return the {@link PropertyMapper}.
     */
    public PropertyMapper getPropertyMapper() {
        return propertyMapper;
    }

}
