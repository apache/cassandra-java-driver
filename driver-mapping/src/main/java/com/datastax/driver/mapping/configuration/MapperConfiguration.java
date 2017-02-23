/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping.configuration;

import com.datastax.driver.mapping.configuration.naming.CommonNamingConventions;
import com.datastax.driver.mapping.configuration.naming.NamingStrategy;
import com.datastax.driver.mapping.configuration.scan.PropertyScanConfiguration;

/**
 * The configuration of the mapper.
 * It configures the following categories:
 * Property scan configurations:
 * <ul>
 * <li>Transient property names (added on top of properties annotated with {@code Transient} or
 * property names that appear in {@code Table} or {@code UDT} annotations.</li>
 * <li>Property scan scope: whether or not to scan properties by getters/setters, whether or not
 * to scan properties by class fields.</li>
 * <li>Property mapping strategy: whether to scan fields that are not blacklisted (opt-out), meaning
 * that unless a property is explicitly annotated with {@code Transient} it will be mapped, or to scan
 * fields that are not white-listed (opt-in), meaning that unless a property is explicitly annotated
 * with either {@code PartitionKey} or {@code ClusteringColumn} or {@code Column} or
 * {@code com.datastax.driver.mapping.annotations.Field} or {@code Computed} it won't be mapped.</li>
 * <li>Hierarchy scan strategy: defines how to scan for properties in parent classes - if
 * hierarchyScanEnabled set to false - parent classes will not be scanned at all.
 * if scanOnlyAnnotatedClasses set to true, only parents annotated with table classes
 * ({@code Table}, {@code UDT}, {@code Accessor}) will be scanned. And lastly - deepestAllowedAncestor
 * will define the deepest parent class to scan - which default to {@code Object.class}</li>
 * </ul>
 * Naming strategy:
 * <ul>
 * <li>The Java property naming convention (e.g. lowerCamelCase, UpperCamelCase, lower_snake_cake, etc...
 * Note that you can define your custom naming convention by extending
 * {@code com.datastax.driver.mapping.configuration.naming.NamingConvention})</li>
 * <li>The Cassandra column naming convention (e.g. lowerCamelCase, UpperCamelCase, lower_snake_cake, etc...
 * Note that you can define your custom naming convention by extending
 * {@code com.datastax.driver.mapping.configuration.naming.NamingConvention})</li>
 * </ul>
 * This is also where you get the configured settings, though those cannot be changed
 * (they are set during the construction of the Mapper object).
 */
public class MapperConfiguration {

    private PropertyScanConfiguration propertyScanConfiguration;

    private NamingStrategy namingStrategy;

    public MapperConfiguration() {
        this.propertyScanConfiguration = new PropertyScanConfiguration();
        this.namingStrategy = new NamingStrategy(new CommonNamingConventions.LowerCamelCase(), new CommonNamingConventions.LowerCase());
    }

    public MapperConfiguration(MapperConfiguration toCopy) {
        this.propertyScanConfiguration = new PropertyScanConfiguration(toCopy.propertyScanConfiguration);
        this.namingStrategy = new NamingStrategy(toCopy.namingStrategy);
    }

    /**
     * Returns the property scanning configuration
     *
     * @return the property scanning configuration
     */
    public PropertyScanConfiguration getPropertyScanConfiguration() {
        return propertyScanConfiguration;
    }

    /**
     * Sets the property scanning configuration
     *
     * @param propertyScanConfiguration property scanning configuration to use
     * @return the MapperConfiguration to enable builder pattern
     */
    public MapperConfiguration setPropertyScanConfiguration(PropertyScanConfiguration propertyScanConfiguration) {
        this.propertyScanConfiguration = propertyScanConfiguration;
        return this;
    }

    /**
     * Returns the naming strategy configuration
     *
     * @return the naming strategy configuration
     */
    public NamingStrategy getNamingStrategy() {
        return namingStrategy;
    }

    /**
     * Sets the naming strategy configuration
     *
     * @param namingStrategy naming strategy to use
     * @return the MapperConfiguration to enable builder pattern
     *
     */
    public MapperConfiguration setNamingStrategy(NamingStrategy namingStrategy) {
        this.namingStrategy = namingStrategy;
        return this;
    }

}
