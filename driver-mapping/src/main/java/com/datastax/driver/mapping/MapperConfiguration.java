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
package com.datastax.driver.mapping;

import java.util.Collection;
import java.util.HashSet;

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
 * This is also where you get the configured settings, though those cannot be changed
 * (they are set during the built of the Mapper object).
 */
public class MapperConfiguration {

    private PropertyScanConfiguration propertyScanConfiguration;

    public MapperConfiguration() {
        this.propertyScanConfiguration = new PropertyScanConfiguration();
    }

    public MapperConfiguration(MapperConfiguration toCopy) {
        this.propertyScanConfiguration = new PropertyScanConfiguration(toCopy.propertyScanConfiguration);
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

    public static class PropertyScanConfiguration {

        private Collection<String> excludedProperties;

        private PropertyScanScope propertyScanScope;

        private PropertyMappingStrategy propertyMappingStrategy;

        private HierarchyScanStrategy hierarchyScanStrategy;

        public PropertyScanConfiguration() {
            this.excludedProperties = new HashSet<String>();
            this.propertyScanScope = new PropertyScanScope();
            this.propertyMappingStrategy = PropertyMappingStrategy.BLACK_LIST;
            this.hierarchyScanStrategy = new HierarchyScanStrategy();
        }

        public PropertyScanConfiguration(PropertyScanConfiguration toCopy) {
            this.excludedProperties = toCopy.excludedProperties;
            this.propertyScanScope = new PropertyScanScope(toCopy.propertyScanScope);
            this.propertyMappingStrategy = toCopy.propertyMappingStrategy;
            this.hierarchyScanStrategy = new HierarchyScanStrategy(toCopy.hierarchyScanStrategy);
        }

        /**
         * Returns a collection of properties to exclude from mapping
         *
         * @return a collection of properties to exclude from mapping
         */
        public Collection<String> getExcludedProperties() {
            return excludedProperties;
        }

        /**
         * Sets a collection of properties to exclude from mapping
         *
         * @param excludedProperties a collection of properties to exclude from mapping
         * @return the PropertyScanConfiguration to enable builder pattern
         */
        public PropertyScanConfiguration setExcludedProperties(Collection<String> excludedProperties) {
            this.excludedProperties = excludedProperties;
            return this;
        }

        /**
         * Returns the property scan scope settings
         *
         * @return the property scan scope settings
         */
        public PropertyScanScope getPropertyScanScope() {
            return propertyScanScope;
        }

        /**
         * Sets the property scan scope settings
         *
         * @param propertyScanScope property scan scope settings object to use
         * @return the PropertyScanConfiguration to enable builder pattern
         */
        public PropertyScanConfiguration setPropertyScanScope(PropertyScanScope propertyScanScope) {
            this.propertyScanScope = propertyScanScope;
            return this;
        }

        /**
         * Returns the PropertyMappingStrategy of the mapper.
         * e.g. whether to scan fields that are not blacklisted (opt-out), meaning
         * that unless a property is explicitly annotated with {@code Transient} it will be mapped, or to scan
         * fields that are not white-listed (opt-in), meaning that unless a property is explicitly annotated
         * with either {@code PartitionKey} or {@code ClusteringColumn} or {@code Column} or
         * {@code com.datastax.driver.mapping.annotations.Field} or {@code Computed} it won't be mapped.
         *
         * @return the PropertyMappingStrategy of the mapper
         */
        public PropertyMappingStrategy getPropertyMappingStrategy() {
            return propertyMappingStrategy;
        }

        /**
         * Sets the PropertyMappingStrategy of the mapper.
         *
         * @param propertyMappingStrategy PropertyMappingStrategy to use
         * @return the PropertyScanConfiguration to enable builder pattern
         */
        public PropertyScanConfiguration setPropertyMappingStrategy(PropertyMappingStrategy propertyMappingStrategy) {
            this.propertyMappingStrategy = propertyMappingStrategy;
            return this;
        }

        /**
         * Returns the HierarchyScanStrategy settings of the mapper
         *
         * @return the HierarchyScanStrategy settings of the mapper
         */
        public HierarchyScanStrategy getHierarchyScanStrategy() {
            return hierarchyScanStrategy;
        }

        /**
         * Sets the HierarchyScanStrategy settings of the mapper
         *
         * @param hierarchyScanStrategy HierarchyScanStrategy settings to use
         * @return the PropertyScanConfiguration to enable builder pattern
         */
        public PropertyScanConfiguration setHierarchyScanStrategy(HierarchyScanStrategy hierarchyScanStrategy) {
            this.hierarchyScanStrategy = hierarchyScanStrategy;
            return this;
        }

    }

    public static class PropertyScanScope {

        private boolean scanFields;

        private boolean scanGetters;

        public PropertyScanScope() {
            this.scanFields = true;
            this.scanGetters = true;
        }

        public PropertyScanScope(PropertyScanScope toCopy) {
            this.scanFields = toCopy.scanFields;
            this.scanGetters = toCopy.scanGetters;
        }

        /**
         * Returns whether or not the scope include class fields
         *
         * @return whether or not the scope include class fields
         */
        public boolean isScanFields() {
            return scanFields;
        }

        /**
         * Set whether or not the scope include class fields
         *
         * @param scanFields    whether or not the scope include class fields
         * @return the PropertyScanScope to enable builder pattern
         */
        public PropertyScanScope setScanFields(boolean scanFields) {
            this.scanFields = scanFields;
            return this;
        }

        /**
         * Returns whether or not the scope include class getters
         *
         * @return whether or not the scope include class getters
         */
        public boolean isScanGetters() {
            return scanGetters;
        }

        /**
         * Set whether or not the scope include class getters
         *
         * @param scanGetters whether or not the scope include class getters
         * @return the PropertyScanScope to enable builder pattern
         */
        public PropertyScanScope setScanGetters(boolean scanGetters) {
            this.scanGetters = scanGetters;
            return this;
        }

    }

    public static class HierarchyScanStrategy {

        private boolean hierarchyScanEnabled;

        private boolean scanOnlyAnnotatedClasses;

        private Class<?> deepestAllowedAncestor;

        public HierarchyScanStrategy() {
            this.hierarchyScanEnabled = true;
            this.scanOnlyAnnotatedClasses = false;
            this.deepestAllowedAncestor = Object.class;
        }

        public HierarchyScanStrategy(HierarchyScanStrategy toCopy) {
            this.hierarchyScanEnabled = toCopy.hierarchyScanEnabled;
            this.scanOnlyAnnotatedClasses = toCopy.scanOnlyAnnotatedClasses;
            this.deepestAllowedAncestor = toCopy.deepestAllowedAncestor;
        }

        /**
         * Returns whether or not the strategy allows scanning of parent classes
         *
         * @return whether or not the strategy allows scanning of parent classes
         */
        public boolean isHierarchyScanEnabled() {
            return hierarchyScanEnabled;
        }

        /**
         * Sets whether or not the strategy allows scanning of parent classes
         *
         * @param hierarchyScanEnabled whether or not the strategy allows scanning of parent classes
         * @return the HierarchyScanStrategy to enable builder pattern
         */
        public HierarchyScanStrategy setHierarchyScanEnabled(boolean hierarchyScanEnabled) {
            this.hierarchyScanEnabled = hierarchyScanEnabled;
            return this;
        }

        /**
         * Returns whether or not the strategy allows scanning of all parent classes or only those
         * annotated with {@code Table} or {@code UDT} or {@code Accessor}
         *
         * @return whether or not the strategy allows scanning of all parent classes or only those
         * annotated with {@code Table} or {@code UDT} or {@code Accessor}
         */
        public boolean isScanOnlyAnnotatedClasses() {
            return scanOnlyAnnotatedClasses;
        }

        /**
         * Sets whether or not the strategy allows scanning of all parent classes or only those
         * annotated with {@code Table} or {@code UDT} or {@code Accessor}
         *
         * @param scanOnlyAnnotatedClasses whether or not the strategy allows scanning of
         *                                 all parent classes or only those annotated with
         *                                 {@code Table} or {@code UDT} or {@code Accessor}
         * @return the HierarchyScanStrategy to enable builder pattern
         */
        public HierarchyScanStrategy setScanOnlyAnnotatedClasses(boolean scanOnlyAnnotatedClasses) {
            this.scanOnlyAnnotatedClasses = scanOnlyAnnotatedClasses;
            return this;
        }

        /**
         * Returns the deepest parent class to the strategy allows to scan
         * (which default to {@code Object.class})
         *
         * @return the deepest parent class to the strategy allows to scan
         * (which default to {@code Object.class})
         */
        public Class<?> getDeepestAllowedAncestor() {
            return deepestAllowedAncestor;
        }

        /**
         * Sets the deepest parent class to the strategy allows to scan
         *
         * @param deepestAllowedAncestor the deepest parent class to the strategy allows to scan
         * @return the HierarchyScanStrategy to enable builder pattern
         */
        public HierarchyScanStrategy setDeepestAllowedAncestor(Class<?> deepestAllowedAncestor) {
            this.deepestAllowedAncestor = deepestAllowedAncestor;
            return this;
        }

    }

    public enum PropertyMappingStrategy {BLACK_LIST, WHITE_LIST}

}
