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
package com.datastax.driver.mapping.configuration.scan;

import java.util.Collection;
import java.util.HashSet;

public class PropertyScanConfiguration {

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

