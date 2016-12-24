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

public class HierarchyScanStrategy {

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