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

public class PropertyScanScope {

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