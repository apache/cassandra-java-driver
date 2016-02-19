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
package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.CodecRegistry;

import java.util.List;

public abstract class Using extends Utils.Appendeable {

    final String optionName;

    private Using(String optionName) {
        this.optionName = optionName;
    }

    static class WithValue extends Using {
        private final long value;

        WithValue(String optionName, long value) {
            super(optionName);
            this.value = value;
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
            sb.append(optionName).append(' ').append(value);
        }

        @Override
        boolean containsBindMarker() {
            return false;
        }
    }

    static class WithMarker extends Using {
        private final BindMarker marker;

        WithMarker(String optionName, BindMarker marker) {
            super(optionName);
            this.marker = marker;
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
            sb.append(optionName).append(' ').append(marker);
        }

        @Override
        boolean containsBindMarker() {
            return true;
        }
    }
}
