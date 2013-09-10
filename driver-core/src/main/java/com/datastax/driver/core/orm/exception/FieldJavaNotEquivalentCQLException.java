/*
 * Copyright 2013 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.datastax.driver.core.orm.exception;

/**
 * Exception that indicates conditions when the object exist, but its type in
 * java isn't equivalent with CQL type
 * 
 * @author otaviojava
 * 
 */
public class FieldJavaNotEquivalentCQLException extends CassandraORMException {

    private static final long serialVersionUID = 1L;

    public FieldJavaNotEquivalentCQLException(String message) {
        super(message);

    }

}
