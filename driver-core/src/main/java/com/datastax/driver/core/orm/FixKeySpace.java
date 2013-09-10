/*
 * Copyright 2013 Otávio Gonçalves de Santana (otaviojava)
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
package com.datastax.driver.core.orm;

import java.util.logging.Logger;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

/**
 * This class verifies if keyspace exists and then try to create
 * 
 * @author otaviojava
 * 
 */
class FixKeySpace {

    private static final String CREATE_KEY_SPACE_CQL = "CREATE KEYSPACE :keySpace WITH replication = {'class': :replication , 'replication_factor': :factor};";

    private static final int DEFAULT_REPLICATION_FACTOR = 3;

    private static final ReplicaStrategy DEFAULT_REPLICA_STRATEGY = ReplicaStrategy.SIMPLES_TRATEGY;

    /**
     * Verify if keySpace exist
     * 
     * @param keySpace
     *            - nome of keyspace
     * @param session
     *            - session of Cassandra
     */
    public final void verifyKeySpace(String keySpace, Session session) {
        verifyKeySpace(keySpace, session, DEFAULT_REPLICA_STRATEGY, DEFAULT_REPLICATION_FACTOR);
    }

    public void verifyKeySpace(String keySpace, Session session,ReplicaStrategy replicaStrategy, int factor) {
        try {
            session.execute("use " + keySpace);
        } catch (InvalidQueryException exception) {
            Logger.getLogger(FixKeySpace.class.getName()).info( "KeySpace does not exist, create a keySpace: " + keySpace);
            createKeySpace(session, keySpace, replicaStrategy, factor);
            verifyKeySpace(keySpace, session);
        }

    }

    public void createKeySpace(Session session, String keySpace, ReplicaStrategy replicaStrategy, int factor) {
        String query = CREATE_KEY_SPACE_CQL.replace(":keySpace", keySpace).replace(":replication", replicaStrategy.getValue()).replace(":factor", String.valueOf(factor));
        session.execute(query);

    }
}
