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

package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.PagingStateException;
import com.datastax.driver.core.utils.Bytes;

/**
 * The paging state of a query.
 *
 * This object represents the next page to be fetched if the query is
 * multi page. It can be saved and reused later on the same statement.
 *
 * The PagingState can be serialized and deserialized either as a String
 * or as a byte array.
 *
 * @see Statement#setPagingState(PagingState)
 */
public class PagingState {

    private byte[] pagingState;
    private byte[] hash;

    PagingState(ByteBuffer pagingState, Statement statement) {
        this.pagingState = Bytes.getArray(pagingState);
        this.hash = hash(statement);
    }

    private PagingState(byte[] complete) {
        // Check the sizes in the beginning of the buffer, otherwise we cannot build the paging state object
        ByteBuffer pagingStateBB = ByteBuffer.wrap(complete);
        int pagingSize = pagingStateBB.getShort();
        int hashSize = pagingStateBB.getShort();
        if (pagingSize + hashSize != pagingStateBB.remaining()) {
            throw new PagingStateException("Cannot deserialize paging state, invalid format. "
                + "The serialized form was corrupted, or not initially generated from a PagingState object.");
        }
        this.pagingState = extractPagingState(complete);
        this.hash = extractHashDigest(complete);
    }

    private byte[] hash(Statement statement) {
        byte[] digest;
        ByteBuffer[] values;
        MessageDigest md;
        assert !(statement instanceof BatchStatement);
        try {
            md = MessageDigest.getInstance("MD5");
            if (statement instanceof BoundStatement) {
                BoundStatement bs = ((BoundStatement)statement);
                md.update(bs.preparedStatement().getQueryString().getBytes());
                values = bs.values;
            } else {
                //it is a RegularStatement since Batch statements are not allowed
                RegularStatement rs = (RegularStatement)statement;
                md.update(rs.getQueryString().getBytes());
                values = rs.getValues();
            }
            if (values != null) {
                for (ByteBuffer value : values) {
                    md.update(value.duplicate());
                }
            }
            md.update(this.pagingState);
            digest = md.digest();

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 doesn't seem to be available on this JVM", e);
        }
        return digest;
    }

    boolean matches(Statement statement) {
        byte[] toTest = hash(statement);
        return Arrays.equals(toTest, this.hash);
    }

    private static byte[] extractPagingState(byte[] complete) {
        ByteBuffer completeBB = ByteBuffer.wrap(complete);
        short size = completeBB.getShort();
        byte[] array = new byte[size];
        // Go forward of 2 bytes
        completeBB.getShort();
        completeBB.get(array);
        return array;
    }

    private static byte[] extractHashDigest(byte[] complete) {
        ByteBuffer completeBB = ByteBuffer.wrap(complete);
        short pagingSize = completeBB.getShort();
        short hashSize = completeBB.getShort();
        completeBB.position(pagingSize + 4);
        byte[] array = new byte[hashSize];
        completeBB.get(array);
        return array;
    }

    private ByteBuffer generateCompleteOutput() {
        ByteBuffer res = ByteBuffer.allocate(pagingState.length + hash.length + 4);

        res.putShort((short)pagingState.length);
        res.putShort((short)hash.length);

        res.put(pagingState);
        res.put(hash);

        res.rewind();

        return res;
    }

    ByteBuffer getRawState() {
        return ByteBuffer.wrap(this.pagingState);
    }

    @Override
    public String toString() {
        return Bytes.toRawHexString(generateCompleteOutput());
    }

    /**
     * Create a PagingState object from a string previously generated with {@link #toString()}.
     *
     * @param string the string value.
     * @return the PagingState object created.
     *
     * @throws PagingStateException if the string does not have the correct format.
     */
    public static PagingState fromString(String string) {
        try {
            byte[] complete = Bytes.fromRawHexString(string, 0);
            return new PagingState(complete);
        } catch (Exception e) {
            throw new PagingStateException("Cannot deserialize paging state, invalid format. "
                + "The serialized form was corrupted, or not initially generated from a PagingState object.", e);
        }
    }

    /**
     * Return a representation of the paging state object as a byte array.
     *
     * @return the paging state as a byte array.
     */
    public byte[] toBytes() {
        return generateCompleteOutput().array();
    }

    /**
     * Create a PagingState object from a byte array previously generated with {@link #toBytes()}.
     *
     * @param pagingState The byte array representation.
     * @return the PagingState object created.
     *
     * @throws PagingStateException if the byte array does not have the correct format.
     */
    public static PagingState fromBytes(byte[] pagingState) {
        return new PagingState(pagingState);
    }
}
