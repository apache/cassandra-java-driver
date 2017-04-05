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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.DriverInternalError;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

class SnappyCompressor extends FrameCompressor {

    private static final Logger logger = LoggerFactory.getLogger(SnappyCompressor.class);

    static final SnappyCompressor instance;

    static {
        SnappyCompressor i;
        try {
            i = new SnappyCompressor();
        } catch (NoClassDefFoundError e) {
            i = null;
            logger.warn("Cannot find Snappy class, you should make sure the Snappy library is in the classpath if you intend to use it. Snappy compression will not be available for the protocol.");
        } catch (Throwable e) {
            i = null;
            logger.warn("Error loading Snappy library ({}). Snappy compression will not be available for the protocol.", e.toString());
        }
        instance = i;
    }

    private SnappyCompressor() {
        // this would throw java.lang.NoClassDefFoundError if Snappy class
        // wasn't found at runtime which should be processed by the calling method
        Snappy.getNativeLibraryVersion();
    }

    @Override
    Frame compress(Frame frame) throws IOException {
        ByteBuf input = frame.body;
        ByteBuf frameBody = input.isDirect() ? compressDirect(input) : compressHeap(input);
        return frame.with(frameBody);
    }

    private ByteBuf compressDirect(ByteBuf input) throws IOException {
        int maxCompressedLength = Snappy.maxCompressedLength(input.readableBytes());
        // If the input is direct we will allocate a direct output buffer as well as this will allow us to use
        // Snappy.compress(ByteBuffer, ByteBuffer) and so eliminate memory copies.
        ByteBuf output = input.alloc().directBuffer(maxCompressedLength);
        try {
            ByteBuffer in = inputNioBuffer(input);
            // Increase reader index.
            input.readerIndex(input.writerIndex());

            ByteBuffer out = outputNioBuffer(output);
            int written = Snappy.compress(in, out);
            // Set the writer index so the amount of written bytes is reflected
            output.writerIndex(output.writerIndex() + written);
        } catch (IOException e) {
            // release output buffer so we not leak and rethrow exception.
            output.release();
            throw e;
        }
        return output;
    }

    private ByteBuf compressHeap(ByteBuf input) throws IOException {
        int maxCompressedLength = Snappy.maxCompressedLength(input.readableBytes());
        int inOffset = input.arrayOffset() + input.readerIndex();
        byte[] in = input.array();
        int len = input.readableBytes();
        // Increase reader index.
        input.readerIndex(input.writerIndex());

        // Allocate a heap buffer from the ByteBufAllocator as we may use a PooledByteBufAllocator and so
        // can eliminate the overhead of allocate a new byte[].
        ByteBuf output = input.alloc().heapBuffer(maxCompressedLength);
        try {
            // Calculate the correct offset.
            int offset = output.arrayOffset() + output.writerIndex();
            byte[] out = output.array();
            int written = Snappy.compress(in, inOffset, len, out, offset);

            // Increase the writerIndex with the written bytes.
            output.writerIndex(output.writerIndex() + written);
        } catch (IOException e) {
            // release output buffer so we not leak and rethrow exception.
            output.release();
            throw e;
        }
        return output;
    }

    @Override
    Frame decompress(Frame frame) throws IOException {
        ByteBuf input = frame.body;
        ByteBuf frameBody = input.isDirect() ? decompressDirect(input) : decompressHeap(input);
        return frame.with(frameBody);
    }

    private ByteBuf decompressDirect(ByteBuf input) throws IOException {
        ByteBuffer in = inputNioBuffer(input);
        // Increase reader index.
        input.readerIndex(input.writerIndex());

        if (!Snappy.isValidCompressedBuffer(in))
            throw new DriverInternalError("Provided frame does not appear to be Snappy compressed");

        // If the input is direct we will allocate a direct output buffer as well as this will allow us to use
        // Snappy.compress(ByteBuffer, ByteBuffer) and so eliminate memory copies.
        ByteBuf output = input.alloc().directBuffer(Snappy.uncompressedLength(in));
        try {
            ByteBuffer out = outputNioBuffer(output);

            int size = Snappy.uncompress(in, out);
            // Set the writer index so the amount of written bytes is reflected
            output.writerIndex(output.writerIndex() + size);
        } catch (IOException e) {
            // release output buffer so we not leak and rethrow exception.
            output.release();
            throw e;
        }
        return output;
    }

    private ByteBuf decompressHeap(ByteBuf input) throws IOException {
        // Not a direct buffer so use byte arrays...
        int inOffset = input.arrayOffset() + input.readerIndex();
        byte[] in = input.array();
        int len = input.readableBytes();
        // Increase reader index.
        input.readerIndex(input.writerIndex());

        if (!Snappy.isValidCompressedBuffer(in, inOffset, len))
            throw new DriverInternalError("Provided frame does not appear to be Snappy compressed");

        // Allocate a heap buffer from the ByteBufAllocator as we may use a PooledByteBufAllocator and so
        // can eliminate the overhead of allocate a new byte[].
        ByteBuf output = input.alloc().heapBuffer(Snappy.uncompressedLength(in, inOffset, len));
        try {
            // Calculate the correct offset.
            int offset = output.arrayOffset() + output.writerIndex();
            byte[] out = output.array();
            int written = Snappy.uncompress(in, inOffset, len, out, offset);

            // Increase the writerIndex with the written bytes.
            output.writerIndex(output.writerIndex() + written);
        } catch (IOException e) {
            // release output buffer so we not leak and rethrow exception.
            output.release();
            throw e;
        }
        return output;
    }
}
