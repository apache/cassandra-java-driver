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

import io.netty.buffer.ByteBuf;
import net.jpountz.lz4.LZ4Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

class LZ4Compressor extends FrameCompressor {

    private static final Logger logger = LoggerFactory.getLogger(LZ4Compressor.class);

    static final LZ4Compressor instance;

    static {
        LZ4Compressor i;
        try {
            i = new LZ4Compressor();
        } catch (NoClassDefFoundError e) {
            i = null;
            logger.warn("Cannot find LZ4 class, you should make sure the LZ4 library is in the classpath if you intend to use it. LZ4 compression will not be available for the protocol.");
        } catch (Throwable e) {
            i = null;
            logger.warn("Error loading LZ4 library ({}). LZ4 compression will not be available for the protocol.", e.toString());
        }
        instance = i;
    }

    private static final int INTEGER_BYTES = 4;
    private final net.jpountz.lz4.LZ4Compressor compressor;
    private final net.jpountz.lz4.LZ4FastDecompressor decompressor;

    private LZ4Compressor() {
        final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
        logger.info("Using {}", lz4Factory.toString());
        compressor = lz4Factory.fastCompressor();
        decompressor = lz4Factory.fastDecompressor();
    }

    @Override
    Frame compress(Frame frame) throws IOException {
        ByteBuf input = frame.body;

        // TODO: JAVA-1306: Use the same API calls for direct and heap buffers when LZ4 updated.
        ByteBuf frameBody = input.isDirect() ? compressDirect(input) : compressHeap(input);
        return frame.with(frameBody);
    }

    private ByteBuf compressDirect(ByteBuf input) throws IOException {
        int maxCompressedLength = compressor.maxCompressedLength(input.readableBytes());
        // If the input is direct we will allocate a direct output buffer as well as this will allow us to use
        // LZ4Compressor.compress and so eliminate memory copies.
        ByteBuf output = input.alloc().directBuffer(INTEGER_BYTES + maxCompressedLength);
        try {
            ByteBuffer in = inputNioBuffer(input);
            // Increase reader index.
            input.readerIndex(input.writerIndex());

            output.writeInt(in.remaining());

            ByteBuffer out = outputNioBuffer(output);
            int written = compressor.compress(in, in.position(), in.remaining(), out, out.position(), out.remaining());
            // Set the writer index so the amount of written bytes is reflected
            output.writerIndex(output.writerIndex() + written);
        } catch (Exception e) {
            // release output buffer so we not leak and rethrow exception.
            output.release();
            throw new IOException(e);
        }
        return output;
    }

    private ByteBuf compressHeap(ByteBuf input) throws IOException {
        int maxCompressedLength = compressor.maxCompressedLength(input.readableBytes());

        // Not a direct buffer so use byte arrays...
        int inOffset = input.arrayOffset() + input.readerIndex();
        byte[] in = input.array();
        int len = input.readableBytes();
        // Increase reader index.
        input.readerIndex(input.writerIndex());

        // Allocate a heap buffer from the ByteBufAllocator as we may use a PooledByteBufAllocator and so
        // can eliminate the overhead of allocate a new byte[].
        ByteBuf output = input.alloc().heapBuffer(INTEGER_BYTES + maxCompressedLength);
        try {
            output.writeInt(len);
            // calculate the correct offset.
            int offset = output.arrayOffset() + output.writerIndex();
            byte[] out = output.array();
            int written = compressor.compress(in, inOffset, len, out, offset);

            // Set the writer index so the amount of written bytes is reflected
            output.writerIndex(output.writerIndex() + written);
        } catch (Exception e) {
            // release output buffer so we not leak and rethrow exception.
            output.release();
            throw new IOException(e);
        }
        return output;
    }

    @Override
    Frame decompress(Frame frame) throws IOException {
        ByteBuf input = frame.body;

        // TODO: JAVA-1306: Use the same API calls for direct and heap buffers when LZ4 updated.
        ByteBuf frameBody = input.isDirect() ? decompressDirect(input) : decompressHeap(input);
        return frame.with(frameBody);
    }

    private ByteBuf decompressDirect(ByteBuf input) throws IOException {
        // If the input is direct we will allocate a direct output buffer as well as this will allow us to use
        // LZ4Compressor.decompress and so eliminate memory copies.
        int readable = input.readableBytes();
        int uncompressedLength = input.readInt();
        ByteBuffer in = inputNioBuffer(input);
        // Increase reader index.
        input.readerIndex(input.writerIndex());
        ByteBuf output = input.alloc().directBuffer(uncompressedLength);
        try {
            ByteBuffer out = outputNioBuffer(output);
            int read = decompressor.decompress(in, in.position(), out, out.position(), out.remaining());
            if (read != readable - INTEGER_BYTES)
                throw new IOException("Compressed lengths mismatch");

            // Set the writer index so the amount of written bytes is reflected
            output.writerIndex(output.writerIndex() + uncompressedLength);
        } catch (Exception e) {
            // release output buffer so we not leak and rethrow exception.
            output.release();
            throw new IOException(e);
        }
        return output;
    }

    private ByteBuf decompressHeap(ByteBuf input) throws IOException {
        // Not a direct buffer so use byte arrays...
        byte[] in = input.array();
        int len = input.readableBytes();
        int uncompressedLength = input.readInt();
        int inOffset = input.arrayOffset() + input.readerIndex();
        // Increase reader index.
        input.readerIndex(input.writerIndex());

        // Allocate a heap buffer from the ByteBufAllocator as we may use a PooledByteBufAllocator and so
        // can eliminate the overhead of allocate a new byte[].
        ByteBuf output = input.alloc().heapBuffer(uncompressedLength);
        try {
            int offset = output.arrayOffset() + output.writerIndex();
            byte out[] = output.array();
            int read = decompressor.decompress(in, inOffset, out, offset, uncompressedLength);
            if (read != len - INTEGER_BYTES)
                throw new IOException("Compressed lengths mismatch");

            // Set the writer index so the amount of written bytes is reflected
            output.writerIndex(output.writerIndex() + uncompressedLength);
        } catch (Exception e) {
            // release output buffer so we not leak and rethrow exception.
            output.release();
            throw new IOException(e);
        }
        return output;
    }
}
