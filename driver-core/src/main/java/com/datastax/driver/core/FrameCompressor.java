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

import com.datastax.driver.core.exceptions.DriverInternalError;
import io.netty.buffer.Unpooled;
import net.jpountz.lz4.LZ4Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.IOException;

abstract class FrameCompressor {

    private static final Logger logger = LoggerFactory.getLogger(FrameCompressor.class);

    public abstract Frame compress(Frame frame) throws IOException;

    public abstract Frame decompress(Frame frame) throws IOException;

    public static class SnappyCompressor extends FrameCompressor {

        public static final SnappyCompressor instance;

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
        public Frame compress(Frame frame) throws IOException {
            byte[] input = CBUtil.readRawBytes(frame.body);
            byte[] output = new byte[Snappy.maxCompressedLength(input.length)];

            int written = Snappy.compress(input, 0, input.length, output, 0);
            return frame.with(Unpooled.wrappedBuffer(output, 0, written));
        }

        @Override
        public Frame decompress(Frame frame) throws IOException {
            byte[] input = CBUtil.readRawBytes(frame.body);

            if (!Snappy.isValidCompressedBuffer(input, 0, input.length))
                throw new DriverInternalError("Provided frame does not appear to be Snappy compressed");

            byte[] output = new byte[Snappy.uncompressedLength(input)];
            int size = Snappy.uncompress(input, 0, input.length, output, 0);
            return frame.with(Unpooled.wrappedBuffer(output, 0, size));
        }
    }

    public static class LZ4Compressor extends FrameCompressor {

        public static final LZ4Compressor instance;

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
        public Frame compress(Frame frame) throws IOException {
            byte[] input = CBUtil.readRawBytes(frame.body);

            int maxCompressedLength = compressor.maxCompressedLength(input.length);
            byte[] output = new byte[INTEGER_BYTES + maxCompressedLength];

            output[0] = (byte) (input.length >>> 24);
            output[1] = (byte) (input.length >>> 16);
            output[2] = (byte) (input.length >>> 8);
            output[3] = (byte) (input.length);

            try {
                int written = compressor.compress(input, 0, input.length, output, INTEGER_BYTES, maxCompressedLength);
                return frame.with(Unpooled.wrappedBuffer(output, 0, INTEGER_BYTES + written));
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public Frame decompress(Frame frame) throws IOException {
            byte[] input = CBUtil.readRawBytes(frame.body);

            int uncompressedLength = ((input[0] & 0xFF) << 24)
                    | ((input[1] & 0xFF) << 16)
                    | ((input[2] & 0xFF) << 8)
                    | ((input[3] & 0xFF));

            byte[] output = new byte[uncompressedLength];

            try {
                int read = decompressor.decompress(input, INTEGER_BYTES, output, 0, uncompressedLength);
                if (read != input.length - INTEGER_BYTES)
                    throw new IOException("Compressed lengths mismatch");

                return frame.with(Unpooled.wrappedBuffer(output));
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }
}
