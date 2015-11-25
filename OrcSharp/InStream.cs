/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace org.apache.hadoop.hive.ql.io.orc
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Text;
    using Google.ProtocolBuffers;
    using org.apache.hadoop.hive.ql.io.orc.external;

    public abstract class InStream : InputStream
    {
        private static Log LOG = LogFactory.getLog(typeof(InStream));
        private const int PROTOBUF_MESSAGE_MAX_LIMIT = 1024 << 20; // 1GB

        protected long? fileId;
        protected string name;
        protected long length;

        public InStream(long? fileId, string name, long length)
        {
            this.fileId = fileId;
            this.name = name;
            this.length = length;
        }

        public string getStreamName()
        {
            return name;
        }

        public long getStreamLength()
        {
            return length;
        }

        public abstract int available();

        public long skip(long len)
        {
            throw new NotImplementedException();
        }

        internal class UncompressedStream : InStream
        {
            private List<DiskRange> bytes;
            protected long currentOffset;
            private ByteBuffer range;
            private int currentRange;

            public UncompressedStream(long? fileId, string name, List<DiskRange> input, long length)
                : base(fileId, name, length)
            {
                reset(input, length);
            }

            protected void reset(List<DiskRange> input, long length)
            {
                this.bytes = input;
                this.length = length;
                currentRange = 0;
                currentOffset = 0;
                range = null;
            }

            public override int ReadByte()
            {
                if (range == null || range.remaining() == 0)
                {
                    if (currentOffset == length)
                    {
                        return -1;
                    }
                    seek(currentOffset);
                }
                currentOffset += 1;
                return 0xff & range.get();
            }

            public override int Read(byte[] data, int offset, int length)
            {
                if (range == null || range.remaining() == 0)
                {
                    if (currentOffset == this.length)
                    {
                        return -1;
                    }
                    seek(currentOffset);
                }
                int actualLength = Math.Min(length, range.remaining());
                range.get(data, offset, actualLength);
                currentOffset += actualLength;
                return actualLength;
            }

            public override int available()
            {
                if (range != null && range.remaining() > 0)
                {
                    return range.remaining();
                }
                return (int)(length - currentOffset);
            }

            public void close()
            {
                currentRange = bytes.Count;
                currentOffset = length;
                // explicit de-ref of bytes[]
                bytes.Clear();
            }

            public override void seek(PositionProvider index)
            {
                seek((long)index.getNext());
            }

            public void seek(long desired)
            {
                if (desired == 0 && bytes.Count == 0)
                {
                    logEmptySeek(name);
                    return;
                }
                int i = 0;
                foreach (DiskRange curRange in bytes)
                {
                    if (desired == 0 && curRange.getData().remaining() == 0)
                    {
                        logEmptySeek(name);
                        return;
                    }
                    if (curRange.getOffset() <= desired &&
                        (desired - curRange.getOffset()) < curRange.getLength())
                    {
                        currentOffset = desired;
                        currentRange = i;
                        this.range = curRange.getData().duplicate();
                        int pos = range.position();
                        pos += (int)(desired - curRange.getOffset()); // this is why we duplicate
                        this.range.position(pos);
                        return;
                    }
                    ++i;
                }
                // if they are seeking to the precise end, go ahead and let them go there
                int segments = bytes.Count;
                if (segments != 0 && desired == bytes[segments - 1].getEnd())
                {
                    currentOffset = desired;
                    currentRange = segments - 1;
                    DiskRange curRange = bytes[currentRange];
                    this.range = curRange.getData().duplicate();
                    int pos = range.position();
                    pos += (int)(desired - curRange.getOffset()); // this is why we duplicate
                    this.range.position(pos);
                    return;
                }
                throw new ArgumentException("Seek in " + name + " to " +
                  desired + " is outside of the data");
            }

            public override String ToString()
            {
                return "uncompressed stream " + name + " position: " + currentOffset +
                    " length: " + length + " range: " + currentRange +
                    " offset: " + (range == null ? 0 : range.position()) + " limit: " + (range == null ? 0 : range.limit());
            }
        }

        private static ByteBuffer allocateBuffer(int size, bool isDirect)
        {
            // TODO: use the same pool as the ORC readers
            if (isDirect)
            {
                return ByteBuffer.allocateDirect(size);
            }
            else
            {
                return ByteBuffer.allocate(size);
            }
        }

        private class CompressedStream : InStream
        {
            private List<DiskRange> bytes;
            private int bufferSize;
            private ByteBuffer uncompressed;
            private CompressionCodec codec;
            private ByteBuffer compressed;
            private long currentOffset;
            private int currentRange;
            private bool isUncompressedOriginal;

            public CompressedStream(long fileId, string name, List<DiskRange> input, long length,
                                    CompressionCodec codec, int bufferSize)
                : base(fileId, name, length)
            {
                this.bytes = input;
                this.codec = codec;
                this.bufferSize = bufferSize;
                currentOffset = 0;
                currentRange = 0;
            }

            private void allocateForUncompressed(int size, bool isDirect)
            {
                uncompressed = allocateBuffer(size, isDirect);
            }

            private void readHeader()
            {
                if (compressed == null || compressed.remaining() <= 0)
                {
                    seek(currentOffset);
                }
                if (compressed.remaining() > OutStream.HEADER_SIZE)
                {
                    int b0 = compressed.get() & 0xff;
                    int b1 = compressed.get() & 0xff;
                    int b2 = compressed.get() & 0xff;
                    bool isOriginal = (b0 & 0x01) == 1;
                    int chunkLength = (b2 << 15) | (b1 << 7) | (b0 >> 1);

                    if (chunkLength > bufferSize)
                    {
                        throw new ArgumentException("Buffer size too small. size = " +
                            bufferSize + " needed = " + chunkLength);
                    }
                    // read 3 bytes, which should be equal to OutStream.HEADER_SIZE always
                    Debug.Assert(OutStream.HEADER_SIZE == 3, "The Orc HEADER_SIZE must be the same in OutStream and InStream");
                    currentOffset += OutStream.HEADER_SIZE;

                    ByteBuffer slice = this.slice(chunkLength);

                    if (isOriginal)
                    {
                        uncompressed = slice;
                        isUncompressedOriginal = true;
                    }
                    else
                    {
                        if (isUncompressedOriginal)
                        {
                            allocateForUncompressed(bufferSize, slice.isDirect());
                            isUncompressedOriginal = false;
                        }
                        else if (uncompressed == null)
                        {
                            allocateForUncompressed(bufferSize, slice.isDirect());
                        }
                        else
                        {
                            uncompressed.clear();
                        }
#if COMPRESSION
                        codec.decompress(slice, uncompressed);
#endif
                    }
                }
                else
                {
                    throw new InvalidOperationException("Can't read header at " + this);
                }
            }

            public override int ReadByte()
            {
                if (uncompressed == null || uncompressed.remaining() == 0)
                {
                    if (currentOffset == length)
                    {
                        return -1;
                    }
                    readHeader();
                }
                return 0xff & uncompressed.get();
            }

            public override int Read(byte[] data, int offset, int length)
            {
                if (uncompressed == null || uncompressed.remaining() == 0)
                {
                    if (currentOffset == this.length)
                    {
                        return -1;
                    }
                    readHeader();
                }
                int actualLength = Math.Min(length, uncompressed.remaining());
                uncompressed.get(data, offset, actualLength);
                return actualLength;
            }

            public override int available()
            {
                if (uncompressed == null || uncompressed.remaining() == 0)
                {
                    if (currentOffset == length)
                    {
                        return 0;
                    }
                    readHeader();
                }
                return uncompressed.remaining();
            }

            public void close()
            {
                uncompressed = null;
                compressed = null;
                currentRange = bytes.Count;
                currentOffset = length;
                bytes.Clear();
            }

            public override void seek(PositionProvider index)
            {
                seek((long)index.getNext());
                long uncompressedBytes = (long)index.getNext();
                if (uncompressedBytes != 0)
                {
                    readHeader();
                    uncompressed.position(uncompressed.position() +
                                          (int)uncompressedBytes);
                }
                else if (uncompressed != null)
                {
                    // mark the uncompressed buffer as done
                    uncompressed.position(uncompressed.limit());
                }
            }

            /* slices a read only contiguous buffer of chunkLength */
            private ByteBuffer slice(int chunkLength)
            {
                int len = chunkLength;
                long oldOffset = currentOffset;
                ByteBuffer slice;
                if (compressed.remaining() >= len)
                {
                    slice = compressed.slice();
                    // simple case
                    slice.limit(len);
                    currentOffset += len;
                    compressed.position(compressed.position() + len);
                    return slice;
                }
                else if (currentRange >= (bytes.Count - 1))
                {
                    // nothing has been modified yet
                    throw new IOException("EOF in " + this + " while trying to read " +
                        chunkLength + " bytes");
                }

                if (LOG.isDebugEnabled())
                {
                    LOG.debug(String.Format(
                        "Crossing into next BufferChunk because compressed only has %d bytes (needs %d)",
                        compressed.remaining(), len));
                }

                // we need to consolidate 2 or more buffers into 1
                // first copy out compressed buffers
                ByteBuffer copy = allocateBuffer(chunkLength, compressed.isDirect());
                currentOffset += compressed.remaining();
                len -= compressed.remaining();
                copy.put(compressed);

                for (int i = currentRange; i < bytes.Count && len > 0; i++)
                {
                    ++currentRange;
                    if (LOG.isDebugEnabled())
                    {
                        LOG.debug(String.Format("Read slow-path, >1 cross block reads with {0}", this.ToString()));
                    }
                    DiskRange range = bytes[i];
                    compressed = range.getData().duplicate();
                    if (compressed.remaining() >= len)
                    {
                        slice = compressed.slice();
                        slice.limit(len);
                        copy.put(slice);
                        currentOffset += len;
                        compressed.position(compressed.position() + len);
                        return copy;
                    }
                    currentOffset += compressed.remaining();
                    len -= compressed.remaining();
                    copy.put(compressed);
                }

                // restore offsets for exception clarity
                seek(oldOffset);
                throw new IOException("EOF in " + this + " while trying to read " +
                    chunkLength + " bytes");
            }

            private void seek(long desired)
            {
                if (desired == 0 && bytes.Count == 0)
                {
                    logEmptySeek(name);
                    return;
                }
                int i = 0;
                foreach (DiskRange range in bytes)
                {
                    if (range.getOffset() <= desired && desired < range.getEnd())
                    {
                        currentRange = i;
                        compressed = range.getData().duplicate();
                        int pos = compressed.position();
                        pos += (int)(desired - range.getOffset());
                        compressed.position(pos);
                        currentOffset = desired;
                        return;
                    }
                    ++i;
                }
                // if they are seeking to the precise end, go ahead and let them go there
                int segments = bytes.Count;
                if (segments != 0 && desired == bytes[segments - 1].getEnd())
                {
                    DiskRange range = bytes[segments - 1];
                    currentRange = segments - 1;
                    compressed = range.getData().duplicate();
                    compressed.position(compressed.limit());
                    currentOffset = desired;
                    return;
                }
                throw new IOException("Seek outside of data in " + this + " to " + desired);
            }

            private String rangeString()
            {
                StringBuilder builder = new StringBuilder();
                int i = 0;
                foreach (DiskRange range in bytes)
                {
                    if (i != 0)
                    {
                        builder.Append("; ");
                    }
                    builder.Append(" range " + i + " = " + range.getOffset()
                        + " to " + (range.getEnd() - range.getOffset()));
                    ++i;
                }
                return builder.ToString();
            }

            public override String ToString()
            {
                return "compressed stream " + name + " position: " + currentOffset +
                    " length: " + length + " range: " + currentRange +
                    " offset: " + (compressed == null ? 0 : compressed.position()) + " limit: " + (compressed == null ? 0 : compressed.limit()) +
                    rangeString() +
                    (uncompressed == null ? "" :
                        " uncompressed: " + uncompressed.position() + " to " +
                            uncompressed.limit());
            }
        }

        public abstract void seek(PositionProvider index);

        private static void logEmptySeek(String name)
        {
            if (LOG.isWarnEnabled())
            {
                LOG.warn("Attempting seek into empty stream (" + name + ") Skipping stream.");
            }
        }

#if VISIBLE_FOR_TESTING
        /**
         * Create an input stream from a list of buffers.
         * @param fileName name of the file
         * @param streamName the name of the stream
         * @param buffers the list of ranges of bytes for the stream
         * @param offsets a list of offsets (the same length as input) that must
         *                contain the first offset of the each set of bytes in input
         * @param length the length in bytes of the stream
         * @param codec the compression codec
         * @param bufferSize the compression buffer size
         * @return an input stream
         * @
         */
        // VisibleForTesting
        [Obsolete]
        internal static InStream create(long? fileId,
                                String streamName,
                                ByteBuffer[] buffers,
                                long[] offsets,
                                long length,
                                CompressionCodec codec,
                                int bufferSize)
        {
            List<DiskRange> input = new List<DiskRange>(buffers.Length);
            for (int i = 0; i < buffers.Length; ++i)
            {
                input.Add(new RecordReaderImpl.BufferChunk(buffers[i], offsets[i]));
            }
            return create(fileId, streamName, input, length, codec, bufferSize);
        }
#endif

        /**
         * Create an input stream from a list of disk ranges with data.
         * @param name the name of the stream
         * @param input the list of ranges of bytes for the stream; from disk or cache
         * @param length the length in bytes of the stream
         * @param codec the compression codec
         * @param bufferSize the compression buffer size
         * @param cache Low-level cache to use to put data, if any. Only works with compressed streams.
         * @return an input stream
         * @
         */
        public static InStream create(long? fileId,
                                      string name,
                                      List<DiskRange> input,
                                      long length,
                                      CompressionCodec codec,
                                      int bufferSize)
        {
            if (codec == null)
            {
                return new UncompressedStream(fileId, name, input, length);
            }
            else
            {
                return new CompressedStream(fileId.Value, name, input, length, codec, bufferSize);
            }
        }

        /**
         * Creates coded input stream (used for protobuf message parsing) with higher message size limit.
         *
         * @param name       the name of the stream
         * @param input      the list of ranges of bytes for the stream; from disk or cache
         * @param length     the length in bytes of the stream
         * @param codec      the compression codec
         * @param bufferSize the compression buffer size
         * @return coded input stream
         * @
         */
        public static CodedInputStream createCodedInputStream(long? fileId,
            String name,
            List<DiskRange> input,
            long length,
            CompressionCodec codec,
            int bufferSize)
        {
            InStream inStream = create(fileId, name, input, length, codec, bufferSize);
            CodedInputStream codedInputStream = CodedInputStream.CreateInstance(inStream);
            codedInputStream.SetSizeLimit(PROTOBUF_MESSAGE_MAX_LIMIT);
            return codedInputStream;
        }
    }
}
