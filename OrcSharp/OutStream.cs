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
    using System.IO;
    using org.apache.hadoop.hive.ql.io.orc.external;

    public class OutStream : PositionedOutputStream
    {
        public interface OutputReceiver
        {
            /**
             * Output the given buffer to the destination
             * @param buffer the buffer to output
             * @
             */
            void output(byte[] buffer);
        }

        public const int HEADER_SIZE = 3;
        private string name;
        private OutputReceiver receiver;
        // if enabled the stream will be suppressed when writing stripe
        private bool _suppress;

        /**
         * Stores the uncompressed bytes that have been serialized, but not
         * compressed yet. When this fills, we compress the entire buffer.
         */
        private MemoryStream current = null;

        /**
         * Stores the compressed bytes until we have a full buffer and then outputs
         * them to the receiver. If no compression is being done, this (and overflow)
         * will always be null and the current buffer will be sent directly to the
         * receiver.
         */
        private MemoryStream compressed = null;

        /**
         * Since the compressed buffer may start with contents from previous
         * compression blocks, we allocate an overflow buffer so that the
         * output of the codec can be split between the two buffers. After the
         * compressed buffer is sent to the receiver, the overflow buffer becomes
         * the new compressed buffer.
         */
        private MemoryStream overflow = null;
        private int _bufferSize;
        private CompressionCodec codec;
        private long compressedBytes = 0;
        private long uncompressedBytes = 0;

        public OutStream(
            string name,
            int bufferSize,
            CompressionCodec codec,
            OutputReceiver receiver)
        {
            this.name = name;
            this._bufferSize = bufferSize;
            this.codec = codec;
            this.receiver = receiver;
            this._suppress = false;
        }

        public override long bufferSize
        {
            get { return _bufferSize; }
        }

        public void clear()
        {
            Flush();
            _suppress = false;
        }

        public override long Position
        {
            get { throw new NotImplementedException(); }
            set { throw new NotImplementedException(); }
        }

        /**
         * Write the length of the compressed bytes. Life is much easier if the
         * header is constant length, so just use 3 bytes. Considering most of the
         * codecs want between 32k (snappy) and 256k (lzo, zlib), 3 bytes should
         * be plenty. We also use the low bit for whether it is the original or
         * compressed bytes.
         * @param buffer the buffer to write the header to
         * @param position the position in the buffer to write at
         * @param val the size in the file
         * @param original is it uncompressed
         */
        private static void writeHeader(
            MemoryStream buffer,
            int position,
            int val,
            bool original)
        {
            long oldPosition = buffer.Position;
            buffer.Position = position;
            buffer.WriteByte((byte)((val << 1) + (original ? 1 : 0)));
            buffer.WriteByte((byte)(val >> 7));
            buffer.WriteByte((byte)(val >> 15));
            buffer.Position = oldPosition;
        }

        private void getNewInputBuffer()
        {
            if (codec == null)
            {
                current = new MemoryStream(_bufferSize);
            }
            else
            {
                current = new MemoryStream(_bufferSize + HEADER_SIZE);
                writeHeader(current, 0, _bufferSize, true);
                current.Position = HEADER_SIZE;
            }
        }

        /**
         * Allocate a new output buffer if we are compressing.
         */
        private MemoryStream getNewOutputBuffer()
        {
            return new MemoryStream(_bufferSize + HEADER_SIZE);
        }

        private void flip()
        {
            current.Capacity = (int)current.Position;
            current.Position = codec == null ? 0 : HEADER_SIZE;
        }

        public override void WriteByte(byte i)
        {
            if (current == null)
            {
                getNewInputBuffer();
            }
            if (current.remaining() < 1)
            {
                spill();
            }
            uncompressedBytes++;
            current.WriteByte(i);
        }

        public override void Write(byte[] bytes, int offset, int length)
        {
            if (current == null)
            {
                getNewInputBuffer();
            }
            int remaining = Math.Min(current.remaining(), length);
            current.Write(bytes, offset, remaining);
            uncompressedBytes += remaining;
            length -= remaining;
            while (length != 0)
            {
                spill();
                offset += remaining;
                remaining = Math.Min(current.remaining(), length);
                current.Write(bytes, offset, remaining);
                uncompressedBytes += remaining;
                length -= remaining;
            }
        }

        private void spill()
        {
            // if there isn't anything in the current buffer, don't spill
            if (current == null ||
                current.Position == (codec == null ? 0 : HEADER_SIZE))
            {
                return;
            }
            flip();
            if (codec == null)
            {
                receiver.output(current.ToArray());
                getNewInputBuffer();
            }
            else
            {
                if (compressed == null)
                {
                    compressed = getNewOutputBuffer();
                }
                else if (overflow == null)
                {
                    overflow = getNewOutputBuffer();
                }
                int sizePosn = (int)compressed.Position;
                compressed.Position += HEADER_SIZE;
                if (codec.compress(current, compressed, overflow))
                {
                    uncompressedBytes = 0;
                    // move position back to after the header
                    current.Position = HEADER_SIZE;
                    // find the total bytes in the chunk
                    int totalBytes = (int)compressed.Position - sizePosn - HEADER_SIZE;
                    if (overflow != null)
                    {
                        totalBytes += (int)overflow.Position;
                    }
                    compressedBytes += totalBytes + HEADER_SIZE;
                    writeHeader(compressed, sizePosn, totalBytes, false);
                    // if we have less than the next header left, spill it.
                    if (compressed.remaining() < HEADER_SIZE)
                    {
                        compressed.flip();
                        receiver.output(compressed.ToArray());
                        compressed = overflow;
                        overflow = null;
                    }
                }
                else
                {
                    compressedBytes += uncompressedBytes + HEADER_SIZE;
                    uncompressedBytes = 0;
                    // we are using the original, but need to spill the current
                    // compressed buffer first. So back up to where we started,
                    // flip it and add it to done.
                    if (sizePosn != 0)
                    {
                        compressed.Position = sizePosn;
                        compressed.flip();
                        receiver.output(compressed.ToArray());
                        compressed = null;
                        // if we have an overflow, clear it and make it the new compress
                        // buffer
                        if (overflow != null)
                        {
                            overflow.clear();
                            compressed = overflow;
                            overflow = null;
                        }
                    }
                    else
                    {
                        compressed.clear();
                        if (overflow != null)
                        {
                            overflow.clear();
                        }
                    }

                    // now add the current buffer into the done list and get a new one.
                    current.Position = 0;
                    // update the header with the current length
                    writeHeader(current, 0, current.Capacity - HEADER_SIZE, true);
                    receiver.output(current.ToArray());
                    getNewInputBuffer();
                }
            }
        }

        public override void getPosition(PositionRecorder recorder)
        {
            if (codec == null)
            {
                recorder.addPosition(uncompressedBytes);
            }
            else
            {
                recorder.addPosition(compressedBytes);
                recorder.addPosition(uncompressedBytes);
            }
        }

        public override void Flush()
        {
            spill();
            if (compressed != null && compressed.Position != 0)
            {
                compressed.flip();
                receiver.output(compressed.ToArray());
                compressed = null;
            }
            uncompressedBytes = 0;
            compressedBytes = 0;
            overflow = null;
            current = null;
        }

        public override string ToString()
        {
            return name;
        }

        public long getBufferSize()
        {
            long result = 0;
            if (current != null)
            {
                result += current.Capacity;
            }
            if (compressed != null)
            {
                result += compressed.Capacity;
            }
            if (overflow != null)
            {
                result += overflow.Capacity;
            }
            return result;
        }

        /**
         * Set suppress flag
         */
        public void suppress()
        {
            _suppress = true;
        }

        /**
         * Returns the state of suppress flag
         * @return value of suppress flag
         */
        public bool isSuppressed()
        {
            return _suppress;
        }
    }

    static class ByteBufferMethods
    {
        public static void flip(this MemoryStream stream)
        {
            stream.Capacity = (int)stream.Position;
            stream.Position = 0;
        }

        public static int remaining(this MemoryStream stream)
        {
            return stream.Capacity - (int)stream.Position;
        }

        public static void clear(this MemoryStream stream)
        {
            stream.Position = 0;
        }
    }
}
