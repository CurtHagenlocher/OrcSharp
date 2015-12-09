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

namespace OrcSharp
{
    using System;
    using System.IO.Compression;
    using OrcSharp.External;

    class ZlibCodec : CompressionCodec
    {
        private readonly CompressionLevel level;

        public ZlibCodec(CompressionLevel level = CompressionLevel.Optimal)
        {
            this.level = level;
        }

        public bool compress(ByteBuffer @in, ByteBuffer @out, ByteBuffer overflow)
        {
            using (ByteBufferOutputStream output = new ByteBufferOutputStream(@out, overflow))
            {
                using (DeflateStream deflater = new DeflateStream(output, level))
                using (ByteBufferInputStream input = new ByteBufferInputStream(@in.duplicate()))
                {
                    input.CopyTo(deflater);
                }
                return output.Okay;
            }
        }

        public void decompress(ByteBuffer @in, ByteBuffer @out)
        {
            using (ByteBufferInputStream input = new ByteBufferInputStream(@in.duplicate()))
            using (DeflateStream inflater = new DeflateStream(input, CompressionMode.Decompress))
            using (ByteBufferOutputStream output = new ByteBufferOutputStream(@out))
            {
                inflater.CopyTo(output);
            }
            @out.flip();
            @in.position(@in.limit());
        }

        public CompressionCodec modify(CompressionModifier[] modifiers)
        {
            if (modifiers == null)
            {
                return this;
            }

            CompressionLevel l = this.level;

            foreach (CompressionModifier m in modifiers)
            {
                switch (m)
                {
                    case CompressionModifier.BINARY:
                        /* filtered == less LZ77, more huffman */
                        /* not supported */
                        break;
                    case CompressionModifier.TEXT:
                        /* not supported */
                        break;
                    case CompressionModifier.FASTEST:
                        // deflate_fast looking for 8 byte patterns
                        l = CompressionLevel.Fastest;
                        break;
                    case CompressionModifier.FAST:
                        // deflate_fast looking for 16 byte patterns
                        l = CompressionLevel.Fastest;
                        break;
                    case CompressionModifier.DEFAULT:
                        // deflate_slow looking for 128 byte patterns
                        l = CompressionLevel.Optimal;
                        break;
                    default:
                        break;
                }
            }
            return new ZlibCodec(l);
        }

        class ByteBufferInputStream : InputStream
        {
            private readonly ByteBuffer[] buffers;
            private int bufferIndex;

            public ByteBufferInputStream(params ByteBuffer[] buffers)
            {
                this.buffers = buffers;
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                while (bufferIndex < buffers.Length)
                {
                    ByteBuffer bb = buffers[bufferIndex];
                    int length = bb == null ? 0 : bb.remaining();
                    if (length > 0)
                    {
                        length = Math.Min(count, length);
                        Array.Copy(bb.array(), bb.arrayOffset() + bb.position(), buffer, offset, length);
                        bb.position(bb.position() + length);
                        return length;
                    }
                    bufferIndex++;
                }
                return 0;
            }
        }

        class ByteBufferOutputStream : OutputStream
        {
            private readonly ByteBuffer[] buffers;
            private int bufferIndex;
            private bool okay;

            public ByteBufferOutputStream(params ByteBuffer[] buffers)
            {
                this.buffers = buffers;
                this.okay = true;
            }

            public bool Okay
            {
                get { return okay; }
            }

            public override void Flush()
            {
            }

            public override long Position
            {
                get { throw new NotImplementedException(); }
                set { throw new NotImplementedException(); }
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                while (bufferIndex < buffers.Length && count > 0)
                {
                    ByteBuffer bb = buffers[bufferIndex];
                    int length = bb == null ? 0 : bb.remaining();
                    if (length > 0)
                    {
                        length = Math.Min(count, length);
                        Array.Copy(buffer, offset, bb.array(), bb.arrayOffset() + bb.position(), length);
                        count -= length;
                        offset += length;
                        bb.position(bb.position() + length);
                    }
                    else
                    {
                        bufferIndex++;
                    }
                }
                okay = count == 0;
            }
        }
    }
}
