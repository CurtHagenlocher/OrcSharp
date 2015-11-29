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
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using OrcSharp.External;
    using Xunit;

    public class TestInStream
    {
        internal class OutputCollector : OutStream.OutputReceiver
        {
            internal DynamicByteArray buffer = new DynamicByteArray();

            public void output(byte[] buffer)
            {
                this.buffer.add(buffer);
            }
        }

        internal class PositionCollector : PositionProvider, PositionRecorder
        {
            private List<long> positions = new List<long>();
            private int index = 0;

            public long getNext()
            {
                return positions[index++];
            }

            public void addPosition(long offset)
            {
                positions.Add(offset);
            }

            public void reset()
            {
                index = 0;
            }

            public override string ToString()
            {
                StringBuilder builder = new StringBuilder("position: ");
                for (int i = 0; i < positions.Count; ++i)
                {
                    if (i != 0)
                    {
                        builder.Append(", ");
                    }
                    builder.Append(positions[i]);
                }
                return builder.ToString();
            }
        }

        [Fact]
        public void testUncompressed()
        {
            OutputCollector collect = new OutputCollector();
            OutStream @out = new OutStream("test", 100, null, collect);
            PositionCollector[] positions = new PositionCollector[1024];
            for (int i = 0; i < 1024; ++i)
            {
                positions[i] = new PositionCollector();
                @out.getPosition(positions[i]);
                @out.WriteByte((byte)i);
            }
            @out.Flush();
            Assert.Equal(1024, collect.buffer.size());
            for (int i = 0; i < 1024; ++i)
            {
                Assert.Equal((byte)i, collect.buffer.get(i));
            }
            ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
            collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
            inBuf.flip();
#pragma warning disable 612
            InStream @in = InStream.create(null, "test", new ByteBuffer[] { inBuf },
                new long[] { 0 }, inBuf.remaining(), null, 100);
#pragma warning restore 612
            Assert.Equal("uncompressed stream test position: 0 length: 1024" +
                         " range: 0 offset: 0 limit: 0",
                         @in.ToString());
            for (int i = 0; i < 1024; ++i)
            {
                int x = @in.ReadByte();
                Assert.Equal(i & 0xff, x);
            }
            for (int i = 1023; i >= 0; --i)
            {
                @in.seek(positions[i]);
                Assert.Equal(i & 0xff, @in.ReadByte());
            }
        }

#if false
        [Fact]
        public void testCompressed()
        {
            OutputCollector collect = new OutputCollector();
            CompressionCodec codec = new ZlibCodec();
            OutStream @out = new OutStream("test", 300, codec, collect);
            PositionCollector[] positions = new PositionCollector[1024];
            for (int i = 0; i < 1024; ++i)
            {
                positions[i] = new PositionCollector();
                @out.getPosition(positions[i]);
                @out.WriteByte((byte)i);
            }
            @out.Flush();
            Assert.Equal("test", @out.ToString());
            Assert.Equal(961, collect.buffer.size());
            ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
            collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
            inBuf.flip();
            InStream @in = InStream.create(null, "test", new ByteBuffer[] { inBuf },
                new long[] { 0 }, inBuf.remaining(), codec, 300);
            Assert.Equal("compressed stream test position: 0 length: 961 range: 0" +
                         " offset: 0 limit: 0 range 0 = 0 to 961",
                         @in.ToString());
            for (int i = 0; i < 1024; ++i)
            {
                int x = @in.ReadByte();
                Assert.Equal(i & 0xff, x);
            }
            Assert.Equal(0, @in.available());
            for (int i = 1023; i >= 0; --i)
            {
                @in.seek(positions[i]);
                Assert.Equal(i & 0xff, @in.ReadByte());
            }
        }

        [Fact]
        public void testCorruptStream()
        {
            OutputCollector collect = new OutputCollector();
            CompressionCodec codec = new ZlibCodec();
            OutStream @out = new OutStream("test", 500, codec, collect);
            PositionCollector[] positions = new PositionCollector[1024];
            for (int i = 0; i < 1024; ++i)
            {
                positions[i] = new PositionCollector();
                @out.getPosition(positions[i]);
                @out.WriteByte((byte)i);
            }
            @out.Flush();

            // now try to read the stream with a buffer that is too small
            ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
            collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
            inBuf.flip();
            InStream @in = InStream.create(null, "test", new ByteBuffer[] { inBuf },
                new long[] { 0 }, inBuf.remaining(), codec, 100);
            byte[] contents = new byte[1024];
            try
            {
                @in.Read(contents, 0, contents.Length);
                fail();
            }
            catch (IllegalArgumentException iae)
            {
                // EXPECTED
            }

            // make a corrupted header
            inBuf.clear();
            inBuf.put((byte)32);
            inBuf.put((byte)0);
            inBuf.flip();
            @in = InStream.create(null, "test2", new ByteBuffer[] { inBuf }, new long[] { 0 },
                inBuf.remaining(), codec, 300);
            try
            {
                @in.ReadByte();
                fail();
            }
            catch (InvalidOperationException ise)
            {
                // EXPECTED
            }
        }

        [Fact]
        public void testDisjointBuffers()
        {
            OutputCollector collect = new OutputCollector();
            CompressionCodec codec = new ZlibCodec();
            OutStream @out = new OutStream("test", 400, codec, collect);
            PositionCollector[] positions = new PositionCollector[1024];
            DataOutput stream = new DataOutputStream(@out);
            for (int i = 0; i < 1024; ++i)
            {
                positions[i] = new PositionCollector();
                @out.getPosition(positions[i]);
                stream.writeInt(i);
            }
            @out.Flush();
            Assert.Equal("test", @out.ToString());
            Assert.Equal(1674, collect.buffer.size());
            ByteBuffer[] inBuf = new ByteBuffer[3];
            inBuf[0] = ByteBuffer.allocate(500);
            inBuf[1] = ByteBuffer.allocate(1200);
            inBuf[2] = ByteBuffer.allocate(500);
            collect.buffer.setByteBuffer(inBuf[0], 0, 483);
            collect.buffer.setByteBuffer(inBuf[1], 483, 1625 - 483);
            collect.buffer.setByteBuffer(inBuf[2], 1625, 1674 - 1625);

            for (int i = 0; i < inBuf.Length; ++i)
            {
                inBuf[i].flip();
            }
            InStream @in = InStream.create(null, "test", inBuf,
                new long[] { 0, 483, 1625 }, 1674, codec, 400);
            Assert.Equal("compressed stream test position: 0 length: 1674 range: 0" +
                         " offset: 0 limit: 0 range 0 = 0 to 483;" +
                         "  range 1 = 483 to 1142;  range 2 = 1625 to 49",
                         @in.ToString());
            DataInputStream inStream = new DataInputStream(@in);
            for (int i = 0; i < 1024; ++i)
            {
                int x = inStream.readInt();
                Assert.Equal(i, x);
            }
            Assert.Equal(0, @in.available());
            for (int i = 1023; i >= 0; --i)
            {
                @in.seek(positions[i]);
                Assert.Equal(i, inStream.readInt());
            }

            @in = InStream.create(null, "test", new ByteBuffer[] { inBuf[1], inBuf[2] },
                new long[] { 483, 1625 }, 1674, codec, 400);
            inStream = new DataInputStream(@in);
            positions[303].reset();
            @in.seek(positions[303]);
            for (int i = 303; i < 1024; ++i)
            {
                Assert.Equal(i, inStream.readInt());
            }

            @in = InStream.create(null, "test", new ByteBuffer[] { inBuf[0], inBuf[2] },
                new long[] { 0, 1625 }, 1674, codec, 400);
            inStream = new DataInputStream(@in);
            positions[1001].reset();
            for (int i = 0; i < 300; ++i)
            {
                Assert.Equal(i, inStream.readInt());
            }
            @in.seek(positions[1001]);
            for (int i = 1001; i < 1024; ++i)
            {
                Assert.Equal(i, inStream.readInt());
            }
        }

        [Fact]
        public void testUncompressedDisjointBuffers()
        {
            OutputCollector collect = new OutputCollector();
            OutStream @out = new OutStream("test", 400, null, collect);
            PositionCollector[] positions = new PositionCollector[1024];
            DataOutput stream = new DataOutputStream(@out);
            for (int i = 0; i < 1024; ++i)
            {
                positions[i] = new PositionCollector();
                @out.getPosition(positions[i]);
                stream.writeInt(i);
            }
            @out.Flush();
            Assert.Equal("test", @out.ToString());
            Assert.Equal(4096, collect.buffer.size());
            ByteBuffer[] inBuf = new ByteBuffer[3];
            inBuf[0] = ByteBuffer.allocate(1100);
            inBuf[1] = ByteBuffer.allocate(2200);
            inBuf[2] = ByteBuffer.allocate(1100);
            collect.buffer.setByteBuffer(inBuf[0], 0, 1024);
            collect.buffer.setByteBuffer(inBuf[1], 1024, 2048);
            collect.buffer.setByteBuffer(inBuf[2], 3072, 1024);

            for (int i = 0; i < inBuf.Length; ++i)
            {
                inBuf[i].flip();
            }
            InStream @in = InStream.create(null, "test", inBuf,
                new long[] { 0, 1024, 3072 }, 4096, null, 400);
            Assert.Equal("uncompressed stream test position: 0 length: 4096" +
                         " range: 0 offset: 0 limit: 0",
                         @in.ToString());
            DataInputStream inStream = new DataInputStream(@in);
            for (int i = 0; i < 1024; ++i)
            {
                int x = inStream.readInt();
                Assert.Equal(i, x);
            }
            Assert.Equal(0, @in.available());
            for (int i = 1023; i >= 0; --i)
            {
                @in.seek(positions[i]);
                Assert.Equal(i, inStream.readInt());
            }

            @in = InStream.create(null, "test", new ByteBuffer[] { inBuf[1], inBuf[2] },
                new long[] { 1024, 3072 }, 4096, null, 400);
            inStream = new DataInputStream(@in);
            positions[256].reset();
            @in.seek(positions[256]);
            for (int i = 256; i < 1024; ++i)
            {
                Assert.Equal(i, inStream.readInt());
            }

            @in = InStream.create(null, "test", new ByteBuffer[] { inBuf[0], inBuf[2] },
                new long[] { 0, 3072 }, 4096, null, 400);
            inStream = new DataInputStream(@in);
            positions[768].reset();
            for (int i = 0; i < 256; ++i)
            {
                Assert.Equal(i, inStream.readInt());
            }
            @in.seek(positions[768]);
            for (int i = 768; i < 1024; ++i)
            {
                Assert.Equal(i, inStream.readInt());
            }
        }
#endif
    }
}
