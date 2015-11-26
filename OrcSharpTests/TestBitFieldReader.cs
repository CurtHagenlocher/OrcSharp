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
    using org.apache.hadoop.hive.ql.io.orc.external;
    using Xunit;

    public class TestBitFieldReader
    {

        public void runSeekTest(CompressionCodec codec)
        {
            TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
            const int COUNT = 16384;
            BitFieldWriter @out = new BitFieldWriter(
                new OutStream("test", 500, codec, collect), 1);
            TestInStream.PositionCollector[] positions =
                new TestInStream.PositionCollector[COUNT];
            for (int i = 0; i < COUNT; ++i)
            {
                positions[i] = new TestInStream.PositionCollector();
                @out.getPosition(positions[i]);
                // test runs, non-runs
                if (i < COUNT / 2)
                {
                    @out.write(i & 1);
                }
                else
                {
                    @out.write((i / 3) & 1);
                }
            }
            @out.flush();
            ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
            collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
            inBuf.flip();
#pragma warning disable 612
            BitFieldReader @in = new BitFieldReader(InStream.create(null, "test",
                new ByteBuffer[] { inBuf }, new long[] { 0 }, inBuf.remaining(),
                codec, 500), 1);
#pragma warning restore 612
            for (int i = 0; i < COUNT; ++i)
            {
                int x = @in.next();
                if (i < COUNT / 2)
                {
                    Assert.Equal(i & 1, x);
                }
                else
                {
                    Assert.Equal((i / 3) & 1, x);
                }
            }
            for (int i = COUNT - 1; i >= 0; --i)
            {
                @in.seek(positions[i]);
                int x = @in.next();
                if (i < COUNT / 2)
                {
                    Assert.Equal(i & 1, x);
                }
                else
                {
                    Assert.Equal((i / 3) & 1, x);
                }
            }
        }

        [Fact]
        public void testUncompressedSeek()
        {
            runSeekTest(null);
        }

#if COMPRESSION
        [Fact]
        public void testCompressedSeek()
        {
            runSeekTest(new ZlibCodec());
        }
#endif

        [Fact]
        public void testBiggerItems()
        {
            TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
            const int COUNT = 16384;
            BitFieldWriter @out = new BitFieldWriter(
                new OutStream("test", 500, null, collect), 3);
            for (int i = 0; i < COUNT; ++i)
            {
                // test runs, non-runs
                if (i < COUNT / 2)
                {
                    @out.write(i & 7);
                }
                else
                {
                    @out.write((i / 3) & 7);
                }
            }
            @out.flush();
            ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
            collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
            inBuf.flip();
#pragma warning disable 612
            BitFieldReader @in = new BitFieldReader(InStream.create(null, "test",
                new ByteBuffer[] { inBuf }, new long[] { 0 }, inBuf.remaining(),
                null, 500), 3);
#pragma warning restore 612
            for (int i = 0; i < COUNT; ++i)
            {
                int x = @in.next();
                if (i < COUNT / 2)
                {
                    Assert.Equal(i & 7, x);
                }
                else
                {
                    Assert.Equal((i / 3) & 7, x);
                }
            }
        }

        [Fact]
        public void testSkips()
        {
            TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
            BitFieldWriter @out = new BitFieldWriter(
                new OutStream("test", 100, null, collect), 1);
            const int COUNT = 16384;
            for (int i = 0; i < COUNT; ++i)
            {
                if (i < COUNT / 2)
                {
                    @out.write(i & 1);
                }
                else
                {
                    @out.write((i / 3) & 1);
                }
            }
            @out.flush();
            ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
            collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
            inBuf.flip();
#pragma warning disable 612
            BitFieldReader @in = new BitFieldReader(InStream.create
                (null, "test", new ByteBuffer[] { inBuf }, new long[] { 0 }, inBuf.remaining(),
                    null, 100), 1);
#pragma warning restore 612
            for (int i = 0; i < COUNT; i += 5)
            {
                int x = (int)@in.next();
                if (i < COUNT / 2)
                {
                    Assert.Equal(i & 1, x);
                }
                else
                {
                    Assert.Equal((i / 3) & 1, x);
                }
                if (i < COUNT - 5)
                {
                    @in.skip(4);
                }
                @in.skip(0);
            }
        }
    }
}
