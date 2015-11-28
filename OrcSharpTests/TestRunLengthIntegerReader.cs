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
    using org.apache.hadoop.hive.ql.io.orc.external;
    using Xunit;

    public class TestRunLengthIntegerReader
    {
        public void runSeekTest(CompressionCodec codec)
        {
            TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
            RunLengthIntegerWriter @out = new RunLengthIntegerWriter(
                new OutStream("test", 1000, codec, collect), true);
            TestInStream.PositionCollector[] positions =
                new TestInStream.PositionCollector[4096];
            Random random = new Random(99);
            int[] junk = new int[2048];
            for (int i = 0; i < junk.Length; ++i)
            {
                junk[i] = random.Next();
            }
            for (int i = 0; i < 4096; ++i)
            {
                positions[i] = new TestInStream.PositionCollector();
                @out.getPosition(positions[i]);
                // test runs, incrementing runs, non-runs
                if (i < 1024)
                {
                    @out.write(i / 4);
                }
                else if (i < 2048)
                {
                    @out.write(2 * i);
                }
                else
                {
                    @out.write(junk[i - 2048]);
                }
            }
            @out.flush();
            ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
            collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
            inBuf.flip();
#pragma warning disable 612
            RunLengthIntegerReader @in = new RunLengthIntegerReader(InStream.create
                (null, "test", new ByteBuffer[] { inBuf }, new long[] { 0 }, inBuf.remaining(),
                    codec, 1000), true);
#pragma warning restore 612
            for (int i = 0; i < 2048; ++i)
            {
                int x = (int)@in.next();
                if (i < 1024)
                {
                    Assert.Equal(i / 4, x);
                }
                else if (i < 2048)
                {
                    Assert.Equal(2 * i, x);
                }
                else
                {
                    Assert.Equal(junk[i - 2048], x);
                }
            }
            for (int i = 2047; i >= 0; --i)
            {
                @in.seek(positions[i]);
                int x = (int)@in.next();
                if (i < 1024)
                {
                    Assert.Equal(i / 4, x);
                }
                else if (i < 2048)
                {
                    Assert.Equal(2 * i, x);
                }
                else
                {
                    Assert.Equal(junk[i - 2048], x);
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
        public void testSkips()
        {
            TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
            RunLengthIntegerWriter @out = new RunLengthIntegerWriter(
                new OutStream("test", 100, null, collect), true);
            for (int i = 0; i < 2048; ++i)
            {
                if (i < 1024)
                {
                    @out.write(i);
                }
                else
                {
                    @out.write(256 * i);
                }
            }
            @out.flush();
            ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
            collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
            inBuf.flip();
#pragma warning disable 612
            RunLengthIntegerReader @in = new RunLengthIntegerReader(InStream.create
                (null, "test", new ByteBuffer[] { inBuf }, new long[] { 0 }, inBuf.remaining(),
                    null, 100), true);
#pragma warning restore 612
            for (int i = 0; i < 2048; i += 10)
            {
                int x = (int)@in.next();
                if (i < 1024)
                {
                    Assert.Equal(i, x);
                }
                else
                {
                    Assert.Equal(256 * i, x);
                }
                if (i < 2038)
                {
                    @in.skip(9);
                }
                @in.skip(0);
            }
        }
    }
}
