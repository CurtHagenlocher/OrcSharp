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
    using System.IO;
    using System.Linq;
    using Xunit;
    using OrcSharp.External;
    using System.Collections.Generic;
    using Serialization;

    public class TestBitPack : WithLocalDirectory
    {
        private const int SIZE = 100;
        private static readonly Random rand = new Random(100);
        const string testFileName = "TestNewIntegerEncoding.orc";

        public TestBitPack()
            : base(testFileName)
        {
        }

        private long[] deltaEncode(long[] inp)
        {
            long[] output = new long[inp.Length];
            SerializationUtils utils = new SerializationUtils();
            for (int i = 0; i < inp.Length; i++)
            {
                output[i] = utils.zigzagEncode(inp[i]);
            }
            return output;
        }

        private long nextLong(Random rng, long n)
        {
            byte[] tmp = new byte[8];
            long bits, val;
            do
            {
                rng.NextBytes(tmp);
                bits = (long)((ulong)(BitConverter.ToInt64(tmp, 0) << 1) >> 1);
                val = bits % n;
            } while (bits - val + (n - 1) < 0L);
            return val;
        }

        private void runTest(int numBits)
        {
            long[] inp = new long[SIZE];
            for (int i = 0; i < SIZE; i++)
            {
                long val = 0;
                if (numBits <= 32)
                {
                    if (numBits == 1)
                    {
                        val = -1 * rand.Next(2);
                    }
                    else
                    {
                        int max = (numBits == 32) ? Int32.MaxValue : (int)Math.Pow(2, numBits - 1);
                        val = rand.Next(max);
                    }
                }
                else
                {
                    val = nextLong(rand, (long)Math.Pow(2, numBits - 2));
                }
                if (val % 2 == 0)
                {
                    val = -val;
                }
                inp[i] = val;
            }
            long[] deltaEncoded = deltaEncode(inp);
            long minInput = deltaEncoded.Min();
            long maxInput = deltaEncoded.Max();
            long rangeInput = maxInput - minInput;
            SerializationUtils utils = new SerializationUtils();
            int fixedWidth = utils.findClosestNumBits(rangeInput);
            TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
            OutStream output = new OutStream("test", SIZE, null, collect);
            utils.writeInts(deltaEncoded, 0, deltaEncoded.Length, fixedWidth, output);
            output.Flush();
            ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
            collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
            inBuf.flip();
            long[] buff = new long[SIZE];
#pragma warning disable 612
            utils.readInts(buff, 0, SIZE, fixedWidth, InStream.create(null, "test", new ByteBuffer[] { inBuf },
                new long[] { 0 }, inBuf.remaining(), null, SIZE));
#pragma warning restore 612
            for (int i = 0; i < SIZE; i++)
            {
                buff[i] = utils.zigzagDecode(buff[i]);
            }
            Assert.Equal(numBits, fixedWidth);
            Assert.Equal(inp, buff);
        }

        [Fact]
        public void test01BitPacking1Bit()
        {
            runTest(1);
        }

        [Fact]
        public void test02BitPacking2Bit()
        {
            runTest(2);
        }

        [Fact]
        public void test03BitPacking3Bit()
        {
            runTest(3);
        }

        [Fact]
        public void test04BitPacking4Bit()
        {
            runTest(4);
        }

        [Fact]
        public void test05BitPacking5Bit()
        {
            runTest(5);
        }

        [Fact]
        public void test06BitPacking6Bit()
        {
            runTest(6);
        }

        [Fact]
        public void test07BitPacking7Bit()
        {
            runTest(7);
        }

        [Fact]
        public void test08BitPacking8Bit()
        {
            runTest(8);
        }

        [Fact]
        public void test09BitPacking9Bit()
        {
            runTest(9);
        }

        [Fact]
        public void test10BitPacking10Bit()
        {
            runTest(10);
        }

        [Fact]
        public void test11BitPacking11Bit()
        {
            runTest(11);
        }

        [Fact]
        public void test12BitPacking12Bit()
        {
            runTest(12);
        }

        [Fact]
        public void test13BitPacking13Bit()
        {
            runTest(13);
        }

        [Fact]
        public void test14BitPacking14Bit()
        {
            runTest(14);
        }

        [Fact]
        public void test15BitPacking15Bit()
        {
            runTest(15);
        }

        [Fact]
        public void test16BitPacking16Bit()
        {
            runTest(16);
        }

        [Fact]
        public void test17BitPacking17Bit()
        {
            runTest(17);
        }

        [Fact]
        public void test18BitPacking18Bit()
        {
            runTest(18);
        }

        [Fact]
        public void test19BitPacking19Bit()
        {
            runTest(19);
        }

        [Fact]
        public void test20BitPacking20Bit()
        {
            runTest(20);
        }

        [Fact]
        public void test21BitPacking21Bit()
        {
            runTest(21);
        }

        [Fact]
        public void test22BitPacking22Bit()
        {
            runTest(22);
        }

        [Fact]
        public void test23BitPacking23Bit()
        {
            runTest(23);
        }

        [Fact]
        public void test24BitPacking24Bit()
        {
            runTest(24);
        }

        [Fact]
        public void test26BitPacking26Bit()
        {
            runTest(26);
        }

        [Fact]
        public void test28BitPacking28Bit()
        {
            runTest(28);
        }

        [Fact]
        public void test30BitPacking30Bit()
        {
            runTest(30);
        }

        [Fact]
        public void test32BitPacking32Bit()
        {
            runTest(32);
        }

        [Fact]
        public void test40BitPacking40Bit()
        {
            runTest(40);
        }

        [Fact]
        public void test48BitPacking48Bit()
        {
            runTest(48);
        }

        [Fact]
        public void test56BitPacking56Bit()
        {
            runTest(56);
        }

        [Fact]
        public void test64BitPacking64Bit()
        {
            runTest(64);
        }

        [Fact]
        public void testBitPack64Large()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            const int size = 1080832;
            long[] inp = new long[size];
            Random rand = new Random(1234);
            for (int i = 0; i < size; i++)
            {
                inp[i] = rand.NextLong();
            }
            List<long> input = inp.ToList();

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .inspector(inspector)
                .compress(CompressionKind.ZLIB)))
            {
                foreach (long l in input)
                {
                    writer.addRow(l);
                }
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            int idx = 0;
            while (rows.hasNext())
            {
                object row = rows.next();
                Assert.Equal(input[idx++], ((long)row));
            }
        }
    }
}
