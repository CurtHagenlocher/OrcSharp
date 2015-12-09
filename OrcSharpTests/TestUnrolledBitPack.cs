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

namespace OrcSharpTests
{
    using System;
    using System.IO;
    using OrcSharp;
    using OrcSharp.Serialization;
    using Xunit;

    public class TestUnrolledBitPack : OrcTestBase
    {
        const string testFileName = "TestUnrolledBitPack.orc";

        public TestUnrolledBitPack() : base(testFileName)
        {
        }

        [Theory]
        [InlineData(-1L)]
        [InlineData(1L)]
        [InlineData(7L)]
        [InlineData(-128L)]
        [InlineData(32000L)]
        [InlineData(8300000L)]
        [InlineData((long)Int32.MaxValue)]
        [InlineData(540000000000L)]
        [InlineData(140000000000000L)]
        [InlineData(36000000000000000L)]
        [InlineData(Int64.MaxValue)]
        public void testBitPacking(long val)
        {
            long[] input = new long[]
            {
                val, 0, val, val, 0, val, 0, val, val, 0, val, 0, val, val, 0, 0,
                val, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val,
                0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0,
                0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0,
                val, 0, val, 0, 0, val, 0, val, 0, 0, val, val
            };

            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));
            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .inspector(inspector)
                .stripeSize(100000)
                .compress(CompressionKind.NONE)
                .bufferSize(10000)))
            {
                foreach (long l in input)
                {
                    writer.addRow(l);
                }
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            using (RecordReader rows = reader.rows())
            {
                int idx = 0;
                while (rows.hasNext())
                {
                    object row = rows.next();
                    Assert.Equal(input[idx++], ((long)row));
                }
            }
        }
    }
}
