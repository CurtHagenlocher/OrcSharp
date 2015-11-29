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
    using System.Collections.Generic;
    using System.IO;
    using System.Runtime.CompilerServices;
    using OrcSharp.Serialization;
    using Xunit;

    public class TestNewIntegerEncoding : WithLocalDirectory
    {
        const string testFileName = "TestNewIntegerEncoding.orc";

        public TestNewIntegerEncoding() : base(testFileName)
        {
        }

#if TIMESTAMP
        public class TSRow
        {
            Timestamp ts;

            public TSRow(Timestamp ts)
            {
                this.ts = ts;
            }
        }
#endif

        public class Row
        {
            int int1;
            long long1;

            public Row(int val, long l)
            {
                this.int1 = val;
                this.long1 = l;
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testBasicRow(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(Row));

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

                writer.addRow(new Row(111, 1111L));
                writer.addRow(new Row(111, 1111L));
                writer.addRow(new Row(111, 1111L));
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            while (rows.hasNext())
            {
                object row = rows.next(null);
                Assert.Equal(111, ((IStrongBox)((OrcStruct)row).getFieldValue(0)).Value);
                Assert.Equal(1111L, ((IStrongBox)((OrcStruct)row).getFieldValue(1)).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testBasicOld(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[]
            {
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6,
                7, 8, 9, 10, 1, 1, 1, 1, 1, 1, 10, 9, 7, 6, 5, 4, 3, 2, 1, 1, 1, 1, 1,
                2, 5, 1, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1,
                9, 2, 6, 3, 7, 1, 9, 2, 6, 2000, 2, 1, 1, 1, 1, 1, 3, 7, 1, 9, 2, 6, 1,
                1, 1, 1, 1
            };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .compress(CompressionKind.NONE)
                    .version(OrcFile.Version.V_0_11)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testBasicNew(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[]
            {
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6,
                7, 8, 9, 10, 1, 1, 1, 1, 1, 1, 10, 9, 7, 6, 5, 4, 3, 2, 1, 1, 1, 1, 1,
                2, 5, 1, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1,
                9, 2, 6, 3, 7, 1, 9, 2, 6, 2000, 2, 1, 1, 1, 1, 1, 3, 7, 1, 9, 2, 6, 1,
                1, 1, 1, 1
            };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testBasicDelta1(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[] { -500, -400, -350, -325, -310 };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testBasicDelta2(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[] { -500, -600, -650, -675, -710 };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testBasicDelta3(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[] { 500, 400, 350, 325, 310 };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testBasicDelta4(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[] { 500, 600, 650, 675, 710 };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testDeltaOverflow(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[]
            {
                4513343538618202719L, 4513343538618202711L, 2911390882471569739L, -9181829309989854913L
            };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testDeltaOverflow2(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[]
            {
                Int64.MaxValue, 4513343538618202711L, 2911390882471569739L, Int64.MinValue
            };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testDeltaOverflow3(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[]
            {
                -4513343538618202711L, -2911390882471569739L, -2, Int64.MaxValue
            };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testIntegerMin(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[] { Int32.MinValue };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testIntegerMax(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[] { Int32.MaxValue };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testLongMin(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[] { Int64.MinValue };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testLongMax(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[] { Int64.MaxValue };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testRandomInt(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            List<long> input = new List<long>();
            Random rand = new Random();
            for (int i = 0; i < 100000; i++)
            {
                input.Add((long)rand.Next());
            }

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testRandomLong(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            List<long> input = new List<long>();
            Random rand = new Random();
            for (int i = 0; i < 100000; i++)
            {
                input.Add(rand.NextLong());
            }

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));
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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBaseNegativeMin(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[]
            {
                20, 2, 3, 2, 1, 3, 17, 71, 35, 2, 1, 139, 2, 2,
                3, 1783, 475, 2, 1, 1, 3, 1, 3, 2, 32, 1, 2, 3, 1, 8, 30, 1, 3, 414, 1,
                1, 135, 3, 3, 1, 414, 2, 1, 2, 2, 594, 2, 5, 6, 4, 11, 1, 2, 2, 1, 1,
                52, 4, 1, 2, 7, 1, 17, 334, 1, 2, 1, 2, 2, 6, 1, 266, 1, 2, 217, 2, 6,
                2, 13, 2, 2, 1, 2, 3, 5, 1, 2, 1, 7244, 11813, 1, 33, 2, -13, 1, 2, 3,
                13, 1, 92, 3, 13, 5, 14, 9, 141, 12, 6, 15, 25, 1, 1, 1, 46, 2, 1, 1,
                141, 3, 1, 1, 1, 1, 2, 1, 4, 34, 5, 78, 8, 1, 2, 2, 1, 9, 10, 2, 1, 4,
                13, 1, 5, 4, 4, 19, 5, 1, 1, 1, 68, 33, 399, 1, 1885, 25, 5, 2, 4, 1,
                1, 2, 16, 1, 2966, 3, 1, 1, 25501, 1, 1, 1, 66, 1, 3, 8, 131, 14, 5, 1,
                2, 2, 1, 1, 8, 1, 1, 2, 1, 5, 9, 2, 3, 112, 13, 2, 2, 1, 5, 10, 3, 1,
                1, 13, 2, 3, 4, 1, 3, 1, 1, 2, 1, 1, 2, 4, 2, 207, 1, 1, 2, 4, 3, 3, 2,
                2, 16
            };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBaseNegativeMin2(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[]
            {
                20, 2, 3, 2, 1, 3, 17, 71, 35, 2, 1, 139, 2, 2,
                3, 1783, 475, 2, 1, 1, 3, 1, 3, 2, 32, 1, 2, 3, 1, 8, 30, 1, 3, 414, 1,
                1, 135, 3, 3, 1, 414, 2, 1, 2, 2, 594, 2, 5, 6, 4, 11, 1, 2, 2, 1, 1,
                52, 4, 1, 2, 7, 1, 17, 334, 1, 2, 1, 2, 2, 6, 1, 266, 1, 2, 217, 2, 6,
                2, 13, 2, 2, 1, 2, 3, 5, 1, 2, 1, 7244, 11813, 1, 33, 2, -1, 1, 2, 3,
                13, 1, 92, 3, 13, 5, 14, 9, 141, 12, 6, 15, 25, 1, 1, 1, 46, 2, 1, 1,
                141, 3, 1, 1, 1, 1, 2, 1, 4, 34, 5, 78, 8, 1, 2, 2, 1, 9, 10, 2, 1, 4,
                13, 1, 5, 4, 4, 19, 5, 1, 1, 1, 68, 33, 399, 1, 1885, 25, 5, 2, 4, 1,
                1, 2, 16, 1, 2966, 3, 1, 1, 25501, 1, 1, 1, 66, 1, 3, 8, 131, 14, 5, 1,
                2, 2, 1, 1, 8, 1, 1, 2, 1, 5, 9, 2, 3, 112, 13, 2, 2, 1, 5, 10, 3, 1,
                1, 13, 2, 3, 4, 1, 3, 1, 1, 2, 1, 1, 2, 4, 2, 207, 1, 1, 2, 4, 3, 3, 2,
                2, 16
            };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBaseNegativeMin3(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[]
            {
                20, 2, 3, 2, 1, 3, 17, 71, 35, 2, 1, 139, 2, 2,
                3, 1783, 475, 2, 1, 1, 3, 1, 3, 2, 32, 1, 2, 3, 1, 8, 30, 1, 3, 414, 1,
                1, 135, 3, 3, 1, 414, 2, 1, 2, 2, 594, 2, 5, 6, 4, 11, 1, 2, 2, 1, 1,
                52, 4, 1, 2, 7, 1, 17, 334, 1, 2, 1, 2, 2, 6, 1, 266, 1, 2, 217, 2, 6,
                2, 13, 2, 2, 1, 2, 3, 5, 1, 2, 1, 7244, 11813, 1, 33, 2, 0, 1, 2, 3,
                13, 1, 92, 3, 13, 5, 14, 9, 141, 12, 6, 15, 25, 1, 1, 1, 46, 2, 1, 1,
                141, 3, 1, 1, 1, 1, 2, 1, 4, 34, 5, 78, 8, 1, 2, 2, 1, 9, 10, 2, 1, 4,
                13, 1, 5, 4, 4, 19, 5, 1, 1, 1, 68, 33, 399, 1, 1885, 25, 5, 2, 4, 1,
                1, 2, 16, 1, 2966, 3, 1, 1, 25501, 1, 1, 1, 66, 1, 3, 8, 131, 14, 5, 1,
                2, 2, 1, 1, 8, 1, 1, 2, 1, 5, 9, 2, 3, 112, 13, 2, 2, 1, 5, 10, 3, 1,
                1, 13, 2, 3, 4, 1, 3, 1, 1, 2, 1, 1, 2, 4, 2, 207, 1, 1, 2, 4, 3, 3, 2,
                2, 16
            };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBaseNegativeMin4(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            long[] input = new long[]
            {
                13, 13, 11, 8, 13, 10, 10, 11, 11, 14, 11, 7, 13,
                12, 12, 11, 15, 12, 12, 9, 8, 10, 13, 11, 8, 6, 5, 6, 11, 7, 15, 10, 7,
                6, 8, 7, 9, 9, 11, 33, 11, 3, 7, 4, 6, 10, 14, 12, 5, 14, 7, 6
            };

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBaseAt0(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            List<long> input = new List<long>();
            Random rand = new Random();
            for (int i = 0; i < 5120; i++)
            {
                input.Add((long)rand.Next(100));
            }
            input[0] = 20000L;

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBaseAt1(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            List<long> input = new List<long>();
            Random rand = new Random();
            for (int i = 0; i < 5120; i++)
            {
                input.Add((long)rand.Next(100));
            }
            input[1] = 20000L;

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBaseAt255(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            List<long> input = new List<long>();
            Random rand = new Random();
            for (int i = 0; i < 5120; i++)
            {
                input.Add((long)rand.Next(100));
            }
            input[255] = 20000L;

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBaseAt256(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            List<long> input = new List<long>();
            Random rand = new Random();
            for (int i = 0; i < 5120; i++)
            {
                input.Add((long)rand.Next(100));
            }
            input[256] = 20000L;

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBase510(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            List<long> input = new List<long>();
            Random rand = new Random();
            for (int i = 0; i < 5120; i++)
            {
                input.Add((long)rand.Next(100));
            }
            input[510] = 20000L;

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBase511(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            List<long> input = new List<long>();
            Random rand = new Random();
            for (int i = 0; i < 5120; i++)
            {
                input.Add((long)rand.Next(100));
            }
            input[511] = 20000L;

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBaseMax1(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            List<long> input = new List<long>();
            Random rand = new Random();
            for (int i = 0; i < 5120; i++)
            {
                input.Add((long)rand.Next(60));
            }
            input[511] = Int64.MaxValue;

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBaseMax2(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            List<long> input = new List<long>();
            Random rand = new Random();
            for (int i = 0; i < 5120; i++)
            {
                input.Add((long)rand.Next(60));
            }
            input[128] = Int64.MaxValue;
            input[256] = Int64.MaxValue;
            input[511] = Int64.MaxValue;

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBaseMax3(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            List<long> input = new List<long>();
            input.Add(371946367L);
            input.Add(11963367L);
            input.Add(68639400007L);
            input.Add(100233367L);
            input.Add(6367L);
            input.Add(10026367L);
            input.Add(3670000L);
            input.Add(3602367L);
            input.Add(4719226367L);
            input.Add(7196367L);
            input.Add(444442L);
            input.Add(210267L);
            input.Add(21033L);
            input.Add(160267L);
            input.Add(400267L);
            input.Add(23634347L);
            input.Add(16027L);
            input.Add(46026367L);
            input.Add(Int64.MaxValue);
            input.Add(33333L);

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBaseMax4(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            List<long> input = new List<long>();
            for (int i = 0; i < 25; i++)
            {
                input.Add(371292224226367L);
                input.Add(119622332222267L);
                input.Add(686329400222007L);
                input.Add(100233333222367L);
                input.Add(636272333322222L);
                input.Add(10202633223267L);
                input.Add(36700222022230L);
                input.Add(36023226224227L);
                input.Add(47192226364427L);
                input.Add(71963622222447L);
                input.Add(22244444222222L);
                input.Add(21220263327442L);
                input.Add(21032233332232L);
                input.Add(16026322232227L);
                input.Add(40022262272212L);
                input.Add(23634342227222L);
                input.Add(16022222222227L);
                input.Add(46026362222227L);
                input.Add(46026362222227L);
                input.Add(33322222222323L);
            }
            input.Add(Int64.MaxValue);

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

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
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }

#if TIMESTAMP
        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testPatchedBaseTimestamp(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(TSRow));

            List<Timestamp> tslist = new List<Timestamp>();
            tslist.add(Timestamp.valueOf("9999-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2003-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("1999-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("1995-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2002-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2010-03-02 00:00:00"));
            tslist.add(Timestamp.valueOf("2005-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2006-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2003-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("1996-08-02 00:00:00"));
            tslist.add(Timestamp.valueOf("1998-11-02 00:00:00"));
            tslist.add(Timestamp.valueOf("2008-10-02 00:00:00"));
            tslist.add(Timestamp.valueOf("1993-08-02 00:00:00"));
            tslist.add(Timestamp.valueOf("2008-01-02 00:00:00"));
            tslist.add(Timestamp.valueOf("2007-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2004-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2008-10-02 00:00:00"));
            tslist.add(Timestamp.valueOf("2003-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2004-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2008-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2005-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("1994-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2006-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2004-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2001-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2000-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2000-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2002-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2006-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2011-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2002-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("2005-01-01 00:00:00"));
            tslist.add(Timestamp.valueOf("1974-01-01 00:00:00"));

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

                foreach (Timestamp ts in tslist)
                {
                    writer.addRow(new TSRow(ts));
                }

                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            int idx = 0;
            while (rows.hasNext())
            {
                object row = rows.next(null);
                Assert.Equal(tslist[idx++].getNanos(),
                      ((TimestampWritable)((OrcStruct)row).getFieldValue(0)).getNanos());
            }
        }
#endif

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testDirectLargeNegatives(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)
                    .encodingStrategy(encodingStrategy));

                writer.addRow(-7486502418706614742L);
                writer.addRow(0L);
                writer.addRow(1L);
                writer.addRow(1L);
                writer.addRow(-5535739865598783616L);
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            object row = rows.next(null);
            Assert.Equal(-7486502418706614742L, ((StrongBox<long>)row).Value);
            row = rows.next(row);
            Assert.Equal(0L, ((StrongBox<long>)row).Value);
            row = rows.next(row);
            Assert.Equal(1L, ((StrongBox<long>)row).Value);
            row = rows.next(row);
            Assert.Equal(1L, ((StrongBox<long>)row).Value);
            row = rows.next(row);
            Assert.Equal(-5535739865598783616L, ((StrongBox<long>)row).Value);
        }

        [Theory]
        [InlineData(OrcFile.EncodingStrategy.COMPRESSION)]
        [InlineData(OrcFile.EncodingStrategy.SPEED)]
        public void testSeek(OrcFile.EncodingStrategy encodingStrategy)
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(long));

            List<long> input = new List<long>();
            Random rand = new Random();
            for (int i = 0; i < 100000; i++)
            {
                input.Add((long)rand.Next());
            }

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .compress(CompressionKind.NONE)
                    .stripeSize(100000)
                    .bufferSize(10000)
                    .version(OrcFile.Version.V_0_11)
                    .encodingStrategy(encodingStrategy));

                foreach (long l in input)
                {
                    writer.addRow(l);
                }
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            int idx = 55555;
            rows.seekToRow(idx);
            while (rows.hasNext())
            {
                object row = rows.next(null);
                Assert.Equal(input[idx++], ((StrongBox<long>)row).Value);
            }
        }
    }
}
