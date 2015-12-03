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
    using System.Linq;
    using OrcSharp.External;
    using OrcSharp.Serialization;
    using OrcSharp.Types;
    using Xunit;

    public class TestOrcSerDeStats : WithLocalDirectory
    {
        public class ListStruct
        {
            List<string> list1;

            public ListStruct(List<string> l1)
            {
                this.list1 = l1;
            }
        }

        public class MapStruct
        {
            Dictionary<string, double> map1;

            public MapStruct(Dictionary<string, double> m1)
            {
                this.map1 = m1;
            }
        }

        public class SimpleStruct
        {
            byte[] bytes1;
            string string1;

            public SimpleStruct(byte[] b1, string s1)
            {
                this.bytes1 = b1;
                this.string1 = s1;
            }
        }

        public class InnerStruct
        {
            int int1;
            internal string string1;

            public InnerStruct(int int1, string string1)
            {
                this.int1 = int1;
                this.string1 = string1;
            }
        }

        public class MiddleStruct
        {
            List<InnerStruct> list = new List<InnerStruct>();

            public MiddleStruct(params InnerStruct[] items)
            {
                list.AddRange(items);
            }
        }

        public class BigRow
        {
            bool boolean1;
            sbyte byte1;
            short short1;
            int int1;
            long long1;
            float float1;
            double double1;
            byte[] bytes1;
            string string1;
            List<InnerStruct> list = new List<InnerStruct>();
            Dictionary<string, InnerStruct> map = new Dictionary<string, InnerStruct>();
            DateTime ts; // Timestamp ts;
            HiveDecimal decimal1;
            MiddleStruct middle;

            public BigRow(bool b1, sbyte b2, short s1, int i1, long l1, float f1,
                double d1,
                byte[] b3, string s2, MiddleStruct m1,
                List<InnerStruct> l2, Dictionary<string, InnerStruct> m2, DateTime ts1, // Timestamp ts1,
                HiveDecimal dec1)
            {
                this.boolean1 = b1;
                this.byte1 = b2;
                this.short1 = s1;
                this.int1 = i1;
                this.long1 = l1;
                this.float1 = f1;
                this.double1 = d1;
                this.bytes1 = b3;
                this.string1 = s2;
                this.middle = m1;
                this.list = l2;
                this.map = m2;
                this.ts = ts1;
                this.decimal1 = dec1;
            }
        }

        const string testFileName = "TestOrcSerDeStats.orc";

        public TestOrcSerDeStats() : base(testFileName)
        {
        }

        private static InnerStruct inner(int i, string s)
        {
            return new InnerStruct(i, s);
        }

        private static Dictionary<string, InnerStruct> map(params InnerStruct[] items)
        {
            Dictionary<string, InnerStruct> result = new Dictionary<string, InnerStruct>();
            foreach (InnerStruct i in items)
            {
                result[i.string1] = i;
            }
            return result;
        }

        private static List<InnerStruct> list(params InnerStruct[] items)
        {
            return items.ToList();
        }

        private static byte[] bytes(params int[] items)
        {
            byte[] result = new byte[items.Length];
            for (int i = 0; i < items.Length; ++i)
            {
                result[i] = (byte)items[i];
            }
            return result;
        }

        [Fact]
        public void testStringAndBinaryStatistics()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(SimpleStruct));

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)))
            {
                writer.addRow(new SimpleStruct(bytes(0, 1, 2, 3, 4), "foo"));
                writer.addRow(new SimpleStruct(bytes(0, 1, 2, 3), "bar"));
                writer.addRow(new SimpleStruct(bytes(0, 1, 2, 3, 4, 5), null));
                writer.addRow(new SimpleStruct(null, "hi"));
                writer.close();
                Assert.Equal(4, writer.getNumberOfRows());
                Assert.Equal(273, writer.getRawDataSize());
            }

            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));
            Assert.Equal(4, reader.getNumberOfRows());
            Assert.Equal(273, reader.getRawDataSize());
            Assert.Equal(15, reader.getRawDataSizeOfColumns(Lists.newArrayList("bytes1")));
            Assert.Equal(258, reader.getRawDataSizeOfColumns(Lists.newArrayList("string1")));
            Assert.Equal(273, reader.getRawDataSizeOfColumns(Lists.newArrayList("bytes1", "string1")));

            // check the stats
            ColumnStatistics[] stats = reader.getStatistics();
            Assert.Equal(4, stats[0].getNumberOfValues());
            Assert.Equal("count: 4 hasNull: False", stats[0].ToString());

            Assert.Equal(3, stats[1].getNumberOfValues());
            Assert.Equal(15, ((BinaryColumnStatistics)stats[1]).getSum());
            Assert.Equal("count: 3 hasNull: True sum: 15", stats[1].ToString());

            Assert.Equal(3, stats[2].getNumberOfValues());
            Assert.Equal("bar", ((StringColumnStatistics)stats[2]).getMinimum());
            Assert.Equal("hi", ((StringColumnStatistics)stats[2]).getMaximum());
            Assert.Equal(8, ((StringColumnStatistics)stats[2]).getSum());
            Assert.Equal("count: 3 hasNull: True min: bar max: hi sum: 8",
                stats[2].ToString());

            // check the inspectors
            StructObjectInspector readerInspector =
                (StructObjectInspector)reader.getObjectInspector();
            Assert.Equal(ObjectInspectorCategory.STRUCT, readerInspector.getCategory());
            Assert.Equal("struct<bytes1:binary,string1:string>", readerInspector.getTypeName());
            IList<StructField> fields = readerInspector.getAllStructFieldRefs();
            BinaryObjectInspector bi = (BinaryObjectInspector)readerInspector.
                getStructFieldRef("bytes1").getFieldObjectInspector();
            StringObjectInspector st = (StringObjectInspector)readerInspector.
                getStructFieldRef("string1").getFieldObjectInspector();
            RecordReader rows = reader.rows();
            object row = rows.next();
            Assert.NotNull(row);
            // check the contents of the first row
            Assert.Equal(bytes(0, 1, 2, 3, 4), bi.get(
                readerInspector.getStructFieldData(row, fields[0])));
            Assert.Equal("foo", st.getPrimitiveJavaObject(readerInspector.
                getStructFieldData(row, fields[1])));

            // check the contents of second row
            Assert.Equal(true, rows.hasNext());
            row = rows.next();
            Assert.Equal(bytes(0, 1, 2, 3), bi.get(
                readerInspector.getStructFieldData(row, fields[0])));
            Assert.Equal("bar", st.getPrimitiveJavaObject(readerInspector.
                getStructFieldData(row, fields[1])));

            // check the contents of second row
            Assert.Equal(true, rows.hasNext());
            row = rows.next();
            Assert.Equal(bytes(0, 1, 2, 3, 4, 5), bi.get(
                readerInspector.getStructFieldData(row, fields[0])));
            Assert.Null(st.getPrimitiveJavaObject(readerInspector.
                getStructFieldData(row, fields[1])));

            // check the contents of second row
            Assert.Equal(true, rows.hasNext());
            row = rows.next();
            Assert.Null(bi.get(
                readerInspector.getStructFieldData(row, fields[0])));
            Assert.Equal("hi", st.getPrimitiveJavaObject(readerInspector.
                getStructFieldData(row, fields[1])));

            // handle the close up
            Assert.Equal(false, rows.hasNext());
            rows.close();
        }


        [Fact]
        public void testOrcSerDeStatsList()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(ListStruct));

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(10000)
                    .bufferSize(10000)))
            {
                for (int row = 0; row < 5000; row++)
                {
                    List<string> test = new List<string>();
                    for (int i = 0; i < 1000; i++)
                    {
                        test.Add("hi");
                    }
                    writer.addRow(new ListStruct(test));
                }
                writer.close();

                Assert.Equal(5000, writer.getNumberOfRows());
                Assert.Equal(430000000, writer.getRawDataSize());
            }

            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));
            // stats from reader
            Assert.Equal(5000, reader.getNumberOfRows());
            Assert.Equal(430000000, reader.getRawDataSize());
            Assert.Equal(430000000, reader.getRawDataSizeOfColumns(Lists.newArrayList("list1")));
        }

        [Fact]
        public void testOrcSerDeStatsMap()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(MapStruct));

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(10000)
                    .bufferSize(10000)))
            {
                for (int row = 0; row < 1000; row++)
                {
                    Dictionary<string, double> test = new Dictionary<string, double>();
                    for (int i = 0; i < 10; i++)
                    {
                        test.Add("hi" + i, 2.0);
                    }
                    writer.addRow(new MapStruct(test));
                }
                writer.close();
                // stats from writer
                Assert.Equal(1000, writer.getNumberOfRows());
                Assert.Equal(950000, writer.getRawDataSize());
            }

            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));
            // stats from reader
            Assert.Equal(1000, reader.getNumberOfRows());
            Assert.Equal(950000, reader.getRawDataSize());
            Assert.Equal(950000, reader.getRawDataSizeOfColumns(Lists.newArrayList("map1")));
        }

        [Fact]
        public void testOrcSerDeStatsSimpleWithNulls()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(SimpleStruct));

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(10000)
                    .bufferSize(10000)))
            {
                for (int row = 0; row < 1000; row++)
                {
                    if (row % 2 == 0)
                    {
                        writer.addRow(new SimpleStruct(new byte[] { 1, 2, 3 }, "hi"));
                    }
                    else
                    {
                        writer.addRow(null);
                    }
                }
                writer.close();
                // stats from writer
                Assert.Equal(1000, writer.getNumberOfRows());
                Assert.Equal(44500, writer.getRawDataSize());
            }

            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));
            // stats from reader
            Assert.Equal(1000, reader.getNumberOfRows());
            Assert.Equal(44500, reader.getRawDataSize());
            Assert.Equal(1500, reader.getRawDataSizeOfColumns(Lists.newArrayList("bytes1")));
            Assert.Equal(43000, reader.getRawDataSizeOfColumns(Lists.newArrayList("string1")));
            Assert.Equal(44500, reader.getRawDataSizeOfColumns(Lists.newArrayList("bytes1", "string1")));
        }

        [Fact]
        public void testOrcSerDeStatsComplex()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(BigRow));

            long rawDataSize;
            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)))
            {
                // 1 + 2 + 4 + 8 + 4 + 8 + 5 + 2 + 4 + 3 + 4 + 4 + 4 + 4 + 4 + 3 = 64
                writer.addRow(new BigRow(false, (sbyte)1, (short)1024, 65536,
                        Int64.MaxValue, (float)1.0, -15.0, bytes(0, 1, 2, 3, 4), "hi",
                        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
                        list(inner(3, "good"), inner(4, "bad")),
                        map(), Timestamp.valueOf("2000-03-12 15:00:00"), HiveDecimal.Parse(
                            "12345678.6547456")));
                // 1 + 2 + 4 + 8 + 4 + 8 + 3 + 4 + 3 + 4 + 4 + 4 + 3 + 4 + 2 + 4 + 3 + 5 + 4 + 5 + 7 + 4 + 7 =
                // 97
                writer.addRow(new BigRow(true, (sbyte)100, (short)2048, 65536,
                    Int64.MaxValue, (float)2.0, -5.0, bytes(), "bye",
                    new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
                    list(inner(100000000, "cat"), inner(-100000, "in"), inner(1234, "hat")),
                    map(inner(5, "chani"), inner(1, "mauddib")), Timestamp.valueOf("2000-03-11 15:00:00"),
                    HiveDecimal.Parse("12345678.6547452")));
                writer.close();
                long rowCount = writer.getNumberOfRows();
                rawDataSize = writer.getRawDataSize();
                Assert.Equal(2, rowCount);
                Assert.Equal(1740, rawDataSize);
            }

            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));

            Assert.Equal(2, reader.getNumberOfRows());
            Assert.Equal(1740, reader.getRawDataSize());
            Assert.Equal(8, reader.getRawDataSizeOfColumns(Lists.newArrayList("boolean1")));
            Assert.Equal(8, reader.getRawDataSizeOfColumns(Lists.newArrayList("byte1")));
            Assert.Equal(8, reader.getRawDataSizeOfColumns(Lists.newArrayList("short1")));
            Assert.Equal(8, reader.getRawDataSizeOfColumns(Lists.newArrayList("int1")));
            Assert.Equal(16, reader.getRawDataSizeOfColumns(Lists.newArrayList("long1")));
            Assert.Equal(8, reader.getRawDataSizeOfColumns(Lists.newArrayList("float1")));
            Assert.Equal(16, reader.getRawDataSizeOfColumns(Lists.newArrayList("double1")));
            Assert.Equal(5, reader.getRawDataSizeOfColumns(Lists.newArrayList("bytes1")));
            Assert.Equal(172, reader.getRawDataSizeOfColumns(Lists.newArrayList("string1")));
            Assert.Equal(455, reader.getRawDataSizeOfColumns(Lists.newArrayList("list")));
            Assert.Equal(368, reader.getRawDataSizeOfColumns(Lists.newArrayList("map")));
            Assert.Equal(364, reader.getRawDataSizeOfColumns(Lists.newArrayList("middle")));
            Assert.Equal(80, reader.getRawDataSizeOfColumns(Lists.newArrayList("ts")));
            Assert.Equal(224, reader.getRawDataSizeOfColumns(Lists.newArrayList("decimal1")));
            Assert.Equal(88, reader.getRawDataSizeOfColumns(Lists.newArrayList("ts", "int1")));
            Assert.Equal(1195,
                reader.getRawDataSizeOfColumns(Lists.newArrayList("middle", "list", "map", "float1")));
            Assert.Equal(185,
                reader.getRawDataSizeOfColumns(Lists.newArrayList("bytes1", "byte1", "string1")));
            Assert.Equal(rawDataSize, reader.getRawDataSizeOfColumns(Lists.newArrayList("boolean1",
                "byte1", "short1", "int1", "long1", "float1", "double1", "bytes1", "string1", "list",
                "map", "middle", "ts", "decimal1")));


            // check the stats
            ColumnStatistics[] stats = reader.getStatistics();
            Assert.Equal(2, stats[1].getNumberOfValues());
            Assert.Equal(1, ((BooleanColumnStatistics)stats[1]).getFalseCount());
            Assert.Equal(1, ((BooleanColumnStatistics)stats[1]).getTrueCount());
            Assert.Equal("count: 2 hasNull: False true: 1", stats[1].ToString());

            Assert.Equal(2048, ((IntegerColumnStatistics)stats[3]).getMaximum());
            Assert.Equal(1024, ((IntegerColumnStatistics)stats[3]).getMinimum());
            Assert.Equal(true, ((IntegerColumnStatistics)stats[3]).isSumDefined());
            Assert.Equal(3072, ((IntegerColumnStatistics)stats[3]).getSum());
            Assert.Equal("count: 2 hasNull: False min: 1024 max: 2048 sum: 3072",
                stats[3].ToString());

            Assert.Equal(Int64.MaxValue,
                ((IntegerColumnStatistics)stats[5]).getMaximum());
            Assert.Equal(Int64.MaxValue,
                ((IntegerColumnStatistics)stats[5]).getMinimum());
            Assert.Equal(false, ((IntegerColumnStatistics)stats[5]).isSumDefined());
            Assert.Equal("count: 2 hasNull: False min: 9223372036854775807 max: 9223372036854775807",
                stats[5].ToString());

            Assert.Equal(-15.0, ((DoubleColumnStatistics)stats[7]).getMinimum());
            Assert.Equal(-5.0, ((DoubleColumnStatistics)stats[7]).getMaximum());
            Assert.Equal(-20.0, ((DoubleColumnStatistics)stats[7]).getSum(), 5);
            Assert.Equal("count: 2 hasNull: False min: -15 max: -5 sum: -20",
                stats[7].ToString());

            Assert.Equal("count: 2 hasNull: False min: bye max: hi sum: 5", stats[9].ToString());
        }

        [Fact]
        public void testOrcSerDeStatsComplexOldFormat()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(BigRow));

            long rawDataSize;
            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .version(OrcFile.Version.V_0_11)
                    .bufferSize(10000)))
            {
                // 1 + 2 + 4 + 8 + 4 + 8 + 5 + 2 + 4 + 3 + 4 + 4 + 4 + 4 + 4 + 3 = 64
                writer.addRow(new BigRow(false, (sbyte)1, (short)1024, 65536,
                        Int64.MaxValue, (float)1.0, -15.0, bytes(0, 1, 2, 3, 4), "hi",
                        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
                        list(inner(3, "good"), inner(4, "bad")),
                        map(), Timestamp.valueOf("2000-03-12 15:00:00"), HiveDecimal.Parse(
                            "12345678.6547456")));
                // 1 + 2 + 4 + 8 + 4 + 8 + 3 + 4 + 3 + 4 + 4 + 4 + 3 + 4 + 2 + 4 + 3 + 5 + 4 + 5 + 7 + 4 + 7 =
                // 97
                writer.addRow(new BigRow(true, (sbyte)100, (short)2048, 65536,
                    Int64.MaxValue, (float)2.0, -5.0, bytes(), "bye",
                    new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
                    list(inner(100000000, "cat"), inner(-100000, "in"), inner(1234, "hat")),
                    map(inner(5, "chani"), inner(1, "mauddib")), Timestamp.valueOf("2000-03-11 15:00:00"),
                    HiveDecimal.Parse("12345678.6547452")));
                writer.close();
                long rowCount = writer.getNumberOfRows();
                rawDataSize = writer.getRawDataSize();
                Assert.Equal(2, rowCount);
                Assert.Equal(1740, rawDataSize);
            }

            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));

            Assert.Equal(2, reader.getNumberOfRows());
            Assert.Equal(1740, reader.getRawDataSize());
            Assert.Equal(8, reader.getRawDataSizeOfColumns(Lists.newArrayList("boolean1")));
            Assert.Equal(8, reader.getRawDataSizeOfColumns(Lists.newArrayList("byte1")));
            Assert.Equal(8, reader.getRawDataSizeOfColumns(Lists.newArrayList("short1")));
            Assert.Equal(8, reader.getRawDataSizeOfColumns(Lists.newArrayList("int1")));
            Assert.Equal(16, reader.getRawDataSizeOfColumns(Lists.newArrayList("long1")));
            Assert.Equal(8, reader.getRawDataSizeOfColumns(Lists.newArrayList("float1")));
            Assert.Equal(16, reader.getRawDataSizeOfColumns(Lists.newArrayList("double1")));
            Assert.Equal(5, reader.getRawDataSizeOfColumns(Lists.newArrayList("bytes1")));
            Assert.Equal(172, reader.getRawDataSizeOfColumns(Lists.newArrayList("string1")));
            Assert.Equal(455, reader.getRawDataSizeOfColumns(Lists.newArrayList("list")));
            Assert.Equal(368, reader.getRawDataSizeOfColumns(Lists.newArrayList("map")));
            Assert.Equal(364, reader.getRawDataSizeOfColumns(Lists.newArrayList("middle")));
            Assert.Equal(80, reader.getRawDataSizeOfColumns(Lists.newArrayList("ts")));
            Assert.Equal(224, reader.getRawDataSizeOfColumns(Lists.newArrayList("decimal1")));
            Assert.Equal(88, reader.getRawDataSizeOfColumns(Lists.newArrayList("ts", "int1")));
            Assert.Equal(1195,
                reader.getRawDataSizeOfColumns(Lists.newArrayList("middle", "list", "map", "float1")));
            Assert.Equal(185,
                reader.getRawDataSizeOfColumns(Lists.newArrayList("bytes1", "byte1", "string1")));
            Assert.Equal(rawDataSize, reader.getRawDataSizeOfColumns(Lists.newArrayList("boolean1",
                "byte1", "short1", "int1", "long1", "float1", "double1", "bytes1", "string1", "list",
                "map", "middle", "ts", "decimal1")));

            // check the stats
            ColumnStatistics[] stats = reader.getStatistics();
            Assert.Equal(2, stats[1].getNumberOfValues());
            Assert.Equal(1, ((BooleanColumnStatistics)stats[1]).getFalseCount());
            Assert.Equal(1, ((BooleanColumnStatistics)stats[1]).getTrueCount());
            Assert.Equal("count: 2 hasNull: False true: 1", stats[1].ToString());

            Assert.Equal(2048, ((IntegerColumnStatistics)stats[3]).getMaximum());
            Assert.Equal(1024, ((IntegerColumnStatistics)stats[3]).getMinimum());
            Assert.Equal(true, ((IntegerColumnStatistics)stats[3]).isSumDefined());
            Assert.Equal(3072, ((IntegerColumnStatistics)stats[3]).getSum());
            Assert.Equal("count: 2 hasNull: False min: 1024 max: 2048 sum: 3072",
                stats[3].ToString());

            Assert.Equal(Int64.MaxValue, ((IntegerColumnStatistics)stats[5]).getMaximum());
            Assert.Equal(Int64.MaxValue, ((IntegerColumnStatistics)stats[5]).getMinimum());
            Assert.Equal(false, ((IntegerColumnStatistics)stats[5]).isSumDefined());
            Assert.Equal("count: 2 hasNull: False min: 9223372036854775807 max: 9223372036854775807",
                stats[5].ToString());

            Assert.Equal(-15.0, ((DoubleColumnStatistics)stats[7]).getMinimum());
            Assert.Equal(-5.0, ((DoubleColumnStatistics)stats[7]).getMaximum());
            Assert.Equal(-20.0, ((DoubleColumnStatistics)stats[7]).getSum(), 5);
            Assert.Equal("count: 2 hasNull: False min: -15 max: -5 sum: -20",
                stats[7].ToString());

            Assert.Equal(5, ((BinaryColumnStatistics)stats[8]).getSum());
            Assert.Equal("count: 2 hasNull: False sum: 5", stats[8].ToString());

            Assert.Equal("bye", ((StringColumnStatistics)stats[9]).getMinimum());
            Assert.Equal("hi", ((StringColumnStatistics)stats[9]).getMaximum());
            Assert.Equal(5, ((StringColumnStatistics)stats[9]).getSum());
            Assert.Equal("count: 2 hasNull: False min: bye max: hi sum: 5", stats[9].ToString());
        }

        [Fact]
        public void testSerdeStatsOldFormat()
        {
            string testFile = Path.Combine(TestHelpers.ResourcesDirectory, "orc-file-11-format.orc");
            Reader reader = OrcFile.createReader(testFile, OrcFile.readerOptions(conf));

            int stripeCount = 0;
            int rowCount = 0;
            long currentOffset = -1;
            foreach (StripeInformation stripe in reader.getStripes())
            {
                stripeCount += 1;
                rowCount += (int)stripe.getNumberOfRows();
                if (currentOffset < 0)
                {
                    currentOffset = stripe.getOffset() + stripe.getIndexLength()
                        + stripe.getDataLength() + stripe.getFooterLength();
                }
                else
                {
                    Assert.Equal(currentOffset, stripe.getOffset());
                    currentOffset += stripe.getIndexLength() + stripe.getDataLength()
                        + stripe.getFooterLength();
                }
            }
            Assert.Equal(reader.getNumberOfRows(), rowCount);
#if JAVA_SIZE
            Assert.Equal(6300000, reader.getRawDataSize());
#endif
            Assert.Equal(2, stripeCount);

            // check the stats
            ColumnStatistics[] stats = reader.getStatistics();
            Assert.Equal(7500, stats[1].getNumberOfValues());
            Assert.Equal(3750, ((BooleanColumnStatistics)stats[1]).getFalseCount());
            Assert.Equal(3750, ((BooleanColumnStatistics)stats[1]).getTrueCount());
            Assert.Equal("count: 7500 hasNull: True true: 3750", stats[1].ToString());

            Assert.Equal(2048, ((IntegerColumnStatistics)stats[3]).getMaximum());
            Assert.Equal(1024, ((IntegerColumnStatistics)stats[3]).getMinimum());
            Assert.Equal(true, ((IntegerColumnStatistics)stats[3]).isSumDefined());
            Assert.Equal(11520000, ((IntegerColumnStatistics)stats[3]).getSum());
            Assert.Equal("count: 7500 hasNull: True min: 1024 max: 2048 sum: 11520000",
                stats[3].ToString());

            Assert.Equal(Int64.MaxValue, ((IntegerColumnStatistics)stats[5]).getMaximum());
            Assert.Equal(Int64.MaxValue, ((IntegerColumnStatistics)stats[5]).getMinimum());
            Assert.Equal(false, ((IntegerColumnStatistics)stats[5]).isSumDefined());
            Assert.Equal(
                "count: 7500 hasNull: True min: 9223372036854775807 max: 9223372036854775807",
                stats[5].ToString());

            Assert.Equal(-15.0, ((DoubleColumnStatistics)stats[7]).getMinimum());
            Assert.Equal(-5.0, ((DoubleColumnStatistics)stats[7]).getMaximum());
            Assert.Equal(-75000.0, ((DoubleColumnStatistics)stats[7]).getSum(), 5);
            Assert.Equal("count: 7500 hasNull: True min: -15 max: -5 sum: -75000",
                stats[7].ToString());

            Assert.Equal("bye", ((StringColumnStatistics)stats[9]).getMinimum());
            Assert.Equal("hi", ((StringColumnStatistics)stats[9]).getMaximum());
            Assert.Equal(0, ((StringColumnStatistics)stats[9]).getSum());
            Assert.Equal("count: 7500 hasNull: True min: bye max: hi sum: 0", stats[9].ToString());

            // old orc format will not have binary statistics. ToString() will show only
            // the general column statistics
            Assert.Equal("count: 7500 hasNull: True", stats[8].ToString());

            // since old orc format doesn't support binary statistics,
            // this should throw ClassCastException
            Assert.Throws<InvalidCastException>(() => ((BinaryColumnStatistics)stats[8]).getSum());
        }

        static class Timestamp
        {
            public static DateTime valueOf(string s)
            {
                return DateTime.Parse(s);
            }
        }
    }
}
