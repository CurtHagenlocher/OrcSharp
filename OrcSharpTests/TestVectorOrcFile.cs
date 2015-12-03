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
    using System.Numerics;
    using System.Runtime.CompilerServices;
    using System.Text;
    using OrcSharp.External;
    using OrcSharp.Query;
    using OrcSharp.Serialization;
    using OrcSharp.Types;
    using Xunit;
    using OrcProto = orc.proto;

    /**
     * Tests for the vectorized reader and writer for ORC files.
     */
    public class TestVectorOrcFile : WithLocalDirectory
    {
        public class InnerStruct
        {
            internal int int1;
            internal string string1;

            public InnerStruct(int int1, string string1)
            {
                this.int1 = int1;
                this.string1 = string1;
            }

            public override string ToString()
            {
                return "{" + int1 + ", " + string1 + "}";
            }
        }

        public class MiddleStruct
        {
            internal List<InnerStruct> list = new List<InnerStruct>();

            public MiddleStruct(params InnerStruct[] items)
            {
                list.AddRange(items);
            }
        }

        public class BigRow
        {
            internal bool boolean1;
            internal byte byte1;
            internal short short1;
            internal int int1;
            internal long long1;
            internal float float1;
            internal double double1;
            internal byte[] bytes1;
            internal string string1;
            internal MiddleStruct middle;
            internal List<InnerStruct> list = new List<InnerStruct>();
            internal Dictionary<string, InnerStruct> map = new Dictionary<string, InnerStruct>();

            public BigRow(bool b1, byte b2, short s1, int i1, long l1, float f1,
                   double d1, byte[] b3, string s2, MiddleStruct m1,
                   List<InnerStruct> l2, Dictionary<string, InnerStruct> m2)
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
                if (m2 != null)
                {
                    this.map = new Dictionary<string, InnerStruct>(m2.Count);
                    foreach (KeyValuePair<string, InnerStruct> item in m2)
                    {
                        this.map.Add(item.Key, item.Value);
                    }
                }
                else
                {
                    this.map = null;
                }
            }
        }

        const string testFileName = "TestVectorOrcFile.orc";

        public TestVectorOrcFile()
            : base(testFileName)
        {
        }

        private static InnerStruct MakeInner(int i, string s)
        {
            return new InnerStruct(i, s);
        }

        private static Dictionary<string, InnerStruct> MakeMap(params InnerStruct[] items)
        {
            Dictionary<string, InnerStruct> result = new Dictionary<string, InnerStruct>(items.Length);
            foreach (InnerStruct i in items)
            {
                result.Add(i.string1, i);
            }
            return result;
        }

        private static List<InnerStruct> MakeList(params InnerStruct[] items)
        {
            return new List<InnerStruct>(items);
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

        private static byte[] bytesArray(params int[] items)
        {
            byte[] result = new byte[items.Length];
            for (int i = 0; i < items.Length; ++i)
            {
                result[i] = (byte)items[i];
            }
            return result;
        }

        private static ByteBuffer byteBuf(params int[] items)
        {
            ByteBuffer result = ByteBuffer.allocate(items.Length);
            foreach (int item in items)
            {
                result.put((byte)item);
            }
            result.flip();
            return result;
        }

        [Fact]
        public void testReadFormat_0_11()
        {
            string oldFilePath = Path.Combine(TestHelpers.ResourcesDirectory, "orc-file-11-format.orc");
            Reader reader = OrcFile.createReader(oldFilePath, OrcFile.readerOptions(conf));

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

            Assert.Equal(Int64.MaxValue,
                ((IntegerColumnStatistics)stats[5]).getMaximum());
            Assert.Equal(Int64.MaxValue,
                ((IntegerColumnStatistics)stats[5]).getMinimum());
            Assert.Equal(false, ((IntegerColumnStatistics)stats[5]).isSumDefined());
            Assert.Equal(
                "count: 7500 hasNull: True min: 9223372036854775807 max: 9223372036854775807",
                stats[5].ToString());

            Assert.Equal(-15.0, ((DoubleColumnStatistics)stats[7]).getMinimum());
            Assert.Equal(-5.0, ((DoubleColumnStatistics)stats[7]).getMaximum());
            Assert.Equal(-75000.0, ((DoubleColumnStatistics)stats[7]).getSum(), 5);
            Assert.Equal("count: 7500 hasNull: True min: -15 max: -5 sum: -75000",
                stats[7].ToString());

            Assert.Equal("count: 7500 hasNull: True min: bye max: hi sum: 0", stats[9].ToString());

            // check the inspectors
            StructObjectInspector readerInspector = (StructObjectInspector)reader
                .getObjectInspector();
            Assert.Equal(ObjectInspectorCategory.STRUCT, readerInspector.getCategory());
            Assert.Equal("struct<boolean1:boolean,byte1:tinyint,short1:smallint,"
                + "int1:int,long1:bigint,float1:float,double1:double,bytes1:"
                + "binary,string1:string,middle:struct<list:array<struct<int1:int,"
                + "string1:string>>>,list:array<struct<int1:int,string1:string>>,"
                + "map:map<string,struct<int1:int,string1:string>>,ts:timestamp,"
                + "decimal1:decimal(38,18)>", readerInspector.getTypeName());
            IList<StructField> fields = readerInspector
                .getAllStructFieldRefs();
            BooleanObjectInspector bo = (BooleanObjectInspector)readerInspector
                .getStructFieldRef("boolean1").getFieldObjectInspector();
            ByteObjectInspector by = (ByteObjectInspector)readerInspector
                .getStructFieldRef("byte1").getFieldObjectInspector();
            ShortObjectInspector sh = (ShortObjectInspector)readerInspector
                .getStructFieldRef("short1").getFieldObjectInspector();
            IntObjectInspector @in = (IntObjectInspector)readerInspector
                .getStructFieldRef("int1").getFieldObjectInspector();
            LongObjectInspector lo = (LongObjectInspector)readerInspector
                .getStructFieldRef("long1").getFieldObjectInspector();
            FloatObjectInspector fl = (FloatObjectInspector)readerInspector
                .getStructFieldRef("float1").getFieldObjectInspector();
            DoubleObjectInspector dbl = (DoubleObjectInspector)readerInspector
                .getStructFieldRef("double1").getFieldObjectInspector();
            BinaryObjectInspector bi = (BinaryObjectInspector)readerInspector
                .getStructFieldRef("bytes1").getFieldObjectInspector();
            StringObjectInspector st = (StringObjectInspector)readerInspector
                .getStructFieldRef("string1").getFieldObjectInspector();
            StructObjectInspector mid = (StructObjectInspector)readerInspector
                .getStructFieldRef("middle").getFieldObjectInspector();
            IList<StructField> midFields = mid.getAllStructFieldRefs();
            ListObjectInspector midli = (ListObjectInspector)midFields[0]
                .getFieldObjectInspector();
            StructObjectInspector inner = (StructObjectInspector)midli
                .getListElementObjectInspector();
            IList<StructField> inFields = inner.getAllStructFieldRefs();
            ListObjectInspector li = (ListObjectInspector)readerInspector
                .getStructFieldRef("list").getFieldObjectInspector();
            MapObjectInspector ma = (MapObjectInspector)readerInspector
                .getStructFieldRef("map").getFieldObjectInspector();
            TimestampObjectInspector tso = (TimestampObjectInspector)readerInspector
                .getStructFieldRef("ts").getFieldObjectInspector();
            HiveDecimalObjectInspector dco = (HiveDecimalObjectInspector)readerInspector
                .getStructFieldRef("decimal1").getFieldObjectInspector();
            StringObjectInspector mk = (StringObjectInspector)ma
                .getMapKeyObjectInspector();
            RecordReader rows = reader.rows();
            object row = rows.next();
            Assert.NotNull(row);
            // check the contents of the first row
            Assert.Equal(false,
                bo.get(readerInspector.getStructFieldData(row, fields[0])));
            Assert.Equal(1,
                by.get(readerInspector.getStructFieldData(row, fields[1])));
            Assert.Equal(1024,
                sh.get(readerInspector.getStructFieldData(row, fields[2])));
            Assert.Equal(65536,
                @in.get(readerInspector.getStructFieldData(row, fields[3])));
            Assert.Equal(Int64.MaxValue,
                lo.get(readerInspector.getStructFieldData(row, fields[4])));
            Assert.Equal(1.0,
                fl.get(readerInspector.getStructFieldData(row, fields[5])), 5);
            Assert.Equal(-15.0,
                dbl.get(readerInspector.getStructFieldData(row, fields[6])),
                5);
            Assert.Equal(bytes(0, 1, 2, 3, 4),
                bi.get(readerInspector.getStructFieldData(row, fields[7])));
            Assert.Equal("hi", st.getPrimitiveJavaObject(readerInspector
                .getStructFieldData(row, fields[8])));
            IList<object> midRow = midli.getList(mid.getStructFieldData(
                readerInspector.getStructFieldData(row, fields[9]),
                midFields[0]));
            Assert.NotNull(midRow);
            Assert.Equal(2, midRow.Count);
            Assert.Equal(1,
                @in.get(inner.getStructFieldData(midRow[0], inFields[0])));
            Assert.Equal("bye", st.getPrimitiveJavaObject(inner.getStructFieldData(
                midRow[0], inFields[1])));
            Assert.Equal(2,
                @in.get(inner.getStructFieldData(midRow[1], inFields[0])));
            Assert.Equal("sigh", st.getPrimitiveJavaObject(inner.getStructFieldData(
                midRow[1], inFields[1])));
            IList<object> list = li.getList(readerInspector.getStructFieldData(row,
                fields[10]));
            Assert.Equal(2, list.Count);
            Assert.Equal(3,
                @in.get(inner.getStructFieldData(list[0], inFields[0])));
            Assert.Equal("good", st.getPrimitiveJavaObject(inner.getStructFieldData(
                list[0], inFields[1])));
            Assert.Equal(4,
                @in.get(inner.getStructFieldData(list[1], inFields[0])));
            Assert.Equal("bad", st.getPrimitiveJavaObject(inner.getStructFieldData(
                list[1], inFields[1])));
            IDictionary<object, object> map = ma.getMap(readerInspector.getStructFieldData(row,
                fields[11]));
            Assert.Equal(0, map.Count);
            Assert.Equal(Timestamp.Parse("2000-03-12 15:00:00"),
                tso.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
                    fields[12])));
            Assert.Equal(HiveDecimal.Parse("12345678.6547456"),
                dco.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
                    fields[13])));

            // check the contents of second row
            Assert.Equal(true, rows.hasNext());
            rows.seekToRow(7499);
            row = rows.next();
            Assert.Equal(true,
                bo.get(readerInspector.getStructFieldData(row, fields[0])));
            Assert.Equal(100,
                by.get(readerInspector.getStructFieldData(row, fields[1])));
            Assert.Equal(2048,
                sh.get(readerInspector.getStructFieldData(row, fields[2])));
            Assert.Equal(65536,
                @in.get(readerInspector.getStructFieldData(row, fields[3])));
            Assert.Equal(Int64.MaxValue,
                lo.get(readerInspector.getStructFieldData(row, fields[4])));
            Assert.Equal(2.0,
                fl.get(readerInspector.getStructFieldData(row, fields[5])), 5);
            Assert.Equal(-5.0,
                dbl.get(readerInspector.getStructFieldData(row, fields[6])),
                5);
            Assert.Equal(bytes(), bi.get(readerInspector
                .getStructFieldData(row, fields[7])));
            Assert.Equal("bye", st.getPrimitiveJavaObject(readerInspector
                .getStructFieldData(row, fields[8])));
            midRow = midli.getList(mid.getStructFieldData(
                readerInspector.getStructFieldData(row, fields[9]),
                midFields[0]));
            Assert.NotNull(midRow);
            Assert.Equal(2, midRow.Count);
            Assert.Equal(1,
                @in.get(inner.getStructFieldData(midRow[0], inFields[0])));
            Assert.Equal("bye", st.getPrimitiveJavaObject(inner.getStructFieldData(
                midRow[0], inFields[1])));
            Assert.Equal(2,
                @in.get(inner.getStructFieldData(midRow[1], inFields[0])));
            Assert.Equal("sigh", st.getPrimitiveJavaObject(inner.getStructFieldData(
                midRow[1], inFields[1])));
            list = li.getList(readerInspector.getStructFieldData(row, fields[10]));
            Assert.Equal(3, list.Count);
            Assert.Equal(100000000,
                @in.get(inner.getStructFieldData(list[0], inFields[0])));
            Assert.Equal("cat", st.getPrimitiveJavaObject(inner.getStructFieldData(
                list[0], inFields[1])));
            Assert.Equal(-100000,
                @in.get(inner.getStructFieldData(list[1], inFields[0])));
            Assert.Equal("in", st.getPrimitiveJavaObject(inner.getStructFieldData(
                list[1], inFields[1])));
            Assert.Equal(1234,
                @in.get(inner.getStructFieldData(list[2], inFields[0])));
            Assert.Equal("hat", st.getPrimitiveJavaObject(inner.getStructFieldData(
                list[2], inFields[1])));
            map = ma.getMap(readerInspector.getStructFieldData(row, fields[11]));
            Assert.Equal(2, map.Count);
            bool[] found = new bool[2];
            foreach (object key in map.Keys)
            {
                string str = mk.getPrimitiveJavaObject(key);
                if (str.Equals("chani"))
                {
                    Assert.Equal(false, found[0]);
                    Assert.Equal(5,
                        @in.get(inner.getStructFieldData(map.get(key), inFields[0])));
                    Assert.Equal(str, st.getPrimitiveJavaObject(inner.getStructFieldData(
                        map.get(key), inFields[1])));
                    found[0] = true;
                }
                else if (str.Equals("mauddib"))
                {
                    Assert.Equal(false, found[1]);
                    Assert.Equal(1,
                        @in.get(inner.getStructFieldData(map.get(key), inFields[0])));
                    Assert.Equal(str, st.getPrimitiveJavaObject(inner.getStructFieldData(
                        map.get(key), inFields[1])));
                    found[1] = true;
                }
                else
                {
                    throw new ArgumentException("Unknown key " + str);
                }
            }
            Assert.Equal(true, found[0]);
            Assert.Equal(true, found[1]);
            Assert.Equal(Timestamp.Parse("2000-03-12 15:00:01"),
                tso.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
                    fields[12])));
            Assert.Equal(HiveDecimal.Parse("12345678.6547457"),
                dco.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
                    fields[13])));

            // handle the close up
            Assert.Equal(false, rows.hasNext());
            rows.close();
        }

        [Fact]
        public void testTimestamp()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(Timestamp));
            TypeDescription schema = TypeDescription.createTimestamp();
            List<Timestamp> tslist = new List<Timestamp>();

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .stripeSize(100000)
                .bufferSize(10000)
                .version(OrcFile.Version.V_0_11)))
            {
                tslist.Add(Timestamp.Parse("2037-01-01 00:00:00.000999"));
                tslist.Add(Timestamp.Parse("2003-01-01 00:00:00.000000222"));
                tslist.Add(Timestamp.Parse("1999-01-01 00:00:00.999999999"));
                tslist.Add(Timestamp.Parse("1995-01-01 00:00:00.688888888"));
                tslist.Add(Timestamp.Parse("2002-01-01 00:00:00.1"));
                tslist.Add(Timestamp.Parse("2010-03-02 00:00:00.000009001"));
                tslist.Add(Timestamp.Parse("2005-01-01 00:00:00.000002229"));
                tslist.Add(Timestamp.Parse("2006-01-01 00:00:00.900203003"));
                tslist.Add(Timestamp.Parse("2003-01-01 00:00:00.800000007"));
                tslist.Add(Timestamp.Parse("1996-08-02 00:00:00.723100809"));
                tslist.Add(Timestamp.Parse("1998-11-02 00:00:00.857340643"));
                tslist.Add(Timestamp.Parse("2008-10-02 00:00:00"));

                VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
                LongColumnVector vec = new LongColumnVector(1024);
                batch.cols[0] = vec;
                batch.reset();
                batch.size = tslist.Count;
                for (int i = 0; i < tslist.Count; ++i)
                {
                    Timestamp ts = tslist[i];
                    vec.vector[i] = ts.Nanoseconds;
                }
                writer.addRowBatch(batch);
                writer.close();
                schema = writer.getSchema();
            }

            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows(null);
            int idx = 0;
            while (rows.hasNext())
            {
                object row = rows.next();
                Assert.Equal(tslist[idx++].getNanos(), ((Timestamp)row).getNanos());
            }
            Assert.Equal(tslist.Count, rows.getRowNumber());
            Assert.Equal(0, schema.getMaximumId());
            bool[] expected = new bool[] { false };
            bool[] included = OrcUtils.includeColumns("", schema);
            Assert.Equal(expected, included);
        }

        [Fact]
        public void testStringAndBinaryStatistics()
        {
            TypeDescription schema = TypeDescription.createStruct()
                .addField("bytes1", TypeDescription.createBinary())
                .addField("string1", TypeDescription.createString());

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .stripeSize(100000)
                .bufferSize(10000)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                batch.size = 4;
                BytesColumnVector field1 = (BytesColumnVector)batch.cols[0];
                BytesColumnVector field2 = (BytesColumnVector)batch.cols[1];
                field1.setVal(0, bytesArray(0, 1, 2, 3, 4));
                field1.setVal(1, bytesArray(0, 1, 2, 3));
                field1.setVal(2, bytesArray(0, 1, 2, 3, 4, 5));
                field1.noNulls = false;
                field1.isNull[3] = true;
                field2.setVal(0, "foo".getBytes());
                field2.setVal(1, "bar".getBytes());
                field2.noNulls = false;
                field2.isNull[2] = true;
                field2.setVal(3, "hi".getBytes());
                writer.addRowBatch(batch);
                writer.close();
                schema = writer.getSchema();
                Assert.Equal(2, schema.getMaximumId());
            }

            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));

            bool[] expected = new bool[] { false, false, true };
            bool[] included = OrcUtils.includeColumns("string1", schema);
            Assert.Equal(expected, included);

            expected = new bool[] { false, false, false };
            included = OrcUtils.includeColumns("", schema);
            Assert.Equal(expected, included);

            expected = new bool[] { false, false, false };
            included = OrcUtils.includeColumns(null, schema);
            Assert.Equal(expected, included);

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
            Assert.Equal(ObjectInspectorCategory.STRUCT,
                readerInspector.getCategory());
            Assert.Equal("struct<bytes1:binary,string1:string>",
                readerInspector.getTypeName());
            IList<StructField> fields =
                readerInspector.getAllStructFieldRefs();
            BinaryObjectInspector bi = (BinaryObjectInspector)readerInspector.
                getStructFieldRef("bytes1").getFieldObjectInspector();
            StringObjectInspector st = (StringObjectInspector)readerInspector.
                getStructFieldRef("string1").getFieldObjectInspector();
            RecordReader rows = reader.rows();
            Object row = rows.next();
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

            // check the contents of third row
            Assert.Equal(true, rows.hasNext());
            row = rows.next();
            Assert.Equal(bytes(0, 1, 2, 3, 4, 5), bi.get(
                readerInspector.getStructFieldData(row, fields[0])));
            Assert.Null(st.getPrimitiveJavaObject(readerInspector.
                getStructFieldData(row, fields[1])));

            // check the contents of fourth row
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
        public void testStripeLevelStats()
        {
            TypeDescription schema = TypeDescription.createStruct()
                .addField("int1", TypeDescription.createInt())
                .addField("string1", TypeDescription.createString());

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .stripeSize(100000)
                .bufferSize(10000)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                batch.size = 1000;
                LongColumnVector field1 = (LongColumnVector)batch.cols[0];
                BytesColumnVector field2 = (BytesColumnVector)batch.cols[1];
                field1.isRepeating = true;
                field2.isRepeating = true;
                for (int b = 0; b < 11; b++)
                {
                    if (b >= 5)
                    {
                        if (b >= 10)
                        {
                            field1.vector[0] = 3;
                            field2.setVal(0, "three".getBytes());
                        }
                        else
                        {
                            field1.vector[0] = 2;
                            field2.setVal(0, "two".getBytes());
                        }
                    }
                    else
                    {
                        field1.vector[0] = 1;
                        field2.setVal(0, "one".getBytes());
                    }
                    writer.addRowBatch(batch);
                }

                writer.close();
                schema = writer.getSchema();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

            Assert.Equal(2, schema.getMaximumId());
            bool[] expected = new bool[] { false, true, false };
            bool[] included = OrcUtils.includeColumns("int1", schema);
            Assert.Equal(expected, included);

            List<StripeStatistics> stats = reader.getStripeStatistics();
            int numStripes = stats.Count;
            Assert.Equal(3, numStripes);
            StripeStatistics ss1 = stats[0];
            StripeStatistics ss2 = stats[1];
            StripeStatistics ss3 = stats[2];

            Assert.Equal(5000, ss1.getColumnStatistics()[0].getNumberOfValues());
            Assert.Equal(5000, ss2.getColumnStatistics()[0].getNumberOfValues());
            Assert.Equal(1000, ss3.getColumnStatistics()[0].getNumberOfValues());

            Assert.Equal(5000, (ss1.getColumnStatistics()[1]).getNumberOfValues());
            Assert.Equal(5000, (ss2.getColumnStatistics()[1]).getNumberOfValues());
            Assert.Equal(1000, (ss3.getColumnStatistics()[1]).getNumberOfValues());
            Assert.Equal(1, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getMinimum());
            Assert.Equal(2, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getMinimum());
            Assert.Equal(3, ((IntegerColumnStatistics)ss3.getColumnStatistics()[1]).getMinimum());
            Assert.Equal(1, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getMaximum());
            Assert.Equal(2, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getMaximum());
            Assert.Equal(3, ((IntegerColumnStatistics)ss3.getColumnStatistics()[1]).getMaximum());
            Assert.Equal(5000, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getSum());
            Assert.Equal(10000, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getSum());
            Assert.Equal(3000, ((IntegerColumnStatistics)ss3.getColumnStatistics()[1]).getSum());

            Assert.Equal(5000, (ss1.getColumnStatistics()[2]).getNumberOfValues());
            Assert.Equal(5000, (ss2.getColumnStatistics()[2]).getNumberOfValues());
            Assert.Equal(1000, (ss3.getColumnStatistics()[2]).getNumberOfValues());
            Assert.Equal("one", ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getMinimum());
            Assert.Equal("two", ((StringColumnStatistics)ss2.getColumnStatistics()[2]).getMinimum());
            Assert.Equal("three", ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getMinimum());
            Assert.Equal("one", ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getMaximum());
            Assert.Equal("two", ((StringColumnStatistics)ss2.getColumnStatistics()[2]).getMaximum());
            Assert.Equal("three", ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getMaximum());
            Assert.Equal(15000, ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getSum());
            Assert.Equal(15000, ((StringColumnStatistics)ss2.getColumnStatistics()[2]).getSum());
            Assert.Equal(5000, ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getSum());

            RecordReaderImpl recordReader = (RecordReaderImpl)reader.rows();
            OrcProto.RowIndex[] index = recordReader.readRowIndex(0, null, null).getRowGroupIndex();
            Assert.Equal(3, index.Length);
            IList<OrcProto.RowIndexEntry> items = index[1].EntryList;
            Assert.Equal(1, items.Count);
            Assert.Equal(3, items[0].PositionsCount);
            Assert.Equal(0UL, items[0].GetPositions(0));
            Assert.Equal(0UL, items[0].GetPositions(1));
            Assert.Equal(0UL, items[0].GetPositions(2));
            Assert.Equal(1, items[0].Statistics.IntStatistics.Minimum);
            index = recordReader.readRowIndex(1, null, null).getRowGroupIndex();
            Assert.Equal(3, index.Length);
            items = index[1].EntryList;
            Assert.Equal(2, items[0].Statistics.IntStatistics.Maximum);
        }

        private static void setInner(StructColumnVector inner, int rowId, int i, string value)
        {
            ((LongColumnVector)inner.fields[0]).vector[rowId] = i;
            if (value != null)
            {
                ((BytesColumnVector)inner.fields[1]).setVal(rowId, value.getBytes());
            }
            else
            {
                inner.fields[1].isNull[rowId] = true;
                inner.fields[1].noNulls = false;
            }
        }

        private static void setInnerList(ListColumnVector list, int rowId, List<InnerStruct> value)
        {
            if (value != null)
            {
                if (list.childCount + value.Count > list.child.isNull.Length)
                {
                    list.child.ensureSize(list.childCount * 2, true);
                }
                list.lengths[rowId] = value.Count;
                list.offsets[rowId] = list.childCount;
                for (int i = 0; i < list.lengths[rowId]; ++i)
                {
                    InnerStruct inner = value[i];
                    setInner((StructColumnVector)list.child, i + list.childCount,
                        inner.int1, inner.string1.ToString());
                }
                list.childCount += value.Count;
            }
            else
            {
                list.isNull[rowId] = true;
                list.noNulls = false;
            }
        }

        private static void setInnerMap(MapColumnVector map, int rowId, Dictionary<string, InnerStruct> value)
        {
            if (value != null)
            {
                if (map.childCount >= map.keys.isNull.Length)
                {
                    map.keys.ensureSize(map.childCount * 2, true);
                    map.values.ensureSize(map.childCount * 2, true);
                }
                map.lengths[rowId] = value.Count;
                int offset = map.childCount;
                map.offsets[rowId] = offset;

                foreach (KeyValuePair<string, InnerStruct> entry in value)
                {
                    ((BytesColumnVector)map.keys).setVal(offset, entry.Key.getBytes());
                    InnerStruct inner = entry.Value;
                    setInner((StructColumnVector)map.values, offset, inner.int1,
                        inner.string1.ToString());
                    offset += 1;
                }
                map.childCount = offset;
            }
            else
            {
                map.isNull[rowId] = true;
                map.noNulls = false;
            }
        }

        private static void setMiddleStruct(StructColumnVector middle, int rowId, MiddleStruct value)
        {
            if (value != null)
            {
                setInnerList((ListColumnVector)middle.fields[0], rowId, value.list);
            }
            else
            {
                middle.isNull[rowId] = true;
                middle.noNulls = false;
            }
        }

        private static void setBigRow(VectorizedRowBatch batch, int rowId,
                                      bool b1, byte b2, short s1,
                                      int i1, long l1, float f1,
                                      double d1, byte[] b3, string s2,
                                      MiddleStruct m1, List<InnerStruct> l2,
                                      Dictionary<string, InnerStruct> m2)
        {
            ((LongColumnVector)batch.cols[0]).vector[rowId] = b1 ? 1 : 0;
            ((LongColumnVector)batch.cols[1]).vector[rowId] = b2;
            ((LongColumnVector)batch.cols[2]).vector[rowId] = s1;
            ((LongColumnVector)batch.cols[3]).vector[rowId] = i1;
            ((LongColumnVector)batch.cols[4]).vector[rowId] = l1;
            ((DoubleColumnVector)batch.cols[5]).vector[rowId] = f1;
            ((DoubleColumnVector)batch.cols[6]).vector[rowId] = d1;
            if (b3 != null)
            {
                ((BytesColumnVector)batch.cols[7]).setVal(rowId, b3, 0, b3.Length);
            }
            else
            {
                batch.cols[7].isNull[rowId] = true;
                batch.cols[7].noNulls = false;
            }
            if (s2 != null)
            {
                ((BytesColumnVector)batch.cols[8]).setVal(rowId, s2.getBytes());
            }
            else
            {
                batch.cols[8].isNull[rowId] = true;
                batch.cols[8].noNulls = false;
            }
            setMiddleStruct((StructColumnVector)batch.cols[9], rowId, m1);
            setInnerList((ListColumnVector)batch.cols[10], rowId, l2);
            setInnerMap((MapColumnVector)batch.cols[11], rowId, m2);
        }

        private static TypeDescription createInnerSchema()
        {
            return TypeDescription.createStruct()
                .addField("int1", TypeDescription.createInt())
                .addField("string1", TypeDescription.createString());
        }

        private static TypeDescription createBigRowSchema()
        {
            return TypeDescription.createStruct()
                .addField("boolean1", TypeDescription.createBoolean())
                .addField("byte1", TypeDescription.createByte())
                .addField("short1", TypeDescription.createShort())
                .addField("int1", TypeDescription.createInt())
                .addField("long1", TypeDescription.createLong())
                .addField("float1", TypeDescription.createFloat())
                .addField("double1", TypeDescription.createDouble())
                .addField("bytes1", TypeDescription.createBinary())
                .addField("string1", TypeDescription.createString())
                .addField("middle", TypeDescription.createStruct()
                    .addField("list", TypeDescription.createList(createInnerSchema())))
                .addField("list", TypeDescription.createList(createInnerSchema()))
                .addField("map", TypeDescription.createMap(
                    TypeDescription.createString(),
                    createInnerSchema()));
        }

        static void assertArrayEquals(bool[] expected, bool[] actual)
        {
            Assert.Equal(expected.Length, actual.Length);
            bool diff = false;
            for (int i = 0; i < expected.Length; ++i)
            {
                if (expected[i] != actual[i])
                {
                    System.Console.WriteLine("Difference at " + i + " expected: " + expected[i] +
                      " actual: " + actual[i]);
                    diff = true;
                }
            }
            Assert.Equal(false, diff);
        }

        [Fact]
        public void test1()
        {
            TypeDescription schema = createBigRowSchema();

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .stripeSize(100000)
                .bufferSize(10000)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                batch.size = 2;
                setBigRow(batch, 0, false, (byte)1, (short)1024, 65536,
                    Int64.MaxValue, (float)1.0, -15.0, bytes(0, 1, 2, 3, 4), "hi",
                    new MiddleStruct(MakeInner(1, "bye"), MakeInner(2, "sigh")),
                    MakeList(MakeInner(3, "good"), MakeInner(4, "bad")),
                    MakeMap());
                setBigRow(batch, 1, true, (byte)100, (short)2048, 65536,
                    Int64.MaxValue, (float)2.0, -5.0, bytes(), "bye",
                    new MiddleStruct(MakeInner(1, "bye"), MakeInner(2, "sigh")),
                    MakeList(MakeInner(100000000, "cat"), MakeInner(-100000, "in"), MakeInner(1234, "hat")),
                    MakeMap(MakeInner(5, "chani"), MakeInner(1, "mauddib")));
                writer.addRowBatch(batch);
                writer.close();
                schema = writer.getSchema();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

            Assert.Equal(23, schema.getMaximumId());
            bool[] expected = new bool[]
            {
                false, false, false, false, false,
                false, false, false, false, false,
                false, false, false, false, false,
                false, false, false, false, false,
                false, false, false, false
            };
            bool[] included = OrcUtils.includeColumns("", schema);
            Assert.Equal(expected, included);

            expected = new bool[]
            {
                false, true, false, false, false,
                false, false, false, false, true,
                true, true, true, true, true,
                false, false, false, false, true,
                true, true, true, true
            };
            included = OrcUtils.includeColumns("boolean1,string1,middle,map", schema);

            assertArrayEquals(expected, included);

            expected = new bool[]
            {
                false, true, false, false, false,
                false, false, false, false, true,
                true, true, true, true, true,
                false, false, false, false, true,
                true, true, true, true
            };
            included = OrcUtils.includeColumns("boolean1,string1,middle,map", schema);
            assertArrayEquals(expected, included);

            expected = new bool[]
            {
                false, true, true, true, true,
                true, true, true, true, true,
                true, true, true, true, true,
                true, true, true, true, true,
                true, true, true, true
            };
            included = OrcUtils.includeColumns(
                "boolean1,byte1,short1,int1,long1,float1,double1,bytes1,string1,middle,list,map",
                schema);
            Assert.Equal(expected, included);

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

            StripeStatistics ss = reader.getStripeStatistics()[0];
            Assert.Equal(2, ss.getColumnStatistics()[0].getNumberOfValues());
            Assert.Equal(1, ((BooleanColumnStatistics)ss.getColumnStatistics()[1]).getTrueCount());
            Assert.Equal(1024, ((IntegerColumnStatistics)ss.getColumnStatistics()[3]).getMinimum());
            Assert.Equal(2048, ((IntegerColumnStatistics)ss.getColumnStatistics()[3]).getMaximum());
            Assert.Equal(3072, ((IntegerColumnStatistics)ss.getColumnStatistics()[3]).getSum());
            Assert.Equal(-15.0, ((DoubleColumnStatistics)stats[7]).getMinimum());
            Assert.Equal(-5.0, ((DoubleColumnStatistics)stats[7]).getMaximum());
            Assert.Equal(-20.0, ((DoubleColumnStatistics)stats[7]).getSum(), 5);
            Assert.Equal("count: 2 hasNull: False min: -15 max: -5 sum: -20",
                stats[7].ToString());

            Assert.Equal("count: 2 hasNull: False min: bye max: hi sum: 5", stats[9].ToString());

            // check the inspectors
            StructObjectInspector readerInspector =
                (StructObjectInspector)reader.getObjectInspector();
            Assert.Equal(ObjectInspectorCategory.STRUCT,
                readerInspector.getCategory());
            Assert.Equal("struct<boolean1:boolean,byte1:tinyint,short1:smallint,"
                + "int1:int,long1:bigint,float1:float,double1:double,bytes1:"
                + "binary,string1:string,middle:struct<list:array<struct<int1:int,"
                + "string1:string>>>,list:array<struct<int1:int,string1:string>>,"
                + "map:map<string,struct<int1:int,string1:string>>>",
                readerInspector.getTypeName());
            IList<StructField> fields =
                readerInspector.getAllStructFieldRefs();
            BooleanObjectInspector bo = (BooleanObjectInspector)readerInspector.
                getStructFieldRef("boolean1").getFieldObjectInspector();
            ByteObjectInspector by = (ByteObjectInspector)readerInspector.
                getStructFieldRef("byte1").getFieldObjectInspector();
            ShortObjectInspector sh = (ShortObjectInspector)readerInspector.
                getStructFieldRef("short1").getFieldObjectInspector();
            IntObjectInspector @in = (IntObjectInspector)readerInspector.
                getStructFieldRef("int1").getFieldObjectInspector();
            LongObjectInspector lo = (LongObjectInspector)readerInspector.
                getStructFieldRef("long1").getFieldObjectInspector();
            FloatObjectInspector fl = (FloatObjectInspector)readerInspector.
                getStructFieldRef("float1").getFieldObjectInspector();
            DoubleObjectInspector dbl = (DoubleObjectInspector)readerInspector.
                getStructFieldRef("double1").getFieldObjectInspector();
            BinaryObjectInspector bi = (BinaryObjectInspector)readerInspector.
                getStructFieldRef("bytes1").getFieldObjectInspector();
            StringObjectInspector st = (StringObjectInspector)readerInspector.
                getStructFieldRef("string1").getFieldObjectInspector();
            StructObjectInspector mid = (StructObjectInspector)readerInspector.
                getStructFieldRef("middle").getFieldObjectInspector();
            IList<StructField> midFields =
                mid.getAllStructFieldRefs();
            ListObjectInspector midli =
                (ListObjectInspector)midFields[0].getFieldObjectInspector();
            StructObjectInspector inner = (StructObjectInspector)
                midli.getListElementObjectInspector();
            IList<StructField> inFields = inner.getAllStructFieldRefs();
            ListObjectInspector li = (ListObjectInspector)readerInspector.
                getStructFieldRef("list").getFieldObjectInspector();
            MapObjectInspector ma = (MapObjectInspector)readerInspector.
                getStructFieldRef("map").getFieldObjectInspector();
            StringObjectInspector mk = (StringObjectInspector)
                ma.getMapKeyObjectInspector();
            RecordReader rows = reader.rows();
            Object row = rows.next();
            Assert.NotNull(row);
            // check the contents of the first row
            Assert.Equal(false,
                bo.get(readerInspector.getStructFieldData(row, fields[0])));
            Assert.Equal(1, by.get(readerInspector.getStructFieldData(row,
                fields[1])));
            Assert.Equal(1024, sh.get(readerInspector.getStructFieldData(row,
                fields[2])));
            Assert.Equal(65536, @in.get(readerInspector.getStructFieldData(row,
                fields[3])));
            Assert.Equal(Int64.MaxValue, lo.get(readerInspector.
                getStructFieldData(row, fields[4])));
            Assert.Equal(1.0, fl.get(readerInspector.getStructFieldData(row,
                fields[5])), 5);
            Assert.Equal(-15.0, dbl.get(readerInspector.getStructFieldData(row,
                fields[6])), 5);
            Assert.Equal(bytes(0, 1, 2, 3, 4), bi.get(
                readerInspector.getStructFieldData(row, fields[7])));
            Assert.Equal("hi", st.getPrimitiveJavaObject(readerInspector.
                getStructFieldData(row, fields[8])));
            IList<object> midRow = midli.getList(mid.getStructFieldData(readerInspector.
                getStructFieldData(row, fields[9]), midFields[0]));
            Assert.NotNull(midRow);
            Assert.Equal(2, midRow.Count);
            Assert.Equal(1, @in.get(inner.getStructFieldData(midRow[0],
                inFields[0])));
            Assert.Equal("bye", st.getPrimitiveJavaObject(inner.getStructFieldData
                (midRow[0], inFields[1])));
            Assert.Equal(2, @in.get(inner.getStructFieldData(midRow[1],
                inFields[0])));
            Assert.Equal("sigh", st.getPrimitiveJavaObject(inner.getStructFieldData
                (midRow[1], inFields[1])));
            IList<object> list = li.getList(readerInspector.getStructFieldData(row,
                fields[10]));
            Assert.Equal(2, list.Count);
            Assert.Equal(3, @in.get(inner.getStructFieldData(list[0],
                inFields[0])));
            Assert.Equal("good", st.getPrimitiveJavaObject(inner.getStructFieldData
                (list[0], inFields[1])));
            Assert.Equal(4, @in.get(inner.getStructFieldData(list[1],
                inFields[0])));
            Assert.Equal("bad", st.getPrimitiveJavaObject(inner.getStructFieldData
                (list[1], inFields[1])));
            IDictionary<object, object> map = ma.getMap(readerInspector.getStructFieldData(row,
                fields[11]));
            Assert.Equal(0, map.Count);

            // check the contents of second row
            Assert.Equal(true, rows.hasNext());
            row = rows.next();
            Assert.Equal(true,
                bo.get(readerInspector.getStructFieldData(row, fields[0])));
            Assert.Equal(100, by.get(readerInspector.getStructFieldData(row,
                fields[1])));
            Assert.Equal(2048, sh.get(readerInspector.getStructFieldData(row,
                fields[2])));
            Assert.Equal(65536, @in.get(readerInspector.getStructFieldData(row,
                fields[3])));
            Assert.Equal(Int64.MaxValue, lo.get(readerInspector.
                getStructFieldData(row, fields[4])));
            Assert.Equal(2.0, fl.get(readerInspector.getStructFieldData(row,
                fields[5])), 5);
            Assert.Equal(-5.0, dbl.get(readerInspector.getStructFieldData(row,
                fields[6])), 5);
            Assert.Equal(bytes(), bi.get(
                readerInspector.getStructFieldData(row, fields[7])));
            Assert.Equal("bye", st.getPrimitiveJavaObject(readerInspector.
                getStructFieldData(row, fields[8])));
            midRow = midli.getList(mid.getStructFieldData(readerInspector.
                getStructFieldData(row, fields[9]), midFields[0]));
            Assert.NotNull(midRow);
            Assert.Equal(2, midRow.Count);
            Assert.Equal(1, @in.get(inner.getStructFieldData(midRow[0],
                inFields[0])));
            Assert.Equal("bye", st.getPrimitiveJavaObject(inner.getStructFieldData
                (midRow[0], inFields[1])));
            Assert.Equal(2, @in.get(inner.getStructFieldData(midRow[1],
                inFields[0])));
            Assert.Equal("sigh", st.getPrimitiveJavaObject(inner.getStructFieldData
                (midRow[1], inFields[1])));
            list = li.getList(readerInspector.getStructFieldData(row,
                fields[10]));
            Assert.Equal(3, list.Count);
            Assert.Equal(100000000, @in.get(inner.getStructFieldData(list[0],
                inFields[0])));
            Assert.Equal("cat", st.getPrimitiveJavaObject(inner.getStructFieldData
                (list[0], inFields[1])));
            Assert.Equal(-100000, @in.get(inner.getStructFieldData(list[1],
                inFields[0])));
            Assert.Equal("in", st.getPrimitiveJavaObject(inner.getStructFieldData
                (list[1], inFields[1])));
            Assert.Equal(1234, @in.get(inner.getStructFieldData(list[2],
                inFields[0])));
            Assert.Equal("hat", st.getPrimitiveJavaObject(inner.getStructFieldData
                (list[2], inFields[1])));
            map = ma.getMap(readerInspector.getStructFieldData(row,
                fields[11]));
            Assert.Equal(2, map.Count);
            bool[] found = new bool[2];
            foreach (object key in map.Keys)
            {
                string str = mk.getPrimitiveJavaObject(key);
                if (str.Equals("chani"))
                {
                    Assert.Equal(false, found[0]);
                    Assert.Equal(5, @in.get(inner.getStructFieldData(map.get(key),
                        inFields[0])));
                    Assert.Equal(str, st.getPrimitiveJavaObject(
                        inner.getStructFieldData(map.get(key), inFields[1])));
                    found[0] = true;
                }
                else if (str.Equals("mauddib"))
                {
                    Assert.Equal(false, found[1]);
                    Assert.Equal(1, @in.get(inner.getStructFieldData(map.get(key),
                        inFields[0])));
                    Assert.Equal(str, st.getPrimitiveJavaObject(
                        inner.getStructFieldData(map.get(key), inFields[1])));
                    found[1] = true;
                }
                else
                {
                    throw new ArgumentException("Unknown key " + str);
                }
            }
            Assert.Equal(true, found[0]);
            Assert.Equal(true, found[1]);

            // handle the close up
            Assert.Equal(false, rows.hasNext());
            rows.close();
        }

        [Fact]
        public void testColumnProjection()
        {
            TypeDescription schema = createInnerSchema();
            Random r1 = new Random(1);
            Random r2 = new Random(2);
            int minInt = 0, maxInt = 0;
            string minStr = null, maxStr = null;

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .stripeSize(1000)
                .compress(CompressionKind.NONE)
                .bufferSize(100)
                .rowIndexStride(1000)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                int x;
                string y;
                batch.size = 1000;
                bool first = true;
                for (int b = 0; b < 21; ++b)
                {
                    for (int r = 0; r < 1000; ++r)
                    {
                        x = r1.Next();
                        y = Long.toHexString(r2.NextLong());
                        if (first || x < minInt)
                        {
                            minInt = x;
                        }
                        if (first || x > maxInt)
                        {
                            maxInt = x;
                        }
                        if (first || y.CompareTo(minStr) < 0)
                        {
                            minStr = y;
                        }
                        if (first || y.CompareTo(maxStr) > 0)
                        {
                            maxStr = y;
                        }
                        first = false;
                        ((LongColumnVector)batch.cols[0]).vector[r] = x;
                        ((BytesColumnVector)batch.cols[1]).setVal(r, y.getBytes());
                    }
                    writer.addRowBatch(batch);
                }
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

            // check out the statistics
            ColumnStatistics[] stats = reader.getStatistics();
            Assert.Equal(3, stats.Length);
            foreach (ColumnStatistics s in stats)
            {
                Assert.Equal(21000, s.getNumberOfValues());
                if (s is IntegerColumnStatistics)
                {
                    Assert.Equal(minInt, ((IntegerColumnStatistics)s).getMinimum());
                    Assert.Equal(maxInt, ((IntegerColumnStatistics)s).getMaximum());
                }
                else if (s is StringColumnStatistics)
                {
                    Assert.Equal(maxStr, ((StringColumnStatistics)s).getMaximum());
                    Assert.Equal(minStr, ((StringColumnStatistics)s).getMinimum());
                }
            }

            // check out the types
            IList<OrcProto.Type> types = reader.getTypes();
            Assert.Equal(3, types.Count);
            Assert.Equal(OrcProto.Type.Types.Kind.STRUCT, types[0].Kind);
            Assert.Equal(2, types[0].SubtypesCount);
            Assert.Equal(1U, types[0].GetSubtypes(0));
            Assert.Equal(2U, types[0].GetSubtypes(1));
            Assert.Equal(OrcProto.Type.Types.Kind.INT, types[1].Kind);
            Assert.Equal(0, types[1].SubtypesCount);
            Assert.Equal(OrcProto.Type.Types.Kind.STRING, types[2].Kind);
            Assert.Equal(0, types[2].SubtypesCount);

            // read the contents and make sure they match
            RecordReader rows1 = reader.rows(new bool[] { true, true, false });
            RecordReader rows2 = reader.rows(new bool[] { true, false, true });
            r1 = new Random(1);
            r2 = new Random(2);
            for (int i = 0; i < 21000; ++i)
            {
                Assert.Equal(true, rows1.hasNext());
                Assert.Equal(true, rows2.hasNext());
                OrcStruct row1 = (OrcStruct)rows1.next();
                OrcStruct row2 = (OrcStruct)rows2.next();
                Assert.Equal(r1.Next(), row1.getFieldValue(0));
                Assert.Equal(Long.toHexString(r2.NextLong()),
                    row2.getFieldValue(1).ToString());
            }
            Assert.Equal(false, rows1.hasNext());
            Assert.Equal(false, rows2.hasNext());
            rows1.close();
            rows2.close();
        }

        [Fact]
        public void testEmptyFile()
        {
            TypeDescription schema = createBigRowSchema();

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .stripeSize(1000)
                .compress(CompressionKind.NONE)
                .bufferSize(100)))
            {
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));
            Assert.Equal(false, reader.rows().hasNext());
            Assert.Equal(CompressionKind.NONE, reader.getCompression());
            Assert.Equal(0, reader.getNumberOfRows());
            Assert.Equal(0, reader.getCompressionSize());
            Assert.Equal(0, reader.getMetadataKeys().Count);
            Assert.Equal(3, reader.getContentLength());
            Assert.Equal(0, reader.getStripes().Count);
        }

        [Fact]
        public void metaData()
        {
            TypeDescription schema = createBigRowSchema();
            ByteBuffer bigBuf = ByteBuffer.allocate(40000);

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .setSchema(schema)
                    .stripeSize(1000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(100)))
            {
                writer.addUserMetadata("my.meta", byteBuf(1, 2, 3, 4, 5, 6, 7, -1, -2, 127,
                                                          -128));
                writer.addUserMetadata("clobber", byteBuf(1, 2, 3));
                writer.addUserMetadata("clobber", byteBuf(4, 3, 2, 1));
                Random random = new Random(0);
                random.NextBytes(bigBuf.array());
                writer.addUserMetadata("big", bigBuf);
                bigBuf.position(0);
                VectorizedRowBatch batch = schema.createRowBatch();
                batch.size = 1;
                setBigRow(batch, 0, true, (byte)127, (short)1024, 42,
                    42L * 1024 * 1024 * 1024, (float)3.1415, -2.713, null,
                    null, null, null, null);
                writer.addRowBatch(batch);
                writer.addUserMetadata("clobber", byteBuf(5, 7, 11, 13, 17, 19));
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            Assert.Equal(byteBuf(5, 7, 11, 13, 17, 19), reader.getMetadataValue("clobber"));
            Assert.Equal(byteBuf(1, 2, 3, 4, 5, 6, 7, -1, -2, 127, -128),
                reader.getMetadataValue("my.meta"));
            Assert.Equal(bigBuf, reader.getMetadataValue("big"));
            try
            {
                reader.getMetadataValue("unknown");
                Assert.True(false);
            }
            catch (ArgumentException)
            {
                // PASS
            }
            int i = 0;
            foreach (string key in reader.getMetadataKeys())
            {
                if ("my.meta".Equals(key) ||
                    "clobber".Equals(key) ||
                    "big".Equals(key))
                {
                    i += 1;
                }
                else
                {
                    throw new ArgumentException("unknown key " + key);
                }
            }
            Assert.Equal(3, i);
            int numStripes = reader.getStripeStatistics().Count;
            Assert.Equal(1, numStripes);
        }

        /**
         * Generate an ORC file with a range of dates and times.
         */
        public void createOrcDateFile(string path, int minYear, int maxYear)
        {
            TypeDescription schema = TypeDescription.createStruct()
                .addField("time", TypeDescription.createTimestamp())
                .addField("date", TypeDescription.createDate());

            using (Stream file = File.OpenWrite(path))
            using (Writer writer = OrcFile.createWriter(path, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .stripeSize(100000)
                .bufferSize(10000)
                .blockPadding(false)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                batch.size = 1000;
                for (int year = minYear; year < maxYear; ++year)
                {
                    for (int ms = 1000; ms < 2000; ++ms)
                    {
                        ((LongColumnVector)batch.cols[0]).vector[ms - 1000] =
                            Timestamp.Parse(year + "-05-05 12:34:56." + ms).Nanoseconds;
                        ((LongColumnVector)batch.cols[1]).vector[ms - 1000] =
                            new Date(year - 1900, 11, 25).Days;
                    }
                    writer.addRowBatch(batch);
                }
                writer.close();
            }

            Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            for (int year = minYear; year < maxYear; ++year)
            {
                for (int ms = 1000; ms < 2000; ++ms)
                {
                    OrcStruct row = (OrcStruct)rows.next();
                    Assert.Equal(Timestamp.Parse(year + "-05-05 12:34:56." + ms),
                        row.getFieldValue(0));
                    Assert.Equal(new Date(year - 1900, 11, 25),
                        row.getFieldValue(1));
                }
            }
        }

        [Fact]
        public void testDate1900()
        {
            createOrcDateFile(testFilePath, 1900, 1970);
        }

        [Fact]
        public void testDate2038()
        {
            createOrcDateFile(testFilePath, 2038, 2250);
        }

        private static void setUnion(VectorizedRowBatch batch, int rowId,
                                     Timestamp? ts, int? tag, int? i, string s,
                                     HiveDecimal dec)
        {
            UnionColumnVector union = (UnionColumnVector)batch.cols[1];
            if (ts != null)
            {
                ((LongColumnVector)batch.cols[0]).vector[rowId] = ts.Value.Nanoseconds;
            }
            else
            {
                batch.cols[0].isNull[rowId] = true;
                batch.cols[0].noNulls = false;
            }
            if (tag != null)
            {
                union.tags[rowId] = tag.Value;
                if (tag == 0)
                {
                    if (i != null)
                    {
                        ((LongColumnVector)union.fields[tag.Value]).vector[rowId] = i.Value;
                    }
                    else
                    {
                        union.fields[tag.Value].isNull[rowId] = true;
                        union.fields[tag.Value].noNulls = false;
                    }
                }
                else if (tag == 1)
                {
                    if (s != null)
                    {
                        ((BytesColumnVector)union.fields[tag.Value]).setVal(rowId, s.getBytes());
                    }
                    else
                    {
                        union.fields[tag.Value].isNull[rowId] = true;
                        union.fields[tag.Value].noNulls = false;
                    }
                }
                else
                {
                    throw new ArgumentException("Bad tag " + tag);
                }
            }
            else
            {
                batch.cols[1].isNull[rowId] = true;
                batch.cols[1].noNulls = false;
            }
            if (dec != null)
            {
                ((DecimalColumnVector)batch.cols[2]).vector[rowId] = dec;
            }
            else
            {
                batch.cols[2].isNull[rowId] = true;
                batch.cols[2].noNulls = false;
            }
        }

        /**
           * We test union, timestamp, and decimal separately since we need to make the
           * object inspector manually. (The Hive reflection-based doesn't handle
           * them properly.)
           */
        [Fact]
        public void testUnionAndTimestamp()
        {
            TypeDescription schema = TypeDescription.createStruct()
                .addField("time", TypeDescription.createTimestamp())
                .addField("union", TypeDescription.createUnion()
                    .addUnionChild(TypeDescription.createInt())
                    .addUnionChild(TypeDescription.createString()))
                .addField("decimal", TypeDescription.createDecimal()
                    .withPrecision(38)
                    .withScale(18));
            HiveDecimal maxValue = HiveDecimal.Parse("10000000000000000000");
            Random rand = new Random(42);

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .stripeSize(1000)
                .compress(CompressionKind.NONE)
                .bufferSize(100)
                .blockPadding(false)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                batch.size = 6;
                setUnion(batch, 0, Timestamp.Parse("2000-03-12 15:00:00"), 0, 42, null,
                         HiveDecimal.Parse("12345678.6547456"));
                setUnion(batch, 1, Timestamp.Parse("2000-03-20 12:00:00.123456789"),
                    1, null, "hello", HiveDecimal.Parse("-5643.234"));

                setUnion(batch, 2, null, null, null, null, null);
                setUnion(batch, 3, null, 0, null, null, null);
                setUnion(batch, 4, null, 1, null, null, null);

                setUnion(batch, 5, Timestamp.Parse("1970-01-01 00:00:00"), 0, 200000,
                    null, HiveDecimal.Parse("10000000000000000000"));
                writer.addRowBatch(batch);

                batch.reset();
                for (int i = 1970; i < 2038; ++i)
                {
                    Timestamp ts = Timestamp.Parse(i + "-05-05 12:34:56." + i);
                    HiveDecimal dec = HiveDecimal.create(rand.NextBigInteger(64), rand.Next(18));
                    if ((i & 1) == 0)
                    {
                        setUnion(batch, batch.size++, ts, 0, i * i, null, dec);
                    }
                    else
                    {
                        setUnion(batch, batch.size++, ts, 1, null, (i * i).ToString(), dec);
                    }
                    if (maxValue.CompareTo(dec) < 0)
                    {
                        maxValue = dec;
                    }
                }
                writer.addRowBatch(batch);
                batch.reset();

                // let's add a lot of constant rows to test the rle
                batch.size = 1000;
                for (int c = 0; c < batch.cols.Length; ++c)
                {
                    batch.cols[c].setRepeating(true);
                }
                setUnion(batch, 0, null, 0, 1732050807, null, null);
                for (int i = 0; i < 5; ++i)
                {
                    writer.addRowBatch(batch);
                }

                batch.reset();
                batch.size = 3;
                setUnion(batch, 0, null, 0, 0, null, null);
                setUnion(batch, 1, null, 0, 10, null, null);
                setUnion(batch, 2, null, 0, 138, null, null);
                writer.addRowBatch(batch);
                writer.close();
                schema = writer.getSchema();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

            Assert.Equal(5, schema.getMaximumId());
            bool[] expected = new bool[] { false, false, false, false, false, false };
            bool[] included = OrcUtils.includeColumns("", schema);
            Assert.Equal(expected, included);

            expected = new bool[] { false, true, false, false, false, true };
            included = OrcUtils.includeColumns("time,decimal", schema);
            Assert.Equal(expected, included);

            expected = new bool[] { false, false, true, true, true, false };
            included = OrcUtils.includeColumns("union", schema);
            Assert.Equal(expected, included);

            Assert.Equal(0, reader.getMetadataKeys().Count);
            Assert.Equal(5077, reader.getNumberOfRows());
            DecimalColumnStatistics stats =
                (DecimalColumnStatistics)reader.getStatistics()[5];
            Assert.Equal(71, stats.getNumberOfValues());
            Assert.Equal(HiveDecimal.Parse("-5643.234"), stats.getMinimum());
            Assert.Equal(maxValue, stats.getMaximum());
            // TODO: fix this
            //    Assert.Equal(null,stats.getSum());
            int stripeCount = 0;
            int rowCount = 0;
            long currentOffset = -1;
            foreach (StripeInformation stripe in reader.getStripes())
            {
                stripeCount += 1;
                rowCount += (int)stripe.getNumberOfRows();
                if (currentOffset < 0)
                {
                    currentOffset = stripe.getOffset() + stripe.getLength();
                }
                else
                {
                    Assert.Equal(currentOffset, stripe.getOffset());
                    currentOffset += stripe.getLength();
                }
            }
            Assert.Equal(reader.getNumberOfRows(), rowCount);
            Assert.Equal(2, stripeCount);
            Assert.Equal(reader.getContentLength(), currentOffset);
            RecordReader rows = reader.rows();
            Assert.Equal(0, rows.getRowNumber());
            Assert.Equal(0.0, rows.getProgress(), 6);
            Assert.Equal(true, rows.hasNext());
            OrcStruct row = (OrcStruct)rows.next();
            Assert.Equal(1, rows.getRowNumber());
            ObjectInspector inspector = reader.getObjectInspector();
            Assert.Equal("struct<time:timestamp,union:uniontype<int,string>,decimal:decimal(38,18)>",
                inspector.getTypeName());
            Assert.Equal(Timestamp.Parse("2000-03-12 15:00:00"), row.getFieldValue(0));
            OrcUnion union = (OrcUnion)row.getFieldValue(1);
            Assert.Equal(0, union.getTag());
            Assert.Equal(42, union.getObject());
            Assert.Equal(HiveDecimal.Parse("12345678.6547456"), row.getFieldValue(2));
            row = (OrcStruct)rows.next();
            Assert.Equal(2, rows.getRowNumber());
            Assert.Equal(Timestamp.Parse("2000-03-20 12:00:00.123456789"), row.getFieldValue(0));
            Assert.Equal(1, union.getTag());
            Assert.Equal("hello", union.getObject());
            Assert.Equal(HiveDecimal.Parse("-5643.234"), row.getFieldValue(2));
            row = (OrcStruct)rows.next();
            Assert.Null(row.getFieldValue(0));
            Assert.Null(row.getFieldValue(1));
            Assert.Null(row.getFieldValue(2));
            row = (OrcStruct)rows.next();
            Assert.Null(row.getFieldValue(0));
            union = (OrcUnion)row.getFieldValue(1);
            Assert.Equal(0, union.getTag());
            Assert.Null(union.getObject());
            Assert.Null(row.getFieldValue(2));
            row = (OrcStruct)rows.next();
            Assert.Null(row.getFieldValue(0));
            Assert.Equal(1, union.getTag());
            Assert.Null(union.getObject());
            Assert.Null(row.getFieldValue(2));
            row = (OrcStruct)rows.next();
            Assert.Equal(Timestamp.Parse("1970-01-01 00:00:00"), row.getFieldValue(0));
            Assert.Equal(200000, union.getObject());
            Assert.Equal(HiveDecimal.Parse("10000000000000000000"), row.getFieldValue(2));
            rand = new Random(42);
            for (int i = 1970; i < 2038; ++i)
            {
                row = (OrcStruct)rows.next();
                Assert.Equal(Timestamp.Parse(i + "-05-05 12:34:56." + i),
                    row.getFieldValue(0));
                if ((i & 1) == 0)
                {
                    Assert.Equal(0, union.getTag());
                    Assert.Equal(i * i, union.getObject());
                }
                else
                {
                    Assert.Equal(1, union.getTag());
                    Assert.Equal((i * i).ToString(), union.getObject());
                }
                Assert.Equal(
                    HiveDecimal.create(rand.NextBigInteger(64), rand.Next(18)),
                    row.getFieldValue(2));
            }
            for (int i = 0; i < 5000; ++i)
            {
                row = (OrcStruct)rows.next();
                Assert.Equal(1732050807, union.getObject());
            }
            row = (OrcStruct)rows.next();
            Assert.Equal(0, union.getObject());
            row = (OrcStruct)rows.next();
            Assert.Equal(10, union.getObject());
            row = (OrcStruct)rows.next();
            Assert.Equal(138, union.getObject());
            Assert.Equal(false, rows.hasNext());
            Assert.Equal(1.0, rows.getProgress(), 5);
            Assert.Equal(reader.getNumberOfRows(), rows.getRowNumber());
            rows.seekToRow(1);
            row = (OrcStruct)rows.next();
            Assert.Equal(Timestamp.Parse("2000-03-20 12:00:00.123456789"), row.getFieldValue(0));
            Assert.Equal(1, union.getTag());
            Assert.Equal("hello", union.getObject());
            Assert.Equal(HiveDecimal.Parse("-5643.234"), row.getFieldValue(2));
            rows.close();
        }

        /**
         * Read and write a randomly generated snappy file.
         * @
         */
        [Fact]
        public void testSnappy()
        {
            TypeDescription schema = createInnerSchema();
            Random rand = new Random(12);

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .stripeSize(1000)
                .compress(CompressionKind.SNAPPY)
                .bufferSize(100)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                batch.size = 1000;
                for (int b = 0; b < 10; ++b)
                {
                    for (int r = 0; r < 1000; ++r)
                    {
                        ((LongColumnVector)batch.cols[0]).vector[r] = rand.Next();
                        ((BytesColumnVector)batch.cols[1]).setVal(r,
                            Integer.toHexString(rand.Next()).getBytes());
                    }
                    writer.addRowBatch(batch);
                }
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            rand = new Random(12);
            OrcStruct row = null;
            for (int i = 0; i < 10000; ++i)
            {
                Assert.Equal(true, rows.hasNext());
                row = (OrcStruct)rows.next();
                Assert.Equal(rand.Next(), row.getFieldValue(0));
                Assert.Equal(Integer.toHexString(rand.Next()),
                    row.getFieldValue(1).ToString());
            }
            Assert.Equal(false, rows.hasNext());
            rows.close();
        }

        /**
         * Read and write a randomly generated snappy file.
         * @
         */
        [Fact]
        public void testWithoutIndex()
        {
            TypeDescription schema = createInnerSchema();
            Random rand = new Random(24);

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .stripeSize(5000)
                .compress(CompressionKind.SNAPPY)
                .bufferSize(1000)
                .rowIndexStride(0)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                batch.size = 5;
                for (int c = 0; c < batch.cols.Length; ++c)
                {
                    batch.cols[c].setRepeating(true);
                }
                for (int i = 0; i < 10000; ++i)
                {
                    ((LongColumnVector)batch.cols[0]).vector[0] = rand.Next();
                    ((BytesColumnVector)batch.cols[1])
                        .setVal(0, Integer.toBinaryString(rand.Next()).getBytes());
                    writer.addRowBatch(batch);
                }
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));
            Assert.Equal(50000, reader.getNumberOfRows());
            Assert.Equal(0, reader.getRowIndexStride());
            StripeInformation stripe = reader.getStripes()[0];
            Assert.Equal(true, stripe.getDataLength() != 0);
            Assert.Equal(0, stripe.getIndexLength());
            RecordReader rows = reader.rows();
            rand = new Random(24);
            for (int i = 0; i < 10000; ++i)
            {
                int intVal = rand.Next();
                string strVal = Integer.toBinaryString(rand.Next());
                for (int j = 0; j < 5; ++j)
                {
                    Assert.Equal(true, rows.hasNext());
                    OrcStruct row = (OrcStruct)rows.next();
                    Assert.Equal(intVal, row.getFieldValue(0));
                    Assert.Equal(strVal, row.getFieldValue(1).ToString());
                }
            }
            Assert.Equal(false, rows.hasNext());
            rows.close();
        }

        [Fact]
        public void testSeek()
        {
            TypeDescription schema = createBigRowSchema();
            Random rand = new Random(42);
            const int COUNT = 32768;
            long[] intValues = new long[COUNT];
            double[] doubleValues = new double[COUNT];
            string[] stringValues = new string[COUNT];
            byte[][] byteValues = new byte[COUNT][];
            string[] words = new string[128];

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .stripeSize(200000)
                .bufferSize(65536)
                .rowIndexStride(1000)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                for (int i = 0; i < words.Length; ++i)
                {
                    words[i] = Integer.toHexString(rand.Next());
                }
                for (int i = 0; i < COUNT / 2; ++i)
                {
                    intValues[2 * i] = rand.NextLong();
                    intValues[2 * i + 1] = intValues[2 * i];
                    stringValues[2 * i] = words[rand.Next(words.Length)];
                    stringValues[2 * i + 1] = stringValues[2 * i];
                }
                for (int i = 0; i < COUNT; ++i)
                {
                    doubleValues[i] = rand.NextDouble();
                    byte[] buf = new byte[20];
                    rand.NextBytes(buf);
                    byteValues[i] = buf;
                }
                for (int i = 0; i < COUNT; ++i)
                {
                    appendRandomRow(batch, intValues, doubleValues, stringValues,
                        byteValues, words, i);
                    if (batch.size == 1024)
                    {
                        writer.addRowBatch(batch);
                        batch.reset();
                    }
                }
                if (batch.size != 0)
                {
                    writer.addRowBatch(batch);
                }
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));
            Assert.Equal(COUNT, reader.getNumberOfRows());
            RecordReader rows = reader.rows();
            // get the row index
            MetadataReader meta = ((RecordReaderImpl)rows).getMetadataReader();
            RecordReaderImpl.Index index =
                meta.readRowIndex(reader.getStripes()[0], null, null, null, null,
                    null);
            // check the primitive columns to make sure they have the right number of
            // items in the first row group
            for (int c = 1; c < 9; ++c)
            {
                OrcProto.RowIndex colIndex = index.getRowGroupIndex()[c];
                Assert.Equal(1000U,
                    colIndex.GetEntry(0).Statistics.NumberOfValues);
            }
            OrcStruct row = null;
            for (int i = COUNT - 1; i >= 0; --i)
            {
                rows.seekToRow(i);
                row = (OrcStruct)rows.next();
                BigRow expected = createRandomRow(intValues, doubleValues,
                    stringValues, byteValues, words, i);
                Assert.Equal(expected.boolean1, row.getFieldValue(0));
                Assert.Equal(expected.byte1, row.getFieldValue(1));
                Assert.Equal(expected.short1, row.getFieldValue(2));
                Assert.Equal(expected.int1, row.getFieldValue(3));
                Assert.Equal(expected.long1, row.getFieldValue(4));
                Assert.Equal(expected.float1, (float)row.getFieldValue(5), 4);
                Assert.Equal(expected.double1, (double)row.getFieldValue(6), 4);
                Assert.Equal(expected.bytes1, row.getFieldValue(7));
                Assert.Equal(expected.string1, row.getFieldValue(8));
                List<InnerStruct> expectedList = expected.middle.list;
                IList<object> actualList =
                    (IList<object>)((OrcStruct)row.getFieldValue(9)).getFieldValue(0);
                compareList(expectedList, actualList);
                compareList(expected.list, (IList<object>)row.getFieldValue(10));
            }
            rows.close();

            long offsetOfStripe2 = 0;
            long offsetOfStripe4 = 0;
            long lastRowOfStripe2 = 0;
            for (int i = 0; i < 5; ++i)
            {
                StripeInformation stripe = reader.getStripes()[i];
                if (i < 2)
                {
                    lastRowOfStripe2 += stripe.getNumberOfRows();
                }
                else if (i == 2)
                {
                    offsetOfStripe2 = stripe.getOffset();
                    lastRowOfStripe2 += stripe.getNumberOfRows() - 1;
                }
                else if (i == 4)
                {
                    offsetOfStripe4 = stripe.getOffset();
                }
            }
            bool[] columns = new bool[reader.getStatistics().Length];
            columns[5] = true; // long colulmn
            columns[9] = true; // text column
            rows = reader.rowsOptions(new RecordReaderOptions()
                .range(offsetOfStripe2, offsetOfStripe4 - offsetOfStripe2)
                .include(columns));
            rows.seekToRow(lastRowOfStripe2);
            for (int i = 0; i < 2; ++i)
            {
                row = (OrcStruct)rows.next();
                BigRow expected = createRandomRow(intValues, doubleValues,
                                                  stringValues, byteValues, words,
                                                  (int)(lastRowOfStripe2 + i));

                Assert.Equal(expected.long1, row.getFieldValue(4));
                Assert.Equal(expected.string1, row.getFieldValue(8));
            }
            rows.close();
        }

        private void compareInner(InnerStruct expect, OrcStruct actual)
        {
            if (expect == null || actual == null)
            {
                Assert.Null(expect);
                Assert.Null(actual);
            }
            else
            {
                Assert.Equal(expect.int1, actual.getFieldValue(0));
                Assert.Equal(expect.string1, actual.getFieldValue(1));
            }
        }

        private void compareList(IList<InnerStruct> expect, IList<object> actual)
        {
            Assert.Equal(expect.Count, actual.Count);
            for (int j = 0; j < expect.Count; ++j)
            {
                compareInner(expect[j], (OrcStruct)actual[j]);
            }
        }

        private void appendRandomRow(VectorizedRowBatch batch,
                                     long[] intValues, double[] doubleValues,
                                     string[] stringValues,
                                     byte[][] byteValues,
                                     string[] words, int i)
        {
            InnerStruct inner = new InnerStruct((int)intValues[i], stringValues[i]);
            InnerStruct inner2 = new InnerStruct((int)(intValues[i] >> 32),
                words[i % words.Length] + "-x");
            setBigRow(batch, batch.size++, (intValues[i] & 1) == 0, (byte)intValues[i],
                (short)intValues[i], (int)intValues[i], intValues[i],
                (float)doubleValues[i], doubleValues[i], byteValues[i], stringValues[i],
                new MiddleStruct(inner, inner2), MakeList(), MakeMap(inner, inner2));
        }

        private BigRow createRandomRow(long[] intValues, double[] doubleValues,
                                       string[] stringValues,
                                       byte[][] byteValues,
                                       string[] words, int i)
        {
            InnerStruct inner = new InnerStruct((int)intValues[i], stringValues[i]);
            InnerStruct inner2 = new InnerStruct((int)(intValues[i] >> 32),
                words[i % words.Length] + "-x");
            return new BigRow((intValues[i] & 1) == 0, (byte)intValues[i],
                (short)intValues[i], (int)intValues[i], intValues[i],
                (float)doubleValues[i], doubleValues[i], byteValues[i], stringValues[i],
                new MiddleStruct(inner, inner2), MakeList(), MakeMap(inner, inner2));
        }

        private class MyMemoryManager : MemoryManager
        {
            long totalSpace;
            double rate;
            internal string path = null;
            long lastAllocation = 0;
            int rows = 0;
            Callback callback;

            public MyMemoryManager(Configuration conf, long totalSpace, double rate)
                : base(totalSpace)
            {
                this.totalSpace = totalSpace;
                this.rate = rate;
            }

            public override void addWriter(string path, long requestedAllocation, Callback callback)
            {
                this.path = path;
                this.lastAllocation = requestedAllocation;
                this.callback = callback;
            }

            public override void removeWriter(string path)
            {
                this.path = null;
                this.lastAllocation = 0;
            }

            public override long getTotalMemoryPool()
            {
                return totalSpace;
            }

            public override double getAllocationScale()
            {
                return rate;
            }

            public override void addedRow(int count)
            {
                rows += count;
                if (rows % 100 == 0)
                {
                    callback.checkMemory(rate);
                }
            }
        }

        [Fact]
        public void testMemoryManagementV11()
        {
            TypeDescription schema = createInnerSchema();
            MyMemoryManager memory = new MyMemoryManager(conf, 10000, 0.1);

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .compress(CompressionKind.NONE)
                .stripeSize(50000)
                .bufferSize(100)
                .rowIndexStride(0)
                .memory(memory)
                .version(OrcFile.Version.V_0_11)))
            {
                Assert.Equal(testFilePath, memory.path);
                VectorizedRowBatch batch = schema.createRowBatch();
                batch.size = 1;
                for (int i = 0; i < 2500; ++i)
                {
                    ((LongColumnVector)batch.cols[0]).vector[0] = i * 300;
                    ((BytesColumnVector)batch.cols[1]).setVal(0,
                        Integer.toHexString(10 * i).getBytes());
                    writer.addRowBatch(batch);
                }
                writer.close();
            }
            Assert.Null(memory.path);
            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));
            int idx = 0;
            foreach (StripeInformation stripe in reader.getStripes())
            {
                idx += 1;
                Assert.True(stripe.getDataLength() < 5000,
                    "stripe " + idx + " is too long at " + stripe.getDataLength());
            }
            Assert.Equal(25, idx);
            Assert.Equal(2500, reader.getNumberOfRows());
        }

        [Fact]
        public void testMemoryManagementV12()
        {
            TypeDescription schema = createInnerSchema();
            MyMemoryManager memory = new MyMemoryManager(conf, 10000, 0.1);

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .compress(CompressionKind.NONE)
                .stripeSize(50000)
                .bufferSize(100)
                .rowIndexStride(0)
                .memory(memory)
                .version(OrcFile.Version.V_0_12)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                Assert.Equal(testFilePath, memory.path);
                batch.size = 1;
                for (int i = 0; i < 2500; ++i)
                {
                    ((LongColumnVector)batch.cols[0]).vector[0] = i * 300;
                    ((BytesColumnVector)batch.cols[1]).setVal(0,
                        Integer.toHexString(10 * i).getBytes());
                    writer.addRowBatch(batch);
                }
                writer.close();
            }
            Assert.Null(memory.path);
            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));
            int idx = 0;
            foreach (StripeInformation stripe in reader.getStripes())
            {
                idx += 1;
                Assert.True(stripe.getDataLength() < 5000,
                    "stripe " + idx + " is too long at " + stripe.getDataLength());
            }
            // with HIVE-7832, the dictionaries will be disabled after writing the first
            // stripe as there are too many distinct values. Hence only 4 stripes as
            // compared to 25 stripes in version 0.11 (above test case)
            Assert.Equal(4, idx);
            Assert.Equal(2500, reader.getNumberOfRows());
        }

        [Fact]
        public void testPredicatePushdown()
        {
            TypeDescription schema = createInnerSchema();

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .stripeSize(400000L)
                .compress(CompressionKind.NONE)
                .bufferSize(500)
                .rowIndexStride(1000)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                batch.ensureSize(3500);
                batch.size = 3500;
                for (int i = 0; i < 3500; ++i)
                {
                    ((LongColumnVector)batch.cols[0]).vector[i] = i * 300;
                    ((BytesColumnVector)batch.cols[1]).setVal(i,
                        Integer.toHexString(10 * i).getBytes());
                }
                writer.addRowBatch(batch);
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            Assert.Equal(3500, reader.getNumberOfRows());

            SearchArgument sarg = SearchArgumentFactory.newBuilder()
                .startAnd()
                  .startNot()
                     .lessThan("int1", PredicateLeaf.Type.LONG, 300000L)
                  .end()
                  .lessThan("int1", PredicateLeaf.Type.LONG, 600000L)
                .end()
                .build();
            RecordReader rows = reader.rowsOptions(new RecordReaderOptions()
                .range(0L, Int64.MaxValue)
                .include(new bool[] { true, true, true })
                .searchArgument(sarg, new string[] { null, "int1", "string1" }));
            Assert.Equal(1000L, rows.getRowNumber());
            OrcStruct row = null;
            for (int i = 1000; i < 2000; ++i)
            {
                Assert.True(rows.hasNext());
                row = (OrcStruct)rows.next();
                Assert.Equal(300 * i, row.getFieldValue(0));
                Assert.Equal(Integer.toHexString(10 * i), row.getFieldValue(1).ToString());
            }
            Assert.True(!rows.hasNext());
            Assert.Equal(3500, rows.getRowNumber());

            // look through the file with no rows selected
            sarg = SearchArgumentFactory.newBuilder()
                .startAnd()
                  .lessThan("int1", PredicateLeaf.Type.LONG, 0L)
                .end()
                .build();
            rows = reader.rowsOptions(new RecordReaderOptions()
                .range(0L, Int64.MaxValue)
                .include(new bool[] { true, true, true })
                .searchArgument(sarg, new string[] { null, "int1", "string1" }));
            Assert.Equal(3500L, rows.getRowNumber());
            Assert.True(!rows.hasNext());

            // select first 100 and last 100 rows
            sarg = SearchArgumentFactory.newBuilder()
                .startOr()
                  .lessThan("int1", PredicateLeaf.Type.LONG, 300L * 100)
                  .startNot()
                    .lessThan("int1", PredicateLeaf.Type.LONG, 300L * 3400)
                  .end()
                .end()
                .build();
            rows = reader.rowsOptions(new RecordReaderOptions()
                .range(0L, Int64.MaxValue)
                .include(new bool[] { true, true, true })
                .searchArgument(sarg, new string[] { null, "int1", "string1" }));
            row = null;
            for (int i = 0; i < 1000; ++i)
            {
                Assert.True(rows.hasNext());
                Assert.Equal(i, rows.getRowNumber());
                row = (OrcStruct)rows.next();
                Assert.Equal(300 * i, row.getFieldValue(0));
                Assert.Equal(Integer.toHexString(10 * i), row.getFieldValue(1).ToString());
            }
            for (int i = 3000; i < 3500; ++i)
            {
                Assert.True(rows.hasNext());
                Assert.Equal(i, rows.getRowNumber());
                row = (OrcStruct)rows.next();
                Assert.Equal(300 * i, row.getFieldValue(0));
                Assert.Equal(Integer.toHexString(10 * i), row.getFieldValue(1).ToString());
            }
            Assert.True(!rows.hasNext());
            Assert.Equal(3500, rows.getRowNumber());
        }

        private static string pad(string value, int length)
        {
            if (value.Length == length)
            {
                return value;
            }
            else if (value.Length > length)
            {
                return value.Substring(0, length);
            }
            else
            {
                StringBuilder buf = new StringBuilder();
                buf.Append(value);
                for (int i = 0; i < length - value.Length; ++i)
                {
                    buf.Append(' ');
                }
                return buf.ToString();
            }
        }

        /**
         * Test all of the types that have distinct ORC writers using the vectorized
         * writer with different combinations of repeating and null values.
         * @
         */
        [Fact]
        public void testRepeating()
        {
            // create a row type with each type that has a unique writer
            // really just folds short, int, and long together
            TypeDescription schema = TypeDescription.createStruct()
                .addField("bin", TypeDescription.createBinary())
                .addField("bool", TypeDescription.createBoolean())
                .addField("byte", TypeDescription.createByte())
                .addField("long", TypeDescription.createLong())
                .addField("float", TypeDescription.createFloat())
                .addField("double", TypeDescription.createDouble())
                .addField("date", TypeDescription.createDate())
                .addField("time", TypeDescription.createTimestamp())
                .addField("dec", TypeDescription.createDecimal()
                    .withPrecision(20).withScale(6))
                .addField("string", TypeDescription.createString())
                .addField("char", TypeDescription.createChar().withMaxLength(10))
                .addField("vc", TypeDescription.createVarchar().withMaxLength(10))
                .addField("struct", TypeDescription.createStruct()
                    .addField("sub1", TypeDescription.createInt()))
                .addField("union", TypeDescription.createUnion()
                    .addUnionChild(TypeDescription.createString())
                    .addUnionChild(TypeDescription.createInt()))
                .addField("list", TypeDescription
                    .createList(TypeDescription.createInt()))
                .addField("map",
                    TypeDescription.createMap(TypeDescription.createString(),
                        TypeDescription.createString()));
            VectorizedRowBatch batch = schema.createRowBatch();

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .rowIndexStride(1000)))
            {
                // write 1024 repeating nulls
                batch.size = 1024;
                for (int c = 0; c < batch.cols.Length; ++c)
                {
                    batch.cols[c].setRepeating(true);
                    batch.cols[c].noNulls = false;
                    batch.cols[c].isNull[0] = true;
                }
                writer.addRowBatch(batch);

                // write 1024 repeating non-null
                for (int c = 0; c < batch.cols.Length; ++c)
                {
                    batch.cols[c].isNull[0] = false;
                }
                ((BytesColumnVector)batch.cols[0]).setVal(0, "Horton".getBytes());
                ((LongColumnVector)batch.cols[1]).vector[0] = 1;
                ((LongColumnVector)batch.cols[2]).vector[0] = 130;
                ((LongColumnVector)batch.cols[3]).vector[0] = 0x123456789abcdef0L;
                ((DoubleColumnVector)batch.cols[4]).vector[0] = 1.125;
                ((DoubleColumnVector)batch.cols[5]).vector[0] = 0.0009765625;
                ((LongColumnVector)batch.cols[6]).vector[0] = new Date(111, 6, 1).Days;
                ((LongColumnVector)batch.cols[7]).vector[0] =
                    new Timestamp(115, 9, 23, 10, 11, 59, 999999999).Nanoseconds;
                ((DecimalColumnVector)batch.cols[8]).vector[0] =
                    HiveDecimal.Parse("1.234567");
                ((BytesColumnVector)batch.cols[9]).setVal(0, "Echelon".getBytes());
                ((BytesColumnVector)batch.cols[10]).setVal(0, "Juggernaut".getBytes());
                ((BytesColumnVector)batch.cols[11]).setVal(0, "Dreadnaught".getBytes());
                ((LongColumnVector)((StructColumnVector)batch.cols[12]).fields[0])
                    .vector[0] = 123;
                ((UnionColumnVector)batch.cols[13]).tags[0] = 1;
                ((LongColumnVector)((UnionColumnVector)batch.cols[13]).fields[1])
                    .vector[0] = 1234;
                ((ListColumnVector)batch.cols[14]).offsets[0] = 0;
                ((ListColumnVector)batch.cols[14]).lengths[0] = 3;
                ((ListColumnVector)batch.cols[14]).child.isRepeating = true;
                ((LongColumnVector)((ListColumnVector)batch.cols[14]).child).vector[0]
                    = 31415;
                ((MapColumnVector)batch.cols[15]).offsets[0] = 0;
                ((MapColumnVector)batch.cols[15]).lengths[0] = 3;
                ((MapColumnVector)batch.cols[15]).values.isRepeating = true;
                ((BytesColumnVector)((MapColumnVector)batch.cols[15]).keys)
                    .setVal(0, "ORC".getBytes());
                ((BytesColumnVector)((MapColumnVector)batch.cols[15]).keys)
                    .setVal(1, "Hive".getBytes());
                ((BytesColumnVector)((MapColumnVector)batch.cols[15]).keys)
                    .setVal(2, "LLAP".getBytes());
                ((BytesColumnVector)((MapColumnVector)batch.cols[15]).values)
                    .setVal(0, "fast".getBytes());
                writer.addRowBatch(batch);

                // write 1024 null without repeat
                for (int c = 0; c < batch.cols.Length; ++c)
                {
                    batch.cols[c].setRepeating(false);
                    batch.cols[c].noNulls = false;
                    Arrays.fill(batch.cols[c].isNull, true);
                }
                writer.addRowBatch(batch);

                // add 1024 rows of non-null, non-repeating
                batch.reset();
                batch.size = 1024;
                ((ListColumnVector)batch.cols[14]).child.ensureSize(3 * 1024, false);
                ((MapColumnVector)batch.cols[15]).keys.ensureSize(3 * 1024, false);
                ((MapColumnVector)batch.cols[15]).values.ensureSize(3 * 1024, false);
                for (int r = 0; r < 1024; ++r)
                {
                    ((BytesColumnVector)batch.cols[0]).setVal(r,
                        Integer.toHexString(r).getBytes());
                    ((LongColumnVector)batch.cols[1]).vector[r] = r % 2;
                    ((LongColumnVector)batch.cols[2]).vector[r] = (r % 255);
                    ((LongColumnVector)batch.cols[3]).vector[r] = 31415L * r;
                    ((DoubleColumnVector)batch.cols[4]).vector[r] = 1.125 * r;
                    ((DoubleColumnVector)batch.cols[5]).vector[r] = 0.0009765625 * r;
                    ((LongColumnVector)batch.cols[6]).vector[r] = new Date(111, 6, 1).Days + r;
                    ((LongColumnVector)batch.cols[7]).vector[r] =
                        new Timestamp(115, 9, 23, 10, 11, 59, 999999999).Nanoseconds + r * 1000000000L;
                    ((DecimalColumnVector)batch.cols[8]).vector[r] =
                        HiveDecimal.Parse("1.234567");
                    ((BytesColumnVector)batch.cols[9]).setVal(r,
                        r.ToString().getBytes());
                    ((BytesColumnVector)batch.cols[10]).setVal(r,
                        Integer.toHexString(r).getBytes());
                    ((BytesColumnVector)batch.cols[11]).setVal(r,
                        Integer.toHexString(r * 128).getBytes());
                    ((LongColumnVector)((StructColumnVector)batch.cols[12]).fields[0])
                        .vector[r] = r + 13;
                    ((UnionColumnVector)batch.cols[13]).tags[r] = 1;
                    ((LongColumnVector)((UnionColumnVector)batch.cols[13]).fields[1])
                        .vector[r] = r + 42;
                    ((ListColumnVector)batch.cols[14]).offsets[r] = 3 * r;
                    ((ListColumnVector)batch.cols[14]).lengths[r] = 3;
                    for (int i = 0; i < 3; ++i)
                    {
                        ((LongColumnVector)((ListColumnVector)batch.cols[14]).child)
                            .vector[3 * r + i] = 31415 + i;
                    }
                    ((MapColumnVector)batch.cols[15]).offsets[r] = 3 * r;
                    ((MapColumnVector)batch.cols[15]).lengths[r] = 3;
                    for (int i = 0; i < 3; ++i)
                    {
                        ((BytesColumnVector)((MapColumnVector)batch.cols[15]).keys)
                            .setVal(3 * r + i, Integer.toHexString(3 * r + i).getBytes());
                        ((BytesColumnVector)((MapColumnVector)batch.cols[15]).values)
                            .setVal(3 * r + i, (3 * r + i).ToString().getBytes());
                    }
                }
                writer.addRowBatch(batch);

                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

            // check the stats
            ColumnStatistics[] stats = reader.getStatistics();
            Assert.Equal(4096, stats[0].getNumberOfValues());
            Assert.Equal(false, stats[0].hasNull());
            foreach (TypeDescription colType in schema.getChildren())
            {
                Assert.Equal(2048, stats[colType.getId()].getNumberOfValues());
                Assert.Equal(true, stats[colType.getId()].hasNull());
            }
            Assert.Equal(8944, ((BinaryColumnStatistics)stats[1]).getSum());
            Assert.Equal(1536, ((BooleanColumnStatistics)stats[2]).getTrueCount());
            Assert.Equal(512, ((BooleanColumnStatistics)stats[2]).getFalseCount());
            Assert.Equal(false, ((IntegerColumnStatistics)stats[4]).isSumDefined());
            Assert.Equal(0, ((IntegerColumnStatistics)stats[4]).getMinimum());
            Assert.Equal(0x123456789abcdef0L,
                ((IntegerColumnStatistics)stats[4]).getMaximum());
            Assert.Equal("0", ((StringColumnStatistics)stats[10]).getMinimum());
            Assert.Equal("Echelon", ((StringColumnStatistics)stats[10]).getMaximum());
            Assert.Equal(10154, ((StringColumnStatistics)stats[10]).getSum());
            Assert.Equal("0         ",
                ((StringColumnStatistics)stats[11]).getMinimum());
            Assert.Equal("ff        ",
                ((StringColumnStatistics)stats[11]).getMaximum());
            Assert.Equal(20480, ((StringColumnStatistics)stats[11]).getSum());
            Assert.Equal("0",
                ((StringColumnStatistics)stats[12]).getMinimum());
            Assert.Equal("ff80",
                ((StringColumnStatistics)stats[12]).getMaximum());
            Assert.Equal(14813, ((StringColumnStatistics)stats[12]).getSum());

            RecordReader rows = reader.rows();
            OrcStruct row = null;

            // read the 1024 nulls
            for (int r = 0; r < 1024; ++r)
            {
                Assert.Equal(true, rows.hasNext());
                row = (OrcStruct)rows.next();
                for (int f = 0; f < row.getNumFields(); ++f)
                {
                    Assert.Null(row.getFieldValue(f));
                }
            }

            // read the 1024 repeat values
            for (int r = 0; r < 1024; ++r)
            {
                Assert.Equal(true, rows.hasNext());
                row = (OrcStruct)rows.next();
                Assert.Equal(bytes(0x48, 0x6f, 0x72, 0x74, 0x6f, 0x6e),
                    (byte[])row.getFieldValue(0));
                Assert.Equal("True", row.getFieldValue(1).ToString());
                Assert.Equal("-126", row.getFieldValue(2).ToString());
                Assert.Equal("1311768467463790320",
                    row.getFieldValue(3).ToString());
                Assert.Equal("1.125", row.getFieldValue(4).ToString());
                Assert.Equal("9.765625E-4", row.getFieldValue(5).ToString());
                Assert.Equal("2011-07-01", row.getFieldValue(6).ToString());
                Assert.Equal("2015-10-23 10:11:59.999999999",
                    row.getFieldValue(7).ToString());
                Assert.Equal("1.234567", row.getFieldValue(8).ToString());
                Assert.Equal("Echelon", row.getFieldValue(9).ToString());
                Assert.Equal("Juggernaut", row.getFieldValue(10).ToString());
                Assert.Equal("Dreadnaugh", row.getFieldValue(11).ToString());
                Assert.Equal("{123}", row.getFieldValue(12).ToString());
                Assert.Equal("union(1, 1234)",
                    row.getFieldValue(13).ToString());
                Assert.Equal("[31415, 31415, 31415]",
                    row.getFieldValue(14).ToString());
                Assert.Equal("{ORC=fast, Hive=fast, LLAP=fast}",
                    row.getFieldValue(15).ToString());
            }

            // read the second set of 1024 nulls
            for (int r = 0; r < 1024; ++r)
            {
                Assert.Equal(true, rows.hasNext());
                row = (OrcStruct)rows.next();
                for (int f = 0; f < row.getNumFields(); ++f)
                {
                    Assert.Null(row.getFieldValue(f));
                }
            }
            for (int r = 0; r < 1024; ++r)
            {
                Assert.Equal(true, rows.hasNext());
                row = (OrcStruct)rows.next();
                byte[] hex = Integer.toHexString(r).getBytes();
                StringBuilder expected = new StringBuilder();
                for (int i = 0; i < hex.Length; ++i)
                {
                    if (i != 0)
                    {
                        expected.Append(' ');
                    }
                    expected.Append(Integer.toHexString(hex[i]));
                }
                Assert.Equal(expected.ToString(),
                    row.getFieldValue(0).ToString());
                Assert.Equal(r % 2 == 1 ? "true" : "false",
                    row.getFieldValue(1).ToString());
                Assert.Equal(((byte)(r % 255)).ToString(),
                    row.getFieldValue(2).ToString());
                Assert.Equal((31415L * r).ToString(),
                    row.getFieldValue(3).ToString());
                Assert.Equal((1.125F * r).ToString(),
                    row.getFieldValue(4).ToString());
                Assert.Equal((0.0009765625 * r).ToString(),
                    row.getFieldValue(5).ToString());
                Assert.Equal(new Date(111, 6, 1 + r).ToString(),
                    row.getFieldValue(6).ToString());
                Assert.Equal(
                    new Timestamp(115, 9, 23, 10, 11, 59 + r, 999999999).ToString(),
                    row.getFieldValue(7).ToString());
                Assert.Equal("1.234567", row.getFieldValue(8).ToString());
                Assert.Equal(r.ToString(),
                    row.getFieldValue(9).ToString());
                Assert.Equal(pad(Integer.toHexString(r), 10),
                    row.getFieldValue(10).ToString());
                Assert.Equal(Integer.toHexString(r * 128),
                    row.getFieldValue(11).ToString());
                Assert.Equal("{" + (r + 13).ToString() + "}",
                    row.getFieldValue(12).ToString());
                Assert.Equal("union(1, " + (r + 42).ToString() + ")",
                    row.getFieldValue(13).ToString());
                Assert.Equal("[31415, 31416, 31417]",
                    row.getFieldValue(14).ToString());
                expected = new StringBuilder();
                expected.Append('{');
                expected.Append(Integer.toHexString(3 * r));
                expected.Append('=');
                expected.Append(3 * r);
                expected.Append(", ");
                expected.Append(Integer.toHexString(3 * r + 1));
                expected.Append('=');
                expected.Append(3 * r + 1);
                expected.Append(", ");
                expected.Append(Integer.toHexString(3 * r + 2));
                expected.Append('=');
                expected.Append(3 * r + 2);
                expected.Append('}');
                Assert.Equal(expected.ToString(),
                    row.getFieldValue(15).ToString());
            }

            // should have no more rows
            Assert.Equal(false, rows.hasNext());
        }

        private static string makeString(BytesColumnVector vector, int row)
        {
            if (vector.isRepeating)
            {
                row = 0;
            }
            if (vector.noNulls || !vector.isNull[row])
            {
                return Encoding.UTF8.GetString(vector.vector[row], vector.start[row],
                    vector.length[row]);
            }
            else
            {
                return null;
            }
        }

        /**
         * Test the char and varchar padding and truncation.
         * @
         */
        [Fact]
        public void testStringPadding()
        {
            TypeDescription schema = TypeDescription.createStruct()
                .addField("char", TypeDescription.createChar().withMaxLength(10))
                .addField("varchar", TypeDescription.createVarchar().withMaxLength(10));
            VectorizedRowBatch batch = schema.createRowBatch();

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .setSchema(schema)))
            {
                batch.size = 4;
                for (int c = 0; c < batch.cols.Length; ++c)
                {
                    ((BytesColumnVector)batch.cols[c]).setVal(0, "".getBytes());
                    ((BytesColumnVector)batch.cols[c]).setVal(1, "xyz".getBytes());
                    ((BytesColumnVector)batch.cols[c]).setVal(2, "0123456789".getBytes());
                    ((BytesColumnVector)batch.cols[c]).setVal(3,
                        "0123456789abcdef".getBytes());
                }
                writer.addRowBatch(batch);
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            batch = rows.nextBatch(null);
            Assert.Equal(4, batch.size);
            // ORC currently trims the output strings. See HIVE-12286
            Assert.Equal("",
                makeString((BytesColumnVector)batch.cols[0], 0));
            Assert.Equal("xyz",
                makeString((BytesColumnVector)batch.cols[0], 1));
            Assert.Equal("0123456789",
                makeString((BytesColumnVector)batch.cols[0], 2));
            Assert.Equal("0123456789",
                makeString((BytesColumnVector)batch.cols[0], 3));
            Assert.Equal("",
                makeString((BytesColumnVector)batch.cols[1], 0));
            Assert.Equal("xyz",
                makeString((BytesColumnVector)batch.cols[1], 1));
            Assert.Equal("0123456789",
                makeString((BytesColumnVector)batch.cols[1], 2));
            Assert.Equal("0123456789",
                makeString((BytesColumnVector)batch.cols[1], 3));
        }

        /**
         * A test case that tests the case where you add a repeating batch
         * to a column that isn't using dictionary encoding.
         * @
         */
        [Fact]
        public void testNonDictionaryRepeatingString()
        {
            TypeDescription schema = TypeDescription.createStruct()
                .addField("str", TypeDescription.createString());
            VectorizedRowBatch batch = schema.createRowBatch();

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)
                .rowIndexStride(1000)))
            {
                batch.size = 1024;
                for (int r = 0; r < batch.size; ++r)
                {
                    ((BytesColumnVector)batch.cols[0]).setVal(r,
                        (r * 10001).ToString().getBytes());
                }
                writer.addRowBatch(batch);
                batch.cols[0].isRepeating = true;
                ((BytesColumnVector)batch.cols[0]).setVal(0, "Halloween".getBytes());
                writer.addRowBatch(batch);
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            batch = rows.nextBatch(null);
            Assert.Equal(1024, batch.size);
            for (int r = 0; r < 1024; ++r)
            {
                Assert.Equal((r * 10001).ToString(),
                    makeString((BytesColumnVector)batch.cols[0], r));
            }
            batch = rows.nextBatch(batch);
            Assert.Equal(1024, batch.size);
            for (int r = 0; r < 1024; ++r)
            {
                Assert.Equal("Halloween",
                    makeString((BytesColumnVector)batch.cols[0], r));
            }
            Assert.Equal(false, rows.hasNext());
        }

        [Fact]
        public void testStructs()
        {
            TypeDescription schema = TypeDescription.createStruct()
                .addField("struct", TypeDescription.createStruct()
                    .addField("inner", TypeDescription.createLong()));

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                batch.size = 1024;
                StructColumnVector outer = (StructColumnVector)batch.cols[0];
                outer.noNulls = false;
                for (int r = 0; r < 1024; ++r)
                {
                    if (r < 200 || (r >= 400 && r < 600) || r >= 800)
                    {
                        outer.isNull[r] = true;
                    }
                    ((LongColumnVector)outer.fields[0]).vector[r] = r;
                }
                writer.addRowBatch(batch);
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            OrcStruct row = null;
            for (int r = 0; r < 1024; ++r)
            {
                Assert.Equal(true, rows.hasNext());
                row = (OrcStruct)rows.next();
                OrcStruct inner = (OrcStruct)row.getFieldValue(0);
                if (r < 200 || (r >= 400 && r < 600) || r >= 800)
                {
                    Assert.Null(inner);
                }
                else
                {
                    Assert.Equal("{" + r + "}", inner.ToString());
                }
            }
            Assert.Equal(false, rows.hasNext());
        }

        /**
         * Test Unions.
         * @
         */
        [Fact]
        public void testUnions()
        {
            TypeDescription schema = TypeDescription.createStruct()
                .addField("outer", TypeDescription.createUnion()
                    .addUnionChild(TypeDescription.createInt())
                    .addUnionChild(TypeDescription.createLong()));

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                batch.size = 1024;
                UnionColumnVector outer = (UnionColumnVector)batch.cols[0];
                batch.cols[0].noNulls = false;
                for (int r = 0; r < 1024; ++r)
                {
                    if (r < 200)
                    {
                        outer.isNull[r] = true;
                    }
                    else if (r < 300)
                    {
                        outer.tags[r] = 0;
                    }
                    else if (r < 400)
                    {
                        outer.tags[r] = 1;
                    }
                    else if (r < 600)
                    {
                        outer.isNull[r] = true;
                    }
                    else if (r < 800)
                    {
                        outer.tags[r] = 1;
                    }
                    else if (r < 1000)
                    {
                        outer.isNull[r] = true;
                    }
                    else
                    {
                        outer.tags[r] = 1;
                    }
                    ((LongColumnVector)outer.fields[0]).vector[r] = r;
                    ((LongColumnVector)outer.fields[1]).vector[r] = -r;
                }
                writer.addRowBatch(batch);
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            OrcStruct row = null;
            for (int r = 0; r < 1024; ++r)
            {
                Assert.Equal(true, rows.hasNext());
                row = (OrcStruct)rows.next();
                OrcUnion inner = (OrcUnion)row.getFieldValue(0);
                if (r < 200)
                {
                    Assert.Null(inner);
                }
                else if (r < 300)
                {
                    Assert.Equal("union(0, " + r + ")", inner.ToString());
                }
                else if (r < 400)
                {
                    Assert.Equal("union(1, " + -r + ")", inner.ToString());
                }
                else if (r < 600)
                {
                    Assert.Null(inner);
                }
                else if (r < 800)
                {
                    Assert.Equal("union(1, " + -r + ")", inner.ToString());
                }
                else if (r < 1000)
                {
                    Assert.Null(inner);
                }
                else
                {
                    Assert.Equal("union(1, " + -r + ")", inner.ToString());
                }
            }
            Assert.Equal(false, rows.hasNext());
        }

        /**
         * Test lists and how they interact with the child column. In particular,
         * put nulls between back to back lists and then make some lists that
         * oper lap.
         * @
         */
        [Fact]
        public void testLists()
        {
            TypeDescription schema = TypeDescription.createStruct()
                .addField("list",
                    TypeDescription.createList(TypeDescription.createLong()));

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                batch.size = 1024;
                ListColumnVector list = (ListColumnVector)batch.cols[0];
                list.noNulls = false;
                for (int r = 0; r < 1024; ++r)
                {
                    if (r < 200)
                    {
                        list.isNull[r] = true;
                    }
                    else if (r < 300)
                    {
                        list.offsets[r] = r - 200;
                        list.lengths[r] = 1;
                    }
                    else if (r < 400)
                    {
                        list.isNull[r] = true;
                    }
                    else if (r < 500)
                    {
                        list.offsets[r] = r - 300;
                        list.lengths[r] = 1;
                    }
                    else if (r < 600)
                    {
                        list.isNull[r] = true;
                    }
                    else if (r < 700)
                    {
                        list.offsets[r] = r;
                        list.lengths[r] = 2;
                    }
                    else
                    {
                        list.isNull[r] = true;
                    }
                    ((LongColumnVector)list.child).vector[r] = r * 10;
                }
                writer.addRowBatch(batch);
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            OrcStruct row = null;
            for (int r = 0; r < 1024; ++r)
            {
                Assert.Equal(true, rows.hasNext());
                row = (OrcStruct)rows.next();
                IList<object> inner = (IList<object>)row.getFieldValue(0);
                if (r < 200)
                {
                    Assert.Null(inner);
                }
                else if (r < 300)
                {
                    Assert.Equal(1, inner.Count);
                    Assert.Equal((r - 200) * 10L, inner[0]);
                }
                else if (r < 400)
                {
                    Assert.Null(inner);
                }
                else if (r < 500)
                {
                    Assert.Equal(1, inner.Count);
                    Assert.Equal((r - 300) * 10L, inner[0]);
                }
                else if (r < 600)
                {
                    Assert.Null(inner);
                }
                else if (r < 700)
                {
                    Assert.Equal(2, inner.Count);
                    Assert.Equal(10L * r, inner[0]);
                    Assert.Equal(10L * (r + 1), inner[1]);
                }
                else
                {
                    Assert.Null(inner);
                }
            }
            Assert.Equal(false, rows.hasNext());
        }

        /**
         * Test maps and how they interact with the child column. In particular,
         * put nulls between back to back lists and then make some lists that
         * oper lap.
         * @
         */
        [Fact]
        public void testMaps()
        {
            TypeDescription schema = TypeDescription.createStruct()
                .addField("map",
                    TypeDescription.createMap(TypeDescription.createLong(),
                        TypeDescription.createLong()));

            using (Stream file = File.OpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .setSchema(schema)))
            {
                VectorizedRowBatch batch = schema.createRowBatch();
                batch.size = 1024;
                MapColumnVector map = (MapColumnVector)batch.cols[0];
                map.noNulls = false;
                for (int r = 0; r < 1024; ++r)
                {
                    if (r < 200)
                    {
                        map.isNull[r] = true;
                    }
                    else if (r < 300)
                    {
                        map.offsets[r] = r - 200;
                        map.lengths[r] = 1;
                    }
                    else if (r < 400)
                    {
                        map.isNull[r] = true;
                    }
                    else if (r < 500)
                    {
                        map.offsets[r] = r - 300;
                        map.lengths[r] = 1;
                    }
                    else if (r < 600)
                    {
                        map.isNull[r] = true;
                    }
                    else if (r < 700)
                    {
                        map.offsets[r] = r;
                        map.lengths[r] = 2;
                    }
                    else
                    {
                        map.isNull[r] = true;
                    }
                    ((LongColumnVector)map.keys).vector[r] = r;
                    ((LongColumnVector)map.values).vector[r] = r * 10;
                }
                writer.addRowBatch(batch);
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            OrcStruct row = null;
            for (int r = 0; r < 1024; ++r)
            {
                Assert.Equal(true, rows.hasNext());
                row = (OrcStruct)rows.next();
                Dictionary<object, object> inner = (Dictionary<object, object>)row.getFieldValue(0);
                if (r < 200)
                {
                    Assert.Null(inner);
                }
                else if (r < 300)
                {
                    Assert.Equal(1, inner.Count);
                    Assert.Equal(r - 200L, inner.First().Key);
                    Assert.Equal((r - 200) * 10L, inner.First().Value);
                }
                else if (r < 400)
                {
                    Assert.Null(inner);
                }
                else if (r < 500)
                {
                    Assert.Equal(1, inner.Count);
                    Assert.Equal(r - 300L, inner.First().Key);
                    Assert.Equal((r - 300) * 10L, inner.First().Value);
                }
                else if (r < 600)
                {
                    Assert.Null(inner);
                }
                else if (r < 700)
                {
                    Assert.Equal(2, inner.Count);
                    Assert.True(inner.ContainsKey((long)r));
                    Assert.Equal(r * 10L, inner[(long)r]);
                    Assert.True(inner.ContainsKey(r + 1L));
                    Assert.Equal(10L * (r + 1), inner[r + 1L]);
                }
                else
                {
                    Assert.Null(inner);
                }
            }
            Assert.Equal(false, rows.hasNext());
        }
    }
}
