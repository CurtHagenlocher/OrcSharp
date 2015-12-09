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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using OrcSharp;
    using OrcSharp.External;
    using OrcSharp.Query;
    using OrcSharp.Serialization;
    using OrcSharp.Types;
    using Xunit;
    using OrcProto = global::orc.proto;

    /**
     * Tests for the top level reader/streamFactory of ORC files.
     */
    public class TestOrcFile : OrcTestBase
    {
        private static Dictionary<string, InnerStruct> makeMap(params InnerStruct[] items)
        {
            Dictionary<string, InnerStruct> result = new Dictionary<string, InnerStruct>();
            foreach (InnerStruct i in items)
            {
                result.Add(i.string1, i);
            }
            return result;
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

        private bool zeroCopy; // TODO: test with both true and false

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
            IList<StructField> fields = readerInspector.getAllStructFieldRefs();
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
            using (RecordReader rows = reader.rows())
            {
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
                    bi.get(readerInspector.getStructFieldData(row,
                        fields[7])));
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

                Assert.Equal(false, rows.hasNext());
            }
        }


        [Fact]
        public void testTimestamp()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(Timestamp));

            List<Timestamp> tslist = new List<Timestamp>();
            using (Stream file = FileOpenWrite(TestFilePath))
            using (Writer writer = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                .inspector(inspector)
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

                foreach (Timestamp ts in tslist)
                {
                    writer.addRow(ts);
                }

                Assert.Equal(0, writer.getSchema().getMaximumId());
                bool[] expected = new bool[] { false };
                bool[] included = OrcUtils.includeColumns("", writer.getSchema());
                Assert.Equal(expected, included);
            }

            Reader reader = OrcFile.createReader(TestFilePath,
                OrcFile.readerOptions(conf));
            using (RecordReader rows = reader.rows())
            {
                int idx = 0;
                while (rows.hasNext())
                {
                    object row = rows.next();
                    Assert.Equal(tslist[idx++].getNanos(), ((Timestamp)row).getNanos());
                }
            }
        }

        [Fact]
        public void testStringAndBinaryStatistics()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(SimpleStruct));

            using (Stream file = FileOpenWrite(TestFilePath))
            using (Writer writer = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)))
            {
                writer.addRow(new SimpleStruct(bytes(0, 1, 2, 3, 4), "foo"));
                writer.addRow(new SimpleStruct(bytes(0, 1, 2, 3), "bar"));
                writer.addRow(new SimpleStruct(bytes(0, 1, 2, 3, 4, 5), null));
                writer.addRow(new SimpleStruct(null, "hi"));
                writer.close();

                TypeDescription schema = writer.getSchema();
                Assert.Equal(2, schema.getMaximumId());
                bool[] expected = new bool[] { false, false, true };
                bool[] included = OrcUtils.includeColumns("string1", schema);
                Assert.Equal(expected, included);

                expected = new bool[] { false, false, false };
                included = OrcUtils.includeColumns("", schema);
                Assert.Equal(expected, included);

                expected = new bool[] { false, false, false };
                included = OrcUtils.includeColumns(null, schema);
                Assert.Equal(expected, included);
            }

            Reader reader = OrcFile.createReader(TestFilePath, OrcFile.readerOptions(conf));

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
            using (RecordReader rows = reader.rows())
            {
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

                Assert.Equal(false, rows.hasNext());
            }
        }

        [Fact]
        public void testStripeLevelStats()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(InnerStruct));

            using (Stream file = FileOpenWrite(TestFilePath))
            using (Writer writer = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)))
            {
                for (int i = 0; i < 11000; i++)
                {
                    if (i >= 5000)
                    {
                        if (i >= 10000)
                        {
                            writer.addRow(new InnerStruct(3, "three"));
                        }
                        else
                        {
                            writer.addRow(new InnerStruct(2, "two"));
                        }
                    }
                    else
                    {
                        writer.addRow(new InnerStruct(1, "one"));
                    }
                }
                writer.close();

                TypeDescription schema = writer.getSchema();
                Assert.Equal(2, schema.getMaximumId());
                bool[] expected = new bool[] { false, true, false };
                bool[] included = OrcUtils.includeColumns("int1", schema);
                Assert.Equal(expected, included);
            }

            Reader reader = OrcFile.createReader(TestFilePath, OrcFile.readerOptions(conf));
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

            using (RecordReaderImpl recordReader = (RecordReaderImpl)reader.rows())
            {
                OrcProto.RowIndex[] index = recordReader.readRowIndex(0, null, null).getRowGroupIndex();
                Assert.Equal(3, index.Length);
                IList<OrcProto.RowIndexEntry> items = index[1].EntryList;
                Assert.Equal(1, items.Count);
                Assert.Equal(3, items[0].PositionsCount);
                Assert.Equal(0, (long)items[0].GetPositions(0));
                Assert.Equal(0, (long)items[0].GetPositions(1));
                Assert.Equal(0, (long)items[0].GetPositions(2));
                Assert.Equal(1, items[0].Statistics.IntStatistics.Minimum);
                index = recordReader.readRowIndex(1, null, null).getRowGroupIndex();
                Assert.Equal(3, index.Length);
                items = index[1].EntryList;
                Assert.Equal(2, items[0].Statistics.IntStatistics.Maximum);
            }
        }

        [Fact]
        public void test1()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(BigRow));

            using (Stream file = FileOpenWrite(TestFilePath))
            using (Writer writer = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(100000)
                    .bufferSize(10000)))
            {
                writer.addRow(new BigRow(false, (sbyte)1, (short)1024, 65536,
                    Int64.MaxValue, (float)1.0, -15.0, bytes(0, 1, 2, 3, 4), "hi",
                    new MiddleStruct(new InnerStruct(1, "bye"), new InnerStruct(2, "sigh")),
                    new List<InnerStruct> { new InnerStruct(3, "good"), new InnerStruct(4, "bad") },
                    makeMap()));
                writer.addRow(new BigRow(true, (sbyte)100, (short)2048, 65536,
                    Int64.MaxValue, (float)2.0, -5.0, bytes(), "bye",
                    new MiddleStruct(new InnerStruct(1, "bye"), new InnerStruct(2, "sigh")),
                    new List<InnerStruct> { new InnerStruct(100000000, "cat"), new InnerStruct(-100000, "in"), new InnerStruct(1234, "hat") },
                    makeMap(new InnerStruct(5, "chani"), new InnerStruct(1, "mauddib"))));
                writer.close();

                TypeDescription schema = writer.getSchema();
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
                Assert.Equal(expected, included);

                expected = new bool[] {
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
            }

            Reader reader = OrcFile.createReader(TestFilePath,
                OrcFile.readerOptions(conf));

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
            IList<StructField> fields = readerInspector.getAllStructFieldRefs();
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
            IList<StructField> midFields = mid.getAllStructFieldRefs();
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
            using (RecordReader rows = reader.rows())
            {
                object row = rows.next();
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
                IList<object> list = li.getList(readerInspector.getStructFieldData(row, fields[10]));
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

                Assert.Equal(false, rows.hasNext());
            }
        }

        [Fact]
        public void columnProjection()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(InnerStruct));

            int minInt = 0, maxInt = 0;
            string minStr = null, maxStr = null;
            Random r1 = null, r2 = null;
            using (Stream file = FileOpenWrite(TestFilePath))
            using (Writer writer = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(1000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(100)
                    .rowIndexStride(1000)))
            {
                r1 = new Random(1);
                r2 = new Random(2);
                int x;
                string y;
                for (int i = 0; i < 21000; ++i)
                {
                    x = r1.Next();
                    y = Long.toHexString(r2.NextLong());
                    if (i == 0 || x < minInt)
                    {
                        minInt = x;
                    }
                    if (i == 0 || x > maxInt)
                    {
                        maxInt = x;
                    }
                    if (i == 0 || y.CompareTo(minStr) < 0)
                    {
                        minStr = y;
                    }
                    if (i == 0 || y.CompareTo(maxStr) > 0)
                    {
                        maxStr = y;
                    }
                    writer.addRow(new InnerStruct(x, y));
                }
            }

            Reader reader = OrcFile.createReader(TestFilePath, OrcFile.readerOptions(conf));

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
            Assert.Equal(1, (int)types[0].GetSubtypes(0));
            Assert.Equal(2, (int)types[0].GetSubtypes(1));
            Assert.Equal(OrcProto.Type.Types.Kind.INT, types[1].Kind);
            Assert.Equal(0, types[1].SubtypesCount);
            Assert.Equal(OrcProto.Type.Types.Kind.STRING, types[2].Kind);
            Assert.Equal(0, types[2].SubtypesCount);

            // read the contents and make sure they match
            using (RecordReader rows1 = reader.rows(new bool[] { true, true, false }))
            using (RecordReader rows2 = reader.rows(new bool[] { true, false, true }))
            {
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
            }
        }

        [Fact]
        public void emptyFile()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(BigRow));

            using (Stream file = FileOpenWrite(TestFilePath))
            using (Writer writer = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(1000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(100)))
            {
            }

            Reader reader = OrcFile.createReader(TestFilePath,
                OrcFile.readerOptions(conf));
            using (RecordReader row = reader.rows())
            {
                Assert.Equal(false, row.hasNext());
            }
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
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(BigRow));

            ByteBuffer bigBuf = ByteBuffer.allocate(40000);
            using (Stream file = FileOpenWrite(TestFilePath))
            using (Writer writer = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(1000)
                    .compress(CompressionKind.NONE)
                    .bufferSize(100)))
            {
                writer.addUserMetadata("my.meta", byteBuf(1, 2, 3, 4, 5, 6, 7, -1, -2, 127, -128));
                writer.addUserMetadata("clobber", byteBuf(1, 2, 3));
                writer.addUserMetadata("clobber", byteBuf(4, 3, 2, 1));
                Random random = new Random(0);
                random.NextBytes(bigBuf.array());
                writer.addUserMetadata("big", bigBuf);
                bigBuf.position(0);
                writer.addRow(new BigRow(true, (sbyte)127, (short)1024, 42,
                    42L * 1024 * 1024 * 1024, (float)3.1415, -2.713, null,
                    null, null, null, null));
                writer.addUserMetadata("clobber", byteBuf(5, 7, 11, 13, 17, 19));
            }

            Reader reader = OrcFile.createReader(TestFilePath, OrcFile.readerOptions(conf));
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
            List<OrcProto.Type> types = new List<OrcProto.Type>();
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.STRUCT).
                AddFieldNames("time").AddFieldNames("date").
                AddSubtypes(1).AddSubtypes(2).Build());
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.TIMESTAMP).
                Build());
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.DATE).
                Build());

            ObjectInspector inspector = OrcStruct.createObjectInspector(0, types);

            using (Stream file = FileOpenWrite(path))
            using (Writer writer = OrcFile.createWriter(path, file, OrcFile.writerOptions(conf)
                .inspector(inspector)
                .stripeSize(100000)
                .bufferSize(10000)
                .blockPadding(false)))
            {
                OrcStruct row = new OrcStruct(2);
                for (int year = minYear; year < maxYear; ++year)
                {
                    for (int ms = 1000; ms < 2000; ++ms)
                    {
                        row.setFieldValue(0, Timestamp.Parse(year + "-05-05 12:34:56." + ms));
                        row.setFieldValue(1, new Date(year - 1900, 11, 25));
                        writer.addRow(row);
                    }
                }
            }

            Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
            using (RecordReader rows = reader.rows())
            {
                for (int year = minYear; year < maxYear; ++year)
                {
                    for (int ms = 1000; ms < 2000; ++ms)
                    {
                        OrcStruct row = (OrcStruct)rows.next();
                        Assert.Equal(
                            Timestamp.Parse(year + "-05-05 12:34:56." + ms),
                            row.getFieldValue(0));
                        Assert.Equal(new Date(year - 1900, 11, 25), row.getFieldValue(1));
                    }
                }
            }
        }

        [Fact]
        public void testDate1900()
        {
            createOrcDateFile(TestFilePath, 1900, 1970);
        }

        [Fact]
        public void testDate2038()
        {
            createOrcDateFile(TestFilePath, 2038, 2250);
        }

        /**
           * We test union, timestamp, and decimal separately since we need to make the
           * object inspector manually. (The Hive reflection-based doesn't handle
           * them properly.)
           */
        [Fact]
        public void testUnionAndTimestamp()
        {
            List<OrcProto.Type> types = new List<OrcProto.Type>();
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.STRUCT).
                AddFieldNames("time").AddFieldNames("union").AddFieldNames("decimal").
                AddSubtypes(1).AddSubtypes(2).AddSubtypes(5).Build());
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.TIMESTAMP).
                Build());
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.UNION).
                AddSubtypes(3).AddSubtypes(4).Build());
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.INT).
                Build());
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.STRING).
                Build());
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.DECIMAL).
                Build());

            ObjectInspector inspector = OrcStruct.createObjectInspector(0, types);

            HiveDecimal maxValue = HiveDecimal.Parse("10000000000000000000");
            OrcStruct row = new OrcStruct(3);
            OrcUnion union = new OrcUnion();
            Random rand;

            using (Stream file = FileOpenWrite(TestFilePath))
            using (Writer writer = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                .inspector(inspector)
                .stripeSize(1000)
                .compress(CompressionKind.NONE)
                .bufferSize(100)
                .blockPadding(false)))
            {
                row.setFieldValue(1, union);
                row.setFieldValue(0, Timestamp.Parse("2000-03-12 15:00:00"));
                HiveDecimal value = HiveDecimal.Parse("12345678.6547456");
                row.setFieldValue(2, value);
                union.set((byte)0, 42);
                writer.addRow(row);
                row.setFieldValue(0, Timestamp.Parse("2000-03-20 12:00:00.123456789"));
                union.set((byte)1, "hello");
                value = HiveDecimal.Parse("-5643.234");
                row.setFieldValue(2, value);
                writer.addRow(row);
                row.setFieldValue(0, null);
                row.setFieldValue(1, null);
                row.setFieldValue(2, null);
                writer.addRow(row);
                row.setFieldValue(1, union);
                union.set((byte)0, null);
                writer.addRow(row);
                union.set((byte)1, null);
                writer.addRow(row);
                union.set((byte)0, 200000);
                row.setFieldValue(0, Timestamp.Parse("1970-01-01 00:00:00"));
                value = HiveDecimal.Parse("10000000000000000000");
                row.setFieldValue(2, value);
                writer.addRow(row);
                rand = new Random(42);
                for (int i = 1970; i < 2038; ++i)
                {
                    row.setFieldValue(0, Timestamp.Parse(i + "-05-05 12:34:56." + i));
                    if ((i & 1) == 0)
                    {
                        union.set((byte)0, (i * i));
                    }
                    else
                    {
                        union.set((byte)1, (i * i).ToString());
                    }
                    value = HiveDecimal.create(rand.NextBigInteger(64), rand.Next(18));
                    row.setFieldValue(2, value);
                    if (maxValue.CompareTo(value) < 0)
                    {
                        maxValue = value;
                    }
                    writer.addRow(row);
                }
                // let's add a lot of constant rows to test the rle
                row.setFieldValue(0, null);
                union.set((byte)0, 1732050807);
                row.setFieldValue(2, null);
                for (int i = 0; i < 5000; ++i)
                {
                    writer.addRow(row);
                }
                union.set((byte)0, 0);
                writer.addRow(row);
                union.set((byte)0, 10);
                writer.addRow(row);
                union.set((byte)0, 138);
                writer.addRow(row);
                writer.close();

                TypeDescription schema = writer.getSchema();
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
            }

            Reader reader = OrcFile.createReader(TestFilePath, OrcFile.readerOptions(conf));

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

            using (RecordReader rows = reader.rows())
            {
                Assert.Equal(0, rows.getRowNumber());
                Assert.Equal(0.0, rows.getProgress(), 6);
                Assert.Equal(true, rows.hasNext());
                row = (OrcStruct)rows.next();
                Assert.Equal(1, rows.getRowNumber());
                inspector = reader.getObjectInspector();
                Assert.Equal("struct<time:timestamp,union:uniontype<int,string>,decimal:decimal(38,18)>",
                    inspector.getTypeName());
                Assert.Equal(Timestamp.Parse("2000-03-12 15:00:00"), row.getFieldValue(0));
                union = (OrcUnion)row.getFieldValue(1);
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
                Assert.Equal(null, row.getFieldValue(0));
                Assert.Equal(null, row.getFieldValue(1));
                Assert.Equal(null, row.getFieldValue(2));
                row = (OrcStruct)rows.next();
                Assert.Equal(null, row.getFieldValue(0));
                union = (OrcUnion)row.getFieldValue(1);
                Assert.Equal(0, union.getTag());
                Assert.Equal(null, union.getObject());
                Assert.Equal(null, row.getFieldValue(2));
                row = (OrcStruct)rows.next();
                Assert.Equal(null, row.getFieldValue(0));
                Assert.Equal(1, union.getTag());
                Assert.Equal(null, union.getObject());
                Assert.Equal(null, row.getFieldValue(2));
                row = (OrcStruct)rows.next();
                Assert.Equal(Timestamp.Parse("1970-01-01 00:00:00"), row.getFieldValue(0));
                Assert.Equal(200000, union.getObject());
                Assert.Equal(HiveDecimal.Parse("10000000000000000000"), row.getFieldValue(2));
                rand = new Random(42);
                for (int i = 1970; i < 2038; ++i)
                {
                    row = (OrcStruct)rows.next();
                    Assert.Equal(Timestamp.Parse(i + "-05-05 12:34:56." + i), row.getFieldValue(0));
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
                    Assert.Equal(HiveDecimal.create(rand.NextBigInteger(64), rand.Next(18)), row.getFieldValue(2));
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
            }
        }

#if COMPRESSION
        /**
         * Read and write a randomly generated snappy file.
         * @
         */
        [Fact]
        public void testSnappy()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(InnerStruct));

            Random rand;
            using (Stream file = FileOpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                .inspector(inspector)
                .stripeSize(1000)
                .compress(CompressionKind.SNAPPY)
                .bufferSize(100)))
            {
                rand = new Random(12);
                for (int i = 0; i < 10000; ++i)
                {
                    writer.addRow(new InnerStruct(rand.Next(),
                        Integer.toHexString(rand.Next())));
                }
            }

            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));
            using (RecordReader rows = reader.rows())
            {
                rand = new Random(12);
                OrcStruct row = null;
                for (int i = 0; i < 10000; ++i)
                {
                    Assert.Equal(true, rows.hasNext());
                    row = (OrcStruct)rows.next();
                    Assert.Equal(rand.Next(), ((IntWritable)row.getFieldValue(0)).get());
                    Assert.Equal(Integer.toHexString(rand.Next()),
                        row.getFieldValue(1).ToString());
                }
                Assert.Equal(false, rows.hasNext());
            }
        }

        /**
         * Read and write a randomly generated snappy file.
         * @
         */
        [Fact]
        public void testWithoutIndex()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(InnerStruct));

            Random rand;

            using (Stream file = FileOpenWrite(testFilePath))
            using (Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(5000)
                    .compress(CompressionKind.SNAPPY)
                    .bufferSize(1000)
                    .rowIndexStride(0)))
            {
                rand = new Random(24);
                for (int i = 0; i < 10000; ++i)
                {
                    InnerStruct row = new InnerStruct(rand.Next(), Integer.toBinaryString(rand.Next()));
                    for (int j = 0; j < 5; ++j)
                    {
                        writer.addRow(row);
                    }
                }
            }

            Reader reader = OrcFile.createReader(testFilePath,
                OrcFile.readerOptions(conf));
            Assert.Equal(50000, reader.getNumberOfRows());
            Assert.Equal(0, reader.getRowIndexStride());
            StripeInformation stripe = reader.getStripes().First();
            Assert.Equal(true, stripe.getDataLength() != 0);
            Assert.Equal(0, stripe.getIndexLength());
            using (RecordReader rows = reader.rows())
            {
                rand = new Random(24);
                for (int i = 0; i < 10000; ++i)
                {
                    int intVal = rand.Next();
                    string strVal = Integer.toBinaryString(rand.Next());
                    for (int j = 0; j < 5; ++j)
                    {
                        Assert.Equal(true, rows.hasNext());
                        OrcStruct orcRow = (OrcStruct)rows.next();
                        Assert.Equal(intVal, orcRow.getFieldValue(0));
                        Assert.Equal(strVal, orcRow.getFieldValue(1).ToString());
                    }
                }
                Assert.Equal(false, rows.hasNext());
            }
        }
#endif

        [Fact]
        public void testSeek()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(BigRow));
            const int COUNT = 32768;
            long[] intValues = new long[COUNT];
            double[] doubleValues = new double[COUNT];
            string[] stringValues = new string[COUNT];
            byte[][] byteValues = new byte[COUNT][];
            string[] words = new string[128];

            using (Stream file = FileOpenWrite(TestFilePath))
            using (Writer writer = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(200000)
                    .bufferSize(65536)
                    .compress(CompressionKind.NONE) // Compression makes this run 20 times slower
                    .rowIndexStride(1000)))
            {
                Random rand = new Random(42);
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
                    writer.addRow(createRandomRow(intValues, doubleValues, stringValues,
                        byteValues, words, i));
                }
            }

            Reader reader = OrcFile.createReader(TestFilePath,
                OrcFile.readerOptions(conf));
            Assert.Equal(COUNT, reader.getNumberOfRows());
            using (RecordReader rows = reader.rows())
            {
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
                    List<object> actualList =
                        (List<object>)((OrcStruct)row.getFieldValue(9)).getFieldValue(0);
                    compareList(expectedList, actualList);
                    compareList(expected.list, (List<object>)row.getFieldValue(10));
                }
            }

            IList<StripeInformation> stripes = reader.getStripes();
            long offsetOfStripe2 = 0;
            long offsetOfStripe4 = 0;
            long lastRowOfStripe2 = 0;
            for (int i = 0; i < 5; ++i)
            {
                StripeInformation stripe = stripes[i];
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
            columns[5] = true; // long column
            columns[9] = true; // text column

            using (RecordReader rows = reader.rowsOptions(new RecordReaderOptions()
                .range(offsetOfStripe2, offsetOfStripe4 - offsetOfStripe2)
                .include(columns)))
            {
                rows.seekToRow(lastRowOfStripe2);
                for (int i = 0; i < 2; ++i)
                {
                    OrcStruct row = (OrcStruct)rows.next();
                    BigRow expected = createRandomRow(intValues, doubleValues,
                                                      stringValues, byteValues, words,
                                                      (int)(lastRowOfStripe2 + i));

                    Assert.Equal(expected.long1, row.getFieldValue(4));
                    Assert.Equal(expected.string1, row.getFieldValue(8));
                }
            }
        }

        [Fact]
        public void testZeroCopySeek()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(BigRow));

            const int COUNT = 32768;
            long[] intValues = new long[COUNT];
            double[] doubleValues = new double[COUNT];
            string[] stringValues = new string[COUNT];
            byte[][] byteValues = new byte[COUNT][];
            string[] words = new string[128];

            using (Stream file = FileOpenWrite(TestFilePath))
            using (Writer writer = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                .inspector(inspector)
                .stripeSize(200000)
                .bufferSize(65536)
                .rowIndexStride(1000)))
            {
                Random rand = new Random(42);
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
                    writer.addRow(createRandomRow(intValues, doubleValues, stringValues,
                        byteValues, words, i));
                }
            }

            Reader reader = OrcFile.createReader(TestFilePath, OrcFile.readerOptions(conf));
            Assert.Equal(COUNT, reader.getNumberOfRows());
            /* enable zero copy record reader */
#if false
            Configuration conf = new Configuration();
            HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ORC_ZEROCOPY, true);
#endif
            using (RecordReader rows = reader.rows())
            {
                /* all tests are identical to the other seek() tests */
                for (int i = COUNT - 1; i >= 0; --i)
                {
                    rows.seekToRow(i);
                    OrcStruct row = (OrcStruct)rows.next();
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
                    List<object> actualList =
                        (List<object>)((OrcStruct)row.getFieldValue(9)).getFieldValue(0);
                    compareList(expectedList, actualList);
                    compareList(expected.list, (List<object>)row.getFieldValue(10));
                }
            }

            IList<StripeInformation> stripes = reader.getStripes();
            long offsetOfStripe2 = 0;
            long offsetOfStripe4 = 0;
            long lastRowOfStripe2 = 0;
            for (int i = 0; i < 5; ++i)
            {
                StripeInformation stripe = stripes[i];
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
            /* use zero copy record reader */
            using (RecordReader rows = reader.rowsOptions(new RecordReaderOptions()
                .range(offsetOfStripe2, offsetOfStripe4 - offsetOfStripe2)
                .include(columns)))
            {
                rows.seekToRow(lastRowOfStripe2);
                for (int i = 0; i < 2; ++i)
                {
                    OrcStruct row = (OrcStruct)rows.next();
                    BigRow expected = createRandomRow(intValues, doubleValues,
                                                      stringValues, byteValues, words,
                                                      (int)(lastRowOfStripe2 + i));

                    Assert.Equal(expected.long1, row.getFieldValue(4));
                    Assert.Equal(expected.string1, row.getFieldValue(8));
                }
            }
        }

        private void compareInner(InnerStruct expect, OrcStruct actual)
        {
            if (expect == null || actual == null)
            {
                Assert.Equal(null, expect);
                Assert.Equal(null, actual);
            }
            else
            {
                Assert.Equal(expect.int1, actual.getFieldValue(0));
                Assert.Equal(expect.string1, actual.getFieldValue(1));
            }
        }

        private void compareList(List<InnerStruct> expect, List<object> actual)
        {
            Assert.Equal(expect.Count, actual.Count);
            for (int j = 0; j < expect.Count; ++j)
            {
                compareInner(expect[j], (OrcStruct)actual[j]);
            }
        }

        private BigRow createRandomRow(long[] intValues, double[] doubleValues,
                                       string[] stringValues,
                                       byte[][] byteValues,
                                       string[] words, int i)
        {
            InnerStruct inner = new InnerStruct((int)intValues[i], stringValues[i]);
            InnerStruct inner2 = new InnerStruct((int)(intValues[i] >> 32),
                words[i % words.Length] + "-x");
            return new BigRow((intValues[i] & 1) == 0, (sbyte)intValues[i],
                (short)intValues[i], (int)intValues[i], intValues[i],
                (float)doubleValues[i], doubleValues[i], byteValues[i], stringValues[i],
                new MiddleStruct(inner, inner2), new List<InnerStruct>(), makeMap(inner, inner2));
        }

        private class MyMemoryManager : MemoryManager
        {
            long totalSpace;
            double rate;
            internal string path = null;
            long lastAllocation = 0;
            int rows = 0;
            MemoryManager.Callback callback;

            public MyMemoryManager(Configuration conf, long totalSpace, double rate)
                : base(totalSpace)
            {
                this.totalSpace = totalSpace;
                this.rate = rate;
            }

            public override void addWriter(string path, long requestedAllocation, MemoryManager.Callback callback)
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
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(InnerStruct));
            MyMemoryManager memory = new MyMemoryManager(conf, 10000, 0.1);

            using (Stream file = FileOpenWrite(TestFilePath))
            using (Writer writer = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .compress(CompressionKind.NONE)
                    .stripeSize(50000)
                    .bufferSize(100)
                    .rowIndexStride(0)
                    .memory(memory)
                    .version(OrcFile.Version.V_0_11)))
            {
                Assert.Equal(TestFilePath, memory.path);
                for (int i = 0; i < 2500; ++i)
                {
                    writer.addRow(new InnerStruct(i * 300, Integer.toHexString(10 * i)));
                }
                writer.close();
                Assert.Equal(null, memory.path);
            }

            Reader reader = OrcFile.createReader(TestFilePath, OrcFile.readerOptions(conf));
            int j = 0;
            foreach (StripeInformation stripe in reader.getStripes())
            {
                j++;
                Assert.True(stripe.getDataLength() < 5000,
                    "stripe " + j + " is too long at " + stripe.getDataLength());
            }
            Assert.Equal(25, j);
            Assert.Equal(2500, reader.getNumberOfRows());
        }

        [Fact]
        public void testMemoryManagementV12()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(InnerStruct));

            MyMemoryManager memory = new MyMemoryManager(conf, 10000, 0.1);
            using (Stream file = FileOpenWrite(TestFilePath))
            using (Writer writer = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .compress(CompressionKind.NONE)
                    .stripeSize(50000)
                    .bufferSize(100)
                    .rowIndexStride(0)
                    .memory(memory)
                    .version(OrcFile.Version.V_0_12)))
            {
                Assert.Equal(TestFilePath, memory.path);
                for (int i = 0; i < 2500; ++i)
                {
                    writer.addRow(new InnerStruct(i * 300, Integer.toHexString(10 * i)));
                }
                writer.close();
                Assert.Equal(null, memory.path);
            }

            Reader reader = OrcFile.createReader(TestFilePath, OrcFile.readerOptions(conf));
            int j = 0;
            foreach (StripeInformation stripe in reader.getStripes())
            {
                j++;
                Assert.True(stripe.getDataLength() < 5000,
                    "stripe " + j + " is too long at " + stripe.getDataLength());
            }
            // with HIVE-7832, the dictionaries will be disabled after writing the first
            // stripe as there are too many distinct values. Hence only 3 stripes as
            // compared to 25 stripes in version 0.11 (above test case)
            Assert.Equal(3, j);
            Assert.Equal(2500, reader.getNumberOfRows());
        }

        [Fact]
        public void testPredicatePushdown()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(InnerStruct));

            using (Stream file = FileOpenWrite(TestFilePath))
            using (Writer writer = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .stripeSize(400000L)
                    .compress(CompressionKind.NONE)
                    .bufferSize(500)
                    .rowIndexStride(1000)))
            {
                for (int i = 0; i < 3500; ++i)
                {
                    writer.addRow(new InnerStruct(i * 300, Integer.toHexString(10 * i)));
                }
            }
            Reader reader = OrcFile.createReader(TestFilePath, OrcFile.readerOptions(conf));
            Assert.Equal(3500, reader.getNumberOfRows());

            SearchArgument sarg = SearchArgumentFactory.newBuilder()
                .startAnd()
                  .startNot()
                     .lessThan("int1", PredicateLeaf.Type.LONG, 300000L)
                  .end()
                  .lessThan("int1", PredicateLeaf.Type.LONG, 600000L)
                .end()
                .build();
            using (RecordReader rows = reader.rowsOptions(new RecordReaderOptions()
                .range(0L, Int64.MaxValue)
                .include(new bool[] { true, true, true })
                .searchArgument(sarg, new string[] { null, "int1", "string1" })))
            {
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
            }

            // look through the file with no rows selected
            sarg = SearchArgumentFactory.newBuilder()
                .startAnd()
                  .lessThan("int1", PredicateLeaf.Type.LONG, 0L)
                .end()
                .build();
            using (RecordReader rows = reader.rowsOptions(new RecordReaderOptions()
                .range(0L, Int64.MaxValue)
                .include(new bool[] { true, true, true })
                .searchArgument(sarg, new String[] { null, "int1", "string1" })))
            {
                Assert.Equal(3500L, rows.getRowNumber());
                Assert.True(!rows.hasNext());
            }

            // select first 100 and last 100 rows
            sarg = SearchArgumentFactory.newBuilder()
                .startOr()
                  .lessThan("int1", PredicateLeaf.Type.LONG, 300L * 100)
                  .startNot()
                    .lessThan("int1", PredicateLeaf.Type.LONG, 300L * 3400)
                  .end()
                .end()
                .build();
            using (RecordReader rows = reader.rowsOptions(new RecordReaderOptions()
                .range(0L, Int64.MaxValue)
                .include(new bool[] { true, true, true })
                .searchArgument(sarg, new string[] { null, "int1", "string1" })))
            {
                for (int i = 0; i < 1000; ++i)
                {
                    Assert.True(rows.hasNext());
                    Assert.Equal(i, rows.getRowNumber());
                    OrcStruct row = (OrcStruct)rows.next();
                    Assert.Equal(300 * i, row.getFieldValue(0));
                    Assert.Equal(Integer.toHexString(10 * i), row.getFieldValue(1).ToString());
                }
                for (int i = 3000; i < 3500; ++i)
                {
                    Assert.True(rows.hasNext());
                    Assert.Equal(i, rows.getRowNumber());
                    OrcStruct row = (OrcStruct)rows.next();
                    Assert.Equal(300 * i, row.getFieldValue(0));
                    Assert.Equal(Integer.toHexString(10 * i), row.getFieldValue(1).ToString());
                }
                Assert.True(!rows.hasNext());
                Assert.Equal(3500, rows.getRowNumber());
            }
        }
    }
}
