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
    using OrcSharp.External;
    using OrcSharp.Query;
    using OrcSharp.Types;
    using Xunit;
    using OrcProto = global::orc.proto;

    public class TestRecordReaderImpl
    {
#if false
        class BufferInStream : InputStream, PositionedReadable, Seekable
        {
            private byte[] buffer;
            private int length;
            private int position = 0;

            public BufferInStream(byte[] bytes, int length)
            {
                this.buffer = bytes;
                this.length = length;
            }

            public int read()
            {
                if (position < length)
                {
                    return buffer[position++];
                }
                return -1;
            }

            public int read(byte[] bytes, int offset, int length)
            {
                int lengthToRead = Math.Min(length, this.length - this.position);
                if (lengthToRead >= 0)
                {
                    for (int i = 0; i < lengthToRead; ++i)
                    {
                        bytes[offset + i] = buffer[position++];
                    }
                    return lengthToRead;
                }
                else
                {
                    return -1;
                }
            }

            public int read(long position, byte[] bytes, int offset, int length)
            {
                this.position = (int)position;
                return read(bytes, offset, length);
            }

            public void readFully(long position, byte[] bytes, int offset,
                                  int length)
            {
                this.position = (int)position;
                while (length > 0)
                {
                    int result = read(bytes, offset, length);
                    offset += result;
                    length -= result;
                    if (result < 0)
                    {
                        throw new IOException("Read past end of buffer at " + offset);
                    }
                }
            }

            public void readFully(long position, byte[] bytes)
            {
                readFully(position, bytes, 0, bytes.Length);
            }

            public void seek(long position)
            {
                this.position = (int)position;
            }

            public long getPos()
            {
                return position;
            }

            public bool seekToNewSource(long position)
            {
                this.position = (int)position;
                return false;
            }
        }

        [Fact]
        public void testMaxLengthToReader()
        {
            Configuration conf = new Configuration();
            OrcProto.Type rowType = OrcProto.Type.CreateBuilder()
                .SetKind(OrcProto.Type.Types.Kind.STRUCT).Build();
            OrcProto.Footer footer = OrcProto.Footer.CreateBuilder()
                .SetHeaderLength(0).SetContentLength(0).SetNumberOfRows(0)
                .SetRowIndexStride(0).AddTypes(rowType).Build();
            OrcProto.PostScript ps = OrcProto.PostScript.CreateBuilder()
                .SetCompression(OrcProto.CompressionKind.NONE)
                .SetFooterLength((ulong)footer.SerializedSize)
                .SetMagic("ORC").AddVersion(0).AddVersion(11).Build();
            DataOutputBuffer buffer = new DataOutputBuffer();
            footer.WriteTo(buffer);
            ps.WriteTo(buffer);
            buffer.write(ps.SerializedSize);
            FileSystem fs = Mockito.mock(typeof(FileSystem), settings);
            FSDataInputStream file = new FSDataInputStream(new BufferInStream(buffer.getData(),
                    buffer.getLength()));
            string p = "/dir/file.orc";
            Mockito.when(fs.open(p)).thenReturn(file);
            OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
            options.filesystem(fs);
            options.maxLength(buffer.getLength());
            Mockito.when(fs.getFileStatus(p))
                .thenReturn(new FileStatus(10, false, 3, 3000, 0, p));
            Reader reader = OrcFile.createReader(p, options);
        }
#endif

        [Fact]
        public void testCompareToRangeInt()
        {
            Assert.Equal(RecordReaderImpl.Location.BEFORE,
                RecordReaderImpl.compareToRange(19L, 20L, 40L));
            Assert.Equal(RecordReaderImpl.Location.AFTER,
                RecordReaderImpl.compareToRange(41L, 20L, 40L));
            Assert.Equal(RecordReaderImpl.Location.MIN,
                RecordReaderImpl.compareToRange(20L, 20L, 40L));
            Assert.Equal(RecordReaderImpl.Location.MIDDLE,
                RecordReaderImpl.compareToRange(21L, 20L, 40L));
            Assert.Equal(RecordReaderImpl.Location.MAX,
                RecordReaderImpl.compareToRange(40L, 20L, 40L));
            Assert.Equal(RecordReaderImpl.Location.BEFORE,
                RecordReaderImpl.compareToRange(0L, 1L, 1L));
            Assert.Equal(RecordReaderImpl.Location.MIN,
                RecordReaderImpl.compareToRange(1L, 1L, 1L));
            Assert.Equal(RecordReaderImpl.Location.AFTER,
                RecordReaderImpl.compareToRange(2L, 1L, 1L));
        }

        [Fact]
        public void testCompareToRangeString()
        {
            Assert.Equal(RecordReaderImpl.Location.BEFORE,
                RecordReaderImpl.compareToRange("a", "b", "c"));
            Assert.Equal(RecordReaderImpl.Location.AFTER,
                RecordReaderImpl.compareToRange("d", "b", "c"));
            Assert.Equal(RecordReaderImpl.Location.MIN,
                RecordReaderImpl.compareToRange("b", "b", "c"));
            Assert.Equal(RecordReaderImpl.Location.MIDDLE,
                RecordReaderImpl.compareToRange("bb", "b", "c"));
            Assert.Equal(RecordReaderImpl.Location.MAX,
                RecordReaderImpl.compareToRange("c", "b", "c"));
            Assert.Equal(RecordReaderImpl.Location.BEFORE,
                RecordReaderImpl.compareToRange("a", "b", "b"));
            Assert.Equal(RecordReaderImpl.Location.MIN,
                RecordReaderImpl.compareToRange("b", "b", "b"));
            Assert.Equal(RecordReaderImpl.Location.AFTER,
                RecordReaderImpl.compareToRange("c", "b", "b"));
        }

        [Fact]
        public void testCompareToCharNeedConvert()
        {
            Assert.Equal(RecordReaderImpl.Location.BEFORE,
                RecordReaderImpl.compareToRange("apple", "hello", "world"));
            Assert.Equal(RecordReaderImpl.Location.AFTER,
                RecordReaderImpl.compareToRange("zombie", "hello", "world"));
            Assert.Equal(RecordReaderImpl.Location.MIN,
                RecordReaderImpl.compareToRange("hello", "hello", "world"));
            Assert.Equal(RecordReaderImpl.Location.MIDDLE,
                RecordReaderImpl.compareToRange("pilot", "hello", "world"));
            Assert.Equal(RecordReaderImpl.Location.MAX,
                RecordReaderImpl.compareToRange("world", "hello", "world"));
            Assert.Equal(RecordReaderImpl.Location.BEFORE,
                RecordReaderImpl.compareToRange("apple", "hello", "hello"));
            Assert.Equal(RecordReaderImpl.Location.MIN,
                RecordReaderImpl.compareToRange("hello", "hello", "hello"));
            Assert.Equal(RecordReaderImpl.Location.AFTER,
                RecordReaderImpl.compareToRange("zombie", "hello", "hello"));
        }

        [Fact]
        public void testGetMin()
        {
            Assert.Equal(10L, RecordReaderImpl.getMin(
                ColumnStatisticsImpl.deserialize(createIntStats(10L, 100L))));
            Assert.Equal(10.0d, RecordReaderImpl.getMin(ColumnStatisticsImpl.deserialize(
                OrcProto.ColumnStatistics.CreateBuilder()
                    .SetDoubleStatistics(OrcProto.DoubleStatistics.CreateBuilder()
                        .SetMinimum(10.0d).SetMaximum(100.0d).Build()).Build())));
            Assert.Equal(null, RecordReaderImpl.getMin(ColumnStatisticsImpl.deserialize(
                OrcProto.ColumnStatistics.CreateBuilder()
                    .SetStringStatistics(OrcProto.StringStatistics.CreateBuilder().Build())
                    .Build())));
            Assert.Equal("a", RecordReaderImpl.getMin(ColumnStatisticsImpl.deserialize(
                OrcProto.ColumnStatistics.CreateBuilder()
                    .SetStringStatistics(OrcProto.StringStatistics.CreateBuilder()
                        .SetMinimum("a").SetMaximum("b").Build()).Build())));
            Assert.Equal("hello", RecordReaderImpl.getMin(ColumnStatisticsImpl
                .deserialize(createStringStats("hello", "world"))));
            Assert.Equal(HiveDecimal.Parse("111.1"), RecordReaderImpl.getMin(ColumnStatisticsImpl
                .deserialize(createDecimalStats("111.1", "112.1"))));
        }

        private static OrcProto.ColumnStatistics createIntStats(long? min, long? max)
        {
            OrcProto.IntegerStatistics.Builder intStats =
                OrcProto.IntegerStatistics.CreateBuilder();
            if (min != null)
            {
                intStats.SetMinimum(min.Value);
            }
            if (max != null)
            {
                intStats.SetMaximum(max.Value);
            }
            return OrcProto.ColumnStatistics.CreateBuilder()
                .SetIntStatistics(intStats.Build()).Build();
        }

        private static OrcProto.ColumnStatistics createBooleanStats(int n, int trueCount)
        {
            OrcProto.BucketStatistics.Builder boolStats = OrcProto.BucketStatistics.CreateBuilder();
            boolStats.AddCount((ulong)trueCount);
            return OrcProto.ColumnStatistics.CreateBuilder().SetNumberOfValues((ulong)n).SetBucketStatistics(
                boolStats.Build()).Build();
        }

        private static OrcProto.ColumnStatistics createIntStats(int min, int max)
        {
            OrcProto.IntegerStatistics.Builder intStats = OrcProto.IntegerStatistics.CreateBuilder();
            intStats.SetMinimum(min);
            intStats.SetMaximum(max);
            return OrcProto.ColumnStatistics.CreateBuilder().SetIntStatistics(intStats.Build()).Build();
        }

        private static OrcProto.ColumnStatistics createDoubleStats(double min, double max)
        {
            OrcProto.DoubleStatistics.Builder dblStats = OrcProto.DoubleStatistics.CreateBuilder();
            dblStats.SetMinimum(min);
            dblStats.SetMaximum(max);
            return OrcProto.ColumnStatistics.CreateBuilder().SetDoubleStatistics(dblStats.Build()).Build();
        }

        private static OrcProto.ColumnStatistics createStringStats(string min, string max,
            bool hasNull)
        {
            OrcProto.StringStatistics.Builder strStats = OrcProto.StringStatistics.CreateBuilder();
            strStats.SetMinimum(min);
            strStats.SetMaximum(max);
            return OrcProto.ColumnStatistics.CreateBuilder().SetStringStatistics(strStats.Build())
                .SetHasNull(hasNull).Build();
        }

        private static OrcProto.ColumnStatistics createStringStats(string min, string max)
        {
            OrcProto.StringStatistics.Builder strStats = OrcProto.StringStatistics.CreateBuilder();
            strStats.SetMinimum(min);
            strStats.SetMaximum(max);
            return OrcProto.ColumnStatistics.CreateBuilder().SetStringStatistics(strStats.Build()).Build();
        }

        private static OrcProto.ColumnStatistics createDateStats(int min, int max)
        {
            OrcProto.DateStatistics.Builder dateStats = OrcProto.DateStatistics.CreateBuilder();
            dateStats.SetMinimum(min);
            dateStats.SetMaximum(max);
            return OrcProto.ColumnStatistics.CreateBuilder().SetDateStatistics(dateStats.Build()).Build();
        }

        private static OrcProto.ColumnStatistics createTimestampStats(long min, long max)
        {
            OrcProto.TimestampStatistics.Builder tsStats = OrcProto.TimestampStatistics.CreateBuilder();
            tsStats.SetMinimum(min);
            tsStats.SetMaximum(max);
            return OrcProto.ColumnStatistics.CreateBuilder().SetTimestampStatistics(tsStats.Build()).Build();
        }

        private static OrcProto.ColumnStatistics createDecimalStats(string min, string max)
        {
            OrcProto.DecimalStatistics.Builder decStats = OrcProto.DecimalStatistics.CreateBuilder();
            decStats.SetMinimum(min);
            decStats.SetMaximum(max);
            return OrcProto.ColumnStatistics.CreateBuilder().SetDecimalStatistics(decStats.Build()).Build();
        }

        private static OrcProto.ColumnStatistics createDecimalStats(string min, string max,
            bool hasNull)
        {
            OrcProto.DecimalStatistics.Builder decStats = OrcProto.DecimalStatistics.CreateBuilder();
            decStats.SetMinimum(min);
            decStats.SetMaximum(max);
            return OrcProto.ColumnStatistics.CreateBuilder().SetDecimalStatistics(decStats.Build())
                .SetHasNull(hasNull).Build();
        }

        [Fact]
        public void testGetMax()
        {
            Assert.Equal(100L, RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(createIntStats(10L, 100L))));
            Assert.Equal(100.0d, RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(
                OrcProto.ColumnStatistics.CreateBuilder()
                    .SetDoubleStatistics(OrcProto.DoubleStatistics.CreateBuilder()
                        .SetMinimum(10.0d).SetMaximum(100.0d).Build()).Build())));
            Assert.Equal(null, RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(
                OrcProto.ColumnStatistics.CreateBuilder()
                    .SetStringStatistics(OrcProto.StringStatistics.CreateBuilder().Build())
                    .Build())));
            Assert.Equal("b", RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(
                OrcProto.ColumnStatistics.CreateBuilder()
                    .SetStringStatistics(OrcProto.StringStatistics.CreateBuilder()
                        .SetMinimum("a").SetMaximum("b").Build()).Build())));
            Assert.Equal("world", RecordReaderImpl.getMax(ColumnStatisticsImpl
                .deserialize(createStringStats("hello", "world"))));
            Assert.Equal(HiveDecimal.Parse("112.1"), RecordReaderImpl.getMax(ColumnStatisticsImpl
                .deserialize(createDecimalStats("111.1", "112.1"))));
        }

        [Fact]
        public void testPredEvalWithBooleanStats()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.BOOLEAN, "x", true, null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createBooleanStats(10, 10), pred, null));
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createBooleanStats(10, 0), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.BOOLEAN, "x", true, null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createBooleanStats(10, 10), pred, null));
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createBooleanStats(10, 0), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.BOOLEAN, "x", false, null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createBooleanStats(10, 10), pred, null));
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createBooleanStats(10, 0), pred, null));
        }

#if false
        [Fact]
        public void testPredEvalWithIntStats()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.FLOAT, "x", 15.0, null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10, 100), pred, null));

            // Stats gets converted to column type. "15" is outside of "10" and "100"
            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.STRING, "x", "15", null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10, 100), pred, null));

            // Integer stats will not be converted date because of days/seconds/millis ambiguity
            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.DATE, "x", new DateWritable(15).get(), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10, 100), pred, null));
        }

        [Fact]
        public void testPredEvalWithDoubleStats()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.FLOAT, "x", 15.0, null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));

            // Stats gets converted to column type. "15.0" is outside of "10.0" and "100.0"
            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.STRING, "x", "15", null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));

            // Double is not converted to date type because of days/seconds/millis ambiguity
            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.DATE, "x", new DateWritable(15).get(), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15 * 1000L), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(150 * 1000L), null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));
        }

        [Fact]
        public void testPredEvalWithStringStats()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 100L, null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("10", "1000"), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.FLOAT, "x", 100.0, null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("10", "1000"), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.STRING, "x", "100", null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("10", "1000"), pred, null));

            // IllegalArgumentException is thrown when converting String to Date, hence YES_NO
            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.DATE, "x", new DateWritable(100).get(), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 1000), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("100"), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("10", "1000"), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(100), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("10", "1000"), pred, null));
        }

        [Fact]
        public void testPredEvalWithDateStats()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
            // Date to Integer conversion is not possible.
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

            // Date to Float conversion is also not possible.
            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.FLOAT, "x", 15.0, null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.STRING, "x", "15", null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.STRING, "x", "1970-01-11", null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.STRING, "x", "15.1", null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.STRING, "x", "__a15__1", null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.STRING, "x", "2000-01-16", null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.STRING, "x", "1970-01-16", null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.DATE, "x", new DateWritable(15).get(), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.DATE, "x", new DateWritable(150).get(), null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

            // Date to Decimal conversion is also not possible.
            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15), null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15L * 24L * 60L * 60L * 1000L), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDateStats(10, 100), pred, null));
        }

        [Fact]
        public void testPredEvalWithDecimalStats()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDecimalStats("10.0", "100.0"), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.FLOAT, "x", 15.0, null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDecimalStats("10.0", "100.0"), pred, null));

            // "15" out of range of "10.0" and "100.0"
            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.STRING, "x", "15", null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createDecimalStats("10.0", "100.0"), pred, null));

            // Decimal to Date not possible.
            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.DATE, "x", new DateWritable(15).get(), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDecimalStats("10.0", "100.0"), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDecimalStats("10.0", "100.0"), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15 * 1000L), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createDecimalStats("10.0", "100.0"), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(150 * 1000L), null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createDecimalStats("10.0", "100.0"), pred, null));
        }

        [Fact]
        public void testPredEvalWithTimestampStats()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.FLOAT, "x", 15.0, null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10, 100), pred, null));
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10000, 100000), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.STRING, "x", "15", null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.STRING, "x", new Timestamp(15).ToString(), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10, 100), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.DATE, "x", new DateWritable(15).get(), null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10, 100), pred, null));
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10 * 24L * 60L * 60L * 1000L,
                    100 * 24L * 60L * 60L * 1000L), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10, 100), pred, null));
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10000, 100000), pred, null));

            pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
                PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15), null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10, 100), pred, null));
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createTimestampStats(10000, 100000), pred, null));
        }
#endif

        [Fact]
        public void testEquals()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.LONG,
                    "x", 15L, null);
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(20L, 30L), pred, null));
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(15L, 30L), pred, null));
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 30L), pred, null));
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 15L), pred, null));
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(0L, 10L), pred, null));
            Assert.Equal(TruthValue.YES_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(15L, 15L), pred, null));
        }

        [Fact]
        public void testNullSafeEquals()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG,
                    "x", 15L, null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(20L, 30L), pred, null));
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(15L, 30L), pred, null));
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 30L), pred, null));
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 15L), pred, null));
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(0L, 10L), pred, null));
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(15L, 15L), pred, null));
        }

        [Fact]
        public void testLessThan()
        {
            PredicateLeaf lessThan = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.LONG,
                    "x", 15L, null);
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(20L, 30L), lessThan, null));
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(15L, 30L), lessThan, null));
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 30L), lessThan, null));
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 15L), lessThan, null));
            Assert.Equal(TruthValue.YES_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(0L, 10L), lessThan, null));
        }

        [Fact]
        public void testLessThanEquals()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.LESS_THAN_EQUALS, PredicateLeaf.Type.LONG,
                    "x", 15L, null);
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(20L, 30L), pred, null));
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(15L, 30L), pred, null));
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 30L), pred, null));
            Assert.Equal(TruthValue.YES_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 15L), pred, null));
            Assert.Equal(TruthValue.YES_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(0L, 10L), pred, null));
        }

        [Fact]
        public void testIn()
        {
            List<object> args = new List<object>();
            args.Add(10L);
            args.Add(20L);
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.IN, PredicateLeaf.Type.LONG,
                    "x", null, args);
            Assert.Equal(TruthValue.YES_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(20L, 20L), pred, null));
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(30L, 30L), pred, null));
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 30L), pred, null));
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(12L, 18L), pred, null));
        }

        [Fact]
        public void testBetween()
        {
            List<object> args = new List<object>();
            args.Add(10L);
            args.Add(20L);
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.BETWEEN, PredicateLeaf.Type.LONG,
                    "x", null, args);
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(0L, 5L), pred, null));
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(30L, 40L), pred, null));
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(5L, 15L), pred, null));
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(15L, 25L), pred, null));
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(5L, 25L), pred, null));
            Assert.Equal(TruthValue.YES_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(10L, 20L), pred, null));
            Assert.Equal(TruthValue.YES_NULL,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(12L, 18L), pred, null));
        }

        [Fact]
        public void testIsNull()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.LONG,
                    "x", null, null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createIntStats(20L, 30L), pred, null));
        }


        [Fact]
        public void testEqualsWithNullInStats()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.STRING,
                    "x", "c", null);
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("d", "e", true), pred, null)); // before
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("a", "b", true), pred, null)); // after
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "c", true), pred, null)); // max
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", true), pred, null)); // min
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "d", true), pred, null)); // middle
            Assert.Equal(TruthValue.YES_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "c", true), pred, null)); // same
        }

        [Fact]
        public void testNullSafeEqualsWithNullInStats()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.STRING,
                    "x", "c", null);
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("d", "e", true), pred, null)); // before
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("a", "b", true), pred, null)); // after
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "c", true), pred, null)); // max
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", true), pred, null)); // min
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "d", true), pred, null)); // middle
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "c", true), pred, null)); // same
        }

        [Fact]
        public void testLessThanWithNullInStats()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.STRING,
                    "x", "c", null);
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("d", "e", true), pred, null)); // before
            Assert.Equal(TruthValue.YES_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("a", "b", true), pred, null)); // after
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "c", true), pred, null)); // max
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", true), pred, null)); // min
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "d", true), pred, null)); // middle
            Assert.Equal(TruthValue.NO_NULL, // min, same stats
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "c", true), pred, null));
        }

        [Fact]
        public void testLessThanEqualsWithNullInStats()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.LESS_THAN_EQUALS, PredicateLeaf.Type.STRING,
                    "x", "c", null);
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("d", "e", true), pred, null)); // before
            Assert.Equal(TruthValue.YES_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("a", "b", true), pred, null)); // after
            Assert.Equal(TruthValue.YES_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "c", true), pred, null)); // max
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", true), pred, null)); // min
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "d", true), pred, null)); // middle
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "c", true), pred, null)); // same
        }

        [Fact]
        public void testInWithNullInStats()
        {
            List<object> args = new List<object>();
            args.Add("c");
            args.Add("f");
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.IN, PredicateLeaf.Type.STRING,
                    "x", null, args);
            Assert.Equal(TruthValue.NO_NULL, // before & after
                RecordReaderImpl.evaluatePredicateProto(createStringStats("d", "e", true), pred, null));
            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("a", "b", true), pred, null)); // after
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("e", "f", true), pred, null)); // max
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", true), pred, null)); // min
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "d", true), pred, null)); // middle
            Assert.Equal(TruthValue.YES_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "c", true), pred, null)); // same
        }

        [Fact]
        public void testBetweenWithNullInStats()
        {
            List<object> args = new List<object>();
            args.Add("c");
            args.Add("f");
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.BETWEEN, PredicateLeaf.Type.STRING,
                    "x", null, args);
            Assert.Equal(TruthValue.YES_NULL, // before & after
                RecordReaderImpl.evaluatePredicateProto(createStringStats("d", "e", true), pred, null));
            Assert.Equal(TruthValue.YES_NULL, // before & max
                RecordReaderImpl.evaluatePredicateProto(createStringStats("e", "f", true), pred, null));
            Assert.Equal(TruthValue.NO_NULL, // before & before
                RecordReaderImpl.evaluatePredicateProto(createStringStats("h", "g", true), pred, null));
            Assert.Equal(TruthValue.YES_NO_NULL, // before & min
                RecordReaderImpl.evaluatePredicateProto(createStringStats("f", "g", true), pred, null));
            Assert.Equal(TruthValue.YES_NO_NULL, // before & middle
                RecordReaderImpl.evaluatePredicateProto(createStringStats("e", "g", true), pred, null));

            Assert.Equal(TruthValue.YES_NULL, // min & after
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "e", true), pred, null));
            Assert.Equal(TruthValue.YES_NULL, // min & max
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "f", true), pred, null));
            Assert.Equal(TruthValue.YES_NO_NULL, // min & middle
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "g", true), pred, null));

            Assert.Equal(TruthValue.NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("a", "b", true), pred, null)); // after
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("a", "c", true), pred, null)); // max
            Assert.Equal(TruthValue.YES_NO_NULL,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("b", "d", true), pred, null)); // middle
            Assert.Equal(TruthValue.YES_NULL, // min & after, same stats
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "c", true), pred, null));
        }

        [Fact]
        public void testIsNullWithNullInStats()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.STRING,
                    "x", null, null);
            Assert.Equal(TruthValue.YES_NO,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", true), pred, null));
            Assert.Equal(TruthValue.NO,
                RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", false), pred, null));
        }

        [Fact]
        public void testOverlap()
        {
            Assert.True(!RecordReaderUtils.overlap(0, 10, -10, -1));
            Assert.True(RecordReaderUtils.overlap(0, 10, -1, 0));
            Assert.True(RecordReaderUtils.overlap(0, 10, -1, 1));
            Assert.True(RecordReaderUtils.overlap(0, 10, 2, 8));
            Assert.True(RecordReaderUtils.overlap(0, 10, 5, 10));
            Assert.True(RecordReaderUtils.overlap(0, 10, 10, 11));
            Assert.True(RecordReaderUtils.overlap(0, 10, 0, 10));
            Assert.True(RecordReaderUtils.overlap(0, 10, -1, 11));
            Assert.True(!RecordReaderUtils.overlap(0, 10, 11, 12));
        }

        private static DiskRangeList diskRanges(params int[] points)
        {
            DiskRangeList head = null, tail = null;
            for (int i = 0; i < points.Length; i += 2)
            {
                DiskRangeList range = new DiskRangeList(points[i], points[i + 1]);
                if (tail == null)
                {
                    head = tail = range;
                }
                else
                {
                    tail = tail.insertAfter(range);
                }
            }
            return head;
        }

        [Fact]
        public void testGetIndexPosition()
        {
            Assert.Equal(0, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.INT,
                    OrcProto.Stream.Types.Kind.PRESENT, true, true));
            Assert.Equal(4, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.INT,
                    OrcProto.Stream.Types.Kind.DATA, true, true));
            Assert.Equal(3, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.INT,
                    OrcProto.Stream.Types.Kind.DATA, false, true));
            Assert.Equal(0, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.INT,
                    OrcProto.Stream.Types.Kind.DATA, true, false));
            Assert.Equal(4, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DICTIONARY, OrcProto.Type.Types.Kind.STRING,
                    OrcProto.Stream.Types.Kind.DATA, true, true));
            Assert.Equal(4, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.BINARY,
                    OrcProto.Stream.Types.Kind.DATA, true, true));
            Assert.Equal(3, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.BINARY,
                    OrcProto.Stream.Types.Kind.DATA, false, true));
            Assert.Equal(6, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.BINARY,
                    OrcProto.Stream.Types.Kind.LENGTH, true, true));
            Assert.Equal(4, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.BINARY,
                    OrcProto.Stream.Types.Kind.LENGTH, false, true));
            Assert.Equal(4, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.DECIMAL,
                    OrcProto.Stream.Types.Kind.DATA, true, true));
            Assert.Equal(3, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.DECIMAL,
                    OrcProto.Stream.Types.Kind.DATA, false, true));
            Assert.Equal(6, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.DECIMAL,
                    OrcProto.Stream.Types.Kind.SECONDARY, true, true));
            Assert.Equal(4, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.DECIMAL,
                    OrcProto.Stream.Types.Kind.SECONDARY, false, true));
            Assert.Equal(4, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.TIMESTAMP,
                    OrcProto.Stream.Types.Kind.DATA, true, true));
            Assert.Equal(3, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.TIMESTAMP,
                    OrcProto.Stream.Types.Kind.DATA, false, true));
            Assert.Equal(7, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.TIMESTAMP,
                    OrcProto.Stream.Types.Kind.SECONDARY, true, true));
            Assert.Equal(5, RecordReaderUtils.getIndexPosition
                (OrcProto.ColumnEncoding.Types.Kind.DIRECT, OrcProto.Type.Types.Kind.TIMESTAMP,
                    OrcProto.Stream.Types.Kind.SECONDARY, false, true));
        }

        [Fact]
        public void testPartialPlan()
        {
            DiskRangeList result;

            // set the streams
            List<OrcProto.Stream> streams = new List<OrcProto.Stream>();
            streams.Add(OrcProto.Stream.CreateBuilder()
                .SetKind(OrcProto.Stream.Types.Kind.PRESENT)
                .SetColumn(1).SetLength(1000).Build());
            streams.Add(OrcProto.Stream.CreateBuilder()
                .SetKind(OrcProto.Stream.Types.Kind.DATA)
                .SetColumn(1).SetLength(99000).Build());
            streams.Add(OrcProto.Stream.CreateBuilder()
                .SetKind(OrcProto.Stream.Types.Kind.PRESENT)
                .SetColumn(2).SetLength(2000).Build());
            streams.Add(OrcProto.Stream.CreateBuilder()
                .SetKind(OrcProto.Stream.Types.Kind.DATA)
                .SetColumn(2).SetLength(98000).Build());

            bool[] columns = new bool[] { true, true, false };
            bool[] rowGroups = new bool[] { true, true, false, false, true, false };

            // set the index
            OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[columns.Length];
            indexes[1] = OrcProto.RowIndex.CreateBuilder()
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(0).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(0)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(100).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(10000)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(200).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(20000)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(300).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(30000)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(400).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(40000)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(500).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(50000)
                    .Build())
                .Build();

            // set encodings
            List<OrcProto.ColumnEncoding> encodings = new List<OrcProto.ColumnEncoding>();
            encodings.Add(OrcProto.ColumnEncoding.CreateBuilder()
                            .SetKind(OrcProto.ColumnEncoding.Types.Kind.DIRECT).Build());
            encodings.Add(OrcProto.ColumnEncoding.CreateBuilder()
                .SetKind(OrcProto.ColumnEncoding.Types.Kind.DIRECT).Build());
            encodings.Add(OrcProto.ColumnEncoding.CreateBuilder()
                .SetKind(OrcProto.ColumnEncoding.Types.Kind.DIRECT).Build());

            // set types struct{x: int, y: int}
            List<OrcProto.Type> types = new List<OrcProto.Type>();
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.STRUCT)
                        .AddSubtypes(1).AddSubtypes(2).AddFieldNames("x")
                        .AddFieldNames("y").Build());
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.INT).Build());
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.INT).Build());

            // filter by rows and groups
            result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
                columns, rowGroups, false, encodings, types, 32768, false);
            Assert.Equal(
                diskRanges(0, 1000, 100, 1000, 400, 1000,
                    1000, 11000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
                    11000, 21000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
                    41000, 51000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP),
                result);
            result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
                columns, rowGroups, false, encodings, types, 32768, true);
            Assert.Equal(
                diskRanges(0, 21000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
                    41000, 51000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP),
                result);

            // if we read no rows, don't read any bytes
            rowGroups = new bool[] { false, false, false, false, false, false };
            result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
                columns, rowGroups, false, encodings, types, 32768, false);
            Assert.Null(result);

            // all rows, but only columns 0 and 2.
            rowGroups = null;
            columns = new bool[] { true, false, true };
            result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
                columns, null, false, encodings, types, 32768, false);
            Assert.Equal(diskRanges(100000, 102000, 102000, 200000), result);
            result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
                columns, null, false, encodings, types, 32768, true);
            Assert.Equal(diskRanges(100000, 200000), result);

            rowGroups = new bool[] { false, true, false, false, false, false };
            indexes[2] = indexes[1];
            indexes[1] = null;
            result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
                columns, rowGroups, false, encodings, types, 32768, false);
            Assert.Equal(
                diskRanges(100100, 102000,
                    112000, 122000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP),
                result);
            result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
                columns, rowGroups, false, encodings, types, 32768, true);
            Assert.Equal(
                diskRanges(100100, 102000,
                    112000, 122000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP),
                result);

            rowGroups = new bool[] { false, false, false, false, false, true };
            indexes[1] = indexes[2];
            columns = new bool[] { true, true, true };
            result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
                columns, rowGroups, false, encodings, types, 32768, false);
            Assert.Equal(diskRanges(500, 1000, 51000, 100000, 100500, 102000, 152000, 200000), result);
            result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
                columns, rowGroups, false, encodings, types, 32768, true);
            Assert.Equal(diskRanges(500, 1000, 51000, 100000, 100500, 102000, 152000, 200000), result);
        }


        [Fact]
        public void testPartialPlanCompressed()
        {
            DiskRangeList result;

            // set the streams
            List<OrcProto.Stream> streams = new List<OrcProto.Stream>();
            streams.Add(OrcProto.Stream.CreateBuilder()
                .SetKind(OrcProto.Stream.Types.Kind.PRESENT)
                .SetColumn(1).SetLength(1000).Build());
            streams.Add(OrcProto.Stream.CreateBuilder()
                .SetKind(OrcProto.Stream.Types.Kind.DATA)
                .SetColumn(1).SetLength(99000).Build());
            streams.Add(OrcProto.Stream.CreateBuilder()
                .SetKind(OrcProto.Stream.Types.Kind.PRESENT)
                .SetColumn(2).SetLength(2000).Build());
            streams.Add(OrcProto.Stream.CreateBuilder()
                .SetKind(OrcProto.Stream.Types.Kind.DATA)
                .SetColumn(2).SetLength(98000).Build());

            bool[] columns = new bool[] { true, true, false };
            bool[] rowGroups = new bool[] { true, true, false, false, true, false };

            // set the index
            OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[columns.Length];
            indexes[1] = OrcProto.RowIndex.CreateBuilder()
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(0).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(0)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(100).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(10000)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(200).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(20000)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(300).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(30000)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(400).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(40000)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(500).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(50000)
                    .Build())
                .Build();

            // set encodings
            List<OrcProto.ColumnEncoding> encodings = new List<OrcProto.ColumnEncoding>();
            encodings.Add(OrcProto.ColumnEncoding.CreateBuilder()
                .SetKind(OrcProto.ColumnEncoding.Types.Kind.DIRECT).Build());
            encodings.Add(OrcProto.ColumnEncoding.CreateBuilder()
                .SetKind(OrcProto.ColumnEncoding.Types.Kind.DIRECT).Build());
            encodings.Add(OrcProto.ColumnEncoding.CreateBuilder()
                .SetKind(OrcProto.ColumnEncoding.Types.Kind.DIRECT).Build());

            // set types struct{x: int, y: int}
            List<OrcProto.Type> types = new List<OrcProto.Type>();
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.STRUCT)
                .AddSubtypes(1).AddSubtypes(2).AddFieldNames("x")
                .AddFieldNames("y").Build());
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.INT).Build());
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.INT).Build());

            // filter by rows and groups
            result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
                columns, rowGroups, true, encodings, types, 32768, false);
            Assert.Equal(
                diskRanges(0, 1000, 100, 1000,
                    400, 1000, 1000, 11000 + (2 * 32771),
                    11000, 21000 + (2 * 32771), 41000, 100000),
                result);

            rowGroups = new bool[] { false, false, false, false, false, true };
            result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
                columns, rowGroups, true, encodings, types, 32768, false);
            Assert.Equal(diskRanges(500, 1000, 51000, 100000), result);
        }

        [Fact]
        public void testPartialPlanString()
        {
            DiskRangeList result;

            // set the streams
            List<OrcProto.Stream> streams = new List<OrcProto.Stream>();
            streams.Add(OrcProto.Stream.CreateBuilder()
                .SetKind(OrcProto.Stream.Types.Kind.PRESENT)
                .SetColumn(1).SetLength(1000).Build());
            streams.Add(OrcProto.Stream.CreateBuilder()
                .SetKind(OrcProto.Stream.Types.Kind.DATA)
                .SetColumn(1).SetLength(94000).Build());
            streams.Add(OrcProto.Stream.CreateBuilder()
                .SetKind(OrcProto.Stream.Types.Kind.LENGTH)
                .SetColumn(1).SetLength(2000).Build());
            streams.Add(OrcProto.Stream.CreateBuilder()
                .SetKind(OrcProto.Stream.Types.Kind.DICTIONARY_DATA)
                .SetColumn(1).SetLength(3000).Build());
            streams.Add(OrcProto.Stream.CreateBuilder()
                .SetKind(OrcProto.Stream.Types.Kind.PRESENT)
                .SetColumn(2).SetLength(2000).Build());
            streams.Add(OrcProto.Stream.CreateBuilder()
                .SetKind(OrcProto.Stream.Types.Kind.DATA)
                .SetColumn(2).SetLength(98000).Build());

            bool[] columns = new bool[] { true, true, false };
            bool[] rowGroups = new bool[] { false, true, false, false, true, true };

            // set the index
            OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[columns.Length];
            indexes[1] = OrcProto.RowIndex.CreateBuilder()
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(0).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(0)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(100).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(10000)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(200).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(20000)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(300).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(30000)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(400).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(40000)
                    .Build())
                .AddEntry(OrcProto.RowIndexEntry.CreateBuilder()
                    .AddPositions(500).AddPositions(UInt64.MaxValue).AddPositions(UInt64.MaxValue)
                    .AddPositions(50000)
                    .Build())
                .Build();

            // set encodings
            List<OrcProto.ColumnEncoding> encodings = new List<OrcProto.ColumnEncoding>();
            encodings.Add(OrcProto.ColumnEncoding.CreateBuilder()
                .SetKind(OrcProto.ColumnEncoding.Types.Kind.DIRECT).Build());
            encodings.Add(OrcProto.ColumnEncoding.CreateBuilder()
                .SetKind(OrcProto.ColumnEncoding.Types.Kind.DICTIONARY).Build());
            encodings.Add(OrcProto.ColumnEncoding.CreateBuilder()
                .SetKind(OrcProto.ColumnEncoding.Types.Kind.DIRECT).Build());

            // set types struct{x: string, y: int}
            List<OrcProto.Type> types = new List<OrcProto.Type>();
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.STRUCT)
                .AddSubtypes(1).AddSubtypes(2).AddFieldNames("x")
                .AddFieldNames("y").Build());
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.STRING).Build());
            types.Add(OrcProto.Type.CreateBuilder().SetKind(OrcProto.Type.Types.Kind.INT).Build());

            // filter by rows and groups
            result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
                columns, rowGroups, false, encodings, types, 32768, false);
            Assert.Equal(
                diskRanges(100, 1000, 400, 1000, 500, 1000,
                    11000, 21000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
                    41000, 51000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
                    51000, 95000, 95000, 97000, 97000, 100000),
                result);
        }

        [Fact]
        public void testIntNullSafeEqualsBloomFilter()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addLong(i);
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createIntStats(10, 100));
            Assert.Equal(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addLong(15);
            Assert.Equal(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testIntEqualsBloomFilter()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addLong(i);
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createIntStats(10, 100));
            Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addLong(15);
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testIntInBloomFilter()
        {
            List<object> args = new List<object>();
            args.Add(15L);
            args.Add(19L);
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.IN, PredicateLeaf.Type.LONG,
                    "x", null, args);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addLong(i);
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createIntStats(10, 100));
            Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addLong(19);
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addLong(15);
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testDoubleNullSafeEqualsBloomFilter()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.FLOAT, "x", 15.0, null);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addDouble(i);
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDoubleStats(10.0, 100.0));
            Assert.Equal(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addDouble(15.0);
            Assert.Equal(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testDoubleEqualsBloomFilter()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.FLOAT, "x", 15.0, null);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addDouble(i);
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDoubleStats(10.0, 100.0));
            Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addDouble(15.0);
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testDoubleInBloomFilter()
        {
            List<object> args = new List<object>();
            args.Add(15.0);
            args.Add(19.0);
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.IN, PredicateLeaf.Type.FLOAT,
                    "x", null, args);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addDouble(i);
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDoubleStats(10.0, 100.0));
            Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addDouble(19.0);
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addDouble(15.0);
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testStringNullSafeEqualsBloomFilter()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.STRING, "x", "str_15", null);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addString("str_" + i);
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createStringStats("str_10", "str_200"));
            Assert.Equal(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addString("str_15");
            Assert.Equal(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testStringEqualsBloomFilter()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.STRING, "x", "str_15", null);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addString("str_" + i);
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createStringStats("str_10", "str_200"));
            Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addString("str_15");
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testStringInBloomFilter()
        {
            List<object> args = new List<object>();
            args.Add("str_15");
            args.Add("str_19");
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.IN, PredicateLeaf.Type.STRING,
                    "x", null, args);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addString("str_" + i);
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createStringStats("str_10", "str_200"));
            Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addString("str_19");
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addString("str_15");
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

#if false
        [Fact]
        public void testDateWritableNullSafeEqualsBloomFilter()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.DATE, "x",
                new DateWritable(15).get(), null);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addLong((new DateWritable(i)).getDays());
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDateStats(10, 100));
            Assert.Equal(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addLong((new DateWritable(15)).getDays());
            Assert.Equal(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testDateWritableEqualsBloomFilter()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.DATE, "x",
                new DateWritable(15).get(), null);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addLong((new DateWritable(i)).getDays());
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDateStats(10, 100));
            Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addLong((new DateWritable(15)).getDays());
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testDateWritableInBloomFilter()
        {
            List<object> args = new List<object>();
            args.Add(new DateWritable(15).get());
            args.Add(new DateWritable(19).get());
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DATE,
                    "x", null, args);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addLong((new DateWritable(i)).getDays());
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDateStats(10, 100));
            Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addLong((new DateWritable(19)).getDays());
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addLong((new DateWritable(15)).getDays());
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testTimestampNullSafeEqualsBloomFilter()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.TIMESTAMP, "x",
                new Timestamp(15),
                null);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addLong((new Timestamp(i)).getTime());
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createTimestampStats(10, 100));
            Assert.Equal(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addLong((new Timestamp(15)).getTime());
            Assert.Equal(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testTimestampEqualsBloomFilter()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15), null);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addLong((new Timestamp(i)).getTime());
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createTimestampStats(10, 100));
            Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addLong((new Timestamp(15)).getTime());
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testTimestampInBloomFilter()
        {
            List<object> args = new List<object>();
            args.Add(new Timestamp(15));
            args.Add(new Timestamp(19));
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.IN, PredicateLeaf.Type.TIMESTAMP,
                    "x", null, args);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addLong((new Timestamp(i)).getTime());
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createTimestampStats(10, 100));
            Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addLong((new Timestamp(19)).getTime());
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addLong((new Timestamp(15)).getTime());
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testDecimalNullSafeEqualsBloomFilter()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.DECIMAL, "x",
                new HiveDecimalWritable("15"),
                null);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addString(HiveDecimal.create(i).ToString());
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200"));
            Assert.Equal(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addString(HiveDecimal.create(15).ToString());
            Assert.Equal(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testDecimalEqualsBloomFilter()
        {
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
                PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.DECIMAL, "x",
                new HiveDecimalWritable("15"),
                null);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addString(HiveDecimal.create(i).ToString());
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200"));
            Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addString(HiveDecimal.create(15).ToString());
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testDecimalInBloomFilter()
        {
            List<object> args = new List<object>();
            args.Add(new HiveDecimalWritable("15"));
            args.Add(new HiveDecimalWritable("19"));
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DECIMAL,
                    "x", null, args);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addString(HiveDecimal.create(i).ToString());
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200"));
            Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addString(HiveDecimal.create(19).ToString());
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addString(HiveDecimal.create(15).ToString());
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }

        [Fact]
        public void testNullsInBloomFilter()
        {
            List<object> args = new List<object>();
            args.Add(new HiveDecimalWritable("15"));
            args.Add(null);
            args.Add(new HiveDecimalWritable("19"));
            PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
                (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DECIMAL,
                    "x", null, args);
            BloomFilter bf = new BloomFilter(10000);
            for (int i = 20; i < 1000; i++)
            {
                bf.addString(HiveDecimal.create(i).ToString());
            }
            ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200", false));
            // hasNull is false, so bloom filter should return NO
            Assert.Equal(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200", true));
            // hasNull is true, so bloom filter should return YES_NO_NULL
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addString(HiveDecimal.create(19).ToString());
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

            bf.addString(HiveDecimal.create(15).ToString());
            Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
        }
#endif
    }
}
