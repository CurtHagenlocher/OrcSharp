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

namespace org.apache.hadoop.hive.ql.io.orc {

import static junit.framework.Assert.assertEquals;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.filters.BloomFilterIO;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl.Location;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.io.sarg.TestSearchArgumentImpl;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;
import org.mockito.MockSettings;
import org.mockito.Mockito;

public class TestRecordReaderImpl {

  // can add .verboseLogging() to cause Mockito to log invocations
  private final MockSettings settings = Mockito.withSettings().verboseLogging();

  static class BufferInStream
      extends InputStream implements PositionedReadable, Seekable {
    private final byte[] buffer;
    private final int length;
    private int position = 0;

    BufferInStream(byte[] bytes, int length) {
      this.buffer = bytes;
      this.length = length;
    }

    @Override
    public int read() {
      if (position < length) {
        return buffer[position++];
      }
      return -1;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
      int lengthToRead = Math.min(length, this.length - this.position);
      if (lengthToRead >= 0) {
        for(int i=0; i < lengthToRead; ++i) {
          bytes[offset + i] = buffer[position++];
        }
        return lengthToRead;
      } else {
        return -1;
      }
    }

    @Override
    public int read(long position, byte[] bytes, int offset, int length) {
      this.position = (int) position;
      return read(bytes, offset, length);
    }

    @Override
    public void readFully(long position, byte[] bytes, int offset,
                          int length) {
      this.position = (int) position;
      while (length > 0) {
        int result = read(bytes, offset, length);
        offset += result;
        length -= result;
        if (result < 0) {
          throw new IOException("Read past end of buffer at " + offset);
        }
      }
    }

    @Override
    public void readFully(long position, byte[] bytes) {
      readFully(position, bytes, 0, bytes.length);
    }

    @Override
    public void seek(long position) {
      this.position = (int) position;
    }

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public bool seekToNewSource(long position) {
      this.position = (int) position;
      return false;
    }
  }

  [Fact]
  public void testMaxLengthToReader()  {
    Configuration conf = new Configuration();
    OrcProto.Type rowType = OrcProto.Type.newBuilder()
        .setKind(OrcProto.Type.Kind.STRUCT).build();
    OrcProto.Footer footer = OrcProto.Footer.newBuilder()
        .setHeaderLength(0).setContentLength(0).setNumberOfRows(0)
        .setRowIndexStride(0).addTypes(rowType).build();
    OrcProto.PostScript ps = OrcProto.PostScript.newBuilder()
        .setCompression(OrcProto.CompressionKind.NONE)
        .setFooterLength(footer.getSerializedSize())
        .setMagic("ORC").addVersion(0).addVersion(11).build();
    DataOutputBuffer buffer = new DataOutputBuffer();
    footer.writeTo(buffer);
    ps.writeTo(buffer);
    buffer.write(ps.getSerializedSize());
    FileSystem fs = Mockito.mock(FileSystem.class, settings);
    FSDataInputStream file =
        new FSDataInputStream(new BufferInStream(buffer.getData(),
            buffer.getLength()));
    Path p = new Path("/dir/file.orc");
    Mockito.when(fs.open(p)).thenReturn(file);
    OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
    options.filesystem(fs);
    options.maxLength(buffer.getLength());
    Mockito.when(fs.getFileStatus(p))
        .thenReturn(new FileStatus(10, false, 3, 3000, 0, p));
    Reader reader = OrcFile.createReader(p, options);
  }

  [Fact]
  public void testCompareToRangeInt()  {
    Assert.Equal(Location.BEFORE,
        RecordReaderImpl.compareToRange(19L, 20L, 40L));
    Assert.Equal(Location.AFTER,
        RecordReaderImpl.compareToRange(41L, 20L, 40L));
    Assert.Equal(Location.MIN,
        RecordReaderImpl.compareToRange(20L, 20L, 40L));
    Assert.Equal(Location.MIDDLE,
        RecordReaderImpl.compareToRange(21L, 20L, 40L));
    Assert.Equal(Location.MAX,
        RecordReaderImpl.compareToRange(40L, 20L, 40L));
    Assert.Equal(Location.BEFORE,
        RecordReaderImpl.compareToRange(0L, 1L, 1L));
    Assert.Equal(Location.MIN,
        RecordReaderImpl.compareToRange(1L, 1L, 1L));
    Assert.Equal(Location.AFTER,
        RecordReaderImpl.compareToRange(2L, 1L, 1L));
  }

  [Fact]
  public void testCompareToRangeString()  {
    Assert.Equal(Location.BEFORE,
        RecordReaderImpl.compareToRange("a", "b", "c"));
    Assert.Equal(Location.AFTER,
        RecordReaderImpl.compareToRange("d", "b", "c"));
    Assert.Equal(Location.MIN,
        RecordReaderImpl.compareToRange("b", "b", "c"));
    Assert.Equal(Location.MIDDLE,
        RecordReaderImpl.compareToRange("bb", "b", "c"));
    Assert.Equal(Location.MAX,
        RecordReaderImpl.compareToRange("c", "b", "c"));
    Assert.Equal(Location.BEFORE,
        RecordReaderImpl.compareToRange("a", "b", "b"));
    Assert.Equal(Location.MIN,
        RecordReaderImpl.compareToRange("b", "b", "b"));
    Assert.Equal(Location.AFTER,
        RecordReaderImpl.compareToRange("c", "b", "b"));
  }

  [Fact]
  public void testCompareToCharNeedConvert()  {
    Assert.Equal(Location.BEFORE,
        RecordReaderImpl.compareToRange("apple", "hello", "world"));
    Assert.Equal(Location.AFTER,
        RecordReaderImpl.compareToRange("zombie", "hello", "world"));
    Assert.Equal(Location.MIN,
        RecordReaderImpl.compareToRange("hello", "hello", "world"));
    Assert.Equal(Location.MIDDLE,
        RecordReaderImpl.compareToRange("pilot", "hello", "world"));
    Assert.Equal(Location.MAX,
        RecordReaderImpl.compareToRange("world", "hello", "world"));
    Assert.Equal(Location.BEFORE,
        RecordReaderImpl.compareToRange("apple", "hello", "hello"));
    Assert.Equal(Location.MIN,
        RecordReaderImpl.compareToRange("hello", "hello", "hello"));
    Assert.Equal(Location.AFTER,
        RecordReaderImpl.compareToRange("zombie", "hello", "hello"));
  }

  [Fact]
  public void testGetMin()  {
    Assert.Equal(10L, RecordReaderImpl.getMin(
        ColumnStatisticsImpl.deserialize(createIntStats(10L, 100L))));
    Assert.Equal(10.0d, RecordReaderImpl.getMin(ColumnStatisticsImpl.deserialize(
        OrcProto.ColumnStatistics.newBuilder()
            .setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder()
                .setMinimum(10.0d).setMaximum(100.0d).build()).build())));
    Assert.Equal(null, RecordReaderImpl.getMin(ColumnStatisticsImpl.deserialize(
        OrcProto.ColumnStatistics.newBuilder()
            .setStringStatistics(OrcProto.StringStatistics.newBuilder().build())
            .build())));
    Assert.Equal("a", RecordReaderImpl.getMin(ColumnStatisticsImpl.deserialize(
        OrcProto.ColumnStatistics.newBuilder()
            .setStringStatistics(OrcProto.StringStatistics.newBuilder()
                .setMinimum("a").setMaximum("b").build()).build())));
    Assert.Equal("hello", RecordReaderImpl.getMin(ColumnStatisticsImpl
        .deserialize(createStringStats("hello", "world"))));
    Assert.Equal(HiveDecimal.create("111.1"), RecordReaderImpl.getMin(ColumnStatisticsImpl
        .deserialize(createDecimalStats("111.1", "112.1"))));
  }

  private static OrcProto.ColumnStatistics createIntStats(Long min,
                                                          Long max) {
    OrcProto.IntegerStatistics.Builder intStats =
        OrcProto.IntegerStatistics.newBuilder();
    if (min != null) {
      intStats.setMinimum(min);
    }
    if (max != null) {
      intStats.setMaximum(max);
    }
    return OrcProto.ColumnStatistics.newBuilder()
        .setIntStatistics(intStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createBooleanStats(int n, int trueCount) {
    OrcProto.BucketStatistics.Builder boolStats = OrcProto.BucketStatistics.newBuilder();
    boolStats.addCount(trueCount);
    return OrcProto.ColumnStatistics.newBuilder().setNumberOfValues(n).setBucketStatistics(
        boolStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createIntStats(int min, int max) {
    OrcProto.IntegerStatistics.Builder intStats = OrcProto.IntegerStatistics.newBuilder();
    intStats.setMinimum(min);
    intStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setIntStatistics(intStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createDoubleStats(double min, double max) {
    OrcProto.DoubleStatistics.Builder dblStats = OrcProto.DoubleStatistics.newBuilder();
    dblStats.setMinimum(min);
    dblStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setDoubleStatistics(dblStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createStringStats(String min, String max,
      bool hasNull) {
    OrcProto.StringStatistics.Builder strStats = OrcProto.StringStatistics.newBuilder();
    strStats.setMinimum(min);
    strStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setStringStatistics(strStats.build())
        .setHasNull(hasNull).build();
  }

  private static OrcProto.ColumnStatistics createStringStats(String min, String max) {
    OrcProto.StringStatistics.Builder strStats = OrcProto.StringStatistics.newBuilder();
    strStats.setMinimum(min);
    strStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setStringStatistics(strStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createDateStats(int min, int max) {
    OrcProto.DateStatistics.Builder dateStats = OrcProto.DateStatistics.newBuilder();
    dateStats.setMinimum(min);
    dateStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setDateStatistics(dateStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createTimestampStats(long min, long max) {
    OrcProto.TimestampStatistics.Builder tsStats = OrcProto.TimestampStatistics.newBuilder();
    tsStats.setMinimum(min);
    tsStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setTimestampStatistics(tsStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createDecimalStats(String min, String max) {
    OrcProto.DecimalStatistics.Builder decStats = OrcProto.DecimalStatistics.newBuilder();
    decStats.setMinimum(min);
    decStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setDecimalStatistics(decStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createDecimalStats(String min, String max,
      bool hasNull) {
    OrcProto.DecimalStatistics.Builder decStats = OrcProto.DecimalStatistics.newBuilder();
    decStats.setMinimum(min);
    decStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setDecimalStatistics(decStats.build())
        .setHasNull(hasNull).build();
  }

  [Fact]
  public void testGetMax()  {
    Assert.Equal(100L, RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(createIntStats(10L, 100L))));
    Assert.Equal(100.0d, RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(
        OrcProto.ColumnStatistics.newBuilder()
            .setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder()
                .setMinimum(10.0d).setMaximum(100.0d).build()).build())));
    Assert.Equal(null, RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(
        OrcProto.ColumnStatistics.newBuilder()
            .setStringStatistics(OrcProto.StringStatistics.newBuilder().build())
            .build())));
    Assert.Equal("b", RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(
        OrcProto.ColumnStatistics.newBuilder()
            .setStringStatistics(OrcProto.StringStatistics.newBuilder()
                .setMinimum("a").setMaximum("b").build()).build())));
    Assert.Equal("world", RecordReaderImpl.getMax(ColumnStatisticsImpl
        .deserialize(createStringStats("hello", "world"))));
    Assert.Equal(HiveDecimal.create("112.1"), RecordReaderImpl.getMax(ColumnStatisticsImpl
        .deserialize(createDecimalStats("111.1", "112.1"))));
  }

  [Fact]
  public void testPredEvalWithBooleanStats()  {
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

  [Fact]
  public void testPredEvalWithIntStats()  {
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
  public void testPredEvalWithDoubleStats()  {
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
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15*1000L), null);
    Assert.Equal(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));

    pred = TestSearchArgumentImpl.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(150*1000L), null);
    Assert.Equal(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createDoubleStats(10.0, 100.0), pred, null));
  }

  [Fact]
  public void testPredEvalWithStringStats()  {
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
  public void testPredEvalWithDateStats()  {
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
  public void testPredEvalWithDecimalStats()  {
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
  public void testPredEvalWithTimestampStats()  {
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
        PredicateLeaf.Type.STRING, "x", new Timestamp(15).toString(), null);
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

  [Fact]
  public void testEquals()  {
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
  public void testNullSafeEquals()  {
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
  public void testLessThan()  {
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
  public void testLessThanEquals()  {
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
  public void testIn()  {
    List<Object> args = new ArrayList<Object>();
    args.add(10L);
    args.add(20L);
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
  public void testBetween()  {
    List<Object> args = new ArrayList<Object>();
    args.add(10L);
    args.add(20L);
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
  public void testIsNull()  {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.LONG,
            "x", null, null);
    Assert.Equal(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createIntStats(20L, 30L), pred, null));
  }


  [Fact]
  public void testEqualsWithNullInStats()  {
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
  public void testNullSafeEqualsWithNullInStats()  {
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
  public void testLessThanWithNullInStats()  {
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
  public void testLessThanEqualsWithNullInStats()  {
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
  public void testInWithNullInStats()  {
    List<Object> args = new ArrayList<Object>();
    args.add("c");
    args.add("f");
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
  public void testBetweenWithNullInStats()  {
    List<Object> args = new ArrayList<Object>();
    args.add("c");
    args.add("f");
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
  public void testIsNullWithNullInStats()  {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.STRING,
            "x", null, null);
    Assert.Equal(TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", true), pred, null));
    Assert.Equal(TruthValue.NO,
        RecordReaderImpl.evaluatePredicateProto(createStringStats("c", "d", false), pred, null));
  }

  [Fact]
  public void testOverlap()  {
    assertTrue(!RecordReaderUtils.overlap(0, 10, -10, -1));
    assertTrue(RecordReaderUtils.overlap(0, 10, -1, 0));
    assertTrue(RecordReaderUtils.overlap(0, 10, -1, 1));
    assertTrue(RecordReaderUtils.overlap(0, 10, 2, 8));
    assertTrue(RecordReaderUtils.overlap(0, 10, 5, 10));
    assertTrue(RecordReaderUtils.overlap(0, 10, 10, 11));
    assertTrue(RecordReaderUtils.overlap(0, 10, 0, 10));
    assertTrue(RecordReaderUtils.overlap(0, 10, -1, 11));
    assertTrue(!RecordReaderUtils.overlap(0, 10, 11, 12));
  }

  private static DiskRangeList diskRanges(Integer... points) {
    DiskRangeList head = null, tail = null;
    for(int i = 0; i < points.length; i += 2) {
      DiskRangeList range = new DiskRangeList(points[i], points[i+1]);
      if (tail == null) {
        head = tail = range;
      } else {
        tail = tail.insertAfter(range);
      }
    }
    return head;
  }

  [Fact]
  public void testGetIndexPosition()  {
    Assert.Equal(0, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.PRESENT, true, true));
    Assert.Equal(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.DATA, true, true));
    Assert.Equal(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.DATA, false, true));
    Assert.Equal(0, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.DATA, true, false));
    Assert.Equal(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DICTIONARY, OrcProto.Type.Kind.STRING,
            OrcProto.Stream.Kind.DATA, true, true));
    Assert.Equal(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.DATA, true, true));
    Assert.Equal(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.DATA, false, true));
    Assert.Equal(6, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.LENGTH, true, true));
    Assert.Equal(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.LENGTH, false, true));
    Assert.Equal(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.DATA, true, true));
    Assert.Equal(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.DATA, false, true));
    Assert.Equal(6, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.SECONDARY, true, true));
    Assert.Equal(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.SECONDARY, false, true));
    Assert.Equal(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.DATA, true, true));
    Assert.Equal(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.DATA, false, true));
    Assert.Equal(7, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.SECONDARY, true, true));
    Assert.Equal(5, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.SECONDARY, false, true));
  }

  [Fact]
  public void testPartialPlan()  {
    DiskRangeList result;

    // set the streams
    List<OrcProto.Stream> streams = new ArrayList<OrcProto.Stream>();
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(1).setLength(1000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(1).setLength(99000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(2).setLength(2000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(2).setLength(98000).build());

    boolean[] columns = new boolean[]{true, true, false};
    boolean[] rowGroups = new boolean[]{true, true, false, false, true, false};

    // set the index
    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[columns.length];
    indexes[1] = OrcProto.RowIndex.newBuilder()
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(0).addPositions(-1).addPositions(-1)
            .addPositions(0)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(100).addPositions(-1).addPositions(-1)
            .addPositions(10000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(200).addPositions(-1).addPositions(-1)
            .addPositions(20000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(300).addPositions(-1).addPositions(-1)
            .addPositions(30000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(400).addPositions(-1).addPositions(-1)
            .addPositions(40000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(500).addPositions(-1).addPositions(-1)
            .addPositions(50000)
            .build())
        .build();

    // set encodings
    List<OrcProto.ColumnEncoding> encodings =
        new ArrayList<OrcProto.ColumnEncoding>();
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
                    .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());

    // set types struct{x: int, y: int}
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT)
                .addSubtypes(1).addSubtypes(2).addFieldNames("x")
                .addFieldNames("y").build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());

    // filter by rows and groups
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, false);
    assertThat(result, is(diskRanges(0, 1000, 100, 1000, 400, 1000,
        1000, 11000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        11000, 21000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        41000, 51000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP)));
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(0, 21000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        41000, 51000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP)));

    // if we read no rows, don't read any bytes
    rowGroups = new boolean[]{false, false, false, false, false, false};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, false);
    assertNull(result);

    // all rows, but only columns 0 and 2.
    rowGroups = null;
    columns = new boolean[]{true, false, true};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, null, false, encodings, types, 32768, false);
    assertThat(result, is(diskRanges(100000, 102000, 102000, 200000)));
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, null, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(100000, 200000)));

    rowGroups = new boolean[]{false, true, false, false, false, false};
    indexes[2] = indexes[1];
    indexes[1] = null;
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, false);
    assertThat(result, is(diskRanges(100100, 102000,
        112000, 122000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP)));
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(100100, 102000,
        112000, 122000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP)));

    rowGroups = new boolean[]{false, false, false, false, false, true};
    indexes[1] = indexes[2];
    columns = new boolean[]{true, true, true};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, false);
    assertThat(result, is(diskRanges(500, 1000, 51000, 100000, 100500, 102000,
        152000, 200000)));
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(500, 1000, 51000, 100000, 100500, 102000,
        152000, 200000)));
  }


  [Fact]
  public void testPartialPlanCompressed()  {
    DiskRangeList result;

    // set the streams
    List<OrcProto.Stream> streams = new ArrayList<OrcProto.Stream>();
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(1).setLength(1000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(1).setLength(99000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(2).setLength(2000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(2).setLength(98000).build());

    boolean[] columns = new boolean[]{true, true, false};
    boolean[] rowGroups = new boolean[]{true, true, false, false, true, false};

    // set the index
    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[columns.length];
    indexes[1] = OrcProto.RowIndex.newBuilder()
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(0).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(0)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(100).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(10000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(200).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(20000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(300).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(30000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(400).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(40000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(500).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(50000)
            .build())
        .build();

    // set encodings
    List<OrcProto.ColumnEncoding> encodings =
        new ArrayList<OrcProto.ColumnEncoding>();
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());

    // set types struct{x: int, y: int}
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT)
        .addSubtypes(1).addSubtypes(2).addFieldNames("x")
        .addFieldNames("y").build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());

    // filter by rows and groups
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, true, encodings, types, 32768, false);
    assertThat(result, is(diskRanges(0, 1000, 100, 1000,
        400, 1000, 1000, 11000+(2*32771),
        11000, 21000+(2*32771), 41000, 100000)));

    rowGroups = new boolean[]{false, false, false, false, false, true};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, true, encodings, types, 32768, false);
    assertThat(result, is(diskRanges(500, 1000, 51000, 100000)));
  }

  [Fact]
  public void testPartialPlanString()  {
    DiskRangeList result;

    // set the streams
    List<OrcProto.Stream> streams = new ArrayList<OrcProto.Stream>();
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(1).setLength(1000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(1).setLength(94000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.LENGTH)
        .setColumn(1).setLength(2000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DICTIONARY_DATA)
        .setColumn(1).setLength(3000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(2).setLength(2000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(2).setLength(98000).build());

    boolean[] columns = new boolean[]{true, true, false};
    boolean[] rowGroups = new boolean[]{false, true, false, false, true, true};

    // set the index
    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[columns.length];
    indexes[1] = OrcProto.RowIndex.newBuilder()
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(0).addPositions(-1).addPositions(-1)
            .addPositions(0)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(100).addPositions(-1).addPositions(-1)
            .addPositions(10000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(200).addPositions(-1).addPositions(-1)
            .addPositions(20000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(300).addPositions(-1).addPositions(-1)
            .addPositions(30000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(400).addPositions(-1).addPositions(-1)
            .addPositions(40000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(500).addPositions(-1).addPositions(-1)
            .addPositions(50000)
            .build())
        .build();

    // set encodings
    List<OrcProto.ColumnEncoding> encodings =
        new ArrayList<OrcProto.ColumnEncoding>();
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DICTIONARY).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());

    // set types struct{x: string, y: int}
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT)
        .addSubtypes(1).addSubtypes(2).addFieldNames("x")
        .addFieldNames("y").build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRING).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());

    // filter by rows and groups
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, false);
    assertThat(result, is(diskRanges(100, 1000, 400, 1000, 500, 1000,
        11000, 21000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        41000, 51000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        51000, 95000, 95000, 97000, 97000, 100000)));
  }

  [Fact]
  public void testIntNullSafeEqualsBloomFilter()  {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createIntStats(10, 100));
    Assert.Equal(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(15);
    Assert.Equal(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  [Fact]
  public void testIntEqualsBloomFilter()  {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createIntStats(10, 100));
    Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(15);
    Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  [Fact]
  public void testIntInBloomFilter()  {
    List<Object> args = new ArrayList<Object>();
    args.add(15L);
    args.add(19L);
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.LONG,
            "x", null, args);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
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
  public void testDoubleNullSafeEqualsBloomFilter()  {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addDouble(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDoubleStats(10.0, 100.0));
    Assert.Equal(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addDouble(15.0);
    Assert.Equal(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  [Fact]
  public void testDoubleEqualsBloomFilter()  {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addDouble(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDoubleStats(10.0, 100.0));
    Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addDouble(15.0);
    Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  [Fact]
  public void testDoubleInBloomFilter()  {
    List<Object> args = new ArrayList<Object>();
    args.add(15.0);
    args.add(19.0);
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.FLOAT,
            "x", null, args);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
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
  public void testStringNullSafeEqualsBloomFilter()  {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.STRING, "x", "str_15", null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString("str_" + i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createStringStats("str_10", "str_200"));
    Assert.Equal(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_15");
    Assert.Equal(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  [Fact]
  public void testStringEqualsBloomFilter()  {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.STRING, "x", "str_15", null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString("str_" + i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createStringStats("str_10", "str_200"));
    Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_15");
    Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  [Fact]
  public void testStringInBloomFilter()  {
    List<Object> args = new ArrayList<Object>();
    args.add("str_15");
    args.add("str_19");
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.STRING,
            "x", null, args);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString("str_" + i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createStringStats("str_10", "str_200"));
    Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_19");
    Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_15");
    Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  [Fact]
  public void testDateWritableNullSafeEqualsBloomFilter()  {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.DATE, "x",
        new DateWritable(15).get(), null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong((new DateWritable(i)).getDays());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDateStats(10, 100));
    Assert.Equal(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new DateWritable(15)).getDays());
    Assert.Equal(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  [Fact]
  public void testDateWritableEqualsBloomFilter()  {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.DATE, "x",
        new DateWritable(15).get(), null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong((new DateWritable(i)).getDays());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDateStats(10, 100));
    Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new DateWritable(15)).getDays());
    Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  [Fact]
  public void testDateWritableInBloomFilter()  {
    List<Object> args = new ArrayList<Object>();
    args.add(new DateWritable(15).get());
    args.add(new DateWritable(19).get());
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DATE,
            "x", null, args);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
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
  public void testTimestampNullSafeEqualsBloomFilter()  {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.TIMESTAMP, "x",
        new Timestamp(15),
        null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong((new Timestamp(i)).getTime());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createTimestampStats(10, 100));
    Assert.Equal(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new Timestamp(15)).getTime());
    Assert.Equal(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  [Fact]
  public void testTimestampEqualsBloomFilter()  {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15), null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong((new Timestamp(i)).getTime());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createTimestampStats(10, 100));
    Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new Timestamp(15)).getTime());
    Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  [Fact]
  public void testTimestampInBloomFilter()  {
    List<Object> args = new ArrayList<Object>();
    args.add(new Timestamp(15));
    args.add(new Timestamp(19));
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.TIMESTAMP,
            "x", null, args);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
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
  public void testDecimalNullSafeEqualsBloomFilter()  {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.DECIMAL, "x",
        new HiveDecimalWritable("15"),
        null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString(HiveDecimal.create(i).toString());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200"));
    Assert.Equal(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(15).toString());
    Assert.Equal(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  [Fact]
  public void testDecimalEqualsBloomFilter()  {
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.DECIMAL, "x",
        new HiveDecimalWritable("15"),
        null);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString(HiveDecimal.create(i).toString());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200"));
    Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(15).toString());
    Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  [Fact]
  public void testDecimalInBloomFilter()  {
    List<Object> args = new ArrayList<Object>();
    args.add(new HiveDecimalWritable("15"));
    args.add(new HiveDecimalWritable("19"));
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DECIMAL,
            "x", null, args);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString(HiveDecimal.create(i).toString());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200"));
    Assert.Equal(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(19).toString());
    Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(15).toString());
    Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  [Fact]
  public void testNullsInBloomFilter()  {
    List<Object> args = new ArrayList<Object>();
    args.add(new HiveDecimalWritable("15"));
    args.add(null);
    args.add(new HiveDecimalWritable("19"));
    PredicateLeaf pred = TestSearchArgumentImpl.createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DECIMAL,
            "x", null, args);
    BloomFilterIO bf = new BloomFilterIO(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString(HiveDecimal.create(i).toString());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200", false));
    // hasNull is false, so bloom filter should return NO
    Assert.Equal(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    cs = ColumnStatisticsImpl.deserialize(createDecimalStats("10", "200", true));
    // hasNull is true, so bloom filter should return YES_NO_NULL
    Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(19).toString());
    Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(15).toString());
    Assert.Equal(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }
}
