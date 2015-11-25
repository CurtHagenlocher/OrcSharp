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
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.Version;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.HiveTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

/**
 * Tests for the top level reader/streamFactory of ORC files.
 */
@RunWith(value = Parameterized.class)
public class TestOrcFile {

  public static class SimpleStruct {
    BytesWritable bytes1;
    Text string1;

    SimpleStruct(BytesWritable b1, String s1) {
      this.bytes1 = b1;
      if(s1 == null) {
        this.string1 = null;
      } else {
        this.string1 = new Text(s1);
      }
    }
  }

  public static class InnerStruct {
    int int1;
    Text string1 = new Text();
    InnerStruct(int int1, String string1) {
      this.int1 = int1;
      this.string1.set(string1);
    }
  }

  public static class MiddleStruct {
    List<InnerStruct> list = new ArrayList<InnerStruct>();

    MiddleStruct(InnerStruct... items) {
      list.clear();
      list.addAll(Arrays.asList(items));
    }
  }

  public static class BigRow {
    bool boolean1;
    Byte byte1;
    Short short1;
    Integer int1;
    Long long1;
    Float float1;
    Double double1;
    BytesWritable bytes1;
    Text string1;
    MiddleStruct middle;
    List<InnerStruct> list = new ArrayList<InnerStruct>();
    Map<Text, InnerStruct> map = new HashMap<Text, InnerStruct>();

    BigRow(Boolean b1, Byte b2, Short s1, Integer i1, Long l1, Float f1,
           Double d1,
           BytesWritable b3, String s2, MiddleStruct m1,
           List<InnerStruct> l2, Map<Text, InnerStruct> m2) {
      this.boolean1 = b1;
      this.byte1 = b2;
      this.short1 = s1;
      this.int1 = i1;
      this.long1 = l1;
      this.float1 = f1;
      this.double1 = d1;
      this.bytes1 = b3;
      if (s2 == null) {
        this.string1 = null;
      } else {
        this.string1 = new Text(s2);
      }
      this.middle = m1;
      this.list = l2;
      this.map = m2;
    }
  }

  private static InnerStruct inner(int i, String s) {
    return new InnerStruct(i, s);
  }

  private static Map<Text, InnerStruct> map(InnerStruct... items)  {
    Map<Text, InnerStruct> result = new HashMap<Text, InnerStruct>();
    for(InnerStruct i: items) {
      result.put(new Text(i.string1), i);
    }
    return result;
  }

  private static List<InnerStruct> list(InnerStruct... items) {
    List<InnerStruct> result = new ArrayList<InnerStruct>();
    result.addAll(Arrays.asList(items));
    return result;
  }

  private static BytesWritable bytes(int... items) {
    BytesWritable result = new BytesWritable();
    result.setSize(items.length);
    for(int i=0; i < items.length; ++i) {
      result.getBytes()[i] = (byte) items[i];
    }
    return result;
  }

  private static ByteBuffer byteBuf(int... items) {
    ByteBuffer result = ByteBuffer.allocate(items.length);
    for(int item: items) {
      result.put((byte) item);
    }
    result.flip();
    return result;
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  private final bool zeroCopy;

  @Parameters
  public static Collection<Boolean[]> data() {
    return Arrays.asList(new Boolean[][] { {false}, {true}});
  }

  public TestOrcFile(Boolean zcr) {
    zeroCopy = zcr.booleanValue();
  }

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem ()  {
    conf = new Configuration();
    if(zeroCopy) {
      conf.setBoolean(HiveConf.ConfVars.HIVE_ORC_ZEROCOPY.varname, zeroCopy);
    }
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  [Fact]
  public void testReadFormat_0_11()  {
    Path oldFilePath =
        new Path(HiveTestUtils.getFileFromClasspath("orc-file-11-format.orc"));
    Reader reader = OrcFile.createReader(oldFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    int stripeCount = 0;
    int rowCount = 0;
    long currentOffset = -1;
    for(StripeInformation stripe : reader.getStripes()) {
      stripeCount += 1;
      rowCount += stripe.getNumberOfRows();
      if (currentOffset < 0) {
        currentOffset = stripe.getOffset() + stripe.getIndexLength()
            + stripe.getDataLength() + stripe.getFooterLength();
      } else {
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
    Assert.Equal(3750, ((BooleanColumnStatistics) stats[1]).getFalseCount());
    Assert.Equal(3750, ((BooleanColumnStatistics) stats[1]).getTrueCount());
    Assert.Equal("count: 7500 hasNull: true true: 3750", stats[1].toString());

    Assert.Equal(2048, ((IntegerColumnStatistics) stats[3]).getMaximum());
    Assert.Equal(1024, ((IntegerColumnStatistics) stats[3]).getMinimum());
    Assert.Equal(true, ((IntegerColumnStatistics) stats[3]).isSumDefined());
    Assert.Equal(11520000, ((IntegerColumnStatistics) stats[3]).getSum());
    Assert.Equal("count: 7500 hasNull: true min: 1024 max: 2048 sum: 11520000",
        stats[3].toString());

    Assert.Equal(Long.MAX_VALUE,
        ((IntegerColumnStatistics) stats[5]).getMaximum());
    Assert.Equal(Long.MAX_VALUE,
        ((IntegerColumnStatistics) stats[5]).getMinimum());
    Assert.Equal(false, ((IntegerColumnStatistics) stats[5]).isSumDefined());
    Assert.Equal(
        "count: 7500 hasNull: true min: 9223372036854775807 max: 9223372036854775807",
        stats[5].toString());

    Assert.Equal(-15.0, ((DoubleColumnStatistics) stats[7]).getMinimum());
    Assert.Equal(-5.0, ((DoubleColumnStatistics) stats[7]).getMaximum());
    Assert.Equal(-75000.0, ((DoubleColumnStatistics) stats[7]).getSum(),
        0.00001);
    Assert.Equal("count: 7500 hasNull: true min: -15.0 max: -5.0 sum: -75000.0",
        stats[7].toString());

    Assert.Equal("count: 7500 hasNull: true min: bye max: hi sum: 0", stats[9].toString());

    // check the inspectors
    StructObjectInspector readerInspector = (StructObjectInspector) reader
        .getObjectInspector();
    Assert.Equal(ObjectInspector.Category.STRUCT, readerInspector.getCategory());
    Assert.Equal("struct<boolean1:boolean,byte1:tinyint,short1:smallint,"
        + "int1:int,long1:bigint,float1:float,double1:double,bytes1:"
        + "binary,string1:string,middle:struct<list:array<struct<int1:int,"
        + "string1:string>>>,list:array<struct<int1:int,string1:string>>,"
        + "map:map<string,struct<int1:int,string1:string>>,ts:timestamp,"
        + "decimal1:decimal(38,18)>", readerInspector.getTypeName());
    List<? extends StructField> fields = readerInspector
        .getAllStructFieldRefs();
    BooleanObjectInspector bo = (BooleanObjectInspector) readerInspector
        .getStructFieldRef("boolean1").getFieldObjectInspector();
    ByteObjectInspector by = (ByteObjectInspector) readerInspector
        .getStructFieldRef("byte1").getFieldObjectInspector();
    ShortObjectInspector sh = (ShortObjectInspector) readerInspector
        .getStructFieldRef("short1").getFieldObjectInspector();
    IntObjectInspector in = (IntObjectInspector) readerInspector
        .getStructFieldRef("int1").getFieldObjectInspector();
    LongObjectInspector lo = (LongObjectInspector) readerInspector
        .getStructFieldRef("long1").getFieldObjectInspector();
    FloatObjectInspector fl = (FloatObjectInspector) readerInspector
        .getStructFieldRef("float1").getFieldObjectInspector();
    DoubleObjectInspector dbl = (DoubleObjectInspector) readerInspector
        .getStructFieldRef("double1").getFieldObjectInspector();
    BinaryObjectInspector bi = (BinaryObjectInspector) readerInspector
        .getStructFieldRef("bytes1").getFieldObjectInspector();
    StringObjectInspector st = (StringObjectInspector) readerInspector
        .getStructFieldRef("string1").getFieldObjectInspector();
    StructObjectInspector mid = (StructObjectInspector) readerInspector
        .getStructFieldRef("middle").getFieldObjectInspector();
    List<? extends StructField> midFields = mid.getAllStructFieldRefs();
    ListObjectInspector midli = (ListObjectInspector) midFields.get(0)
        .getFieldObjectInspector();
    StructObjectInspector inner = (StructObjectInspector) midli
        .getListElementObjectInspector();
    List<? extends StructField> inFields = inner.getAllStructFieldRefs();
    ListObjectInspector li = (ListObjectInspector) readerInspector
        .getStructFieldRef("list").getFieldObjectInspector();
    MapObjectInspector ma = (MapObjectInspector) readerInspector
        .getStructFieldRef("map").getFieldObjectInspector();
    TimestampObjectInspector tso = (TimestampObjectInspector) readerInspector
        .getStructFieldRef("ts").getFieldObjectInspector();
    HiveDecimalObjectInspector dco = (HiveDecimalObjectInspector) readerInspector
        .getStructFieldRef("decimal1").getFieldObjectInspector();
    StringObjectInspector mk = (StringObjectInspector) ma
        .getMapKeyObjectInspector();
    RecordReader rows = reader.rows();
    Object row = rows.next(null);
    assertNotNull(row);
    // check the contents of the first row
    Assert.Equal(false,
        bo.get(readerInspector.getStructFieldData(row, fields.get(0))));
    Assert.Equal(1,
        by.get(readerInspector.getStructFieldData(row, fields.get(1))));
    Assert.Equal(1024,
        sh.get(readerInspector.getStructFieldData(row, fields.get(2))));
    Assert.Equal(65536,
        in.get(readerInspector.getStructFieldData(row, fields.get(3))));
    Assert.Equal(Long.MAX_VALUE,
        lo.get(readerInspector.getStructFieldData(row, fields.get(4))));
    Assert.Equal(1.0,
        fl.get(readerInspector.getStructFieldData(row, fields.get(5))), 0.00001);
    Assert.Equal(-15.0,
        dbl.get(readerInspector.getStructFieldData(row, fields.get(6))),
        0.00001);
    Assert.Equal(bytes(0, 1, 2, 3, 4),
        bi.getPrimitiveWritableObject(readerInspector.getStructFieldData(row,
            fields.get(7))));
    Assert.Equal("hi", st.getPrimitiveJavaObject(readerInspector
        .getStructFieldData(row, fields.get(8))));
    List<?> midRow = midli.getList(mid.getStructFieldData(
        readerInspector.getStructFieldData(row, fields.get(9)),
        midFields.get(0)));
    assertNotNull(midRow);
    Assert.Equal(2, midRow.size());
    Assert.Equal(1,
        in.get(inner.getStructFieldData(midRow.get(0), inFields.get(0))));
    Assert.Equal("bye", st.getPrimitiveJavaObject(inner.getStructFieldData(
        midRow.get(0), inFields.get(1))));
    Assert.Equal(2,
        in.get(inner.getStructFieldData(midRow.get(1), inFields.get(0))));
    Assert.Equal("sigh", st.getPrimitiveJavaObject(inner.getStructFieldData(
        midRow.get(1), inFields.get(1))));
    List<?> list = li.getList(readerInspector.getStructFieldData(row,
        fields.get(10)));
    Assert.Equal(2, list.size());
    Assert.Equal(3,
        in.get(inner.getStructFieldData(list.get(0), inFields.get(0))));
    Assert.Equal("good", st.getPrimitiveJavaObject(inner.getStructFieldData(
        list.get(0), inFields.get(1))));
    Assert.Equal(4,
        in.get(inner.getStructFieldData(list.get(1), inFields.get(0))));
    Assert.Equal("bad", st.getPrimitiveJavaObject(inner.getStructFieldData(
        list.get(1), inFields.get(1))));
    Map<?, ?> map = ma.getMap(readerInspector.getStructFieldData(row,
        fields.get(11)));
    Assert.Equal(0, map.size());
    Assert.Equal(Timestamp.valueOf("2000-03-12 15:00:00"),
        tso.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
            fields.get(12))));
    Assert.Equal(HiveDecimal.create("12345678.6547456"),
        dco.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
            fields.get(13))));

    // check the contents of second row
    Assert.Equal(true, rows.hasNext());
    rows.seekToRow(7499);
    row = rows.next(null);
    Assert.Equal(true,
        bo.get(readerInspector.getStructFieldData(row, fields.get(0))));
    Assert.Equal(100,
        by.get(readerInspector.getStructFieldData(row, fields.get(1))));
    Assert.Equal(2048,
        sh.get(readerInspector.getStructFieldData(row, fields.get(2))));
    Assert.Equal(65536,
        in.get(readerInspector.getStructFieldData(row, fields.get(3))));
    Assert.Equal(Long.MAX_VALUE,
        lo.get(readerInspector.getStructFieldData(row, fields.get(4))));
    Assert.Equal(2.0,
        fl.get(readerInspector.getStructFieldData(row, fields.get(5))), 0.00001);
    Assert.Equal(-5.0,
        dbl.get(readerInspector.getStructFieldData(row, fields.get(6))),
        0.00001);
    Assert.Equal(bytes(), bi.getPrimitiveWritableObject(readerInspector
        .getStructFieldData(row, fields.get(7))));
    Assert.Equal("bye", st.getPrimitiveJavaObject(readerInspector
        .getStructFieldData(row, fields.get(8))));
    midRow = midli.getList(mid.getStructFieldData(
        readerInspector.getStructFieldData(row, fields.get(9)),
        midFields.get(0)));
    assertNotNull(midRow);
    Assert.Equal(2, midRow.size());
    Assert.Equal(1,
        in.get(inner.getStructFieldData(midRow.get(0), inFields.get(0))));
    Assert.Equal("bye", st.getPrimitiveJavaObject(inner.getStructFieldData(
        midRow.get(0), inFields.get(1))));
    Assert.Equal(2,
        in.get(inner.getStructFieldData(midRow.get(1), inFields.get(0))));
    Assert.Equal("sigh", st.getPrimitiveJavaObject(inner.getStructFieldData(
        midRow.get(1), inFields.get(1))));
    list = li.getList(readerInspector.getStructFieldData(row, fields.get(10)));
    Assert.Equal(3, list.size());
    Assert.Equal(100000000,
        in.get(inner.getStructFieldData(list.get(0), inFields.get(0))));
    Assert.Equal("cat", st.getPrimitiveJavaObject(inner.getStructFieldData(
        list.get(0), inFields.get(1))));
    Assert.Equal(-100000,
        in.get(inner.getStructFieldData(list.get(1), inFields.get(0))));
    Assert.Equal("in", st.getPrimitiveJavaObject(inner.getStructFieldData(
        list.get(1), inFields.get(1))));
    Assert.Equal(1234,
        in.get(inner.getStructFieldData(list.get(2), inFields.get(0))));
    Assert.Equal("hat", st.getPrimitiveJavaObject(inner.getStructFieldData(
        list.get(2), inFields.get(1))));
    map = ma.getMap(readerInspector.getStructFieldData(row, fields.get(11)));
    Assert.Equal(2, map.size());
    boolean[] found = new boolean[2];
    for(Object key : map.keySet()) {
      String str = mk.getPrimitiveJavaObject(key);
      if (str.equals("chani")) {
        Assert.Equal(false, found[0]);
        Assert.Equal(5,
            in.get(inner.getStructFieldData(map.get(key), inFields.get(0))));
        Assert.Equal(str, st.getPrimitiveJavaObject(inner.getStructFieldData(
            map.get(key), inFields.get(1))));
        found[0] = true;
      } else if (str.equals("mauddib")) {
        Assert.Equal(false, found[1]);
        Assert.Equal(1,
            in.get(inner.getStructFieldData(map.get(key), inFields.get(0))));
        Assert.Equal(str, st.getPrimitiveJavaObject(inner.getStructFieldData(
            map.get(key), inFields.get(1))));
        found[1] = true;
      } else {
        throw new IllegalArgumentException("Unknown key " + str);
      }
    }
    Assert.Equal(true, found[0]);
    Assert.Equal(true, found[1]);
    Assert.Equal(Timestamp.valueOf("2000-03-12 15:00:01"),
        tso.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
            fields.get(12))));
    Assert.Equal(HiveDecimal.create("12345678.6547457"),
        dco.getPrimitiveJavaObject(readerInspector.getStructFieldData(row,
            fields.get(13))));

    // handle the close up
    Assert.Equal(false, rows.hasNext());
    rows.close();
  }

  [Fact]
  public void testTimestamp()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Timestamp.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000).bufferSize(10000)
            .version(OrcFile.Version.V_0_11));
    List<Timestamp> tslist = Lists.newArrayList();
    tslist.add(Timestamp.valueOf("2037-01-01 00:00:00.000999"));
    tslist.add(Timestamp.valueOf("2003-01-01 00:00:00.000000222"));
    tslist.add(Timestamp.valueOf("1999-01-01 00:00:00.999999999"));
    tslist.add(Timestamp.valueOf("1995-01-01 00:00:00.688888888"));
    tslist.add(Timestamp.valueOf("2002-01-01 00:00:00.1"));
    tslist.add(Timestamp.valueOf("2010-03-02 00:00:00.000009001"));
    tslist.add(Timestamp.valueOf("2005-01-01 00:00:00.000002229"));
    tslist.add(Timestamp.valueOf("2006-01-01 00:00:00.900203003"));
    tslist.add(Timestamp.valueOf("2003-01-01 00:00:00.800000007"));
    tslist.add(Timestamp.valueOf("1996-08-02 00:00:00.723100809"));
    tslist.add(Timestamp.valueOf("1998-11-02 00:00:00.857340643"));
    tslist.add(Timestamp.valueOf("2008-10-02 00:00:00"));

    for (Timestamp ts : tslist) {
      writer.addRow(ts);
    }

    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows(null);
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(tslist.get(idx++).getNanos(), ((TimestampWritable) row).getNanos());
    }
    Assert.Equal(0, writer.getSchema().getMaximumId());
    boolean[] expected = new boolean[] {false};
    boolean[] included = OrcUtils.includeColumns("", writer.getSchema());
    Assert.Equal(true, Arrays.equals(expected, included));
  }

  [Fact]
  public void testStringAndBinaryStatistics()  {

    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (SimpleStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .bufferSize(10000));
    writer.addRow(new SimpleStruct(bytes(0,1,2,3,4), "foo"));
    writer.addRow(new SimpleStruct(bytes(0,1,2,3), "bar"));
    writer.addRow(new SimpleStruct(bytes(0,1,2,3,4,5), null));
    writer.addRow(new SimpleStruct(null, "hi"));
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    TypeDescription schema = writer.getSchema();
    Assert.Equal(2, schema.getMaximumId());
    boolean[] expected = new boolean[] {false, false, true};
    boolean[] included = OrcUtils.includeColumns("string1", schema);
    Assert.Equal(true, Arrays.equals(expected, included));

    expected = new boolean[] {false, false, false};
    included = OrcUtils.includeColumns("", schema);
    Assert.Equal(true, Arrays.equals(expected, included));

    expected = new boolean[] {false, false, false};
    included = OrcUtils.includeColumns(null, schema);
    Assert.Equal(true, Arrays.equals(expected, included));

    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    Assert.Equal(4, stats[0].getNumberOfValues());
    Assert.Equal("count: 4 hasNull: false", stats[0].toString());

    Assert.Equal(3, stats[1].getNumberOfValues());
    Assert.Equal(15, ((BinaryColumnStatistics) stats[1]).getSum());
    Assert.Equal("count: 3 hasNull: true sum: 15", stats[1].toString());

    Assert.Equal(3, stats[2].getNumberOfValues());
    Assert.Equal("bar", ((StringColumnStatistics) stats[2]).getMinimum());
    Assert.Equal("hi", ((StringColumnStatistics) stats[2]).getMaximum());
    Assert.Equal(8, ((StringColumnStatistics) stats[2]).getSum());
    Assert.Equal("count: 3 hasNull: true min: bar max: hi sum: 8",
        stats[2].toString());

    // check the inspectors
    StructObjectInspector readerInspector =
        (StructObjectInspector) reader.getObjectInspector();
    Assert.Equal(ObjectInspector.Category.STRUCT,
        readerInspector.getCategory());
    Assert.Equal("struct<bytes1:binary,string1:string>",
        readerInspector.getTypeName());
    List<? extends StructField> fields =
        readerInspector.getAllStructFieldRefs();
    BinaryObjectInspector bi = (BinaryObjectInspector) readerInspector.
        getStructFieldRef("bytes1").getFieldObjectInspector();
    StringObjectInspector st = (StringObjectInspector) readerInspector.
        getStructFieldRef("string1").getFieldObjectInspector();
    RecordReader rows = reader.rows();
    Object row = rows.next(null);
    assertNotNull(row);
    // check the contents of the first row
    Assert.Equal(bytes(0,1,2,3,4), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(0))));
    Assert.Equal("foo", st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(1))));

    // check the contents of second row
    Assert.Equal(true, rows.hasNext());
    row = rows.next(row);
    Assert.Equal(bytes(0,1,2,3), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(0))));
    Assert.Equal("bar", st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(1))));

    // check the contents of second row
    Assert.Equal(true, rows.hasNext());
    row = rows.next(row);
    Assert.Equal(bytes(0,1,2,3,4,5), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(0))));
    assertNull(st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(1))));

    // check the contents of second row
    Assert.Equal(true, rows.hasNext());
    row = rows.next(row);
    assertNull(bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(0))));
    Assert.Equal("hi", st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(1))));

    // handle the close up
    Assert.Equal(false, rows.hasNext());
    rows.close();
  }


  [Fact]
  public void testStripeLevelStats()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .inspector(inspector)
            .stripeSize(100000)
            .bufferSize(10000));
    for (int i = 0; i < 11000; i++) {
      if (i >= 5000) {
        if (i >= 10000) {
          writer.addRow(new InnerStruct(3, "three"));
        } else {
          writer.addRow(new InnerStruct(2, "two"));
        }
      } else {
        writer.addRow(new InnerStruct(1, "one"));
      }
    }

    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    TypeDescription schema = writer.getSchema();
    Assert.Equal(2, schema.getMaximumId());
    boolean[] expected = new boolean[] {false, true, false};
    boolean[] included = OrcUtils.includeColumns("int1", schema);
    Assert.Equal(true, Arrays.equals(expected, included));

    List<StripeStatistics> stats = reader.getStripeStatistics();
    int numStripes = stats.size();
    Assert.Equal(3, numStripes);
    StripeStatistics ss1 = stats.get(0);
    StripeStatistics ss2 = stats.get(1);
    StripeStatistics ss3 = stats.get(2);

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
    Assert.Equal("two", ((StringColumnStatistics) ss2.getColumnStatistics()[2]).getMaximum());
    Assert.Equal("three", ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getMaximum());
    Assert.Equal(15000, ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getSum());
    Assert.Equal(15000, ((StringColumnStatistics)ss2.getColumnStatistics()[2]).getSum());
    Assert.Equal(5000, ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getSum());

    RecordReaderImpl recordReader = (RecordReaderImpl) reader.rows();
    OrcProto.RowIndex[] index = recordReader.readRowIndex(0, null, null).getRowGroupIndex();
    Assert.Equal(3, index.length);
    List<OrcProto.RowIndexEntry> items = index[1].getEntryList();
    Assert.Equal(1, items.size());
    Assert.Equal(3, items.get(0).getPositionsCount());
    Assert.Equal(0, items.get(0).getPositions(0));
    Assert.Equal(0, items.get(0).getPositions(1));
    Assert.Equal(0, items.get(0).getPositions(2));
    Assert.Equal(1,
                 items.get(0).getStatistics().getIntStatistics().getMinimum());
    index = recordReader.readRowIndex(1, null, null).getRowGroupIndex();
    Assert.Equal(3, index.length);
    items = index[1].getEntryList();
    Assert.Equal(2,
        items.get(0).getStatistics().getIntStatistics().getMaximum());
  }

  [Fact]
  public void test1()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .inspector(inspector)
            .stripeSize(100000)
            .bufferSize(10000));
    writer.addRow(new BigRow(false, (byte) 1, (short) 1024, 65536,
        Long.MAX_VALUE, (float) 1.0, -15.0, bytes(0, 1, 2, 3, 4), "hi",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(3, "good"), inner(4, "bad")),
        map()));
    writer.addRow(new BigRow(true, (byte) 100, (short) 2048, 65536,
        Long.MAX_VALUE, (float) 2.0, -5.0, bytes(), "bye",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(100000000, "cat"), inner(-100000, "in"), inner(1234, "hat")),
        map(inner(5, "chani"), inner(1, "mauddib"))));
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    TypeDescription schema = writer.getSchema();
    Assert.Equal(23, schema.getMaximumId());
    boolean[] expected = new boolean[] {false, false, false, false, false,
        false, false, false, false, false,
        false, false, false, false, false,
        false, false, false, false, false,
        false, false, false, false};
    boolean[] included = OrcUtils.includeColumns("", schema);
    Assert.Equal(true, Arrays.equals(expected, included));

    expected = new boolean[] {false, true, false, false, false,
        false, false, false, false, true,
        true, true, true, true, true,
        false, false, false, false, true,
        true, true, true, true};
    included = OrcUtils.includeColumns("boolean1,string1,middle,map", schema);
    Assert.Equal(true, Arrays.equals(expected, included));

    expected = new boolean[] {false, true, false, false, false,
        false, false, false, false, true,
        true, true, true, true, true,
        false, false, false, false, true,
        true, true, true, true};
    included = OrcUtils.includeColumns("boolean1,string1,middle,map", schema);
    Assert.Equal(true, Arrays.equals(expected, included));

    expected = new boolean[] {false, true, true, true, true,
        true, true, true, true, true,
        true, true, true, true, true,
        true, true, true, true, true,
        true, true, true, true};
    included = OrcUtils.includeColumns(
        "boolean1,byte1,short1,int1,long1,float1,double1,bytes1,string1,middle,list,map",
        schema);
    Assert.Equal(true, Arrays.equals(expected, included));

    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    Assert.Equal(2, stats[1].getNumberOfValues());
    Assert.Equal(1, ((BooleanColumnStatistics) stats[1]).getFalseCount());
    Assert.Equal(1, ((BooleanColumnStatistics) stats[1]).getTrueCount());
    Assert.Equal("count: 2 hasNull: false true: 1", stats[1].toString());

    Assert.Equal(2048, ((IntegerColumnStatistics) stats[3]).getMaximum());
    Assert.Equal(1024, ((IntegerColumnStatistics) stats[3]).getMinimum());
    Assert.Equal(true, ((IntegerColumnStatistics) stats[3]).isSumDefined());
    Assert.Equal(3072, ((IntegerColumnStatistics) stats[3]).getSum());
    Assert.Equal("count: 2 hasNull: false min: 1024 max: 2048 sum: 3072",
        stats[3].toString());

    StripeStatistics ss = reader.getStripeStatistics().get(0);
    Assert.Equal(2, ss.getColumnStatistics()[0].getNumberOfValues());
    Assert.Equal(1, ((BooleanColumnStatistics) ss.getColumnStatistics()[1]).getTrueCount());
    Assert.Equal(1024, ((IntegerColumnStatistics) ss.getColumnStatistics()[3]).getMinimum());
    Assert.Equal(2048, ((IntegerColumnStatistics) ss.getColumnStatistics()[3]).getMaximum());
    Assert.Equal(3072, ((IntegerColumnStatistics) ss.getColumnStatistics()[3]).getSum());
    Assert.Equal(-15.0, ((DoubleColumnStatistics) stats[7]).getMinimum());
    Assert.Equal(-5.0, ((DoubleColumnStatistics) stats[7]).getMaximum());
    Assert.Equal(-20.0, ((DoubleColumnStatistics) stats[7]).getSum(), 0.00001);
    Assert.Equal("count: 2 hasNull: false min: -15.0 max: -5.0 sum: -20.0",
        stats[7].toString());

    Assert.Equal("count: 2 hasNull: false min: bye max: hi sum: 5", stats[9].toString());

    // check the inspectors
    StructObjectInspector readerInspector =
        (StructObjectInspector) reader.getObjectInspector();
    Assert.Equal(ObjectInspector.Category.STRUCT,
        readerInspector.getCategory());
    Assert.Equal("struct<boolean1:boolean,byte1:tinyint,short1:smallint,"
        + "int1:int,long1:bigint,float1:float,double1:double,bytes1:"
        + "binary,string1:string,middle:struct<list:array<struct<int1:int,"
        + "string1:string>>>,list:array<struct<int1:int,string1:string>>,"
        + "map:map<string,struct<int1:int,string1:string>>>",
        readerInspector.getTypeName());
    List<? extends StructField> fields =
        readerInspector.getAllStructFieldRefs();
    BooleanObjectInspector bo = (BooleanObjectInspector) readerInspector.
        getStructFieldRef("boolean1").getFieldObjectInspector();
    ByteObjectInspector by = (ByteObjectInspector) readerInspector.
        getStructFieldRef("byte1").getFieldObjectInspector();
    ShortObjectInspector sh = (ShortObjectInspector) readerInspector.
        getStructFieldRef("short1").getFieldObjectInspector();
    IntObjectInspector in = (IntObjectInspector) readerInspector.
        getStructFieldRef("int1").getFieldObjectInspector();
    LongObjectInspector lo = (LongObjectInspector) readerInspector.
        getStructFieldRef("long1").getFieldObjectInspector();
    FloatObjectInspector fl = (FloatObjectInspector) readerInspector.
        getStructFieldRef("float1").getFieldObjectInspector();
    DoubleObjectInspector dbl = (DoubleObjectInspector) readerInspector.
        getStructFieldRef("double1").getFieldObjectInspector();
    BinaryObjectInspector bi = (BinaryObjectInspector) readerInspector.
        getStructFieldRef("bytes1").getFieldObjectInspector();
    StringObjectInspector st = (StringObjectInspector) readerInspector.
        getStructFieldRef("string1").getFieldObjectInspector();
    StructObjectInspector mid = (StructObjectInspector) readerInspector.
        getStructFieldRef("middle").getFieldObjectInspector();
    List<? extends StructField> midFields =
        mid.getAllStructFieldRefs();
    ListObjectInspector midli =
        (ListObjectInspector) midFields.get(0).getFieldObjectInspector();
    StructObjectInspector inner = (StructObjectInspector)
        midli.getListElementObjectInspector();
    List<? extends StructField> inFields = inner.getAllStructFieldRefs();
    ListObjectInspector li = (ListObjectInspector) readerInspector.
        getStructFieldRef("list").getFieldObjectInspector();
    MapObjectInspector ma = (MapObjectInspector) readerInspector.
        getStructFieldRef("map").getFieldObjectInspector();
    StringObjectInspector mk = (StringObjectInspector)
        ma.getMapKeyObjectInspector();
    RecordReader rows = reader.rows();
    Object row = rows.next(null);
    assertNotNull(row);
    // check the contents of the first row
    Assert.Equal(false,
        bo.get(readerInspector.getStructFieldData(row, fields.get(0))));
    Assert.Equal(1, by.get(readerInspector.getStructFieldData(row,
        fields.get(1))));
    Assert.Equal(1024, sh.get(readerInspector.getStructFieldData(row,
        fields.get(2))));
    Assert.Equal(65536, in.get(readerInspector.getStructFieldData(row,
        fields.get(3))));
    Assert.Equal(Long.MAX_VALUE, lo.get(readerInspector.
        getStructFieldData(row, fields.get(4))));
    Assert.Equal(1.0, fl.get(readerInspector.getStructFieldData(row,
        fields.get(5))), 0.00001);
    Assert.Equal(-15.0, dbl.get(readerInspector.getStructFieldData(row,
        fields.get(6))), 0.00001);
    Assert.Equal(bytes(0,1,2,3,4), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(7))));
    Assert.Equal("hi", st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(8))));
    List<?> midRow = midli.getList(mid.getStructFieldData(readerInspector.
        getStructFieldData(row, fields.get(9)), midFields.get(0)));
    assertNotNull(midRow);
    Assert.Equal(2, midRow.size());
    Assert.Equal(1, in.get(inner.getStructFieldData(midRow.get(0),
        inFields.get(0))));
    Assert.Equal("bye", st.getPrimitiveJavaObject(inner.getStructFieldData
        (midRow.get(0), inFields.get(1))));
    Assert.Equal(2, in.get(inner.getStructFieldData(midRow.get(1),
        inFields.get(0))));
    Assert.Equal("sigh", st.getPrimitiveJavaObject(inner.getStructFieldData
        (midRow.get(1), inFields.get(1))));
    List<?> list = li.getList(readerInspector.getStructFieldData(row,
        fields.get(10)));
    Assert.Equal(2, list.size());
    Assert.Equal(3, in.get(inner.getStructFieldData(list.get(0),
        inFields.get(0))));
    Assert.Equal("good", st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(0), inFields.get(1))));
    Assert.Equal(4, in.get(inner.getStructFieldData(list.get(1),
        inFields.get(0))));
    Assert.Equal("bad", st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(1), inFields.get(1))));
    Map<?,?> map = ma.getMap(readerInspector.getStructFieldData(row,
        fields.get(11)));
    Assert.Equal(0, map.size());

    // check the contents of second row
    Assert.Equal(true, rows.hasNext());
    row = rows.next(row);
    Assert.Equal(true,
        bo.get(readerInspector.getStructFieldData(row, fields.get(0))));
    Assert.Equal(100, by.get(readerInspector.getStructFieldData(row,
        fields.get(1))));
    Assert.Equal(2048, sh.get(readerInspector.getStructFieldData(row,
        fields.get(2))));
    Assert.Equal(65536, in.get(readerInspector.getStructFieldData(row,
        fields.get(3))));
    Assert.Equal(Long.MAX_VALUE, lo.get(readerInspector.
        getStructFieldData(row, fields.get(4))));
    Assert.Equal(2.0, fl.get(readerInspector.getStructFieldData(row,
        fields.get(5))), 0.00001);
    Assert.Equal(-5.0, dbl.get(readerInspector.getStructFieldData(row,
        fields.get(6))), 0.00001);
    Assert.Equal(bytes(), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(7))));
    Assert.Equal("bye", st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(8))));
    midRow = midli.getList(mid.getStructFieldData(readerInspector.
        getStructFieldData(row, fields.get(9)), midFields.get(0)));
    assertNotNull(midRow);
    Assert.Equal(2, midRow.size());
    Assert.Equal(1, in.get(inner.getStructFieldData(midRow.get(0),
        inFields.get(0))));
    Assert.Equal("bye", st.getPrimitiveJavaObject(inner.getStructFieldData
        (midRow.get(0), inFields.get(1))));
    Assert.Equal(2, in.get(inner.getStructFieldData(midRow.get(1),
        inFields.get(0))));
    Assert.Equal("sigh", st.getPrimitiveJavaObject(inner.getStructFieldData
        (midRow.get(1), inFields.get(1))));
    list = li.getList(readerInspector.getStructFieldData(row,
        fields.get(10)));
    Assert.Equal(3, list.size());
    Assert.Equal(100000000, in.get(inner.getStructFieldData(list.get(0),
        inFields.get(0))));
    Assert.Equal("cat", st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(0), inFields.get(1))));
    Assert.Equal(-100000, in.get(inner.getStructFieldData(list.get(1),
        inFields.get(0))));
    Assert.Equal("in", st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(1), inFields.get(1))));
    Assert.Equal(1234, in.get(inner.getStructFieldData(list.get(2),
        inFields.get(0))));
    Assert.Equal("hat", st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(2), inFields.get(1))));
    map = ma.getMap(readerInspector.getStructFieldData(row,
        fields.get(11)));
    Assert.Equal(2, map.size());
    boolean[] found = new boolean[2];
    for(Object key: map.keySet()) {
      String str = mk.getPrimitiveJavaObject(key);
      if (str.equals("chani")) {
        Assert.Equal(false, found[0]);
        Assert.Equal(5, in.get(inner.getStructFieldData(map.get(key),
            inFields.get(0))));
        Assert.Equal(str, st.getPrimitiveJavaObject(
            inner.getStructFieldData(map.get(key), inFields.get(1))));
        found[0] = true;
      } else if (str.equals("mauddib")) {
        Assert.Equal(false, found[1]);
        Assert.Equal(1, in.get(inner.getStructFieldData(map.get(key),
            inFields.get(0))));
        Assert.Equal(str, st.getPrimitiveJavaObject(
            inner.getStructFieldData(map.get(key), inFields.get(1))));
        found[1] = true;
      } else {
        throw new IllegalArgumentException("Unknown key " + str);
      }
    }
    Assert.Equal(true, found[0]);
    Assert.Equal(true, found[1]);

    // handle the close up
    Assert.Equal(false, rows.hasNext());
    rows.close();
  }

  [Fact]
  public void columnProjection()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(100)
                                         .rowIndexStride(1000));
    Random r1 = new Random(1);
    Random r2 = new Random(2);
    int x;
    int minInt=0, maxInt=0;
    String y;
    String minStr = null, maxStr = null;
    for(int i=0; i < 21000; ++i) {
      x = r1.nextInt();
      y = Long.toHexString(r2.nextLong());
      if (i == 0 || x < minInt) {
        minInt = x;
      }
      if (i == 0 || x > maxInt) {
        maxInt = x;
      }
      if (i == 0 || y.compareTo(minStr) < 0) {
        minStr = y;
      }
      if (i == 0 || y.compareTo(maxStr) > 0) {
        maxStr = y;
      }
      writer.addRow(inner(x, y));
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    // check out the statistics
    ColumnStatistics[] stats = reader.getStatistics();
    Assert.Equal(3, stats.length);
    for(ColumnStatistics s: stats) {
      Assert.Equal(21000, s.getNumberOfValues());
      if (s instanceof IntegerColumnStatistics) {
        Assert.Equal(minInt, ((IntegerColumnStatistics) s).getMinimum());
        Assert.Equal(maxInt, ((IntegerColumnStatistics) s).getMaximum());
      } else if (s instanceof  StringColumnStatistics) {
        Assert.Equal(maxStr, ((StringColumnStatistics) s).getMaximum());
        Assert.Equal(minStr, ((StringColumnStatistics) s).getMinimum());
      }
    }

    // check out the types
    List<OrcProto.Type> types = reader.getTypes();
    Assert.Equal(3, types.size());
    Assert.Equal(OrcProto.Type.Kind.STRUCT, types.get(0).getKind());
    Assert.Equal(2, types.get(0).getSubtypesCount());
    Assert.Equal(1, types.get(0).getSubtypes(0));
    Assert.Equal(2, types.get(0).getSubtypes(1));
    Assert.Equal(OrcProto.Type.Kind.INT, types.get(1).getKind());
    Assert.Equal(0, types.get(1).getSubtypesCount());
    Assert.Equal(OrcProto.Type.Kind.STRING, types.get(2).getKind());
    Assert.Equal(0, types.get(2).getSubtypesCount());

    // read the contents and make sure they match
    RecordReader rows1 = reader.rows(new boolean[]{true, true, false});
    RecordReader rows2 = reader.rows(new boolean[]{true, false, true});
    r1 = new Random(1);
    r2 = new Random(2);
    OrcStruct row1 = null;
    OrcStruct row2 = null;
    for(int i = 0; i < 21000; ++i) {
      Assert.Equal(true, rows1.hasNext());
      Assert.Equal(true, rows2.hasNext());
      row1 = (OrcStruct) rows1.next(row1);
      row2 = (OrcStruct) rows2.next(row2);
      Assert.Equal(r1.nextInt(), ((IntWritable) row1.getFieldValue(0)).get());
      Assert.Equal(Long.toHexString(r2.nextLong()),
          row2.getFieldValue(1).toString());
    }
    Assert.Equal(false, rows1.hasNext());
    Assert.Equal(false, rows2.hasNext());
    rows1.close();
    rows2.close();
  }

  [Fact]
  public void emptyFile()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(100));
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    Assert.Equal(false, reader.rows().hasNext());
    Assert.Equal(CompressionKind.NONE, reader.getCompression());
    Assert.Equal(0, reader.getNumberOfRows());
    Assert.Equal(0, reader.getCompressionSize());
    Assert.Equal(false, reader.getMetadataKeys().iterator().hasNext());
    Assert.Equal(3, reader.getContentLength());
    Assert.Equal(false, reader.getStripes().iterator().hasNext());
  }

  [Fact]
  public void metaData()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(100));
    writer.addUserMetadata("my.meta", byteBuf(1, 2, 3, 4, 5, 6, 7, -1, -2, 127,
                                              -128));
    writer.addUserMetadata("clobber", byteBuf(1, 2, 3));
    writer.addUserMetadata("clobber", byteBuf(4, 3, 2, 1));
    ByteBuffer bigBuf = ByteBuffer.allocate(40000);
    Random random = new Random(0);
    random.nextBytes(bigBuf.array());
    writer.addUserMetadata("big", bigBuf);
    bigBuf.position(0);
    writer.addRow(new BigRow(true, (byte) 127, (short) 1024, 42,
        42L * 1024 * 1024 * 1024, (float) 3.1415, -2.713, null,
        null, null, null, null));
    writer.addUserMetadata("clobber", byteBuf(5,7,11,13,17,19));
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    Assert.Equal(byteBuf(5,7,11,13,17,19), reader.getMetadataValue("clobber"));
    Assert.Equal(byteBuf(1,2,3,4,5,6,7,-1,-2,127,-128),
        reader.getMetadataValue("my.meta"));
    Assert.Equal(bigBuf, reader.getMetadataValue("big"));
    try {
      reader.getMetadataValue("unknown");
      assertTrue(false);
    } catch (IllegalArgumentException iae) {
      // PASS
    }
    int i = 0;
    for(String key: reader.getMetadataKeys()) {
      if ("my.meta".equals(key) ||
          "clobber".equals(key) ||
          "big".equals(key)) {
        i += 1;
      } else {
        throw new IllegalArgumentException("unknown key " + key);
      }
    }
    Assert.Equal(3, i);
    int numStripes = reader.getStripeStatistics().size();
    Assert.Equal(1, numStripes);
  }

  /**
   * Generate an ORC file with a range of dates and times.
   */
  public void createOrcDateFile(Path file, int minYear, int maxYear
                                ) {
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT).
        addFieldNames("time").addFieldNames("date").
        addSubtypes(1).addSubtypes(2).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.TIMESTAMP).
        build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.DATE).
        build());

    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = OrcStruct.createObjectInspector(0, types);
    }
    Writer writer = OrcFile.createWriter(file,
        OrcFile.writerOptions(conf)
            .inspector(inspector)
            .stripeSize(100000)
            .bufferSize(10000)
            .blockPadding(false));
    OrcStruct row = new OrcStruct(2);
    for (int year = minYear; year < maxYear; ++year) {
      for (int ms = 1000; ms < 2000; ++ms) {
        row.setFieldValue(0,
            new TimestampWritable(Timestamp.valueOf(year + "-05-05 12:34:56."
                + ms)));
        row.setFieldValue(1,
            new DateWritable(new Date(year - 1900, 11, 25)));
        writer.addRow(row);
      }
    }
    writer.close();
    Reader reader = OrcFile.createReader(file,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    for (int year = minYear; year < maxYear; ++year) {
      for(int ms = 1000; ms < 2000; ++ms) {
        row = (OrcStruct) rows.next(row);
        Assert.Equal(new TimestampWritable
                (Timestamp.valueOf(year + "-05-05 12:34:56." + ms)),
            row.getFieldValue(0));
        Assert.Equal(new DateWritable(new Date(year - 1900, 11, 25)),
            row.getFieldValue(1));
      }
    }
  }

  [Fact]
  public void testDate1900()  {
    createOrcDateFile(testFilePath, 1900, 1970);
  }

  [Fact]
  public void testDate2038()  {
    createOrcDateFile(testFilePath, 2038, 2250);
  }

  /**
     * We test union, timestamp, and decimal separately since we need to make the
     * object inspector manually. (The Hive reflection-based doesn't handle
     * them properly.)
     */
  [Fact]
  public void testUnionAndTimestamp()  {
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT).
        addFieldNames("time").addFieldNames("union").addFieldNames("decimal").
        addSubtypes(1).addSubtypes(2).addSubtypes(5).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.TIMESTAMP).
        build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.UNION).
        addSubtypes(3).addSubtypes(4).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).
        build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRING).
        build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.DECIMAL).
        build());

    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = OrcStruct.createObjectInspector(0, types);
    }
    HiveDecimal maxValue = HiveDecimal.create("10000000000000000000");
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(100)
                                         .blockPadding(false));
    OrcStruct row = new OrcStruct(3);
    OrcUnion union = new OrcUnion();
    row.setFieldValue(1, union);
    row.setFieldValue(0, new TimestampWritable(Timestamp.valueOf("2000-03-12 15:00:00")));
    HiveDecimal value = HiveDecimal.create("12345678.6547456");
    row.setFieldValue(2, new HiveDecimalWritable(value));
    union.set((byte) 0, new IntWritable(42));
    writer.addRow(row);
    row.setFieldValue(0, new TimestampWritable(Timestamp.valueOf("2000-03-20 12:00:00.123456789")));
    union.set((byte) 1, new Text("hello"));
    value = HiveDecimal.create("-5643.234");
    row.setFieldValue(2, new HiveDecimalWritable(value));
    writer.addRow(row);
    row.setFieldValue(0, null);
    row.setFieldValue(1, null);
    row.setFieldValue(2, null);
    writer.addRow(row);
    row.setFieldValue(1, union);
    union.set((byte) 0, null);
    writer.addRow(row);
    union.set((byte) 1, null);
    writer.addRow(row);
    union.set((byte) 0, new IntWritable(200000));
    row.setFieldValue(0, new TimestampWritable
        (Timestamp.valueOf("1970-01-01 00:00:00")));
    value = HiveDecimal.create("10000000000000000000");
    row.setFieldValue(2, new HiveDecimalWritable(value));
    writer.addRow(row);
    Random rand = new Random(42);
    for(int i=1970; i < 2038; ++i) {
      row.setFieldValue(0, new TimestampWritable(Timestamp.valueOf(i +
          "-05-05 12:34:56." + i)));
      if ((i & 1) == 0) {
        union.set((byte) 0, new IntWritable(i*i));
      } else {
        union.set((byte) 1, new Text(Integer.toString(i * i)));
      }
      value = HiveDecimal.create(new BigInteger(64, rand),
          rand.nextInt(18));
      row.setFieldValue(2, new HiveDecimalWritable(value));
      if (maxValue.compareTo(value) < 0) {
        maxValue = value;
      }
      writer.addRow(row);
    }
    // let's add a lot of constant rows to test the rle
    row.setFieldValue(0, null);
    union.set((byte) 0, new IntWritable(1732050807));
    row.setFieldValue(2, null);
    for(int i=0; i < 5000; ++i) {
      writer.addRow(row);
    }
    union.set((byte) 0, new IntWritable(0));
    writer.addRow(row);
    union.set((byte) 0, new IntWritable(10));
    writer.addRow(row);
    union.set((byte) 0, new IntWritable(138));
    writer.addRow(row);
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    TypeDescription schema = writer.getSchema();
    Assert.Equal(5, schema.getMaximumId());
    boolean[] expected = new boolean[] {false, false, false, false, false, false};
    boolean[] included = OrcUtils.includeColumns("", schema);
    Assert.Equal(true, Arrays.equals(expected, included));

    expected = new boolean[] {false, true, false, false, false, true};
    included = OrcUtils.includeColumns("time,decimal", schema);
    Assert.Equal(true, Arrays.equals(expected, included));

    expected = new boolean[] {false, false, true, true, true, false};
    included = OrcUtils.includeColumns("union", schema);
    Assert.Equal(true, Arrays.equals(expected, included));

    Assert.Equal(false, reader.getMetadataKeys().iterator().hasNext());
    Assert.Equal(5077, reader.getNumberOfRows());
    DecimalColumnStatistics stats =
        (DecimalColumnStatistics) reader.getStatistics()[5];
    Assert.Equal(71, stats.getNumberOfValues());
    Assert.Equal(HiveDecimal.create("-5643.234"), stats.getMinimum());
    Assert.Equal(maxValue, stats.getMaximum());
    // TODO: fix this
//    Assert.Equal(null,stats.getSum());
    int stripeCount = 0;
    int rowCount = 0;
    long currentOffset = -1;
    for(StripeInformation stripe: reader.getStripes()) {
      stripeCount += 1;
      rowCount += stripe.getNumberOfRows();
      if (currentOffset < 0) {
        currentOffset = stripe.getOffset() + stripe.getLength();
      } else {
        Assert.Equal(currentOffset, stripe.getOffset());
        currentOffset += stripe.getLength();
      }
    }
    Assert.Equal(reader.getNumberOfRows(), rowCount);
    Assert.Equal(2, stripeCount);
    Assert.Equal(reader.getContentLength(), currentOffset);
    RecordReader rows = reader.rows();
    Assert.Equal(0, rows.getRowNumber());
    Assert.Equal(0.0, rows.getProgress(), 0.000001);
    Assert.Equal(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    Assert.Equal(1, rows.getRowNumber());
    inspector = reader.getObjectInspector();
    Assert.Equal("struct<time:timestamp,union:uniontype<int,string>,decimal:decimal(38,18)>",
        inspector.getTypeName());
    Assert.Equal(new TimestampWritable(Timestamp.valueOf("2000-03-12 15:00:00")),
        row.getFieldValue(0));
    union = (OrcUnion) row.getFieldValue(1);
    Assert.Equal(0, union.getTag());
    Assert.Equal(new IntWritable(42), union.getObject());
    Assert.Equal(new HiveDecimalWritable(HiveDecimal.create("12345678.6547456")),
        row.getFieldValue(2));
    row = (OrcStruct) rows.next(row);
    Assert.Equal(2, rows.getRowNumber());
    Assert.Equal(new TimestampWritable(Timestamp.valueOf("2000-03-20 12:00:00.123456789")),
        row.getFieldValue(0));
    Assert.Equal(1, union.getTag());
    Assert.Equal(new Text("hello"), union.getObject());
    Assert.Equal(new HiveDecimalWritable(HiveDecimal.create("-5643.234")),
        row.getFieldValue(2));
    row = (OrcStruct) rows.next(row);
    Assert.Equal(null, row.getFieldValue(0));
    Assert.Equal(null, row.getFieldValue(1));
    Assert.Equal(null, row.getFieldValue(2));
    row = (OrcStruct) rows.next(row);
    Assert.Equal(null, row.getFieldValue(0));
    union = (OrcUnion) row.getFieldValue(1);
    Assert.Equal(0, union.getTag());
    Assert.Equal(null, union.getObject());
    Assert.Equal(null, row.getFieldValue(2));
    row = (OrcStruct) rows.next(row);
    Assert.Equal(null, row.getFieldValue(0));
    Assert.Equal(1, union.getTag());
    Assert.Equal(null, union.getObject());
    Assert.Equal(null, row.getFieldValue(2));
    row = (OrcStruct) rows.next(row);
    Assert.Equal(new TimestampWritable(Timestamp.valueOf("1970-01-01 00:00:00")),
        row.getFieldValue(0));
    Assert.Equal(new IntWritable(200000), union.getObject());
    Assert.Equal(new HiveDecimalWritable(HiveDecimal.create("10000000000000000000")),
                 row.getFieldValue(2));
    rand = new Random(42);
    for(int i=1970; i < 2038; ++i) {
      row = (OrcStruct) rows.next(row);
      Assert.Equal(new TimestampWritable(Timestamp.valueOf(i + "-05-05 12:34:56." + i)),
          row.getFieldValue(0));
      if ((i & 1) == 0) {
        Assert.Equal(0, union.getTag());
        Assert.Equal(new IntWritable(i*i), union.getObject());
      } else {
        Assert.Equal(1, union.getTag());
        Assert.Equal(new Text(Integer.toString(i * i)), union.getObject());
      }
      Assert.Equal(new HiveDecimalWritable(HiveDecimal.create(new BigInteger(64, rand),
                                   rand.nextInt(18))), row.getFieldValue(2));
    }
    for(int i=0; i < 5000; ++i) {
      row = (OrcStruct) rows.next(row);
      Assert.Equal(new IntWritable(1732050807), union.getObject());
    }
    row = (OrcStruct) rows.next(row);
    Assert.Equal(new IntWritable(0), union.getObject());
    row = (OrcStruct) rows.next(row);
    Assert.Equal(new IntWritable(10), union.getObject());
    row = (OrcStruct) rows.next(row);
    Assert.Equal(new IntWritable(138), union.getObject());
    Assert.Equal(false, rows.hasNext());
    Assert.Equal(1.0, rows.getProgress(), 0.00001);
    Assert.Equal(reader.getNumberOfRows(), rows.getRowNumber());
    rows.seekToRow(1);
    row = (OrcStruct) rows.next(row);
    Assert.Equal(new TimestampWritable(Timestamp.valueOf("2000-03-20 12:00:00.123456789")),
        row.getFieldValue(0));
    Assert.Equal(1, union.getTag());
    Assert.Equal(new Text("hello"), union.getObject());
    Assert.Equal(new HiveDecimalWritable(HiveDecimal.create("-5643.234")), row.getFieldValue(2));
    rows.close();
  }

  /**
   * Read and write a randomly generated snappy file.
   * @
   */
  [Fact]
  public void testSnappy()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.SNAPPY)
                                         .bufferSize(100));
    Random rand = new Random(12);
    for(int i=0; i < 10000; ++i) {
      writer.addRow(new InnerStruct(rand.nextInt(),
          Integer.toHexString(rand.nextInt())));
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    rand = new Random(12);
    OrcStruct row = null;
    for(int i=0; i < 10000; ++i) {
      Assert.Equal(true, rows.hasNext());
      row = (OrcStruct) rows.next(row);
      Assert.Equal(rand.nextInt(), ((IntWritable) row.getFieldValue(0)).get());
      Assert.Equal(Integer.toHexString(rand.nextInt()),
          row.getFieldValue(1).toString());
    }
    Assert.Equal(false, rows.hasNext());
    rows.close();
  }

  /**
   * Read and write a randomly generated snappy file.
   * @
   */
  [Fact]
  public void testWithoutIndex()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(5000)
                                         .compress(CompressionKind.SNAPPY)
                                         .bufferSize(1000)
                                         .rowIndexStride(0));
    Random rand = new Random(24);
    for(int i=0; i < 10000; ++i) {
      InnerStruct row = new InnerStruct(rand.nextInt(),
          Integer.toBinaryString(rand.nextInt()));
      for(int j=0; j< 5; ++j) {
        writer.addRow(row);
      }
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    Assert.Equal(50000, reader.getNumberOfRows());
    Assert.Equal(0, reader.getRowIndexStride());
    StripeInformation stripe = reader.getStripes().iterator().next();
    Assert.Equal(true, stripe.getDataLength() != 0);
    Assert.Equal(0, stripe.getIndexLength());
    RecordReader rows = reader.rows();
    rand = new Random(24);
    OrcStruct row = null;
    for(int i=0; i < 10000; ++i) {
      int intVal = rand.nextInt();
      String strVal = Integer.toBinaryString(rand.nextInt());
      for(int j=0; j < 5; ++j) {
        Assert.Equal(true, rows.hasNext());
        row = (OrcStruct) rows.next(row);
        Assert.Equal(intVal, ((IntWritable) row.getFieldValue(0)).get());
        Assert.Equal(strVal, row.getFieldValue(1).toString());
      }
    }
    Assert.Equal(false, rows.hasNext());
    rows.close();
  }

  [Fact]
  public void testSeek()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(200000)
                                         .bufferSize(65536)
                                         .rowIndexStride(1000));
    Random rand = new Random(42);
    final int COUNT=32768;
    long[] intValues= new long[COUNT];
    double[] doubleValues = new double[COUNT];
    String[] stringValues = new String[COUNT];
    BytesWritable[] byteValues = new BytesWritable[COUNT];
    String[] words = new String[128];
    for(int i=0; i < words.length; ++i) {
      words[i] = Integer.toHexString(rand.nextInt());
    }
    for(int i=0; i < COUNT/2; ++i) {
      intValues[2*i] = rand.nextLong();
      intValues[2*i+1] = intValues[2*i];
      stringValues[2*i] = words[rand.nextInt(words.length)];
      stringValues[2*i+1] = stringValues[2*i];
    }
    for(int i=0; i < COUNT; ++i) {
      doubleValues[i] = rand.nextDouble();
      byte[] buf = new byte[20];
      rand.nextBytes(buf);
      byteValues[i] = new BytesWritable(buf);
    }
    for(int i=0; i < COUNT; ++i) {
      writer.addRow(createRandomRow(intValues, doubleValues, stringValues,
          byteValues, words, i));
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    Assert.Equal(COUNT, reader.getNumberOfRows());
    RecordReader rows = reader.rows();
    OrcStruct row = null;
    for(int i=COUNT-1; i >= 0; --i) {
      rows.seekToRow(i);
      row = (OrcStruct) rows.next(row);
      BigRow expected = createRandomRow(intValues, doubleValues,
          stringValues, byteValues, words, i);
      Assert.Equal(expected.boolean1.booleanValue(),
          ((BooleanWritable) row.getFieldValue(0)).get());
      Assert.Equal(expected.byte1.byteValue(),
          ((ByteWritable) row.getFieldValue(1)).get());
      Assert.Equal(expected.short1.shortValue(),
          ((ShortWritable) row.getFieldValue(2)).get());
      Assert.Equal(expected.int1.intValue(),
          ((IntWritable) row.getFieldValue(3)).get());
      Assert.Equal(expected.long1.longValue(),
          ((LongWritable) row.getFieldValue(4)).get());
      Assert.Equal(expected.float1,
          ((FloatWritable) row.getFieldValue(5)).get(), 0.0001);
      Assert.Equal(expected.double1,
          ((DoubleWritable) row.getFieldValue(6)).get(), 0.0001);
      Assert.Equal(expected.bytes1, row.getFieldValue(7));
      Assert.Equal(expected.string1, row.getFieldValue(8));
      List<InnerStruct> expectedList = expected.middle.list;
      List<OrcStruct> actualList =
          (List<OrcStruct>) ((OrcStruct) row.getFieldValue(9)).getFieldValue(0);
      compareList(expectedList, actualList);
      compareList(expected.list, (List<OrcStruct>) row.getFieldValue(10));
    }
    rows.close();
    Iterator<StripeInformation> stripeIterator =
      reader.getStripes().iterator();
    long offsetOfStripe2 = 0;
    long offsetOfStripe4 = 0;
    long lastRowOfStripe2 = 0;
    for(int i = 0; i < 5; ++i) {
      StripeInformation stripe = stripeIterator.next();
      if (i < 2) {
        lastRowOfStripe2 += stripe.getNumberOfRows();
      } else if (i == 2) {
        offsetOfStripe2 = stripe.getOffset();
        lastRowOfStripe2 += stripe.getNumberOfRows() - 1;
      } else if (i == 4) {
        offsetOfStripe4 = stripe.getOffset();
      }
    }
    boolean[] columns = new boolean[reader.getStatistics().length];
    columns[5] = true; // long colulmn
    columns[9] = true; // text column
    rows = reader.rowsOptions(new Reader.Options()
        .range(offsetOfStripe2, offsetOfStripe4 - offsetOfStripe2)
        .include(columns));
    rows.seekToRow(lastRowOfStripe2);
    for(int i = 0; i < 2; ++i) {
      row = (OrcStruct) rows.next(row);
      BigRow expected = createRandomRow(intValues, doubleValues,
                                        stringValues, byteValues, words,
                                        (int) (lastRowOfStripe2 + i));

      Assert.Equal(expected.long1.longValue(),
          ((LongWritable) row.getFieldValue(4)).get());
      Assert.Equal(expected.string1, row.getFieldValue(8));
    }
    rows.close();
  }

  [Fact]
  public void testZeroCopySeek()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(200000)
                                         .bufferSize(65536)
                                         .rowIndexStride(1000));
    Random rand = new Random(42);
    final int COUNT=32768;
    long[] intValues= new long[COUNT];
    double[] doubleValues = new double[COUNT];
    String[] stringValues = new String[COUNT];
    BytesWritable[] byteValues = new BytesWritable[COUNT];
    String[] words = new String[128];
    for(int i=0; i < words.length; ++i) {
      words[i] = Integer.toHexString(rand.nextInt());
    }
    for(int i=0; i < COUNT/2; ++i) {
      intValues[2*i] = rand.nextLong();
      intValues[2*i+1] = intValues[2*i];
      stringValues[2*i] = words[rand.nextInt(words.length)];
      stringValues[2*i+1] = stringValues[2*i];
    }
    for(int i=0; i < COUNT; ++i) {
      doubleValues[i] = rand.nextDouble();
      byte[] buf = new byte[20];
      rand.nextBytes(buf);
      byteValues[i] = new BytesWritable(buf);
    }
    for(int i=0; i < COUNT; ++i) {
      writer.addRow(createRandomRow(intValues, doubleValues, stringValues,
          byteValues, words, i));
    }
    writer.close();
    writer = null;
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    Assert.Equal(COUNT, reader.getNumberOfRows());
    /* enable zero copy record reader */
    Configuration conf = new Configuration();
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ORC_ZEROCOPY, true);
    RecordReader rows = reader.rows();
    /* all tests are identical to the other seek() tests */
    OrcStruct row = null;
    for(int i=COUNT-1; i >= 0; --i) {
      rows.seekToRow(i);
      row = (OrcStruct) rows.next(row);
      BigRow expected = createRandomRow(intValues, doubleValues,
          stringValues, byteValues, words, i);
      Assert.Equal(expected.boolean1.booleanValue(),
          ((BooleanWritable) row.getFieldValue(0)).get());
      Assert.Equal(expected.byte1.byteValue(),
          ((ByteWritable) row.getFieldValue(1)).get());
      Assert.Equal(expected.short1.shortValue(),
          ((ShortWritable) row.getFieldValue(2)).get());
      Assert.Equal(expected.int1.intValue(),
          ((IntWritable) row.getFieldValue(3)).get());
      Assert.Equal(expected.long1.longValue(),
          ((LongWritable) row.getFieldValue(4)).get());
      Assert.Equal(expected.float1.floatValue(),
          ((FloatWritable) row.getFieldValue(5)).get(), 0.0001);
      Assert.Equal(expected.double1.doubleValue(),
          ((DoubleWritable) row.getFieldValue(6)).get(), 0.0001);
      Assert.Equal(expected.bytes1, row.getFieldValue(7));
      Assert.Equal(expected.string1, row.getFieldValue(8));
      List<InnerStruct> expectedList = expected.middle.list;
      List<OrcStruct> actualList =
          (List) ((OrcStruct) row.getFieldValue(9)).getFieldValue(0);
      compareList(expectedList, actualList);
      compareList(expected.list, (List) row.getFieldValue(10));
    }
    rows.close();
    Iterator<StripeInformation> stripeIterator =
      reader.getStripes().iterator();
    long offsetOfStripe2 = 0;
    long offsetOfStripe4 = 0;
    long lastRowOfStripe2 = 0;
    for(int i = 0; i < 5; ++i) {
      StripeInformation stripe = stripeIterator.next();
      if (i < 2) {
        lastRowOfStripe2 += stripe.getNumberOfRows();
      } else if (i == 2) {
        offsetOfStripe2 = stripe.getOffset();
        lastRowOfStripe2 += stripe.getNumberOfRows() - 1;
      } else if (i == 4) {
        offsetOfStripe4 = stripe.getOffset();
      }
    }
    boolean[] columns = new boolean[reader.getStatistics().length];
    columns[5] = true; // long colulmn
    columns[9] = true; // text column
    /* use zero copy record reader */
    rows = reader.rowsOptions(new Reader.Options()
        .range(offsetOfStripe2, offsetOfStripe4 - offsetOfStripe2)
        .include(columns));
    rows.seekToRow(lastRowOfStripe2);
    for(int i = 0; i < 2; ++i) {
      row = (OrcStruct) rows.next(row);
      BigRow expected = createRandomRow(intValues, doubleValues,
                                        stringValues, byteValues, words,
                                        (int) (lastRowOfStripe2 + i));

      Assert.Equal(expected.long1.longValue(),
          ((LongWritable) row.getFieldValue(4)).get());
      Assert.Equal(expected.string1, row.getFieldValue(8));
    }
    rows.close();
  }

  private void compareInner(InnerStruct expect,
                            OrcStruct actual)  {
    if (expect == null || actual == null) {
      Assert.Equal(null, expect);
      Assert.Equal(null, actual);
    } else {
      Assert.Equal(expect.int1, ((IntWritable) actual.getFieldValue(0)).get());
      Assert.Equal(expect.string1, actual.getFieldValue(1));
    }
  }

  private void compareList(List<InnerStruct> expect,
                           List<OrcStruct> actual)  {
    Assert.Equal(expect.size(), actual.size());
    for(int j=0; j < expect.size(); ++j) {
      compareInner(expect.get(j), actual.get(j));
    }
  }

  private BigRow createRandomRow(long[] intValues, double[] doubleValues,
                                 String[] stringValues,
                                 BytesWritable[] byteValues,
                                 String[] words, int i) {
    InnerStruct inner = new InnerStruct((int) intValues[i], stringValues[i]);
    InnerStruct inner2 = new InnerStruct((int) (intValues[i] >> 32),
        words[i % words.length] + "-x");
    return new BigRow((intValues[i] & 1) == 0, (byte) intValues[i],
        (short) intValues[i], (int) intValues[i], intValues[i],
        (float) doubleValues[i], doubleValues[i], byteValues[i],stringValues[i],
        new MiddleStruct(inner, inner2), list(), map(inner,inner2));
  }

  private static class MyMemoryManager extends MemoryManager {
    final long totalSpace;
    double rate;
    Path path = null;
    long lastAllocation = 0;
    int rows = 0;
    MemoryManager.Callback callback;

    MyMemoryManager(Configuration conf, long totalSpace, double rate) {
      super(conf);
      this.totalSpace = totalSpace;
      this.rate = rate;
    }

    @Override
    void addWriter(Path path, long requestedAllocation,
                   MemoryManager.Callback callback) {
      this.path = path;
      this.lastAllocation = requestedAllocation;
      this.callback = callback;
    }

    @Override
    synchronized void removeWriter(Path path) {
      this.path = null;
      this.lastAllocation = 0;
    }

    @Override
    long getTotalMemoryPool() {
      return totalSpace;
    }

    @Override
    double getAllocationScale() {
      return rate;
    }

    @Override
    void addedRow(int count) {
      rows += count;
      if (rows % 100 == 0) {
        callback.checkMemory(rate);
      }
    }
  }

  [Fact]
  public void testMemoryManagementV11()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    MyMemoryManager memory = new MyMemoryManager(conf, 10000, 0.1);
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .compress(CompressionKind.NONE)
                                         .stripeSize(50000)
                                         .bufferSize(100)
                                         .rowIndexStride(0)
                                         .memory(memory)
                                         .version(Version.V_0_11));
    Assert.Equal(testFilePath, memory.path);
    for(int i=0; i < 2500; ++i) {
      writer.addRow(new InnerStruct(i*300, Integer.toHexString(10*i)));
    }
    writer.close();
    Assert.Equal(null, memory.path);
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    int i = 0;
    for(StripeInformation stripe: reader.getStripes()) {
      i += 1;
      assertTrue("stripe " + i + " is too long at " + stripe.getDataLength(),
          stripe.getDataLength() < 5000);
    }
    Assert.Equal(25, i);
    Assert.Equal(2500, reader.getNumberOfRows());
  }

  [Fact]
  public void testMemoryManagementV12()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    MyMemoryManager memory = new MyMemoryManager(conf, 10000, 0.1);
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .compress(CompressionKind.NONE)
                                         .stripeSize(50000)
                                         .bufferSize(100)
                                         .rowIndexStride(0)
                                         .memory(memory)
                                         .version(Version.V_0_12));
    Assert.Equal(testFilePath, memory.path);
    for(int i=0; i < 2500; ++i) {
      writer.addRow(new InnerStruct(i*300, Integer.toHexString(10*i)));
    }
    writer.close();
    Assert.Equal(null, memory.path);
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    int i = 0;
    for(StripeInformation stripe: reader.getStripes()) {
      i += 1;
      assertTrue("stripe " + i + " is too long at " + stripe.getDataLength(),
          stripe.getDataLength() < 5000);
    }
    // with HIVE-7832, the dictionaries will be disabled after writing the first
    // stripe as there are too many distinct values. Hence only 4 stripes as
    // compared to 25 stripes in version 0.11 (above test case)
    Assert.Equal(4, i);
    Assert.Equal(2500, reader.getNumberOfRows());
  }

  [Fact]
  public void testPredicatePushdown()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        400000L, CompressionKind.NONE, 500, 1000);
    for(int i=0; i < 3500; ++i) {
      writer.addRow(new InnerStruct(i*300, Integer.toHexString(10*i)));
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    Assert.Equal(3500, reader.getNumberOfRows());

    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
          .startNot()
             .lessThan("int1", PredicateLeaf.Type.LONG, 300000L)
          .end()
          .lessThan("int1", PredicateLeaf.Type.LONG, 600000L)
        .end()
        .build();
    RecordReader rows = reader.rowsOptions(new Reader.Options()
        .range(0L, Long.MAX_VALUE)
        .include(new boolean[]{true, true, true})
        .searchArgument(sarg, new String[]{null, "int1", "string1"}));
    Assert.Equal(1000L, rows.getRowNumber());
    OrcStruct row = null;
    for(int i=1000; i < 2000; ++i) {
      assertTrue(rows.hasNext());
      row = (OrcStruct) rows.next(row);
      Assert.Equal(300 * i, ((IntWritable) row.getFieldValue(0)).get());
      Assert.Equal(Integer.toHexString(10*i), row.getFieldValue(1).toString());
    }
    assertTrue(!rows.hasNext());
    Assert.Equal(3500, rows.getRowNumber());

    // look through the file with no rows selected
    sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
          .lessThan("int1", PredicateLeaf.Type.LONG, 0L)
        .end()
        .build();
    rows = reader.rowsOptions(new Reader.Options()
        .range(0L, Long.MAX_VALUE)
        .include(new boolean[]{true, true, true})
        .searchArgument(sarg, new String[]{null, "int1", "string1"}));
    Assert.Equal(3500L, rows.getRowNumber());
    assertTrue(!rows.hasNext());

    // select first 100 and last 100 rows
    sarg = SearchArgumentFactory.newBuilder()
        .startOr()
          .lessThan("int1", PredicateLeaf.Type.LONG, 300L * 100)
          .startNot()
            .lessThan("int1", PredicateLeaf.Type.LONG, 300L * 3400)
          .end()
        .end()
        .build();
    rows = reader.rowsOptions(new Reader.Options()
        .range(0L, Long.MAX_VALUE)
        .include(new boolean[]{true, true, true})
        .searchArgument(sarg, new String[]{null, "int1", "string1"}));
    row = null;
    for(int i=0; i < 1000; ++i) {
      assertTrue(rows.hasNext());
      Assert.Equal(i, rows.getRowNumber());
      row = (OrcStruct) rows.next(row);
      Assert.Equal(300 * i, ((IntWritable) row.getFieldValue(0)).get());
      Assert.Equal(Integer.toHexString(10*i), row.getFieldValue(1).toString());
    }
    for(int i=3000; i < 3500; ++i) {
      assertTrue(rows.hasNext());
      Assert.Equal(i, rows.getRowNumber());
      row = (OrcStruct) rows.next(row);
      Assert.Equal(300 * i, ((IntWritable) row.getFieldValue(0)).get());
      Assert.Equal(Integer.toHexString(10*i), row.getFieldValue(1).toString());
    }
    assertTrue(!rows.hasNext());
    Assert.Equal(3500, rows.getRowNumber());
  }
}
