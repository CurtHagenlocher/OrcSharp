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

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.HiveTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.google.common.collect.Lists;

public class TestOrcSerDeStats {

  public static class ListStruct {
    List<String> list1;

    public ListStruct(List<String> l1) {
      this.list1 = l1;
    }
  }

  public static class MapStruct {
    Map<String, Double> map1;

    public MapStruct(Map<String, Double> m1) {
      this.map1 = m1;
    }
  }

  public static class SimpleStruct {
    BytesWritable bytes1;
    Text string1;

    SimpleStruct(BytesWritable b1, String s1) {
      this.bytes1 = b1;
      if (s1 == null) {
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
    List<InnerStruct> list = new ArrayList<InnerStruct>();
    Map<Text, InnerStruct> map = new HashMap<Text, InnerStruct>();
    Timestamp ts;
    HiveDecimal decimal1;
    MiddleStruct middle;

    BigRow(Boolean b1, Byte b2, Short s1, Integer i1, Long l1, Float f1,
        Double d1,
        BytesWritable b3, String s2, MiddleStruct m1,
        List<InnerStruct> l2, Map<Text, InnerStruct> m2, Timestamp ts1,
        HiveDecimal dec1) {
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
      this.ts = ts1;
      this.decimal1 = dec1;
    }
  }

  private static InnerStruct inner(int i, String s) {
    return new InnerStruct(i, s);
  }

  private static Map<Text, InnerStruct> map(InnerStruct... items) {
    Map<Text, InnerStruct> result = new HashMap<Text, InnerStruct>();
    for (InnerStruct i : items) {
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
    for (int i = 0; i < items.length; ++i) {
      result.getBytes()[i] = (byte) items[i];
    }
    return result;
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem()  {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcSerDeStats." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  [Fact]
  public void testStringAndBinaryStatistics()  {

    ObjectInspector inspector;
    synchronized (TestOrcSerDeStats.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (SimpleStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .inspector(inspector)
            .stripeSize(100000)
            .bufferSize(10000));
    writer.addRow(new SimpleStruct(bytes(0, 1, 2, 3, 4), "foo"));
    writer.addRow(new SimpleStruct(bytes(0, 1, 2, 3), "bar"));
    writer.addRow(new SimpleStruct(bytes(0, 1, 2, 3, 4, 5), null));
    writer.addRow(new SimpleStruct(null, "hi"));
    writer.close();
    Assert.Equal(4, writer.getNumberOfRows());
    Assert.Equal(273, writer.getRawDataSize());
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    Assert.Equal(4, reader.getNumberOfRows());
    Assert.Equal(273, reader.getRawDataSize());
    Assert.Equal(15, reader.getRawDataSizeOfColumns(Lists.newArrayList("bytes1")));
    Assert.Equal(258, reader.getRawDataSizeOfColumns(Lists.newArrayList("string1")));
    Assert.Equal(273, reader.getRawDataSizeOfColumns(Lists.newArrayList("bytes1", "string1")));

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
    Assert.Equal(bytes(0, 1, 2, 3, 4), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(0))));
    Assert.Equal("foo", st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(1))));

    // check the contents of second row
    Assert.Equal(true, rows.hasNext());
    row = rows.next(row);
    Assert.Equal(bytes(0, 1, 2, 3), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(0))));
    Assert.Equal("bar", st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(1))));

    // check the contents of second row
    Assert.Equal(true, rows.hasNext());
    row = rows.next(row);
    Assert.Equal(bytes(0, 1, 2, 3, 4, 5), bi.getPrimitiveWritableObject(
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
  public void testOrcSerDeStatsList()  {
    ObjectInspector inspector;
    synchronized (TestOrcSerDeStats.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (ListStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .inspector(inspector)
            .stripeSize(10000)
            .bufferSize(10000));
    for (int row = 0; row < 5000; row++) {
      List<String> test = new ArrayList<String>();
      for (int i = 0; i < 1000; i++) {
        test.add("hi");
      }
      writer.addRow(new ListStruct(test));
    }
    writer.close();
    Assert.Equal(5000, writer.getNumberOfRows());
    Assert.Equal(430000000, writer.getRawDataSize());

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    // stats from reader
    Assert.Equal(5000, reader.getNumberOfRows());
    Assert.Equal(430000000, reader.getRawDataSize());
    Assert.Equal(430000000, reader.getRawDataSizeOfColumns(Lists.newArrayList("list1")));
  }

  [Fact]
  public void testOrcSerDeStatsMap()  {
    ObjectInspector inspector;
    synchronized (TestOrcSerDeStats.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MapStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .inspector(inspector)
            .stripeSize(10000)
            .bufferSize(10000));
    for (int row = 0; row < 1000; row++) {
      Map<String, Double> test = new HashMap<String, Double>();
      for (int i = 0; i < 10; i++) {
        test.put("hi" + i, 2.0);
      }
      writer.addRow(new MapStruct(test));
    }
    writer.close();
    // stats from writer
    Assert.Equal(1000, writer.getNumberOfRows());
    Assert.Equal(950000, writer.getRawDataSize());

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    // stats from reader
    Assert.Equal(1000, reader.getNumberOfRows());
    Assert.Equal(950000, reader.getRawDataSize());
    Assert.Equal(950000, reader.getRawDataSizeOfColumns(Lists.newArrayList("map1")));
  }

  [Fact]
  public void testOrcSerDeStatsSimpleWithNulls()  {
    ObjectInspector inspector;
    synchronized (TestOrcSerDeStats.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (SimpleStruct.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .inspector(inspector)
            .stripeSize(10000)
            .bufferSize(10000));
    for (int row = 0; row < 1000; row++) {
      if (row % 2 == 0) {
        writer.addRow(new SimpleStruct(new BytesWritable(new byte[] {1, 2, 3}), "hi"));
      } else {
        writer.addRow(null);
      }
    }
    writer.close();
    // stats from writer
    Assert.Equal(1000, writer.getNumberOfRows());
    Assert.Equal(44500, writer.getRawDataSize());

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    // stats from reader
    Assert.Equal(1000, reader.getNumberOfRows());
    Assert.Equal(44500, reader.getRawDataSize());
    Assert.Equal(1500, reader.getRawDataSizeOfColumns(Lists.newArrayList("bytes1")));
    Assert.Equal(43000, reader.getRawDataSizeOfColumns(Lists.newArrayList("string1")));
    Assert.Equal(44500, reader.getRawDataSizeOfColumns(Lists.newArrayList("bytes1", "string1")));
  }

  [Fact]
  public void testOrcSerDeStatsComplex()  {
    ObjectInspector inspector;
    synchronized (TestOrcSerDeStats.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .inspector(inspector)
            .stripeSize(100000)
            .bufferSize(10000));
    // 1 + 2 + 4 + 8 + 4 + 8 + 5 + 2 + 4 + 3 + 4 + 4 + 4 + 4 + 4 + 3 = 64
    writer.addRow(new BigRow(false, (byte) 1, (short) 1024, 65536,
        Long.MAX_VALUE, (float) 1.0, -15.0, bytes(0, 1, 2, 3, 4), "hi",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(3, "good"), inner(4, "bad")),
        map(), Timestamp.valueOf("2000-03-12 15:00:00"), HiveDecimal.create(
            "12345678.6547456")));
    // 1 + 2 + 4 + 8 + 4 + 8 + 3 + 4 + 3 + 4 + 4 + 4 + 3 + 4 + 2 + 4 + 3 + 5 + 4 + 5 + 7 + 4 + 7 =
    // 97
    writer.addRow(new BigRow(true, (byte) 100, (short) 2048, 65536,
        Long.MAX_VALUE, (float) 2.0, -5.0, bytes(), "bye",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(100000000, "cat"), inner(-100000, "in"), inner(1234, "hat")),
        map(inner(5, "chani"), inner(1, "mauddib")), Timestamp.valueOf("2000-03-11 15:00:00"),
        HiveDecimal.create("12345678.6547452")));
    writer.close();
    long rowCount = writer.getNumberOfRows();
    long rawDataSize = writer.getRawDataSize();
    Assert.Equal(2, rowCount);
    Assert.Equal(1740, rawDataSize);
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

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
    Assert.Equal(1, ((BooleanColumnStatistics) stats[1]).getFalseCount());
    Assert.Equal(1, ((BooleanColumnStatistics) stats[1]).getTrueCount());
    Assert.Equal("count: 2 hasNull: false true: 1", stats[1].toString());

    Assert.Equal(2048, ((IntegerColumnStatistics) stats[3]).getMaximum());
    Assert.Equal(1024, ((IntegerColumnStatistics) stats[3]).getMinimum());
    Assert.Equal(true, ((IntegerColumnStatistics) stats[3]).isSumDefined());
    Assert.Equal(3072, ((IntegerColumnStatistics) stats[3]).getSum());
    Assert.Equal("count: 2 hasNull: false min: 1024 max: 2048 sum: 3072",
        stats[3].toString());

    Assert.Equal(Long.MAX_VALUE,
        ((IntegerColumnStatistics) stats[5]).getMaximum());
    Assert.Equal(Long.MAX_VALUE,
        ((IntegerColumnStatistics) stats[5]).getMinimum());
    Assert.Equal(false, ((IntegerColumnStatistics) stats[5]).isSumDefined());
    Assert.Equal("count: 2 hasNull: false min: 9223372036854775807 max: 9223372036854775807",
        stats[5].toString());

    Assert.Equal(-15.0, ((DoubleColumnStatistics) stats[7]).getMinimum());
    Assert.Equal(-5.0, ((DoubleColumnStatistics) stats[7]).getMaximum());
    Assert.Equal(-20.0, ((DoubleColumnStatistics) stats[7]).getSum(), 0.00001);
    Assert.Equal("count: 2 hasNull: false min: -15.0 max: -5.0 sum: -20.0",
        stats[7].toString());

    Assert.Equal("count: 2 hasNull: false min: bye max: hi sum: 5", stats[9].toString());
  }

  [Fact]
  public void testOrcSerDeStatsComplexOldFormat()  {
    ObjectInspector inspector;
    synchronized (TestOrcSerDeStats.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .inspector(inspector)
            .stripeSize(100000)
            .version(OrcFile.Version.V_0_11)
            .bufferSize(10000));
    // 1 + 2 + 4 + 8 + 4 + 8 + 5 + 2 + 4 + 3 + 4 + 4 + 4 + 4 + 4 + 3 = 64
    writer.addRow(new BigRow(false, (byte) 1, (short) 1024, 65536,
        Long.MAX_VALUE, (float) 1.0, -15.0, bytes(0, 1, 2, 3, 4), "hi",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(3, "good"), inner(4, "bad")),
        map(), Timestamp.valueOf("2000-03-12 15:00:00"), HiveDecimal.create(
            "12345678.6547456")));
    // 1 + 2 + 4 + 8 + 4 + 8 + 3 + 4 + 3 + 4 + 4 + 4 + 3 + 4 + 2 + 4 + 3 + 5 + 4 + 5 + 7 + 4 + 7 =
    // 97
    writer.addRow(new BigRow(true, (byte) 100, (short) 2048, 65536,
        Long.MAX_VALUE, (float) 2.0, -5.0, bytes(), "bye",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(100000000, "cat"), inner(-100000, "in"), inner(1234, "hat")),
        map(inner(5, "chani"), inner(1, "mauddib")), Timestamp.valueOf("2000-03-11 15:00:00"),
        HiveDecimal.create("12345678.6547452")));
    writer.close();
    long rowCount = writer.getNumberOfRows();
    long rawDataSize = writer.getRawDataSize();
    Assert.Equal(2, rowCount);
    Assert.Equal(1740, rawDataSize);
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

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
    Assert.Equal(1, ((BooleanColumnStatistics) stats[1]).getFalseCount());
    Assert.Equal(1, ((BooleanColumnStatistics) stats[1]).getTrueCount());
    Assert.Equal("count: 2 hasNull: false true: 1", stats[1].toString());

    Assert.Equal(2048, ((IntegerColumnStatistics) stats[3]).getMaximum());
    Assert.Equal(1024, ((IntegerColumnStatistics) stats[3]).getMinimum());
    Assert.Equal(true, ((IntegerColumnStatistics) stats[3]).isSumDefined());
    Assert.Equal(3072, ((IntegerColumnStatistics) stats[3]).getSum());
    Assert.Equal("count: 2 hasNull: false min: 1024 max: 2048 sum: 3072",
        stats[3].toString());

    Assert.Equal(Long.MAX_VALUE,
        ((IntegerColumnStatistics) stats[5]).getMaximum());
    Assert.Equal(Long.MAX_VALUE,
        ((IntegerColumnStatistics) stats[5]).getMinimum());
    Assert.Equal(false, ((IntegerColumnStatistics) stats[5]).isSumDefined());
    Assert.Equal("count: 2 hasNull: false min: 9223372036854775807 max: 9223372036854775807",
        stats[5].toString());

    Assert.Equal(-15.0, ((DoubleColumnStatistics) stats[7]).getMinimum());
    Assert.Equal(-5.0, ((DoubleColumnStatistics) stats[7]).getMaximum());
    Assert.Equal(-20.0, ((DoubleColumnStatistics) stats[7]).getSum(), 0.00001);
    Assert.Equal("count: 2 hasNull: false min: -15.0 max: -5.0 sum: -20.0",
        stats[7].toString());

    Assert.Equal(5, ((BinaryColumnStatistics) stats[8]).getSum());
    Assert.Equal("count: 2 hasNull: false sum: 5", stats[8].toString());

    Assert.Equal("bye", ((StringColumnStatistics) stats[9]).getMinimum());
    Assert.Equal("hi", ((StringColumnStatistics) stats[9]).getMaximum());
    Assert.Equal(5, ((StringColumnStatistics) stats[9]).getSum());
    Assert.Equal("count: 2 hasNull: false min: bye max: hi sum: 5", stats[9].toString());
  }

  [Fact](expected = ClassCastException.class)
  public void testSerdeStatsOldFormat()  {
    Path oldFilePath = new Path(HiveTestUtils.getFileFromClasspath("orc-file-11-format.orc"));
    Reader reader = OrcFile.createReader(oldFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    int stripeCount = 0;
    int rowCount = 0;
    long currentOffset = -1;
    for (StripeInformation stripe : reader.getStripes()) {
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
    Assert.Equal(6300000, reader.getRawDataSize());
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

    Assert.Equal("bye", ((StringColumnStatistics) stats[9]).getMinimum());
    Assert.Equal("hi", ((StringColumnStatistics) stats[9]).getMaximum());
    Assert.Equal(0, ((StringColumnStatistics) stats[9]).getSum());
    Assert.Equal("count: 7500 hasNull: true min: bye max: hi sum: 0", stats[9].toString());

    // old orc format will not have binary statistics. toString() will show only
    // the general column statistics
    Assert.Equal("count: 7500 hasNull: true", stats[8].toString());
    // since old orc format doesn't support binary statistics,
    // this should throw ClassCastException
    Assert.Equal(5, ((BinaryColumnStatistics) stats[8]).getSum());

  }

}