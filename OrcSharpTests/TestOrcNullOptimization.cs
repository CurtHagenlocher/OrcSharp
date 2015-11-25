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

namespace org.apache.hadoop.hive.ql.io.orc
{
    using System;
    using System.Collections.Generic;
    using Xunit;
    using OrcProto = global::orc.proto;

    public class TestOrcNullOptimization
    {

        public class MyStruct
        {
            int? a;
            string b;
            bool? c;
            List<InnerStruct> list = new List<InnerStruct>();

            public MyStruct(int? a, string b, bool? c, List<InnerStruct> l)
            {
                this.a = a;
                this.b = b;
                this.c = c;
                this.list = l;
            }
        }

        public class InnerStruct
        {
            int z;

            public InnerStruct(int z)
            {
                this.z = z;
            }
        }

        Path workDir = new Path(System.getProperty("test.tmp.dir",
            "target" + File.separator + "test" + File.separator + "tmp"));

        Configuration conf;
        FileSystem fs;
        Path testFilePath;

        [Rule]
        public TestName testCaseName = new TestName();

        [Before]
        public void openFileSystem()
        {
            conf = new Configuration();
            fs = FileSystem.getLocal(conf);
            testFilePath = new Path(workDir, "TestOrcNullOptimization." +
                testCaseName.getMethodName() + ".orc");
            fs.delete(testFilePath, false);
        }

        [Fact]
        public void testMultiStripeWithNull()  {
    ObjectInspector inspector;
    lock (typeof(TestOrcNullOptimization)) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (typeof(MyStruct), ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000));
    Random rand = new Random(100);
    writer.addRow(new MyStruct(null, null, true,
                               new List<InnerStruct> { new InnerStruct(100) }));
    for (int i = 2; i < 20000; i++) {
      writer.addRow(new MyStruct(rand.Next(1), "a", true, new List<InnerStruct> { new InnerStruct(100)}));
    }
    writer.addRow(new MyStruct(null, null, true,
                               new List<InnerStruct> { new InnerStruct(100)}));
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    Assert.Equal(20000, reader.getNumberOfRows());
    Assert.Equal(20000, stats[0].getNumberOfValues());

    Assert.Equal(0, ((IntegerColumnStatistics) stats[1]).getMaximum());
    Assert.Equal(0, ((IntegerColumnStatistics) stats[1]).getMinimum());
    Assert.Equal(true, ((IntegerColumnStatistics) stats[1]).isSumDefined());
    Assert.Equal(0, ((IntegerColumnStatistics) stats[1]).getSum());
    Assert.Equal("count: 19998 hasNull: true min: 0 max: 0 sum: 0",
        stats[1].ToString());

    Assert.Equal("a", ((StringColumnStatistics) stats[2]).getMaximum());
    Assert.Equal("a", ((StringColumnStatistics) stats[2]).getMinimum());
    Assert.Equal(19998, stats[2].getNumberOfValues());
    Assert.Equal("count: 19998 hasNull: true min: a max: a sum: 19998",
        stats[2].ToString());

    // check the inspectors
    StructObjectInspector readerInspector =
        (StructObjectInspector) reader.getObjectInspector();
    Assert.Equal(ObjectInspector.Category.STRUCT,
        readerInspector.getCategory());
    Assert.Equal("struct<a:int,b:string,c:boolean,list:array<struct<z:int>>>",
        readerInspector.getTypeName());

    RecordReader rows = reader.rows();

    List<bool> expected = new List<bool>();
    foreach (StripeInformation sinfo in reader.getStripes()) {
      expected.Add(false);
    }
    // only the first and last stripe will have PRESENT stream
    expected[0] = true;
    expected[expected.Count - 1] = true;

    List<Boolean> got = Lists.newArrayList();
    // check if the strip footer contains PRESENT stream
    foreach (StripeInformation sinfo in reader.getStripes()) {
      OrcProto.StripeFooter sf =
        ((RecordReaderImpl) rows).readStripeFooter(sinfo);
      got.Add(sf.ToString().indexOf(OrcProto.Stream.Kind.PRESENT.toString())
              != -1);
    }
    Assert.Equal(expected, got);

    // row 1
    OrcStruct row = (OrcStruct) rows.next(null);
    assertNotNull(row);
    assertNull(row.getFieldValue(0));
    assertNull(row.getFieldValue(1));
    Assert.Equal(new BooleanWritable(true), row.getFieldValue(2));
    Assert.Equal(new IntWritable(100),
        ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                 getFieldValue(0));

    rows.seekToRow(19998);
    // last-1 row
    row = (OrcStruct) rows.next(null);
    assertNotNull(row);
    assertNotNull(row.getFieldValue(1));
    Assert.Equal(new IntWritable(0), row.getFieldValue(0));
    Assert.Equal(new BooleanWritable(true), row.getFieldValue(2));
    Assert.Equal(new IntWritable(100),
        ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                 getFieldValue(0));

    // last row
    row = (OrcStruct) rows.next(row);
    assertNotNull(row);
    assertNull(row.getFieldValue(0));
    assertNull(row.getFieldValue(1));
    Assert.Equal(new BooleanWritable(true), row.getFieldValue(2));
    Assert.Equal(new IntWritable(100),
        ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                 getFieldValue(0));

    rows.close();
  }

        [Fact]
        public void testMultiStripeWithoutNull()  {
    ObjectInspector inspector;
    lock (typeof(TestOrcNullOptimization)) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (typeof(MyStruct), ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000));
    Random rand = new Random(100);
    for (int i = 1; i < 20000; i++) {
      writer.addRow(new MyStruct(rand.Next(1), "a", true, Lists
          .newArrayList(new InnerStruct(100))));
    }
    writer.addRow(new MyStruct(0, "b", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    Assert.Equal(20000, reader.getNumberOfRows());
    Assert.Equal(20000, stats[0].getNumberOfValues());

    Assert.Equal(0, ((IntegerColumnStatistics) stats[1]).getMaximum());
    Assert.Equal(0, ((IntegerColumnStatistics) stats[1]).getMinimum());
    Assert.Equal(true, ((IntegerColumnStatistics) stats[1]).isSumDefined());
    Assert.Equal(0, ((IntegerColumnStatistics) stats[1]).getSum());
    Assert.Equal("count: 20000 hasNull: false min: 0 max: 0 sum: 0",
        stats[1].ToString());

    Assert.Equal("b", ((StringColumnStatistics) stats[2]).getMaximum());
    Assert.Equal("a", ((StringColumnStatistics) stats[2]).getMinimum());
    Assert.Equal(20000, stats[2].getNumberOfValues());
    Assert.Equal("count: 20000 hasNull: false min: a max: b sum: 20000",
        stats[2].ToString());

    // check the inspectors
    StructObjectInspector readerInspector =
        (StructObjectInspector) reader.getObjectInspector();
    Assert.Equal(ObjectInspector.Category.STRUCT,
        readerInspector.getCategory());
    Assert.Equal("struct<a:int,b:string,c:boolean,list:array<struct<z:int>>>",
        readerInspector.getTypeName());

    RecordReader rows = reader.rows();

    // none of the stripes will have PRESENT stream
    List<bool> expected = Lists.newArrayList();
    foreach (StripeInformation sinfo in reader.getStripes()) {
      expected.Add(false);
    }

    List<bool> got = Lists.newArrayList();
    // check if the strip footer contains PRESENT stream
    foreach (StripeInformation sinfo in reader.getStripes()) {
      OrcProto.StripeFooter sf =
        ((RecordReaderImpl) rows).readStripeFooter(sinfo);
      got.Add(sf.ToString().indexOf(OrcProto.Stream.Kind.PRESENT.toString())
              != -1);
    }
    Assert.Equal(expected, got);

    rows.seekToRow(19998);
    // last-1 row
    OrcStruct row = (OrcStruct) rows.next(null);
    assertNotNull(row);
    assertNotNull(row.getFieldValue(1));
    Assert.Equal(new IntWritable(0), row.getFieldValue(0));
    Assert.Equal("a", row.getFieldValue(1).toString());
    Assert.Equal(new BooleanWritable(true), row.getFieldValue(2));
    Assert.Equal(new IntWritable(100),
                 ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                   getFieldValue(0));

    // last row
    row = (OrcStruct) rows.next(row);
    assertNotNull(row);
    assertNotNull(row.getFieldValue(0));
    assertNotNull(row.getFieldValue(1));
    Assert.Equal("b", row.getFieldValue(1).toString());
    Assert.Equal(new BooleanWritable(true), row.getFieldValue(2));
    Assert.Equal(new IntWritable(100),
                 ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                   getFieldValue(0));
    rows.close();
  }

        [Fact]
        public void testColumnsWithNullAndCompression()  {
    ObjectInspector inspector;
    lock (typeof(TestOrcNullOptimization)) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (typeof(MyStruct), ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .bufferSize(10000));
    writer.addRow(new MyStruct(3, "a", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.addRow(new MyStruct(null, "b", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.addRow(new MyStruct(3, null, false,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.addRow(new MyStruct(3, "d", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.addRow(new MyStruct(2, "e", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.addRow(new MyStruct(2, "f", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.addRow(new MyStruct(2, "g", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.addRow(new MyStruct(2, "h", true,
                               Lists.newArrayList(new InnerStruct(100))));
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    Assert.Equal(8, reader.getNumberOfRows());
    Assert.Equal(8, stats[0].getNumberOfValues());

    Assert.Equal(3, ((IntegerColumnStatistics) stats[1]).getMaximum());
    Assert.Equal(2, ((IntegerColumnStatistics) stats[1]).getMinimum());
    Assert.Equal(true, ((IntegerColumnStatistics) stats[1]).isSumDefined());
    Assert.Equal(17, ((IntegerColumnStatistics) stats[1]).getSum());
    Assert.Equal("count: 7 hasNull: true min: 2 max: 3 sum: 17",
        stats[1].toString());

    Assert.Equal("h", ((StringColumnStatistics) stats[2]).getMaximum());
    Assert.Equal("a", ((StringColumnStatistics) stats[2]).getMinimum());
    Assert.Equal(7, stats[2].getNumberOfValues());
    Assert.Equal("count: 7 hasNull: true min: a max: h sum: 7",
        stats[2].toString());

    // check the inspectors
    StructObjectInspector readerInspector =
        (StructObjectInspector) reader.getObjectInspector();
    Assert.Equal(ObjectInspector.Category.STRUCT,
        readerInspector.getCategory());
    Assert.Equal("struct<a:int,b:string,c:boolean,list:array<struct<z:int>>>",
        readerInspector.getTypeName());

    RecordReader rows = reader.rows();
    // only the last strip will have PRESENT stream
    List<Boolean> expected = Lists.newArrayList();
    foreach (StripeInformation sinfo in reader.getStripes()) {
      expected.add(false);
    }
    expected.set(expected.size() - 1, true);

    List<Boolean> got = Lists.newArrayList();
    // check if the strip footer contains PRESENT stream
    foreach (StripeInformation sinfo in reader.getStripes()) {
      OrcProto.StripeFooter sf =
        ((RecordReaderImpl) rows).readStripeFooter(sinfo);
      got.add(sf.toString().indexOf(OrcProto.Stream.Kind.PRESENT.toString())
              != -1);
    }
    Assert.Equal(expected, got);

    // row 1
    OrcStruct row = (OrcStruct) rows.next(null);
    assertNotNull(row);
    Assert.Equal(new IntWritable(3), row.getFieldValue(0));
    Assert.Equal("a", row.getFieldValue(1).toString());
    Assert.Equal(new BooleanWritable(true), row.getFieldValue(2));
    Assert.Equal(new IntWritable(100),
        ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                 getFieldValue(0));

    // row 2
    row = (OrcStruct) rows.next(row);
    assertNotNull(row);
    assertNull(row.getFieldValue(0));
    Assert.Equal("b", row.getFieldValue(1).toString());
    Assert.Equal(new BooleanWritable(true), row.getFieldValue(2));
    Assert.Equal(new IntWritable(100),
        ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                 getFieldValue(0));

    // row 3
    row = (OrcStruct) rows.next(row);
    assertNotNull(row);
    assertNull(row.getFieldValue(1));
    Assert.Equal(new IntWritable(3), row.getFieldValue(0));
    Assert.Equal(new BooleanWritable(false), row.getFieldValue(2));
    Assert.Equal(new IntWritable(100),
                 ((OrcStruct) ((ArrayList<?>) row.getFieldValue(3)).get(0)).
                 getFieldValue(0));
    rows.close();
  }
    }
}