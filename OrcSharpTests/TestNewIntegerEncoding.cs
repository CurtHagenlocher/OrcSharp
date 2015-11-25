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
    using Xunit;

//@RunWith(value = Parameterized.class)
public class TestNewIntegerEncoding {

  private EncodingStrategy encodingStrategy;

  public TestNewIntegerEncoding(EncodingStrategy es) {
    this.encodingStrategy = es;
  }

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { EncodingStrategy.COMPRESSION },
        { EncodingStrategy.SPEED } };
    return Arrays.asList(data);
  }

  public static class TSRow {
    Timestamp ts;

    public TSRow(Timestamp ts) {
      this.ts = ts;
    }
  }

  public static class Row {
    Integer int1;
    Long long1;

    public Row(int val, long l) {
      this.int1 = val;
      this.long1 = l;
    }
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir", "target"
      + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem()  {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile."
        + testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  [Fact]
  public void testBasicRow()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Row.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    writer.addRow(new Row(111, 1111L));
    writer.addRow(new Row(111, 1111L));
    writer.addRow(new Row(111, 1111L));
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(new IntWritable(111), ((OrcStruct) row).getFieldValue(0));
      Assert.Equal(new LongWritable(1111), ((OrcStruct) row).getFieldValue(1));
    }
  }

  [Fact]
  public void testBasicOld()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    long[] inp = new long[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6,
        7, 8, 9, 10, 1, 1, 1, 1, 1, 1, 10, 9, 7, 6, 5, 4, 3, 2, 1, 1, 1, 1, 1,
        2, 5, 1, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1,
        9, 2, 6, 3, 7, 1, 9, 2, 6, 2000, 2, 1, 1, 1, 1, 1, 3, 7, 1, 9, 2, 6, 1,
        1, 1, 1, 1 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .compress(CompressionKind.NONE)
                                         .version(OrcFile.Version.V_0_11)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testBasicNew()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    long[] inp = new long[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6,
        7, 8, 9, 10, 1, 1, 1, 1, 1, 1, 10, 9, 7, 6, 5, 4, 3, 2, 1, 1, 1, 1, 1,
        2, 5, 1, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1,
        9, 2, 6, 3, 7, 1, 9, 2, 6, 2000, 2, 1, 1, 1, 1, 1, 3, 7, 1, 9, 2, 6, 1,
        1, 1, 1, 1 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }
  
  [Fact]
  public void testBasicDelta1()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    long[] inp = new long[] { -500, -400, -350, -325, -310 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testBasicDelta2()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    long[] inp = new long[] { -500, -600, -650, -675, -710 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testBasicDelta3()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    long[] inp = new long[] { 500, 400, 350, 325, 310 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testBasicDelta4()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    long[] inp = new long[] { 500, 600, 650, 675, 710 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testDeltaOverflow()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory
          .getReflectionObjectInspector(Long.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    long[] inp = new long[]{4513343538618202719l, 4513343538618202711l,
        2911390882471569739l,
        -9181829309989854913l};
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(10000));
    for (Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile
        .createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testDeltaOverflow2()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory
          .getReflectionObjectInspector(Long.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    long[] inp = new long[]{Long.MAX_VALUE, 4513343538618202711l,
        2911390882471569739l,
        Long.MIN_VALUE};
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(10000));
    for (Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile
        .createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testDeltaOverflow3()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory
          .getReflectionObjectInspector(Long.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    long[] inp = new long[]{-4513343538618202711l, -2911390882471569739l, -2,
        Long.MAX_VALUE};
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(10000));
    for (Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile
        .createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testIntegerMin()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    input.add((long) Integer.MIN_VALUE);

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testIntegerMax()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    input.add((long) Integer.MAX_VALUE);

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testLongMin()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    input.add(Long.MIN_VALUE);

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testLongMax()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    input.add(Long.MAX_VALUE);

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testRandomInt()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 100000; i++) {
      input.add((long) rand.nextInt());
    }

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testRandomLong()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 100000; i++) {
      input.add(rand.nextLong());
    }

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBaseNegativeMin()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    long[] inp = new long[] { 20, 2, 3, 2, 1, 3, 17, 71, 35, 2, 1, 139, 2, 2,
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
        2, 16 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBaseNegativeMin2()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    long[] inp = new long[] { 20, 2, 3, 2, 1, 3, 17, 71, 35, 2, 1, 139, 2, 2,
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
        2, 16 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBaseNegativeMin3()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    long[] inp = new long[] { 20, 2, 3, 2, 1, 3, 17, 71, 35, 2, 1, 139, 2, 2,
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
        2, 16 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBaseNegativeMin4()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    long[] inp = new long[] { 13, 13, 11, 8, 13, 10, 10, 11, 11, 14, 11, 7, 13,
        12, 12, 11, 15, 12, 12, 9, 8, 10, 13, 11, 8, 6, 5, 6, 11, 7, 15, 10, 7,
        6, 8, 7, 9, 9, 11, 33, 11, 3, 7, 4, 6, 10, 14, 12, 5, 14, 7, 6 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBaseAt0()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(100));
    }
    input.set(0, 20000L);

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBaseAt1()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(100));
    }
    input.set(1, 20000L);

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBaseAt255()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(100));
    }
    input.set(255, 20000L);

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBaseAt256()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(100));
    }
    input.set(256, 20000L);

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBase510()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(100));
    }
    input.set(510, 20000L);

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBase511()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(100));
    }
    input.set(511, 20000L);

    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .stripeSize(100000)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBaseMax1()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for (int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(60));
    }
    input.set(511, Long.MAX_VALUE);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
        .inspector(inspector)
        .stripeSize(100000)
        .bufferSize(10000)
        .encodingStrategy(encodingStrategy));
    for (Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBaseMax2()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for (int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(60));
    }
    input.set(128, Long.MAX_VALUE);
    input.set(256, Long.MAX_VALUE);
    input.set(511, Long.MAX_VALUE);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
        .inspector(inspector)
        .stripeSize(100000)
        .bufferSize(10000)
        .encodingStrategy(encodingStrategy));
    for (Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBaseMax3()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    input.add(371946367L);
    input.add(11963367L);
    input.add(68639400007L);
    input.add(100233367L);
    input.add(6367L);
    input.add(10026367L);
    input.add(3670000L);
    input.add(3602367L);
    input.add(4719226367L);
    input.add(7196367L);
    input.add(444442L);
    input.add(210267L);
    input.add(21033L);
    input.add(160267L);
    input.add(400267L);
    input.add(23634347L);
    input.add(16027L);
    input.add(46026367L);
    input.add(Long.MAX_VALUE);
    input.add(33333L);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
        .inspector(inspector)
        .stripeSize(100000)
        .bufferSize(10000)
        .encodingStrategy(encodingStrategy));
    for (Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBaseMax4()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    for (int i = 0; i < 25; i++) {
      input.add(371292224226367L);
      input.add(119622332222267L);
      input.add(686329400222007L);
      input.add(100233333222367L);
      input.add(636272333322222L);
      input.add(10202633223267L);
      input.add(36700222022230L);
      input.add(36023226224227L);
      input.add(47192226364427L);
      input.add(71963622222447L);
      input.add(22244444222222L);
      input.add(21220263327442L);
      input.add(21032233332232L);
      input.add(16026322232227L);
      input.add(40022262272212L);
      input.add(23634342227222L);
      input.add(16022222222227L);
      input.add(46026362222227L);
      input.add(46026362222227L);
      input.add(33322222222323L);
    }
    input.add(Long.MAX_VALUE);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
        .inspector(inspector)
        .stripeSize(100000)
        .bufferSize(10000)
        .encodingStrategy(encodingStrategy));
    for (Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

  [Fact]
  public void testPatchedBaseTimestamp()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(TSRow.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
        .inspector(inspector)
        .stripeSize(100000)
        .bufferSize(10000)
        .encodingStrategy(encodingStrategy));

    List<Timestamp> tslist = Lists.newArrayList();
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

    for (Timestamp ts : tslist) {
      writer.addRow(new TSRow(ts));
    }

    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(tslist.get(idx++).getNanos(),
          ((TimestampWritable) ((OrcStruct) row).getFieldValue(0)).getNanos());
    }
  }

  [Fact]
  public void testDirectLargeNegatives()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
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

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    Object row = rows.next(null);
    Assert.Equal(-7486502418706614742L, ((LongWritable) row).get());
    row = rows.next(row);
    Assert.Equal(0L, ((LongWritable) row).get());
    row = rows.next(row);
    Assert.Equal(1L, ((LongWritable) row).get());
    row = rows.next(row);
    Assert.Equal(1L, ((LongWritable) row).get());
    row = rows.next(row);
    Assert.Equal(-5535739865598783616L, ((LongWritable) row).get());
  }

  [Fact]
  public void testSeek()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(
          Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 100000; i++) {
      input.add((long) rand.nextInt());
    }
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .inspector(inspector)
                                         .compress(CompressionKind.NONE)
                                         .stripeSize(100000)
                                         .bufferSize(10000)
                                         .version(OrcFile.Version.V_0_11)
                                         .encodingStrategy(encodingStrategy));
    for(Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 55555;
    rows.seekToRow(idx);
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }
}