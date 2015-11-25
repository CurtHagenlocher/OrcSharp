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
    using System;
using Xunit;

@RunWith(value = Parameterized.class)
public class TestUnrolledBitPack {

  private long val;

  public TestUnrolledBitPack(long val) {
    this.val = val;
  }

  @Parameters
  public static Collection<object[]> data() {
    object[][] data = new object[][] { { -1 }, { 1 }, { 7 }, { -128 }, { 32000 }, { 8300000 },
        { Int32.MaxValue }, { 540000000000L }, { 140000000000000L }, { 36000000000000000L },
        { Int64.MaxValue } };
    return Arrays.asList(data);
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir", "target" + File.separator + "test"
      + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem()  {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." + testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  [Fact]
  public void testBitPacking()  {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    long[] inp = new long[] { val, 0, val, val, 0, val, 0, val, val, 0, val, 0, val, val, 0, 0,
        val, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val,
        0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0,
        0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0, val, 0, val, 0, 0, val, 0,
        val, 0, val, 0, 0, val, 0, val, 0, 0, val, val };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(10000));
    for (Long l : input) {
      writer.addRow(l);
    }
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.hasNext()) {
      Object row = rows.next(null);
      Assert.Equal(input.get(idx++).longValue(), ((LongWritable) row).get());
    }
  }

}
