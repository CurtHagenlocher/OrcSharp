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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestOrcRecordUpdater {

  [Fact]
  public void testAccessors()  {
    OrcStruct event = new OrcStruct(OrcRecordUpdater.FIELDS);
    event.setFieldValue(OrcRecordUpdater.OPERATION,
        new IntWritable(OrcRecordUpdater.INSERT_OPERATION));
    event.setFieldValue(OrcRecordUpdater.CURRENT_TRANSACTION,
        new LongWritable(100));
    event.setFieldValue(OrcRecordUpdater.ORIGINAL_TRANSACTION,
        new LongWritable(50));
    event.setFieldValue(OrcRecordUpdater.BUCKET, new IntWritable(200));
    event.setFieldValue(OrcRecordUpdater.ROW_ID, new LongWritable(300));
    Assert.Equal(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(event));
    Assert.Equal(50, OrcRecordUpdater.getOriginalTransaction(event));
    Assert.Equal(100, OrcRecordUpdater.getCurrentTransaction(event));
    Assert.Equal(200, OrcRecordUpdater.getBucket(event));
    Assert.Equal(300, OrcRecordUpdater.getRowId(event));
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  static class MyRow {
    Text field;
    RecordIdentifier ROW__ID;

    MyRow(String val) {
      field = new Text(val);
      ROW__ID = null;
    }

    MyRow(String val, long rowId, long origTxn, int bucket) {
      field = new Text(val);
      ROW__ID = new RecordIdentifier(origTxn, bucket, rowId);
    }

  }

  [Fact]
  public void testWriter()  {
    Path root = new Path(workDir, "testWriter");
    Configuration conf = new Configuration();
    // Must use raw local because the checksummer doesn't honor flushes.
    FileSystem fs = FileSystem.getLocal(conf).getRaw();
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .filesystem(fs)
        .bucket(10)
        .writingBase(false)
        .minimumTransactionId(10)
        .maximumTransactionId(19)
        .inspector(inspector)
        .reporter(Reporter.NULL)
        .finalDestination(root);
    RecordUpdater updater = new OrcRecordUpdater(root, options);
    updater.insert(11, new MyRow("first"));
    updater.insert(11, new MyRow("second"));
    updater.insert(11, new MyRow("third"));
    updater.flush();
    updater.insert(12, new MyRow("fourth"));
    updater.insert(12, new MyRow("fifth"));
    updater.flush();

    // Check the stats
    Assert.Equal(5L, updater.getStats().getRowCount());

    Path bucketPath = AcidUtils.createFilename(root, options);
    Path sidePath = OrcRecordUpdater.getSideFile(bucketPath);
    DataInputStream side = fs.open(sidePath);

    // read the stopping point for the first flush and make sure we only see
    // 3 rows
    long len = side.readLong();
    Reader reader = OrcFile.createReader(bucketPath,
        new OrcFile.ReaderOptions(conf).filesystem(fs).maxLength(len));
    Assert.Equal(3, reader.getNumberOfRows());

    // read the second flush and make sure we see all 5 rows
    len = side.readLong();
    side.close();
    reader = OrcFile.createReader(bucketPath,
        new OrcFile.ReaderOptions(conf).filesystem(fs).maxLength(len));
    Assert.Equal(5, reader.getNumberOfRows());
    RecordReader rows = reader.rows();

    // check the contents of the file
    Assert.Equal(true, rows.hasNext());
    OrcStruct row = (OrcStruct) rows.next(null);
    Assert.Equal(OrcRecordUpdater.INSERT_OPERATION,
        OrcRecordUpdater.getOperation(row));
    Assert.Equal(11, OrcRecordUpdater.getCurrentTransaction(row));
    Assert.Equal(11, OrcRecordUpdater.getOriginalTransaction(row));
    Assert.Equal(10, OrcRecordUpdater.getBucket(row));
    Assert.Equal(0, OrcRecordUpdater.getRowId(row));
    Assert.Equal("first",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    Assert.Equal(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    Assert.Equal(1, OrcRecordUpdater.getRowId(row));
    Assert.Equal(10, OrcRecordUpdater.getBucket(row));
    Assert.Equal("second",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    Assert.Equal(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    Assert.Equal(2, OrcRecordUpdater.getRowId(row));
    Assert.Equal(10, OrcRecordUpdater.getBucket(row));
    Assert.Equal("third",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    Assert.Equal(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    Assert.Equal(12, OrcRecordUpdater.getCurrentTransaction(row));
    Assert.Equal(12, OrcRecordUpdater.getOriginalTransaction(row));
    Assert.Equal(10, OrcRecordUpdater.getBucket(row));
    Assert.Equal(0, OrcRecordUpdater.getRowId(row));
    Assert.Equal("fourth",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    Assert.Equal(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    Assert.Equal(1, OrcRecordUpdater.getRowId(row));
    Assert.Equal("fifth",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    Assert.Equal(false, rows.hasNext());

    // add one more record and close
    updater.insert(20, new MyRow("sixth"));
    updater.close(false);
    reader = OrcFile.createReader(bucketPath,
        new OrcFile.ReaderOptions(conf).filesystem(fs));
    Assert.Equal(6, reader.getNumberOfRows());
    Assert.Equal(6L, updater.getStats().getRowCount());

    Assert.Equal(false, fs.exists(sidePath));
  }

  [Fact]
  public void testUpdates()  {
    Path root = new Path(workDir, "testUpdates");
    Configuration conf = new Configuration();
    FileSystem fs = root.getFileSystem(conf);
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    int bucket = 20;
    AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
        .filesystem(fs)
        .bucket(bucket)
        .writingBase(false)
        .minimumTransactionId(100)
        .maximumTransactionId(100)
        .inspector(inspector)
        .reporter(Reporter.NULL)
        .recordIdColumn(1)
        .finalDestination(root);
    RecordUpdater updater = new OrcRecordUpdater(root, options);
    updater.update(100, new MyRow("update", 30, 10, bucket));
    updater.delete(100, new MyRow("", 60, 40, bucket));
    Assert.Equal(-1L, updater.getStats().getRowCount());
    updater.close(false);
    Path bucketPath = AcidUtils.createFilename(root, options);

    Reader reader = OrcFile.createReader(bucketPath,
        new OrcFile.ReaderOptions(conf).filesystem(fs));
    Assert.Equal(2, reader.getNumberOfRows());

    RecordReader rows = reader.rows();

    // check the contents of the file
    Assert.Equal(true, rows.hasNext());
    OrcStruct row = (OrcStruct) rows.next(null);
    Assert.Equal(OrcRecordUpdater.UPDATE_OPERATION,
        OrcRecordUpdater.getOperation(row));
    Assert.Equal(100, OrcRecordUpdater.getCurrentTransaction(row));
    Assert.Equal(10, OrcRecordUpdater.getOriginalTransaction(row));
    Assert.Equal(20, OrcRecordUpdater.getBucket(row));
    Assert.Equal(30, OrcRecordUpdater.getRowId(row));
    Assert.Equal("update",
        OrcRecordUpdater.getRow(row).getFieldValue(0).toString());
    Assert.Equal(true, rows.hasNext());
    row = (OrcStruct) rows.next(null);
    Assert.Equal(100, OrcRecordUpdater.getCurrentTransaction(row));
    Assert.Equal(40, OrcRecordUpdater.getOriginalTransaction(row));
    Assert.Equal(20, OrcRecordUpdater.getBucket(row));
    Assert.Equal(60, OrcRecordUpdater.getRowId(row));
    assertNull(OrcRecordUpdater.getRow(row));
    Assert.Equal(false, rows.hasNext());
  }
}
