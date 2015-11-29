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
    using System.IO;
    using org.apache.hadoop.hive.ql.io.orc.external;
    using Xunit;
    using OrcProto = global::orc.proto;

    public class TestStringDictionary : WithLocalDirectory
    {
        const string testFileName = "TestStringDictionary.orc";

        public TestStringDictionary() : base(testFileName)
        {
        }

        [Fact]
        public void testTooManyDistinct()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(string));

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000));
                for (int i = 0; i < 20000; i++)
                {
                    writer.addRow(i.ToString());
                }
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            int idx = 0;
            while (rows.hasNext())
            {
                object row = rows.next(null);
                Assert.Equal(new Text((idx++).ToString()), row);
            }

            // make sure the encoding type is correct
            foreach (StripeInformation stripe in reader.getStripes())
            {
                // hacky but does the job, this casting will work as long this test resides
                // within the same package as ORC reader
                OrcProto.StripeFooter footer = ((RecordReaderImpl)rows).readStripeFooter(stripe);
                for (int i = 0; i < footer.ColumnsCount; ++i)
                {
                    OrcProto.ColumnEncoding encoding = footer.GetColumns(i);
                    Assert.Equal(OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2, encoding.Kind);
                }
            }
        }

        [Fact]
        public void testHalfDistinct()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(string));
            int[] input = new int[20000];

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000));
                Random rand = new Random(123);
                for (int i = 0; i < 20000; i++)
                {
                    input[i] = rand.Next(10000);
                }

                for (int i = 0; i < 20000; i++)
                {
                    writer.addRow(input[i].ToString());
                }
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            int idx = 0;
            while (rows.hasNext())
            {
                object row = rows.next(null);
                Assert.Equal(new Text(input[idx++].ToString()), row);
            }

            // make sure the encoding type is correct
            foreach (StripeInformation stripe in reader.getStripes())
            {
                // hacky but does the job, this casting will work as long this test resides
                // within the same package as ORC reader
                OrcProto.StripeFooter footer = ((RecordReaderImpl)rows).readStripeFooter(stripe);
                for (int i = 0; i < footer.ColumnsCount; ++i)
                {
                    OrcProto.ColumnEncoding encoding = footer.GetColumns(i);
                    Assert.Equal(OrcProto.ColumnEncoding.Types.Kind.DICTIONARY_V2, encoding.Kind);
                }
            }
        }

        [Fact]
        public void testTooManyDistinctCheckDisabled()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(string));

            // conf.setBoolean(ConfVars.HIVE_ORC_ROW_INDEX_STRIDE_DICTIONARY_CHECK.varname, false);
            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000));
                for (int i = 0; i < 20000; i++)
                {
                    writer.addRow(i.ToString());
                }
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            int idx = 0;
            while (rows.hasNext())
            {
                object row = rows.next(null);
                Assert.Equal(new Text((idx++).ToString()), row);
            }

            // make sure the encoding type is correct
            foreach (StripeInformation stripe in reader.getStripes())
            {
                // hacky but does the job, this casting will work as long this test resides
                // within the same package as ORC reader
                OrcProto.StripeFooter footer = ((RecordReaderImpl)rows).readStripeFooter(stripe);
                for (int i = 0; i < footer.ColumnsCount; ++i)
                {
                    OrcProto.ColumnEncoding encoding = footer.GetColumns(i);
                    Assert.Equal(OrcProto.ColumnEncoding.Types.Kind.DIRECT_V2, encoding.Kind);
                }
            }
        }

        [Fact]
        public void testHalfDistinctCheckDisabled()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(string));
            int[] input = new int[20000];

            // conf.setBoolean(ConfVars.HIVE_ORC_ROW_INDEX_STRIDE_DICTIONARY_CHECK.varname, false);
            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .compress(CompressionKind.NONE)
                    .bufferSize(10000));
                Random rand = new Random(123);
                for (int i = 0; i < 20000; i++)
                {
                    input[i] = rand.Next(10000);
                }

                for (int i = 0; i < 20000; i++)
                {
                    writer.addRow(input[i].ToString());
                }
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            int idx = 0;
            while (rows.hasNext())
            {
                object row = rows.next(null);
                Assert.Equal(new Text(input[idx++].ToString()), row);
            }

            // make sure the encoding type is correct
            foreach (StripeInformation stripe in reader.getStripes())
            {
                // hacky but does the job, this casting will work as long this test resides
                // within the same package as ORC reader
                OrcProto.StripeFooter footer = ((RecordReaderImpl)rows).readStripeFooter(stripe);
                for (int i = 0; i < footer.ColumnsCount; ++i)
                {
                    OrcProto.ColumnEncoding encoding = footer.GetColumns(i);
                    Assert.Equal(OrcProto.ColumnEncoding.Types.Kind.DICTIONARY_V2, encoding.Kind);
                }
            }
        }

        [Fact]
        public void testTooManyDistinctV11AlwaysDictionary()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(string));

            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, OrcFile.writerOptions(conf)
                    .inspector(inspector)
                    .compress(CompressionKind.NONE)
                    .version(OrcFile.Version.V_0_11)
                    .bufferSize(10000));
                for (int i = 0; i < 20000; i++)
                {
                    writer.addRow(i.ToString());
                }
                writer.close();
            }

            Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            int idx = 0;
            while (rows.hasNext())
            {
                object row = rows.next(null);
                Assert.Equal(new Text((idx++).ToString()), row);
            }

            // make sure the encoding type is correct
            foreach (StripeInformation stripe in reader.getStripes())
            {
                // hacky but does the job, this casting will work as long this test resides
                // within the same package as ORC reader
                OrcProto.StripeFooter footer = ((RecordReaderImpl)rows).readStripeFooter(stripe);
                for (int i = 0; i < footer.ColumnsCount; ++i)
                {
                    OrcProto.ColumnEncoding encoding = footer.GetColumns(i);
                    Assert.Equal(OrcProto.ColumnEncoding.Types.Kind.DICTIONARY, encoding.Kind);
                }
            }
        }
    }
}
