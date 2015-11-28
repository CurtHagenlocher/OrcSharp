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

    public class TestJsonFileDump : WithLocalDirectory
    {
        const string testFileName = "TestJsonFileDump.orc";

        public TestJsonFileDump() : base(testFileName)
        {
        }

        class MyRecord
        {
            int i;
            long l;
            string s;

            public MyRecord(int i, long l, string s)
            {
                this.i = i;
                this.l = l;
                this.s = s;
            }
        }

        [Fact]
        public void testJsonDump()
        {
            ObjectInspector inspector;
            inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(MyRecord));
            // conf.set(HiveConf.ConfVars.HIVE_ORC_ENCODING_STRATEGY.varname, "COMPRESSION");
            OrcFile.WriterOptions options = OrcFile.writerOptions(conf)
                .inspector(inspector)
                .stripeSize(100000)
                .compress(CompressionKind.ZLIB)
                .bufferSize(10000)
                .rowIndexStride(1000)
                .bloomFilterColumns("s");
            using (Stream file = File.OpenWrite(testFilePath))
            {
                Writer writer = OrcFile.createWriter(testFilePath, file, options);
                Random r1 = new Random(1);
                for (int i = 0; i < 21000; ++i)
                {
                    if (i % 100 == 0)
                    {
                        writer.addRow(new MyRecord(r1.Next(), r1.NextLong(), null));
                    }
                    else
                    {
                        writer.addRow(new MyRecord(r1.Next(), r1.NextLong(),
                            TestHelpers.words[r1.Next(TestHelpers.words.Length)]));
                    }
                }

                writer.close();
            }

            const string outputFilename = "orc-file-dump.json";
            using (CaptureStdout capture = new CaptureStdout(Path.Combine(workDir, outputFilename)))
            {
                FileDump.Main(new string[] { testFilePath.ToString(), "-j", "-p", "--rowindex=3" });
            }

            TestHelpers.CompareFilesByLine(outputFilename, Path.Combine(workDir, outputFilename));
        }
    }
}
