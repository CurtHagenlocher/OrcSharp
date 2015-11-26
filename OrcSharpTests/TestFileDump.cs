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
    using org.apache.hadoop.hive.ql.io.orc.external;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using Xunit;

    // Temporary
    public class TestOrcFile
    {
    }

    public class TestFileDump : IDisposable
    {
        static readonly string[] words = new string[]
            {
                "It", "was", "the", "best", "of", "times,",
                "it", "was", "the", "worst", "of", "times,", "it", "was", "the", "age",
                "of", "wisdom,", "it", "was", "the", "age", "of", "foolishness,", "it",
                "was", "the", "epoch", "of", "belief,", "it", "was", "the", "epoch",
                "of", "incredulity,", "it", "was", "the", "season", "of", "Light,",
                "it", "was", "the", "season", "of", "Darkness,", "it", "was", "the",
                "spring", "of", "hope,", "it", "was", "the", "winter", "of", "despair,",
                "we", "had", "everything", "before", "us,", "we", "had", "nothing",
                "before", "us,", "we", "were", "all", "going", "direct", "to",
                "Heaven,", "we", "were", "all", "going", "direct", "the", "other",
                "way"
            };

        string workDir;
        Configuration conf;
        // FileSystem fs;
        const string testFilePath = "TestFileDump.testDump.orc";

        public TestFileDump()
        {
            conf = new Configuration();
            // workDir = new Path(System.getProperty("test.tmp.dir"));
            // fs = FileSystem.getLocal(conf);
            // fs.setWorkingDirectory(workDir);
            // fs.delete(testFilePath, false);
            workDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(workDir, true);
            }
            catch (IOException)
            {
            }
        }

        internal class MyRecord
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

        internal class AllTypesRecord
        {
            internal class Struct
            {
                int i;
                string s;

                public Struct(int i, string s)
                {
                    this.i = i;
                    this.s = s;
                }
            }
            bool b;
            byte bt;
            short s;
            int i;
            long l;
            float f;
            double d;
            HiveDecimal de;
#if false
            Timestamp t;
            Date dt;
#endif
            string str;
#if false
            HiveChar c;
            HiveVarchar vc;
#endif
            Dictionary<string, string> m;
            List<int> a;
            Struct st;

            public AllTypesRecord(bool b, byte bt, short s, int i, long l, float f, double d, HiveDecimal de,
#if false
                           Timestamp t, Date dt,
#endif
                           string str,
#if false
                           HiveChar c, HiveVarchar vc,
#endif
                           Dictionary<string, string> m, List<int> a, Struct st)
            {
                this.b = b;
                this.bt = bt;
                this.s = s;
                this.i = i;
                this.l = l;
                this.f = f;
                this.d = d;
                this.de = de;
#if false
                this.t = t;
                this.dt = dt;
#endif
                this.str = str;
#if false
                this.c = c;
                this.vc = vc;
#endif
                this.m = m;
                this.a = a;
                this.st = st;
            }
        }

        static void checkOutput(string expected, string actual)
        {
            using (StreamReader eStream = File.OpenText(expected))
            using (StreamReader aStream = File.OpenText(actual))
            {
                string expectedLine = eStream.ReadLine().Trim();
                while (expectedLine != null)
                {
                    string actualLine = aStream.ReadLine().Trim();
                    System.Console.WriteLine("actual:   " + actualLine);
                    System.Console.WriteLine("expected: " + expectedLine);
                    Assert.Equal(expectedLine, actualLine);
                    expectedLine = eStream.ReadLine();
                    expectedLine = expectedLine == null ? null : expectedLine.Trim();
                }
                Assert.Null(eStream.ReadLine());
                Assert.Null(aStream.ReadLine());
            }
        }

        [Fact]
        public void testDump()
        {
            // conf.set(HiveConf.ConfVars.HIVE_ORC_ENCODING_STRATEGY.varname, "COMPRESSION");
            using (Stream file = File.OpenWrite(testFilePath))
            {
                OrcFile.WriterOptions options = new OrcFile.WriterOptions(new Properties(), conf);
                options.inspector(ObjectInspectorFactory.getReflectionObjectInspector(typeof(MyRecord)));
                options.stripeSize(100000);
                options.compress(CompressionKind.ZLIB);
                options.bufferSize(10000);
                options.rowIndexStride(1000);
                Writer writer = OrcFile.createWriter(testFilePath, file, options);
                Random r1 = new Random(1);
                for (int i = 0; i < 21000; ++i)
                {
                    writer.addRow(new MyRecord(r1.Next(), r1.NextLong(),
                        words[r1.Next(words.Length)]));
                }
                writer.close();
            }

            string outputFilename = "orc-file-dump.out";
            using (CaptureStdout capture = new CaptureStdout(Path.Combine(workDir, outputFilename)))
            {
                FileDump.Main(new string[] { testFilePath.ToString(), "--rowindex=1,2,3" });
            }

            checkOutput(outputFilename, Path.Combine(workDir, outputFilename));
        }

        [Fact]
        public void testDataDump()
        {
            using (Stream file = File.OpenWrite(testFilePath))
            {
                OrcFile.WriterOptions options = new OrcFile.WriterOptions(new Properties(), conf);
                options.inspector(ObjectInspectorFactory.getReflectionObjectInspector(typeof(AllTypesRecord)));
                options.stripeSize(100000);
                options.compress(CompressionKind.NONE);
                options.bufferSize(10000);
                options.rowIndexStride(1000);
                Writer writer = OrcFile.createWriter(testFilePath, file, options);
                Dictionary<string, string> m = new Dictionary<string, string>(2);
                m.Add("k1", "v1");
                writer.addRow(new AllTypesRecord(
                    true,
                    (byte)10,
                    (short)100,
                    1000,
                    10000L,
                    4.0f,
                    20.0,
                    HiveDecimal.Parse("4.2222"),
#if false
                new Timestamp(1416967764000L),
                new Date(1416967764000L),
#endif
                "string",
#if false
                new HiveChar("hello", 5),
                new HiveVarchar("hello", 10),
#endif
                m,
                    new List<int> { 100, 200 },
                    new AllTypesRecord.Struct(10, "foo")));
                m.Clear();
                m.Add("k3", "v3");
                writer.addRow(new AllTypesRecord(
                    false,
                    (byte)20,
                    (short)200,
                    2000,
                    20000L,
                    8.0f,
                    40.0,
                    HiveDecimal.Parse("2.2222"),
#if false
                new Timestamp(1416967364000L),
                new Date(1411967764000L),
#endif
                "abcd",
#if false
                new HiveChar("world", 5),
                new HiveVarchar("world", 10),
#endif
                m,
                    new List<int> { 200, 300 },
                    new AllTypesRecord.Struct(20, "bar")));

                writer.close();
            }

            string[] lines;
            using (MemoryStream buffer = new MemoryStream())
            using (CaptureStdout capture = new CaptureStdout(buffer))
            {
                FileDump.Main(new string[] { testFilePath.ToString(), "-d" });
                capture.Flush();

                lines = Encoding.UTF8.GetString(buffer.ToArray()).Split('\n');
            }
            Assert.Equal(2, lines.Length);

            // Don't be fooled by the big space in the middle, this line is quite long
            Assert.Equal("{\"b\":true,\"bt\":10,\"s\":100,\"i\":1000,\"l\":10000,\"f\":4,\"d\":20,\"de\":\"4.2222\",\"t\":\"2014-11-25 18:09:24\",\"dt\":\"2014-11-25\",\"str\":\"string\",\"c\":\"hello                                                                                                                                                                                                                                                          \",\"vc\":\"hello\",\"m\":[{\"_key\":\"k1\",\"_value\":\"v1\"}],\"a\":[100,200],\"st\":{\"i\":10,\"s\":\"foo\"}}", lines[0]);
            Assert.Equal("{\"b\":false,\"bt\":20,\"s\":200,\"i\":2000,\"l\":20000,\"f\":8,\"d\":40,\"de\":\"2.2222\",\"t\":\"2014-11-25 18:02:44\",\"dt\":\"2014-09-28\",\"str\":\"abcd\",\"c\":\"world                                                                                                                                                                                                                                                          \",\"vc\":\"world\",\"m\":[{\"_key\":\"k3\",\"_value\":\"v3\"}],\"a\":[200,300],\"st\":{\"i\":20,\"s\":\"bar\"}}", lines[1]);
        }

        // Test that if the fraction of rows that have distinct strings is greater than the configured
        // threshold dictionary encoding is turned off.  If dictionary encoding is turned off the length
        // of the dictionary stream for the column will be 0 in the ORC file dump.
        [Fact]
        public void testDictionaryThreshold()
        {
            // conf.set(HiveConf.ConfVars.HIVE_ORC_ENCODING_STRATEGY.varname, "COMPRESSION");
            // conf.setFloat(HiveConf.ConfVars.HIVE_ORC_DICTIONARY_KEY_SIZE_THRESHOLD.varname, 0.49f);
            using (Stream file = File.OpenWrite(testFilePath))
            {
                OrcFile.WriterOptions options = new OrcFile.WriterOptions(new Properties(), conf);
                options.inspector(ObjectInspectorFactory.getReflectionObjectInspector(typeof(MyRecord)));
                options.stripeSize(100000);
                options.compress(CompressionKind.ZLIB);
                options.bufferSize(10000);
                options.rowIndexStride(1000);
                Writer writer = OrcFile.createWriter(testFilePath, file, options);
                Random r1 = new Random(1);
                int nextInt = 0;
                for (int i = 0; i < 21000; ++i)
                {
                    // Write out the same string twice, this guarantees the fraction of rows with
                    // distinct strings is 0.5
                    if (i % 2 == 0)
                    {
                        nextInt = r1.Next(words.Length);
                        // Append the value of i to the word, this guarantees when an index or word is repeated
                        // the actual string is unique.
                        words[nextInt] += "-" + i;
                    }
                    writer.addRow(new MyRecord(r1.Next(), r1.NextLong(),
                        words[nextInt]));
                }
                writer.close();
            }

            string outputFilename = "orc-file-dump-dictionary-threshold.out";
            using (CaptureStdout capture = new CaptureStdout(Path.Combine(workDir, outputFilename)))
            {
                FileDump.Main(new string[] { testFilePath.ToString(), "--rowindex=1,2,3" });
            }

            checkOutput(outputFilename, Path.Combine(workDir, outputFilename));
        }

        [Fact]
        public void testBloomFilter()
        {
            // conf.set(HiveConf.ConfVars.HIVE_ORC_ENCODING_STRATEGY.varname, "COMPRESSION");
            using (Stream file = File.OpenWrite(testFilePath))
            {
                OrcFile.WriterOptions options = new OrcFile.WriterOptions(new Properties(), conf);
                options.inspector(ObjectInspectorFactory.getReflectionObjectInspector(typeof(MyRecord)));
                options.stripeSize(100000);
                options.compress(CompressionKind.ZLIB);
                options.bufferSize(10000);
                options.rowIndexStride(1000);
                options.bloomFilterColumns("S");
                Writer writer = OrcFile.createWriter(testFilePath, file, options);
                Random r1 = new Random(1);
                for (int i = 0; i < 21000; ++i)
                {
                    writer.addRow(new MyRecord(r1.Next(), r1.NextLong(),
                        words[r1.Next(words.Length)]));
                }
                writer.close();
            }

            string outputFilename = "orc-file-dump-bloomfilter.out";
            using (CaptureStdout capture = new CaptureStdout(Path.Combine(workDir, outputFilename)))
            {
                FileDump.Main(new string[] { testFilePath.ToString(), "--rowindex=3" });
            }

            checkOutput(outputFilename, Path.Combine(workDir, outputFilename));
        }

        [Fact]
        public void testBloomFilter2()
        {
            // conf.set(HiveConf.ConfVars.HIVE_ORC_ENCODING_STRATEGY.varname, "COMPRESSION");
            using (Stream file = File.OpenWrite(testFilePath))
            {
                OrcFile.WriterOptions options = new OrcFile.WriterOptions(new Properties(), conf);
                options.inspector(ObjectInspectorFactory.getReflectionObjectInspector(typeof(MyRecord)));
                options.stripeSize(100000);
                options.compress(CompressionKind.ZLIB);
                options.bufferSize(10000);
                options.rowIndexStride(1000);
                options.bloomFilterColumns("l");
                options.bloomFilterFpp(0.01);
                Writer writer = OrcFile.createWriter(testFilePath, file, options);
                Random r1 = new Random(1);
                for (int i = 0; i < 21000; ++i)
                {
                    writer.addRow(new MyRecord(r1.Next(), r1.NextLong(),
                        words[r1.Next(words.Length)]));
                }
                writer.close();
            }

            string outputFilename = "orc-file-dump-bloomfilter2.out";
            using (CaptureStdout capture = new CaptureStdout(Path.Combine(workDir, outputFilename)))
            {
                FileDump.Main(new string[] { testFilePath.ToString(), "--rowindex=2" });
            }

            checkOutput(outputFilename, Path.Combine(workDir, outputFilename));
        }

        class CaptureStdout : IDisposable
        {
            private TextWriter original;
            private Stream output;

            public CaptureStdout(string path)
            {
                original = System.Console.Out;
                output = File.OpenWrite(path);
                System.Console.SetOut(new StreamWriter(output));
            }

            public CaptureStdout(Stream stream)
            {
                original = System.Console.Out;
                output = stream;
                System.Console.SetOut(new StreamWriter(output));
            }

            public void Flush()
            {
                System.Console.Out.Flush();
            }

            void IDisposable.Dispose()
            {
                System.Console.Out.Flush();
                System.Console.SetOut(original);
                output.Close();
            }
        }
    }
}
