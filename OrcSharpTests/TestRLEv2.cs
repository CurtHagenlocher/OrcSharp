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

namespace OrcSharpTests
{
    using System;
    using System.IO;
    using OrcSharp;
    using OrcSharp.Serialization;
    using Xunit;

    public class TestRLEv2 : OrcTestBase
    {
        [Fact]
        public void testFixedDeltaZero()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(int));

            using (Stream file = File.OpenWrite(TestFilePath))
            using (Writer w = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                .compress(CompressionKind.NONE)
                .inspector(inspector)
                .rowIndexStride(0)
                .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
                .version(OrcFile.Version.V_0_12)))
            {
                for (int i = 0; i < 5120; ++i)
                {
                    w.addRow(123);
                }
            }

            using (CaptureStdoutToMemory capture = new CaptureStdoutToMemory())
            {
                FileDump.Main(TestFilePath);

                // 10 runs of 512 elements. Each run has 2 bytes header, 2 bytes base (base = 123,
                // zigzag encoded varint) and 1 byte delta (delta = 0). In total, 5 bytes per run.
                Assert.True(capture.Text.Contains("Stream: column 0 section DATA start: 3 length 50"));
            }
        }

        [Fact]
        public void testFixedDeltaOne()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(int));

            using (Stream file = File.OpenWrite(TestFilePath))
            using (Writer w = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                .compress(CompressionKind.NONE)
                .inspector(inspector)
                .rowIndexStride(0)
                .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
                .version(OrcFile.Version.V_0_12)))
            {
                for (int i = 0; i < 5120; ++i)
                {
                    w.addRow(i % 512);
                }
            }

            using (CaptureStdoutToMemory capture = new CaptureStdoutToMemory())
            {
                FileDump.Main(TestFilePath);

                // 10 runs of 512 elements. Each run has 2 bytes header, 1 byte base (base = 0)
                // and 1 byte delta (delta = 1). In total, 4 bytes per run.
                Assert.True(capture.Text.Contains("Stream: column 0 section DATA start: 3 length 40"));
            }
        }

        [Fact]
        public void testFixedDeltaOneDescending()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(int));

            using (Stream file = File.OpenWrite(TestFilePath))
            using (Writer w = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                .compress(CompressionKind.NONE)
                .inspector(inspector)
                .rowIndexStride(0)
                .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
                .version(OrcFile.Version.V_0_12)))
            {
                for (int i = 0; i < 5120; ++i)
                {
                    w.addRow(512 - (i % 512));
                }
            }

            using (CaptureStdoutToMemory capture = new CaptureStdoutToMemory())
            {
                FileDump.Main(TestFilePath);

                // 10 runs of 512 elements. Each run has 2 bytes header, 2 byte base (base = 512, zigzag + varint)
                // and 1 byte delta (delta = 1). In total, 5 bytes per run.
                Assert.True(capture.Text.Contains("Stream: column 0 section DATA start: 3 length 50"));
            }
        }

        [Fact]
        public void testFixedDeltaLarge()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(int));

            using (Stream file = File.OpenWrite(TestFilePath))
            using (Writer w = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                .compress(CompressionKind.NONE)
                .inspector(inspector)
                .rowIndexStride(0)
                .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
                .version(OrcFile.Version.V_0_12)))
            {
                for (int i = 0; i < 5120; ++i)
                {
                    w.addRow(i % 512 + ((i % 512) * 100));
                }
            }

            using (CaptureStdoutToMemory capture = new CaptureStdoutToMemory())
            {
                FileDump.Main(TestFilePath);

                // 10 runs of 512 elements. Each run has 2 bytes header, 1 byte base (base = 0)
                // and 2 bytes delta (delta = 100, zigzag encoded varint). In total, 5 bytes per run.
                Assert.True(capture.Text.Contains("Stream: column 0 section DATA start: 3 length 50"));
            }
        }

        [Fact]
        public void testFixedDeltaLargeDescending()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(int));

            using (Stream file = File.OpenWrite(TestFilePath))
            using (Writer w = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                .compress(CompressionKind.NONE)
                .inspector(inspector)
                .rowIndexStride(0)
                .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
                .version(OrcFile.Version.V_0_12)))
            {
                for (int i = 0; i < 5120; ++i)
                {
                    w.addRow((512 - i % 512) + ((i % 512) * 100));
                }
            }

            using (CaptureStdoutToMemory capture = new CaptureStdoutToMemory())
            {
                FileDump.Main(TestFilePath);

                // 10 runs of 512 elements. Each run has 2 bytes header, 2 byte base (base = 512, zigzag + varint)
                // and 2 bytes delta (delta = 100, zigzag encoded varint). In total, 6 bytes per run.
                Assert.True(capture.Text.Contains("Stream: column 0 section DATA start: 3 length 60"));
            }
        }

        [Fact]
        public void testShortRepeat()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(int));

            using (Stream file = File.OpenWrite(TestFilePath))
            using (Writer w = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                .compress(CompressionKind.NONE)
                .inspector(inspector)
                .rowIndexStride(0)
                .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
                .version(OrcFile.Version.V_0_12)))
            {
                for (int i = 0; i < 5; ++i)
                {
                    w.addRow(10);
                }
            }

            using (CaptureStdoutToMemory capture = new CaptureStdoutToMemory())
            {
                FileDump.Main(TestFilePath);

                // 1 byte header + 1 byte value
                Assert.True(capture.Text.Contains("Stream: column 0 section DATA start: 3 length 2"));
            }
        }

        [Fact]
        public void testDeltaUnknownSign()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(int));

            using (Stream file = File.OpenWrite(TestFilePath))
            using (Writer w = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                .compress(CompressionKind.NONE)
                .inspector(inspector)
                .rowIndexStride(0)
                .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
                .version(OrcFile.Version.V_0_12)))
            {
                w.addRow(0);
                for (int i = 0; i < 511; ++i)
                {
                    w.addRow(i);
                }
            }

            using (CaptureStdoutToMemory capture = new CaptureStdoutToMemory())
            {
                FileDump.Main(TestFilePath);

                // monotonicity will be undetermined for this sequence 0,0,1,2,3,...510. Hence DIRECT encoding
                // will be used. 2 bytes for header and 640 bytes for data (512 values with fixed bit of 10 bits
                // each, 5120/8 = 640). Total bytes 642
                Assert.True(capture.Text.Contains("Stream: column 0 section DATA start: 3 length 642"));
            }
        }

        [Fact]
        public void testPatchedBase()
        {
            ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(typeof(int));

            using (Stream file = File.OpenWrite(TestFilePath))
            using (Writer w = OrcFile.createWriter(TestFilePath, file, OrcFile.writerOptions(conf)
                .compress(CompressionKind.NONE)
                .inspector(inspector)
                .rowIndexStride(0)
                .encodingStrategy(OrcFile.EncodingStrategy.COMPRESSION)
                .version(OrcFile.Version.V_0_12)))
            {
                Random rand = new Random(123);
                w.addRow(10000000);
                for (int i = 0; i < 511; ++i)
                {
                    w.addRow(rand.Next(i + 1));
                }
            }

            using (CaptureStdoutToMemory capture = new CaptureStdoutToMemory())
            {
                FileDump.Main(TestFilePath);

                // use PATCHED_BASE encoding
                Assert.True(capture.Text.Contains("Stream: column 0 section DATA start: 3 length 583"));
            }
        }
    }
}
