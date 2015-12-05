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

namespace OrcSharp
{
    using System;
    using System.IO;
    using OrcSharp.External;
    using OrcSharp.Serialization;
    using Xunit;

    public class TestWriter : IDisposable
    {
        const string filename = "test.orc";

        public void Dispose()
        {
            try
            {
                File.Delete(filename);
            }
            catch (IOException)
            {
            }
        }

        [Fact]
        public void SimpleTest()
        {
            OrcFile.WriterOptions options = new OrcFile.WriterOptions(new Properties(), new Configuration());
            options.inspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
            using (Stream file = File.Create(filename))
            using (Writer writer = OrcFile.createWriter(filename, file, options))
            {
                writer.addRow("hello");
            }

            Reader reader = OrcFile.createReader(() => File.OpenRead(filename), filename);
            using (RecordReader recordReader = reader.rows())
            {
                object value = recordReader.next();
                Assert.True(value is string);
                Assert.Equal("hello", value);
            }
        }
    }
}
