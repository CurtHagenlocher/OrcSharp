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
    using System.Diagnostics;
    using System.IO;
    using OrcSharp.External;

    public class WithLocalDirectory : IDisposable
    {
        protected readonly string workDir;
        protected readonly Configuration conf;
        protected readonly string testFilePath;

        public WithLocalDirectory(string filename)
        {
            conf = new Configuration();
            workDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(workDir);
            testFilePath = Path.Combine(workDir, filename);
        }

        public virtual void Dispose()
        {
            try
            {
                Directory.Delete(workDir, true);
            }
            catch (IOException)
            {
            }
        }

        public Stream FileOpenWrite(string path)
        {
            return new WrappedStream(path);
        }

        class WrappedStream : Stream
        {
            Stream baseStream;
            StackTrace closeTrace;

            public WrappedStream(string path)
            {
                this.baseStream = File.OpenWrite(path);
            }

            public override bool CanRead
            {
                get { return baseStream.CanRead; }
            }

            public override bool CanSeek
            {
                get { return baseStream.CanSeek; }
            }

            public override bool CanTimeout
            {
                get { return baseStream.CanTimeout; }
            }

            public override bool CanWrite
            {
                get { return baseStream.CanWrite; }
            }

            public override long Length
            {
                get { return baseStream.Length; }
            }

            public override long Position
            {
                get { return baseStream.Position; }
                set { baseStream.Position = value; }
            }

            public override int ReadTimeout
            {
                get { return baseStream.ReadTimeout; }
                set { baseStream.ReadTimeout = value; }
            }

            public override int WriteTimeout
            {
                get { return baseStream.WriteTimeout; }
                set { baseStream.WriteTimeout = value; }
            }

            public override void Close()
            {
                closeTrace = new StackTrace();
                baseStream.Close();
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    baseStream.Dispose();
                }
            }

            public override void Flush()
            {
                baseStream.Flush();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                return baseStream.Read(buffer, offset, count);
            }

            public override int ReadByte()
            {
                return baseStream.ReadByte();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                return baseStream.Seek(offset, origin);
            }

            public override void SetLength(long value)
            {
                baseStream.SetLength(value);
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                if (closeTrace != null)
                {
                    string tmp = closeTrace.ToString();
                    System.Console.WriteLine(tmp);
                }
                baseStream.Write(buffer, offset, count);
            }

            public override void WriteByte(byte value)
            {
                baseStream.WriteByte(value);
            }
        }
    }
}
