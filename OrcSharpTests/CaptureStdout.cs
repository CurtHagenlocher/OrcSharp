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
