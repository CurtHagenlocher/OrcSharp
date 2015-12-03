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

namespace OrcSharp.Types
{
    using System;
    using System.Text;
    using OrcSharp.External;

    sealed public class Text
    {
        private string value;

        internal static string decode(byte[] array, int offset, int len)
        {
            return Encoding.UTF8.GetString(array, offset, len);
        }

        public Text() : this(string.Empty)
        {
        }

        public Text(string value)
        {
            this.value = value;
        }

        public Text(Text value)
        {
            this.value = value.value;
        }

        public string Value
        {
            get { return value; }
        }

        internal void clear()
        {
            throw new NotImplementedException();
        }

        internal void enforceMaxLength(int maxLength)
        {
            if (value.Length > maxLength)
            {
                value = value.Substring(0, maxLength);
            }
        }

        internal void readWithKnownLength(InStream stream, int len)
        {
            byte[] tmp = new byte[len];
            stream.readFully(tmp, 0, len);
            value = decode(tmp, 0, len);
        }

        internal void set(string value)
        {
            this.value = value;
        }

        public override bool Equals(object obj)
        {
            return obj is Text && value == ((Text)obj).value;
        }

        public override string ToString()
        {
            return this.value;
        }
    }
}
