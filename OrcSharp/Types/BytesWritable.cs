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
    using System.Globalization;
    using System.Text;

    sealed public class BytesWritable : IEquatable<BytesWritable>
    {
        private byte[] bytes;

        public BytesWritable()
        {
        }

        public BytesWritable(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public void setSize(int len)
        {
            bytes = new byte[len];
        }

        public byte[] getBytes()
        {
            return bytes;
        }

        internal int getLength()
        {
            return bytes.Length;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as BytesWritable);
        }

        public bool Equals(BytesWritable other)
        {
            if (other == null)
            {
                return false;
            }

            if (bytes == null)
            {
                return other.bytes == null;
            }

            if (other.bytes == null || bytes.Length != other.bytes.Length)
            {
                return false;
            }

            for (int i = 0; i < bytes.Length; i++)
            {
                if (bytes[i] != other.bytes[i])
                {
                    return false;
                }
            }

            return true;
        }

        public override string ToString()
        {
            StringBuilder builder = new StringBuilder(3 * bytes.Length + 1);
            for (int i = 0; i < bytes.Length; i++)
            {
                if (i > 0)
                {
                    builder.Append(' ');
                }
                builder.Append(bytes[i].ToString("x2"));
            }
            return builder.ToString();
        }
    }
}
