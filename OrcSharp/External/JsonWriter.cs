/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
    using System.Text;

    class JsonWriter
    {
        private MemoryStream buffer;
        private TextWriter @out;
        private bool first = false;

        public JsonWriter()
        {
            buffer = new MemoryStream();
            @out = new StreamWriter(this.buffer);
        }

        public JsonWriter(TextWriter @out)
        {
            // TODO: Complete member initialization
            this.@out = @out;
        }

        public void array()
        {
            @out.Write('[');
            first = true;
        }

        public JsonWriter key(string value)
        {
            First();
            WriteQuotedString(value);
            @out.Write(':');
            return this;
        }

        public void endArray()
        {
            @out.Write(']');
        }

        public void newObject()
        {
            @out.Write('{');
            first = true;
        }

        public void endObject()
        {
            @out.Write('}');
        }

        public void value(int value)
        {
            First();
            @out.Write(value);
        }

        public void value(long value)
        {
            First();
            @out.Write(value);
        }

        public void value(double value)
        {
            First();
            @out.Write(value);
        }

        public void value(string value)
        {
            First();
            if (value == null)
            {
                @out.Write("null");
            }
            else
            {
                WriteQuotedString(value);
            }
        }

        public void value(bool value)
        {
            First();
            @out.Write(value ? "true" : "false");
        }

        public override string ToString()
        {
            this.@out.Flush();
            return Encoding.UTF8.GetString(this.buffer.ToArray());
        }

        private void First()
        {
            if (first)
            {
                first = false;
            }
            else
            {
                @out.Write(',');
            }
        }

        private void WriteQuotedString(string value)
        {
            @out.Write('"');
            @out.Write(value);
            @out.Write('"');
        }
    }
}
