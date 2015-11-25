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

    class JSONWriter
    {
        private System.IO.TextWriter @out;

        public JSONWriter(System.IO.TextWriter @out)
        {
            // TODO: Complete member initialization
            this.@out = @out;
        }

        public void array()
        {
        }

        public JSONWriter key(string v)
        {
            throw new NotImplementedException();
        }

        public void endArray()
        {
            throw new NotImplementedException();
        }

        public void newObject()
        {
            throw new NotImplementedException();
        }

        public void endObject()
        {
            throw new NotImplementedException();
        }

        public void value(int v)
        {
            throw new NotImplementedException();
        }

        public void value(long v)
        {
            throw new NotImplementedException();
        }

        public void value(double v)
        {
            throw new NotImplementedException();
        }

        public void value(string v)
        {
            throw new NotImplementedException();
        }

        public void value(bool v)
        {
            throw new NotImplementedException();
        }
    }

    class JSONStringer : JSONWriter
    {
    }
}
