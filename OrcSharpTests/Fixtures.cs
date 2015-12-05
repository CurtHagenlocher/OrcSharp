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
    using System.Collections.Generic;

    public class SimpleStruct
    {
        internal byte[] bytes1;
        internal string string1;

        public SimpleStruct(byte[] b1, string s1)
        {
            this.bytes1 = b1;
            this.string1 = s1;
        }
    }

    public class InnerStruct
    {
        internal int int1;
        internal string string1;

        public InnerStruct(int int1, string string1)
        {
            this.int1 = int1;
            this.string1 = string1;
        }

        public override string ToString()
        {
            return "{" + int1 + ", " + string1 + "}";
        }
    }

    public class MiddleStruct
    {
        internal List<InnerStruct> list = new List<InnerStruct>();

        public MiddleStruct(params InnerStruct[] items)
        {
            list.AddRange(items);
        }
    }

    public class BigRow
    {
        internal bool boolean1;
        internal sbyte byte1;
        internal short short1;
        internal int int1;
        internal long long1;
        internal float float1;
        internal double double1;
        internal byte[] bytes1;
        internal string string1;
        internal MiddleStruct middle;
        internal List<InnerStruct> list = new List<InnerStruct>();
        internal Dictionary<string, InnerStruct> map = new Dictionary<string, InnerStruct>();

        public BigRow(bool b1, sbyte b2, short s1, int i1, long l1, float f1,
               double d1,
               byte[] b3, string s2, MiddleStruct m1,
               List<InnerStruct> l2, Dictionary<string, InnerStruct> m2)
        {
            this.boolean1 = b1;
            this.byte1 = b2;
            this.short1 = s1;
            this.int1 = i1;
            this.long1 = l1;
            this.float1 = f1;
            this.double1 = d1;
            this.bytes1 = b3;
            this.string1 = s2;
            this.middle = m1;
            this.list = l2;
            this.map = m2;
        }
    }
}
