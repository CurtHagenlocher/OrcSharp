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
    using System;
    using Xunit;

    public class TestDynamicArray
    {
        [Fact]
        public void testByteArray()
        {
            DynamicByteArray dba = new DynamicByteArray(3, 10);
            dba.add((byte)0);
            dba.add((byte)1);
            dba.set(3, (byte)3);
            dba.set(2, (byte)2);
            dba.add((byte)4);
            Assert.Equal("{0,1,2,3,4}", dba.ToString());
            Assert.Equal(5, dba.size());
            byte[] val;
            val = new byte[0];
            Assert.Equal(0, dba.compare(val, 0, 0, 2, 0));
            Assert.Equal(-1, dba.compare(val, 0, 0, 2, 1));
            val = new byte[] { 3, 42 };
            Assert.Equal(1, dba.compare(val, 0, 1, 2, 0));
            Assert.Equal(1, dba.compare(val, 0, 1, 2, 1));
            Assert.Equal(0, dba.compare(val, 0, 1, 3, 1));
            Assert.Equal(-1, dba.compare(val, 0, 1, 3, 2));
            Assert.Equal(1, dba.compare(val, 0, 2, 3, 1));
            val = new byte[256];
            for (int b = -128; b < 128; ++b)
            {
                dba.add((byte)b);
                val[b + 128] = (byte)b;
            }
            Assert.Equal(0, dba.compare(val, 0, 256, 5, 256));
            Assert.Equal(1, dba.compare(val, 0, 1, 0, 1));
            Assert.Equal(1, dba.compare(val, 254, 1, 0, 1));
            Assert.Equal(1, dba.compare(val, 120, 1, 64, 1));
            val = new byte[1024];
            Random rand = new Random(1701);
            for (int i = 0; i < val.Length; ++i)
            {
                rand.NextBytes(val);
            }
            dba.add(val, 0, 1024);
            Assert.Equal(1285, dba.size());
            Assert.Equal(0, dba.compare(val, 0, 1024, 261, 1024));
        }

        [Fact]
        public void testIntArray()
        {
            DynamicIntArray dia = new DynamicIntArray(10);
            for (int i = 0; i < 10000; ++i)
            {
                dia.add(2 * i);
            }
            Assert.Equal(10000, dia.size());
            for (int i = 0; i < 10000; ++i)
            {
                Assert.Equal(2 * i, dia.get(i));
            }
            dia.clear();
            Assert.Equal(0, dia.size());
            dia.add(3);
            dia.add(12);
            dia.add(65);
            Assert.Equal("{3,12,65}", dia.ToString());
            for (int i = 0; i < 5; ++i)
            {
                dia.increment(i, 3);
            }
            Assert.Equal("{6,15,68,3,3}", dia.ToString());
        }
    }
}
