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
    using System.Globalization;
    using Xunit;

    /**
     *
     */
    public class TestBloomFilter
    {
        private const int COUNT = 100;
        Random rand = new Random(123);

        [Fact]
        public void testBloomIllegalArgs()
        {
            Assert.Throws<ArgumentException>(() => new BloomFilter(0, 0));
            Assert.Throws<ArgumentException>(() => new BloomFilter(0, 0.1));
            Assert.Throws<ArgumentException>(() => new BloomFilter(1, 0.0));
            Assert.Throws<ArgumentException>(() => new BloomFilter(1, 1.0));
            Assert.Throws<ArgumentException>(() => new BloomFilter(-1, -1));
        }

        [Fact]
        public void testBloomNumBits()
        {
            Assert.Equal(0, BloomFilter.optimalNumOfBits(0, 0));
            Assert.Equal(0, BloomFilter.optimalNumOfBits(0, 1));
            Assert.Equal(0, BloomFilter.optimalNumOfBits(1, 1));
            Assert.Equal(7, BloomFilter.optimalNumOfBits(1, 0.03));
            Assert.Equal(72, BloomFilter.optimalNumOfBits(10, 0.03));
            Assert.Equal(729, BloomFilter.optimalNumOfBits(100, 0.03));
            Assert.Equal(7298, BloomFilter.optimalNumOfBits(1000, 0.03));
            Assert.Equal(72984, BloomFilter.optimalNumOfBits(10000, 0.03));
            Assert.Equal(729844, BloomFilter.optimalNumOfBits(100000, 0.03));
            Assert.Equal(7298440, BloomFilter.optimalNumOfBits(1000000, 0.03));
            Assert.Equal(6235224, BloomFilter.optimalNumOfBits(1000000, 0.05));
        }

        [Fact]
        public void testBloomNumHashFunctions()
        {
            Assert.Equal(1, BloomFilter.optimalNumOfHashFunctions(-1, -1));
            Assert.Equal(1, BloomFilter.optimalNumOfHashFunctions(0, 0));
            Assert.Equal(1, BloomFilter.optimalNumOfHashFunctions(10, 0));
            Assert.Equal(1, BloomFilter.optimalNumOfHashFunctions(10, 10));
            Assert.Equal(7, BloomFilter.optimalNumOfHashFunctions(10, 100));
            Assert.Equal(1, BloomFilter.optimalNumOfHashFunctions(100, 100));
            Assert.Equal(1, BloomFilter.optimalNumOfHashFunctions(1000, 100));
            Assert.Equal(1, BloomFilter.optimalNumOfHashFunctions(10000, 100));
            Assert.Equal(1, BloomFilter.optimalNumOfHashFunctions(100000, 100));
            Assert.Equal(1, BloomFilter.optimalNumOfHashFunctions(1000000, 100));
        }

        [Fact]
        public void testBloomFilterBytes()
        {
            BloomFilter bf = new BloomFilter(10000);
            byte[] val = new byte[] { 1, 2, 3 };
            byte[] val1 = new byte[] { 1, 2, 3, 4 };
            byte[] val2 = new byte[] { 1, 2, 3, 4, 5 };
            byte[] val3 = new byte[] { 1, 2, 3, 4, 5, 6 };

            Assert.Equal(false, bf.test(val));
            Assert.Equal(false, bf.test(val1));
            Assert.Equal(false, bf.test(val2));
            Assert.Equal(false, bf.test(val3));
            bf.add(val);
            Assert.Equal(true, bf.test(val));
            Assert.Equal(false, bf.test(val1));
            Assert.Equal(false, bf.test(val2));
            Assert.Equal(false, bf.test(val3));
            bf.add(val1);
            Assert.Equal(true, bf.test(val));
            Assert.Equal(true, bf.test(val1));
            Assert.Equal(false, bf.test(val2));
            Assert.Equal(false, bf.test(val3));
            bf.add(val2);
            Assert.Equal(true, bf.test(val));
            Assert.Equal(true, bf.test(val1));
            Assert.Equal(true, bf.test(val2));
            Assert.Equal(false, bf.test(val3));
            bf.add(val3);
            Assert.Equal(true, bf.test(val));
            Assert.Equal(true, bf.test(val1));
            Assert.Equal(true, bf.test(val2));
            Assert.Equal(true, bf.test(val3));

            byte[] randVal = new byte[COUNT];
            for (int i = 0; i < COUNT; i++)
            {
                rand.NextBytes(randVal);
                bf.add(randVal);
            }
            // last value should be present
            Assert.Equal(true, bf.test(randVal));
            // most likely this value should not exist
            randVal[0] = 0;
            randVal[1] = 0;
            randVal[2] = 0;
            randVal[3] = 0;
            randVal[4] = 0;
            Assert.Equal(false, bf.test(randVal));

            Assert.Equal(7800, bf.sizeInBytes());
        }

        [Fact]
        public void testBloomFilterByte()
        {
            BloomFilter bf = new BloomFilter(10000);
            byte val = Byte.MinValue;
            byte val1 = 1;
            byte val2 = 2;
            byte val3 = Byte.MaxValue;

            Assert.Equal(false, bf.testLong(val));
            Assert.Equal(false, bf.testLong(val1));
            Assert.Equal(false, bf.testLong(val2));
            Assert.Equal(false, bf.testLong(val3));
            bf.addLong(val);
            Assert.Equal(true, bf.testLong(val));
            Assert.Equal(false, bf.testLong(val1));
            Assert.Equal(false, bf.testLong(val2));
            Assert.Equal(false, bf.testLong(val3));
            bf.addLong(val1);
            Assert.Equal(true, bf.testLong(val));
            Assert.Equal(true, bf.testLong(val1));
            Assert.Equal(false, bf.testLong(val2));
            Assert.Equal(false, bf.testLong(val3));
            bf.addLong(val2);
            Assert.Equal(true, bf.testLong(val));
            Assert.Equal(true, bf.testLong(val1));
            Assert.Equal(true, bf.testLong(val2));
            Assert.Equal(false, bf.testLong(val3));
            bf.addLong(val3);
            Assert.Equal(true, bf.testLong(val));
            Assert.Equal(true, bf.testLong(val1));
            Assert.Equal(true, bf.testLong(val2));
            Assert.Equal(true, bf.testLong(val3));

            byte randVal = 0;
            for (int i = 0; i < COUNT; i++)
            {
                randVal = (byte)rand.Next(Byte.MaxValue);
                bf.addLong(randVal);
            }
            // last value should be present
            Assert.Equal(true, bf.testLong(randVal));
            // most likely this value should not exist
            Assert.Equal(false, bf.testLong(unchecked((byte)-120)));

            Assert.Equal(7800, bf.sizeInBytes());
        }

        [Fact]
        public void testBloomFilterInt()
        {
            BloomFilter bf = new BloomFilter(10000);
            int val = Int32.MinValue;
            int val1 = 1;
            int val2 = 2;
            int val3 = Int32.MaxValue;

            Assert.Equal(false, bf.testLong(val));
            Assert.Equal(false, bf.testLong(val1));
            Assert.Equal(false, bf.testLong(val2));
            Assert.Equal(false, bf.testLong(val3));
            bf.addLong(val);
            Assert.Equal(true, bf.testLong(val));
            Assert.Equal(false, bf.testLong(val1));
            Assert.Equal(false, bf.testLong(val2));
            Assert.Equal(false, bf.testLong(val3));
            bf.addLong(val1);
            Assert.Equal(true, bf.testLong(val));
            Assert.Equal(true, bf.testLong(val1));
            Assert.Equal(false, bf.testLong(val2));
            Assert.Equal(false, bf.testLong(val3));
            bf.addLong(val2);
            Assert.Equal(true, bf.testLong(val));
            Assert.Equal(true, bf.testLong(val1));
            Assert.Equal(true, bf.testLong(val2));
            Assert.Equal(false, bf.testLong(val3));
            bf.addLong(val3);
            Assert.Equal(true, bf.testLong(val));
            Assert.Equal(true, bf.testLong(val1));
            Assert.Equal(true, bf.testLong(val2));
            Assert.Equal(true, bf.testLong(val3));

            int randVal = 0;
            for (int i = 0; i < COUNT; i++)
            {
                randVal = rand.Next();
                bf.addLong(randVal);
            }
            // last value should be present
            Assert.Equal(true, bf.testLong(randVal));
            // most likely this value should not exist
            Assert.Equal(false, bf.testLong(-120));

            Assert.Equal(7800, bf.sizeInBytes());
        }

        [Fact]
        public void testBloomFilterLong()
        {
            BloomFilter bf = new BloomFilter(10000);
            long val = Int64.MinValue;
            long val1 = 1;
            long val2 = 2;
            long val3 = Int64.MaxValue;

            Assert.Equal(false, bf.testLong(val));
            Assert.Equal(false, bf.testLong(val1));
            Assert.Equal(false, bf.testLong(val2));
            Assert.Equal(false, bf.testLong(val3));
            bf.addLong(val);
            Assert.Equal(true, bf.testLong(val));
            Assert.Equal(false, bf.testLong(val1));
            Assert.Equal(false, bf.testLong(val2));
            Assert.Equal(false, bf.testLong(val3));
            bf.addLong(val1);
            Assert.Equal(true, bf.testLong(val));
            Assert.Equal(true, bf.testLong(val1));
            Assert.Equal(false, bf.testLong(val2));
            Assert.Equal(false, bf.testLong(val3));
            bf.addLong(val2);
            Assert.Equal(true, bf.testLong(val));
            Assert.Equal(true, bf.testLong(val1));
            Assert.Equal(true, bf.testLong(val2));
            Assert.Equal(false, bf.testLong(val3));
            bf.addLong(val3);
            Assert.Equal(true, bf.testLong(val));
            Assert.Equal(true, bf.testLong(val1));
            Assert.Equal(true, bf.testLong(val2));
            Assert.Equal(true, bf.testLong(val3));

            long randVal = 0;
            for (int i = 0; i < COUNT; i++)
            {
                randVal = rand.NextLong();
                bf.addLong(randVal);
            }
            // last value should be present
            Assert.Equal(true, bf.testLong(randVal));
            // most likely this value should not exist
            Assert.Equal(false, bf.testLong(-120));

            Assert.Equal(7800, bf.sizeInBytes());
        }

        [Fact]
        public void testBloomFilterFloat()
        {
            BloomFilter bf = new BloomFilter(10000);
            float val = Single.MinValue;
            float val1 = 1.1f;
            float val2 = 2.2f;
            float val3 = Single.MaxValue;

            Assert.Equal(false, bf.testDouble(val));
            Assert.Equal(false, bf.testDouble(val1));
            Assert.Equal(false, bf.testDouble(val2));
            Assert.Equal(false, bf.testDouble(val3));
            bf.addDouble(val);
            Assert.Equal(true, bf.testDouble(val));
            Assert.Equal(false, bf.testDouble(val1));
            Assert.Equal(false, bf.testDouble(val2));
            Assert.Equal(false, bf.testDouble(val3));
            bf.addDouble(val1);
            Assert.Equal(true, bf.testDouble(val));
            Assert.Equal(true, bf.testDouble(val1));
            Assert.Equal(false, bf.testDouble(val2));
            Assert.Equal(false, bf.testDouble(val3));
            bf.addDouble(val2);
            Assert.Equal(true, bf.testDouble(val));
            Assert.Equal(true, bf.testDouble(val1));
            Assert.Equal(true, bf.testDouble(val2));
            Assert.Equal(false, bf.testDouble(val3));
            bf.addDouble(val3);
            Assert.Equal(true, bf.testDouble(val));
            Assert.Equal(true, bf.testDouble(val1));
            Assert.Equal(true, bf.testDouble(val2));
            Assert.Equal(true, bf.testDouble(val3));

            float randVal = 0;
            for (int i = 0; i < COUNT; i++)
            {
                randVal = rand.NextFloat();
                bf.addDouble(randVal);
            }
            // last value should be present
            Assert.Equal(true, bf.testDouble(randVal));
            // most likely this value should not exist
            Assert.Equal(false, bf.testDouble(-120.2f));

            Assert.Equal(7800, bf.sizeInBytes());
        }

        [Fact]
        public void testBloomFilterDouble()
        {
            BloomFilter bf = new BloomFilter(10000);
            double val = Double.MinValue;
            double val1 = 1.1d;
            double val2 = 2.2d;
            double val3 = Double.MaxValue;

            Assert.Equal(false, bf.testDouble(val));
            Assert.Equal(false, bf.testDouble(val1));
            Assert.Equal(false, bf.testDouble(val2));
            Assert.Equal(false, bf.testDouble(val3));
            bf.addDouble(val);
            Assert.Equal(true, bf.testDouble(val));
            Assert.Equal(false, bf.testDouble(val1));
            Assert.Equal(false, bf.testDouble(val2));
            Assert.Equal(false, bf.testDouble(val3));
            bf.addDouble(val1);
            Assert.Equal(true, bf.testDouble(val));
            Assert.Equal(true, bf.testDouble(val1));
            Assert.Equal(false, bf.testDouble(val2));
            Assert.Equal(false, bf.testDouble(val3));
            bf.addDouble(val2);
            Assert.Equal(true, bf.testDouble(val));
            Assert.Equal(true, bf.testDouble(val1));
            Assert.Equal(true, bf.testDouble(val2));
            Assert.Equal(false, bf.testDouble(val3));
            bf.addDouble(val3);
            Assert.Equal(true, bf.testDouble(val));
            Assert.Equal(true, bf.testDouble(val1));
            Assert.Equal(true, bf.testDouble(val2));
            Assert.Equal(true, bf.testDouble(val3));

            double randVal = 0;
            for (int i = 0; i < COUNT; i++)
            {
                randVal = rand.NextDouble();
                bf.addDouble(randVal);
            }
            // last value should be present
            Assert.Equal(true, bf.testDouble(randVal));
            // most likely this value should not exist
            Assert.Equal(false, bf.testDouble(-120.2d));

            Assert.Equal(7800, bf.sizeInBytes());
        }

        [Fact]
        public void testBloomFilterString()
        {
            BloomFilter bf = new BloomFilter(100000);
            String val = "bloo";
            String val1 = "bloom fil";
            String val2 = "bloom filter";
            String val3 = "cuckoo filter";

            Assert.Equal(false, bf.testString(val));
            Assert.Equal(false, bf.testString(val1));
            Assert.Equal(false, bf.testString(val2));
            Assert.Equal(false, bf.testString(val3));
            bf.addString(val);
            Assert.Equal(true, bf.testString(val));
            Assert.Equal(false, bf.testString(val1));
            Assert.Equal(false, bf.testString(val2));
            Assert.Equal(false, bf.testString(val3));
            bf.addString(val1);
            Assert.Equal(true, bf.testString(val));
            Assert.Equal(true, bf.testString(val1));
            Assert.Equal(false, bf.testString(val2));
            Assert.Equal(false, bf.testString(val3));
            bf.addString(val2);
            Assert.Equal(true, bf.testString(val));
            Assert.Equal(true, bf.testString(val1));
            Assert.Equal(true, bf.testString(val2));
            Assert.Equal(false, bf.testString(val3));
            bf.addString(val3);
            Assert.Equal(true, bf.testString(val));
            Assert.Equal(true, bf.testString(val1));
            Assert.Equal(true, bf.testString(val2));
            Assert.Equal(true, bf.testString(val3));

            long randVal = 0;
            for (int i = 0; i < COUNT; i++)
            {
                randVal = rand.NextLong();
                bf.addString(randVal.ToString(CultureInfo.InvariantCulture));
            }
            // last value should be present
            Assert.Equal(true, bf.testString(randVal.ToString(CultureInfo.InvariantCulture)));
            // most likely this value should not exist
            Assert.Equal(false, bf.testString((-120L).ToString(CultureInfo.InvariantCulture)));

            Assert.Equal(77944, bf.sizeInBytes());
        }

        [Fact]
        public void testMerge()
        {
            BloomFilter bf = new BloomFilter(10000);
            String val = "bloo";
            String val1 = "bloom fil";
            String val2 = "bloom filter";
            String val3 = "cuckoo filter";
            bf.addString(val);
            bf.addString(val1);
            bf.addString(val2);
            bf.addString(val3);

            BloomFilter bf2 = new BloomFilter(10000);
            String v = "2_bloo";
            String v1 = "2_bloom fil";
            String v2 = "2_bloom filter";
            String v3 = "2_cuckoo filter";
            bf2.addString(v);
            bf2.addString(v1);
            bf2.addString(v2);
            bf2.addString(v3);

            Assert.Equal(true, bf.testString(val));
            Assert.Equal(true, bf.testString(val1));
            Assert.Equal(true, bf.testString(val2));
            Assert.Equal(true, bf.testString(val3));
            Assert.Equal(false, bf.testString(v));
            Assert.Equal(false, bf.testString(v1));
            Assert.Equal(false, bf.testString(v2));
            Assert.Equal(false, bf.testString(v3));

            bf.merge(bf2);

            Assert.Equal(true, bf.testString(val));
            Assert.Equal(true, bf.testString(val1));
            Assert.Equal(true, bf.testString(val2));
            Assert.Equal(true, bf.testString(val3));
            Assert.Equal(true, bf.testString(v));
            Assert.Equal(true, bf.testString(v1));
            Assert.Equal(true, bf.testString(v2));
            Assert.Equal(true, bf.testString(v3));
        }
    }
}
