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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using org.apache.hadoop.hive.ql.io.orc.external;

    /**
     * BloomFilter is a probabilistic data structure for set membership check. BloomFilters are
     * highly space efficient when compared to using a HashSet. Because of the probabilistic nature of
     * bloom filter false positive (element not present in bloom filter but test() says true) are
     * possible but false negatives are not possible (if element is present then test() will never
     * say false). The false positive probability is configurable (default: 5%) depending on which
     * storage requirement may increase or decrease. Lower the false positive probability greater
     * is the space requirement.
     * Bloom filters are sensitive to number of elements that will be inserted in the bloom filter.
     * During the creation of bloom filter expected number of entries must be specified. If the number
     * of insertions exceed the specified initial number of entries then false positive probability will
     * increase accordingly.
     *
     * Internally, this implementation of bloom filter uses Murmur3 fast non-cryptographic hash
     * algorithm. Although Murmur2 is slightly faster than Murmur3 in Java, it suffers from hash
     * collisions for specific sequence of repeating bytes. Check the following link for more info
     * https://code.google.com/p/smhasher/wiki/MurmurHash2Flaw
     */
    public class BloomFilter
    {
        private const int BitsPerLong = 64;
        private const double DEFAULT_FPP = 0.05;

        private static double Log2 = Math.Log(2.0);

        private readonly BitSet bitSet;
        private readonly int numBits;
        private readonly int numHashFunctions;

        public BloomFilter(IList<ulong> bitsetList, int numHashFunctions)
        {
            this.bitSet = new BitSet(bitsetList.ToArray());
            this.numHashFunctions = numHashFunctions;
            this.numBits = (int)this.bitSet.bitSize();
        }

        public BloomFilter(long expectedEntries, double fpp = DEFAULT_FPP)
        {
            if (expectedEntries <= 0)
            {
                throw new ArgumentException("expectedEntries should be > 0");
            }
            if (fpp <= 0.0 || fpp >= 1.0)
            {
                throw new ArgumentException("False positive probability should be > 0.0 & < 1.0");
            }
            int nb = optimalNumOfBits(expectedEntries, fpp);
            // make 'm' multiple of 64
            this.numBits = nb + (BitsPerLong - (nb % BitsPerLong));
            this.numHashFunctions = optimalNumOfHashFunctions(expectedEntries, numBits);
            this.bitSet = new BitSet(numBits);
        }

        /**
         * A constructor to support rebuilding the BloomFilter from a serialized representation.
         * @param bits
         * @param numBits
         * @param numFuncs
         */
        public BloomFilter(IList<ulong> bits, int numBits, int numFuncs)
        {
            bitSet = new BitSet(bits.ToArray());
            this.numBits = numBits;
            numHashFunctions = numFuncs;
        }

        internal static int optimalNumOfHashFunctions(long n, long m)
        {
            return Math.Max(1, (int)Math.Round((double)m / n * Log2));
        }

        internal static int optimalNumOfBits(long n, double p)
        {
            if (p == 0.0) return 0;
            return (int)(-n * Math.Log(p) / (Log2 * Log2));
        }

        public void add(byte[] val)
        {
            if (val == null)
            {
                addBytes(val, -1, -1);
            }
            else
            {
                addBytes(val, 0, val.Length);
            }
        }

        public void addBytes(byte[] val, int offset, int length)
        {
            // We use the trick mentioned in "Less Hashing, Same Performance: Building a Better Bloom Filter"
            // by Kirsch et.al. From abstract 'only two hash functions are necessary to effectively
            // implement a Bloom filter without any loss in the asymptotic false positive probability'

            // Lets split up 64-bit hashcode into two 32-bit hash codes and employ the technique mentioned
            // in the above paper
            long hash64 = val == null ? Murmur3.NULL_HASHCODE
                : Murmur3.hash64(val, offset, length);
            addHash((ulong)hash64);
        }

        private void addHash(ulong hash64)
        {
            int hash1 = (int)hash64;
            int hash2 = (int)(hash64 >> 32);

            for (int i = 1; i <= numHashFunctions; i++)
            {
                int combinedHash = hash1 + (i * hash2);
                // hashcode should be positive, flip all the bits if it's negative
                if (combinedHash < 0)
                {
                    combinedHash = ~combinedHash;
                }
                int pos = combinedHash % numBits;
                bitSet.set(pos);
            }
        }

        public void addString(string val)
        {
            if (val == null)
            {
                add(null);
            }
            else
            {
                add(Encoding.UTF8.GetBytes(val));
            }
        }

        public void addLong(long val)
        {
            addHash((ulong)getLongHash(val));
        }

        public void addDouble(double val)
        {
            addLong(BitConverter.DoubleToInt64Bits(val));
        }

        public bool test(byte[] val)
        {
            if (val == null)
            {
                return testBytes(val, -1, -1);
            }
            return testBytes(val, 0, val.Length);
        }

        public bool testBytes(byte[] val, int offset, int length)
        {
            long hash64 = val == null ? Murmur3.NULL_HASHCODE
                : Murmur3.hash64(val, offset, length);
            return testHash((ulong)hash64);
        }

        private bool testHash(ulong hash64)
        {
            int hash1 = (int)hash64;
            int hash2 = (int)(hash64 >> 32);

            for (int i = 1; i <= numHashFunctions; i++)
            {
                int combinedHash = hash1 + (i * hash2);
                // hashcode should be positive, flip all the bits if it's negative
                if (combinedHash < 0)
                {
                    combinedHash = ~combinedHash;
                }
                int pos = combinedHash % numBits;
                if (!bitSet.get(pos))
                {
                    return false;
                }
            }
            return true;
        }

        public bool testString(string val)
        {
            if (val == null)
            {
                return test(null);
            }
            else
            {
                return test(Encoding.UTF8.GetBytes(val));
            }
        }

        public bool testLong(long val)
        {
            return testHash((ulong)getLongHash(val));
        }

        // Thomas Wang's integer hash function
        // http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm
        private long getLongHash(long key)
        {
            key = (~key) + (key << 21); // key = (key << 21) - key - 1;
            key = key ^ (key >> 24);
            key = (key + (key << 3)) + (key << 8); // key * 265
            key = key ^ (key >> 14);
            key = (key + (key << 2)) + (key << 4); // key * 21
            key = key ^ (key >> 28);
            key = key + (key << 31);
            return key;
        }

        public bool testDouble(double val)
        {
            return testLong(BitConverter.DoubleToInt64Bits(val));
        }

        public long sizeInBytes()
        {
            return getBitSize() / 8;
        }

        public int getBitSize()
        {
            return bitSet.getData().Length * BitsPerLong;
        }

        public int getNumHashFunctions()
        {
            return numHashFunctions;
        }

        public ulong[] getBitSet()
        {
            return bitSet.getData();
        }

        public override string ToString()
        {
            return "m: " + numBits + " k: " + numHashFunctions;
        }

        /**
         * Merge the specified bloom filter with current bloom filter.
         *
         * @param that - bloom filter to merge
         */
        public void merge(BloomFilter that)
        {
            if (this != that && this.numBits == that.numBits && this.numHashFunctions == that.numHashFunctions)
            {
                this.bitSet.putAll(that.bitSet);
            }
            else
            {
                throw new ArgumentException("BloomFilters are not compatible for merging." +
                    " this - " + this.ToString() + " that - " + that.ToString());
            }
        }

        public void reset()
        {
            this.bitSet.clear();
        }

        /**
         * Bare metal bit set implementation. For performance reasons, this implementation does not check
         * for index bounds nor expand the bit set size if the specified index is greater than the size.
         */
        public class BitSet
        {
            private readonly ulong[] data;

            public BitSet(long bits)
            {
                this.data = new ulong[(int)Math.Ceiling((double)bits / (double)BitsPerLong)];
            }

            /**
             * Deserialize long array as bit set.
             *
             * @param data - bit array
             */
            public BitSet(ulong[] data)
            {
                Debug.Assert(data.Length > 0, "data length is zero!");
                this.data = data;
            }

            /**
             * Sets the bit at specified index.
             *
             * @param index - position
             */
            public void set(int index)
            {
                uint idx = (uint)index;
                data[idx >> 6] |= (1UL << index);
            }

            /**
             * Returns true if the bit is set in the specified index.
             *
             * @param index - position
             * @return - value at the bit position
             */
            public bool get(int index)
            {
                uint idx = (uint)index;
                return (data[idx >> 6] & (1UL << index)) != 0;
            }

            /**
             * Number of bits
             */
            public long bitSize()
            {
                return (long)data.Length * BitsPerLong;
            }

            public ulong[] getData()
            {
                return data;
            }

            /**
             * Combines the two BitArrays using bitwise OR.
             */
            public void putAll(BitSet array)
            {
                Debug.Assert(data.Length == array.data.Length,
                    "BitArrays must be of equal length (" + data.Length + "!= " + array.data.Length + ")");
                for (int i = 0; i < data.Length; i++)
                {
                    data[i] |= array.data[i];
                }
            }

            /**
             * Clear the bit set.
             */
            public void clear()
            {
                Array.Clear(data, 0, data.Length);
            }
        }
    }
}
