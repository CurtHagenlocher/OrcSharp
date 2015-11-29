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

namespace OrcSharp.External
{
    /**
     * Murmur3 is successor to Murmur2 fast non-crytographic hash algorithms.
     *
     * Murmur3 32 and 128 bit variants.
     * 32-bit Java port of https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp#94
     * 128-bit Java port of https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp#255
     *
     * This is a public domain code with no copyrights.
     * From homepage of MurmurHash (https://code.google.com/p/smhasher/),
     * "All MurmurHash versions are public domain software, and the author disclaims all copyright
     * to their code."
     */
    public class Murmur3
    {
        // from 64-bit linear congruential generator
        public const long NULL_HASHCODE = 2862933555777941757L;

        // Constants for 32 bit variant
        private const int C1_32 = unchecked((int)0xcc9e2d51U);
        private const int C2_32 = 0x1b873593;
        private const int R1_32 = 15;
        private const int R2_32 = 13;
        private const int M_32 = 5;
        private const int N_32 = unchecked((int)0xe6546b64U);

        // Constants for 128 bit variant
        private const long C1 = unchecked((long)0x87c37b91114253d5UL);
        private const long C2 = 0x4cf5ad432745937fL;
        private const int R1 = 31;
        private const int R2 = 27;
        private const int R3 = 33;
        private const int M = 5;
        private const int N1 = 0x52dce729;
        private const int N2 = 0x38495ab5;

        private const int DEFAULT_SEED = 104729;

        /**
         * Murmur3 32-bit variant.
         *
         * @param data - input byte array
         * @return - hashcode
         */
        public static int hash32(byte[] data)
        {
            return hash32(data, data.Length, DEFAULT_SEED);
        }

        /**
         * Murmur3 32-bit variant.
         *
         * @param data   - input byte array
         * @param length - length of array
         * @param seed   - seed. (default 0)
         * @return - hashcode
         */
        public static int hash32(byte[] data, int length, int seed)
        {
            int hash = seed;
            int nblocks = length >> 2;

            // body
            for (int i = 0; i < nblocks; i++)
            {
                int i_4 = i << 2;
                int k = (data[i_4] & 0xff)
                    | ((data[i_4 + 1] & 0xff) << 8)
                    | ((data[i_4 + 2] & 0xff) << 16)
                    | ((data[i_4 + 3] & 0xff) << 24);

                // mix functions
                k *= C1_32;
                k = rotl32(k, R1_32);
                k *= C2_32;
                hash ^= k;
                hash = rotl32(hash, R2_32) * M_32 + N_32;
            }

            // tail
            int idx = nblocks << 2;
            int k1 = 0;
            switch (length - idx)
            {
                case 3:
                    k1 ^= data[idx + 2] << 16;
                    goto case 2;
                case 2:
                    k1 ^= data[idx + 1] << 8;
                    goto case 1;
                case 1:
                    k1 ^= data[idx];

                    // mix functions
                    k1 *= C1_32;
                    k1 = rotl32(k1, R1_32);
                    k1 *= C2_32;
                    hash ^= k1;
                    break;
            }

            // finalization
            hash ^= length;
            hash ^= (int)((uint)hash >> 16);
            hash *= unchecked((int)0x85ebca6bU);
            hash ^= (int)((uint)hash >> 13);
            hash *= unchecked((int)0xc2b2ae35U);
            hash ^= (int)((uint)hash >> 16);

            return hash;
        }

        /**
         * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
         *
         * @param data - input byte array
         * @return - hashcode
         */
        public static long hash64(byte[] data)
        {
            return hash64(data, 0, data.Length, DEFAULT_SEED);
        }

        public static long hash64(byte[] data, int offset, int length)
        {
            return hash64(data, offset, length, DEFAULT_SEED);
        }

        /**
         * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
         *
         * @param data   - input byte array
         * @param length - length of array
         * @param seed   - seed. (default is 0)
         * @return - hashcode
         */
        public static long hash64(byte[] data, int offset, int length, int seed)
        {
            long hash = seed;
            int nblocks = length >> 3;

            // body
            for (int i = 0; i < nblocks; i++)
            {
                int i8 = i << 3;
                long k = ((long)data[offset + i8] & 0xff)
                    | (((long)data[offset + i8 + 1] & 0xff) << 8)
                    | (((long)data[offset + i8 + 2] & 0xff) << 16)
                    | (((long)data[offset + i8 + 3] & 0xff) << 24)
                    | (((long)data[offset + i8 + 4] & 0xff) << 32)
                    | (((long)data[offset + i8 + 5] & 0xff) << 40)
                    | (((long)data[offset + i8 + 6] & 0xff) << 48)
                    | (((long)data[offset + i8 + 7] & 0xff) << 56);

                // mix functions
                k *= C1;
                k = rotl64(k, R1);
                k *= C2;
                hash ^= k;
                hash = rotl64(hash, R2) * M + N1;
            }

            // tail
            long k1 = 0;
            int tailStart = nblocks << 3;
            switch (length - tailStart)
            {
                case 7:
                    k1 ^= ((long)data[offset + tailStart + 6] & 0xff) << 48;
                    goto case 6;
                case 6:
                    k1 ^= ((long)data[offset + tailStart + 5] & 0xff) << 40;
                    goto case 5;
                case 5:
                    k1 ^= ((long)data[offset + tailStart + 4] & 0xff) << 32;
                    goto case 4;
                case 4:
                    k1 ^= ((long)data[offset + tailStart + 3] & 0xff) << 24;
                    goto case 3;
                case 3:
                    k1 ^= ((long)data[offset + tailStart + 2] & 0xff) << 16;
                    goto case 2;
                case 2:
                    k1 ^= ((long)data[offset + tailStart + 1] & 0xff) << 8;
                    goto case 1;
                case 1:
                    k1 ^= ((long)data[offset + tailStart] & 0xff);
                    k1 *= C1;
                    k1 = rotl64(k1, R1);
                    k1 *= C2;
                    hash ^= k1;
                    break;
            }

            // finalization
            hash ^= length;
            hash = fmix64(hash);

            return hash;
        }

        /**
         * Murmur3 128-bit variant.
         *
         * @param data - input byte array
         * @return - hashcode (2 longs)
         */
        public static long[] hash128(byte[] data)
        {
            return hash128(data, 0, data.Length, DEFAULT_SEED);
        }

        /**
         * Murmur3 128-bit variant.
         *
         * @param data   - input byte array
         * @param offset - the first element of array
         * @param length - length of array
         * @param seed   - seed. (default is 0)
         * @return - hashcode (2 longs)
         */
        public static long[] hash128(byte[] data, int offset, int length, int seed)
        {
            long h1 = seed;
            long h2 = seed;
            int nblocks = length >> 4;
            long k1, k2;

            // body
            for (int i = 0; i < nblocks; i++)
            {
                int i16 = i << 4;
                k1 = ((long)data[offset + i16] & 0xff)
                    | (((long)data[offset + i16 + 1] & 0xff) << 8)
                    | (((long)data[offset + i16 + 2] & 0xff) << 16)
                    | (((long)data[offset + i16 + 3] & 0xff) << 24)
                    | (((long)data[offset + i16 + 4] & 0xff) << 32)
                    | (((long)data[offset + i16 + 5] & 0xff) << 40)
                    | (((long)data[offset + i16 + 6] & 0xff) << 48)
                    | (((long)data[offset + i16 + 7] & 0xff) << 56);

                k2 = ((long)data[offset + i16 + 8] & 0xff)
                    | (((long)data[offset + i16 + 9] & 0xff) << 8)
                    | (((long)data[offset + i16 + 10] & 0xff) << 16)
                    | (((long)data[offset + i16 + 11] & 0xff) << 24)
                    | (((long)data[offset + i16 + 12] & 0xff) << 32)
                    | (((long)data[offset + i16 + 13] & 0xff) << 40)
                    | (((long)data[offset + i16 + 14] & 0xff) << 48)
                    | (((long)data[offset + i16 + 15] & 0xff) << 56);

                // mix functions for k1
                k1 *= C1;
                k1 = rotl64(k1, R1);
                k1 *= C2;
                h1 ^= k1;
                h1 = rotl64(h1, R2);
                h1 += h2;
                h1 = h1 * M + N1;

                // mix functions for k2
                k2 *= C2;
                k2 = rotl64(k2, R3);
                k2 *= C1;
                h2 ^= k2;
                h2 = rotl64(h2, R1);
                h2 += h1;
                h2 = h2 * M + N2;
            }

            // tail
            k1 = 0;
            k2 = 0;
            int tailStart = nblocks << 4;
            switch (length - tailStart)
            {
                case 15:
                    k2 ^= (long)(data[offset + tailStart + 14] & 0xff) << 48;
                    goto case 14;
                case 14:
                    k2 ^= (long)(data[offset + tailStart + 13] & 0xff) << 40;
                    goto case 13;
                case 13:
                    k2 ^= (long)(data[offset + tailStart + 12] & 0xff) << 32;
                    goto case 12;
                case 12:
                    k2 ^= (long)(data[offset + tailStart + 11] & 0xff) << 24;
                    goto case 11;
                case 11:
                    k2 ^= (long)(data[offset + tailStart + 10] & 0xff) << 16;
                    goto case 10;
                case 10:
                    k2 ^= (long)(data[offset + tailStart + 9] & 0xff) << 8;
                    goto case 9;
                case 9:
                    k2 ^= (long)(data[offset + tailStart + 8] & 0xff);
                    k2 *= C2;
                    k2 = rotl64(k2, R3);
                    k2 *= C1;
                    h2 ^= k2;
                    goto case 8;

                case 8:
                    k1 ^= (long)(data[offset + tailStart + 7] & 0xff) << 56;
                    goto case 7;
                case 7:
                    k1 ^= (long)(data[offset + tailStart + 6] & 0xff) << 48;
                    goto case 6;
                case 6:
                    k1 ^= (long)(data[offset + tailStart + 5] & 0xff) << 40;
                    goto case 5;
                case 5:
                    k1 ^= (long)(data[offset + tailStart + 4] & 0xff) << 32;
                    goto case 4;
                case 4:
                    k1 ^= (long)(data[offset + tailStart + 3] & 0xff) << 24;
                    goto case 3;
                case 3:
                    k1 ^= (long)(data[offset + tailStart + 2] & 0xff) << 16;
                    goto case 2;
                case 2:
                    k1 ^= (long)(data[offset + tailStart + 1] & 0xff) << 8;
                    goto case 1;
                case 1:
                    k1 ^= (long)(data[offset + tailStart] & 0xff);
                    k1 *= C1;
                    k1 = rotl64(k1, R1);
                    k1 *= C2;
                    h1 ^= k1;
                    break;
            }

            // finalization
            h1 ^= length;
            h2 ^= length;

            h1 += h2;
            h2 += h1;

            h1 = fmix64(h1);
            h2 = fmix64(h2);

            h1 += h2;
            h2 += h1;

            return new long[] { h1, h2 };
        }

        private static long fmix64(long h)
        {
            h ^= (long)((ulong)h >> 33);
            h *= unchecked((long)0xff51afd7ed558ccdUL);
            h ^= (long)((ulong)h >> 33);
            h *= unchecked((long)0xc4ceb9fe1a85ec53UL);
            h ^= (long)((ulong)h >> 33);
            return h;
        }

        private static int rotl32(int x, int r)
        {
            return (x << r) | (int)((uint)x >> (32 - r));
        }

        private static long rotl64(long x, int r)
        {
            return (x << r) | (long)((ulong)x >> (64 - r));
        }
    }
}
