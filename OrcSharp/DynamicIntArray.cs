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
    using System.Text;

    /**
     * Dynamic int array that uses primitive types and chunks to avoid copying
     * large number of integers when it resizes.
     *
     * The motivation for this class is memory optimization, i.e. space efficient
     * storage of potentially huge arrays without good a-priori size guesses.
     *
     * The API of this class is between a primitive array and a AbstractList. It's
     * not a Collection implementation because it handles primitive types, but the
     * API could be extended to support iterators and the like.
     *
     * NOTE: Like standard Collection implementations/arrays, this class is not
     * synchronized.
     */
    sealed public class DynamicIntArray
    {
        const int DEFAULT_CHUNKSIZE = 8 * 1024;
        const int INIT_CHUNKS = 128;

        private int chunkSize;       // our allocation size
        private int[][] data;              // the real data
        private int length;                // max set element index +1
        private int initializedChunks = 0; // the number of created chunks

        public DynamicIntArray(int chunkSize = DEFAULT_CHUNKSIZE)
        {
            this.chunkSize = chunkSize;

            data = new int[INIT_CHUNKS][];
        }

        /**
         * Ensure that the given index is valid.
         */
        private void grow(int chunkIndex)
        {
            if (chunkIndex >= initializedChunks)
            {
                if (chunkIndex >= data.Length)
                {
                    int newSize = Math.Max(chunkIndex + 1, 2 * data.Length);
                    int[][] newChunk = new int[newSize][];
                    Array.Copy(data, 0, newChunk, 0, data.Length);
                    data = newChunk;
                }
                for (int i = initializedChunks; i <= chunkIndex; ++i)
                {
                    data[i] = new int[chunkSize];
                }
                initializedChunks = chunkIndex + 1;
            }
        }

        public int get(int index)
        {
            if (index >= length)
            {
                throw new IndexOutOfRangeException("Index " + index +
                                                      " is outside of 0.." +
                                                      (length - 1));
            }
            int i = index / chunkSize;
            int j = index % chunkSize;
            return data[i][j];
        }

        public void set(int index, int value)
        {
            int i = index / chunkSize;
            int j = index % chunkSize;
            grow(i);
            if (index >= length)
            {
                length = index + 1;
            }
            data[i][j] = value;
        }

        public void increment(int index, int value)
        {
            int i = index / chunkSize;
            int j = index % chunkSize;
            grow(i);
            if (index >= length)
            {
                length = index + 1;
            }
            data[i][j] += value;
        }

        public void add(int value)
        {
            int i = length / chunkSize;
            int j = length % chunkSize;
            grow(i);
            data[i][j] = value;
            length += 1;
        }

        public int size()
        {
            return length;
        }

        public void clear()
        {
            length = 0;
            for (int i = 0; i < data.Length; ++i)
            {
                data[i] = null;
            }
            initializedChunks = 0;
        }

        public override string ToString()
        {
            int i;
            StringBuilder sb = new StringBuilder(length * 4);

            sb.Append('{');
            int l = length - 1;
            for (i = 0; i < l; i++)
            {
                sb.Append(get(i));
                sb.Append(',');
            }
            sb.Append(get(i));
            sb.Append('}');

            return sb.ToString();
        }

        public int getSizeInBytes()
        {
            return 4 * initializedChunks * chunkSize;
        }
    }
}
