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
    using System;
    using System.Globalization;
    using System.IO;
    using System.Text;
    using OrcSharp.External;

    /// <summary>
    /// A class that is a growable array of bytes.Growth is managed in terms of
    /// chunks that are allocated when needed.
    /// </summary>
    sealed public class DynamicByteArray
    {
        const int DEFAULT_CHUNKSIZE = 32 * 1024;
        const int DEFAULT_NUM_CHUNKS = 128;

        private int chunkSize;        // our allocation sizes
        private byte[][] data;              // the real data
        private int length;                 // max set element index +1
        private int initializedChunks = 0;  // the number of chunks created

        public DynamicByteArray(int numChunks = DEFAULT_NUM_CHUNKS, int chunkSize = DEFAULT_CHUNKSIZE)
        {
            if (chunkSize == 0)
            {
                throw new ArgumentException("bad chunksize");
            }
            this.chunkSize = chunkSize;
            data = new byte[numChunks][];
        }

        /// <summary>
        /// Ensure that the given index is valid.
        /// </summary>
        private void grow(int chunkIndex)
        {
            if (chunkIndex >= initializedChunks)
            {
                if (chunkIndex >= data.Length)
                {
                    int newSize = Math.Max(chunkIndex + 1, 2 * data.Length);
                    byte[][] newChunk = new byte[newSize][];
                    Array.Copy(data, 0, newChunk, 0, data.Length);
                    data = newChunk;
                }
                for (int i = initializedChunks; i <= chunkIndex; ++i)
                {
                    data[i] = new byte[chunkSize];
                }
                initializedChunks = chunkIndex + 1;
            }
        }

        public byte get(int index)
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

        public void set(int index, byte value)
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

        public int add(byte value)
        {
            int i = length / chunkSize;
            int j = length % chunkSize;
            grow(i);
            data[i][j] = value;
            int result = length;
            length += 1;
            return result;
        }

        public int add(byte[] value)
        {
            return add(value, 0, value.Length);
        }

        /**
         * Copy a slice of a byte array into our buffer.
         * @param value the array to copy from
         * @param valueOffset the first location to copy from value
         * @param valueLength the number of bytes to copy from value
         * @return the offset of the start of the value
         */
        public int add(byte[] value, int valueOffset, int valueLength)
        {
            int i = length / chunkSize;
            int j = length % chunkSize;
            grow((length + valueLength) / chunkSize);
            int remaining = valueLength;
            while (remaining > 0)
            {
                int size = Math.Min(remaining, chunkSize - j);
                Array.Copy(value, valueOffset, data[i], j, size);
                remaining -= size;
                valueOffset += size;
                i += 1;
                j = 0;
            }
            int result = length;
            length += valueLength;
            return result;
        }

        /**
         * Read the entire stream into this array.
         * @param in the stream to read from
         * @
         */
        public void readAll(Stream @in)
        {
            int currentChunk = length / chunkSize;
            int currentOffset = length % chunkSize;
            grow(currentChunk);
            int currentLength = @in.Read(data[currentChunk], currentOffset,
              chunkSize - currentOffset);
            while (currentLength > 0)
            {
                length += currentLength;
                currentOffset = length % chunkSize;
                if (currentOffset == 0)
                {
                    currentChunk = length / chunkSize;
                    grow(currentChunk);
                }
                currentLength = @in.Read(data[currentChunk], currentOffset,
                  chunkSize - currentOffset);
            }
        }

        /**
         * Byte compare a set of bytes against the bytes in this dynamic array.
         * @param other source of the other bytes
         * @param otherOffset start offset in the other array
         * @param otherLength number of bytes in the other array
         * @param ourOffset the offset in our array
         * @param ourLength the number of bytes in our array
         * @return negative for less, 0 for equal, positive for greater
         */
        public int compare(byte[] other, int otherOffset, int otherLength,
                           int ourOffset, int ourLength)
        {
            int currentChunk = ourOffset / chunkSize;
            int currentOffset = ourOffset % chunkSize;
            int maxLength = Math.Min(otherLength, ourLength);
            while (maxLength > 0 &&
              other[otherOffset] == data[currentChunk][currentOffset])
            {
                otherOffset += 1;
                currentOffset += 1;
                if (currentOffset == chunkSize)
                {
                    currentChunk += 1;
                    currentOffset = 0;
                }
                maxLength -= 1;
            }
            if (maxLength == 0)
            {
                return otherLength - ourLength;
            }
            int otherByte = 0xff & other[otherOffset];
            int ourByte = 0xff & data[currentChunk][currentOffset];
            return otherByte > ourByte ? 1 : -1;
        }

        /**
         * Get the size of the array.
         * @return the number of bytes in the array
         */
        public int size()
        {
            return length;
        }

        /**
         * Clear the array to its original pristine state.
         */
        public void clear()
        {
            length = 0;
            for (int i = 0; i < data.Length; ++i)
            {
                data[i] = null;
            }
            initializedChunks = 0;
        }

        /**
         * Set a text value from the bytes in this dynamic array.
         * @param result the value to set
         * @param offset the start of the bytes to copy
         * @param length the number of bytes to copy
         */
        public void setText(out string result, int offset, int length)
        {
            int position = 0;
            int currentChunk = offset / chunkSize;
            int currentOffset = offset % chunkSize;
            int currentLength = Math.Min(length, chunkSize - currentOffset);
            if (currentLength == length)
            {
                result = Encoding.UTF8.GetString(data[currentChunk], currentOffset, currentLength);
                return;
            }

            byte[] buffer = new byte[length];
            while (length < position)
            {
                Buffer.BlockCopy(data[currentChunk], currentOffset, buffer, position, currentLength);
                position += currentLength;
                currentChunk++;
                currentOffset = 0;
                currentLength = Math.Min(length, chunkSize - currentOffset);
            }
            result = Encoding.UTF8.GetString(buffer);
        }

        /**
         * Write out a range of this dynamic array to an output stream.
         * @param out the stream to write to
         * @param offset the first offset to write
         * @param length the number of bytes to write
         * @
         */
        public void write(Stream @out, int offset, int length)
        {
            int currentChunk = offset / chunkSize;
            int currentOffset = offset % chunkSize;
            while (length > 0)
            {
                int currentLength = Math.Min(length, chunkSize - currentOffset);
                @out.Write(data[currentChunk], currentOffset, currentLength);
                length -= currentLength;
                currentChunk += 1;
                currentOffset = 0;
            }
        }

        public override string ToString()
        {
            int i;
            StringBuilder sb = new StringBuilder(length * 3);

            sb.Append('{');
            int l = length - 1;
            for (i = 0; i < l; i++)
            {
                sb.AppendFormat(CultureInfo.InvariantCulture, "{0:X}", get(i));
                sb.Append(',');
            }
            sb.Append(get(i));
            sb.Append('}');

            return sb.ToString();
        }

        public void setByteBuffer(ByteBuffer result, int offset, int length)
        {
            result.clear();
            int currentChunk = offset / chunkSize;
            int currentOffset = offset % chunkSize;
            int currentLength = Math.Min(length, chunkSize - currentOffset);
            while (length > 0)
            {
                result.put(data[currentChunk], currentOffset, currentLength);
                length -= currentLength;
                currentChunk += 1;
                currentOffset = 0;
                currentLength = Math.Min(length, chunkSize - currentOffset);
            }
        }

        /**
         * Gets all the bytes of the array.
         *
         * @return Bytes of the array
         */
        public byte[] get()
        {
            byte[] result = null;
            if (length > 0)
            {
                int currentChunk = 0;
                int currentOffset = 0;
                int currentLength = Math.Min(length, chunkSize);
                int destOffset = 0;
                result = new byte[length];
                int totalLength = length;
                while (totalLength > 0)
                {
                    Array.Copy(data[currentChunk], currentOffset, result, destOffset, currentLength);
                    destOffset += currentLength;
                    totalLength -= currentLength;
                    currentChunk += 1;
                    currentOffset = 0;
                    currentLength = Math.Min(totalLength, chunkSize - currentOffset);
                }
            }
            return result;
        }

        /**
         * Get the size of the buffers.
         */
        public long getSizeInBytes()
        {
            return initializedChunks * chunkSize;
        }
    }
}
