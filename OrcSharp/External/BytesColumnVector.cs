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
    using System;
    using System.Text;

    /**
      * This class supports string and binary data by value reference -- i.e. each field is
      * explicitly present, as opposed to provided by a dictionary reference.
      * In some cases, all the values will be in the same byte array to begin with,
      * but this need not be the case. If each value is in a separate byte
      * array to start with, or not all of the values are in the same original
      * byte array, you can still assign data by reference into this column vector.
      * This gives flexibility to use this in multiple situations.
      * <p>
      * When setting data by reference, the caller
      * is responsible for allocating the byte arrays used to hold the data.
      * You can also set data by value, as long as you call the initBuffer() method first.
      * You can mix "by value" and "by reference" in the same column vector,
      * though that use is probably not typical.
      */
    public class BytesColumnVector : ColumnVector
    {
        public byte[][] vector;
        public int[] start;          // start offset of each field

        /*
         * The length of each field. If the value repeats for every entry, then it is stored
         * in vector[0] and isRepeating from the superclass is set to true.
         */
        public int[] length;
        private byte[] buffer;   // optional buffer to use when actually copying in data
        private int nextFree;    // next free position in buffer

        // Estimate that there will be 16 bytes per entry
        const int DEFAULT_BUFFER_SIZE = 16 * DEFAULT_SIZE;

        // Proportion of extra space to provide when allocating more buffer space.
        const float EXTRA_SPACE_FACTOR = (float)1.2;

        /**
         * Use this constructor for normal operation.
         * All column vectors should be the default size normally.
         */
        public BytesColumnVector()
            : this(DEFAULT_SIZE)
        {
        }

        /**
         * Don't call this constructor except for testing purposes.
         *
         * @param size  number of elements in the column vector
         */
        internal BytesColumnVector(int size)
            : base(size)
        {
            vector = new byte[size][];
            start = new int[size];
            length = new int[size];
        }

        /**
         * Additional reset work for BytesColumnVector (releasing scratch bytes for by value strings).
         */
        public override void reset()
        {
            base.reset();
            initBuffer(0);
        }

        /** Set a field by reference.
         *
         * @param elementNum index within column vector to set
         * @param sourceBuf container of source data
         * @param start start byte position within source
         * @param length  length of source byte sequence
         */
        public void setRef(int elementNum, byte[] sourceBuf, int start, int length)
        {
            vector[elementNum] = sourceBuf;
            this.start[elementNum] = start;
            this.length[elementNum] = length;
        }

        /**
         * You must call initBuffer first before using setVal().
         * Provide the estimated number of bytes needed to hold
         * a full column vector worth of byte string data.
         *
         * @param estimatedValueSize  Estimated size of buffer space needed
         */
        public void initBuffer(int estimatedValueSize)
        {
            nextFree = 0;

            // if buffer is already allocated, keep using it, don't re-allocate
            if (buffer != null)
            {
                return;
            }

            // allocate a little extra space to limit need to re-allocate
            int bufferSize = this.vector.Length * (int)(estimatedValueSize * EXTRA_SPACE_FACTOR);
            if (bufferSize < DEFAULT_BUFFER_SIZE)
            {
                bufferSize = DEFAULT_BUFFER_SIZE;
            }
            buffer = new byte[bufferSize];
        }

        /**
         * Initialize buffer to default size.
         */
        public void initBuffer()
        {
            initBuffer(0);
        }

        /**
         * @return amount of buffer space currently allocated
         */
        public int bufferSize()
        {
            if (buffer == null)
            {
                return 0;
            }
            return buffer.Length;
        }

        /**
         * Set a field by actually copying in to a local buffer.
         * If you must actually copy data in to the array, use this method.
         * DO NOT USE this method unless it's not practical to set data by reference with setRef().
         * Setting data by reference tends to run a lot faster than copying data in.
         *
         * @param elementNum index within column vector to set
         * @param sourceBuf container of source data
         * @param start start byte position within source
         * @param length  length of source byte sequence
         */
        public void setVal(int elementNum, byte[] sourceBuf, int start, int length)
        {
            if ((nextFree + length) > buffer.Length)
            {
                increaseBufferSpace(length);
            }
            Array.Copy(sourceBuf, start, buffer, nextFree, length);
            vector[elementNum] = buffer;
            this.start[elementNum] = nextFree;
            this.length[elementNum] = length;
            nextFree += length;
        }

        /**
         * Set a field by actually copying in to a local buffer.
         * If you must actually copy data in to the array, use this method.
         * DO NOT USE this method unless it's not practical to set data by reference with setRef().
         * Setting data by reference tends to run a lot faster than copying data in.
         *
         * @param elementNum index within column vector to set
         * @param sourceBuf container of source data
         */
        public void setVal(int elementNum, byte[] sourceBuf)
        {
            setVal(elementNum, sourceBuf, 0, sourceBuf.Length);
        }

        /**
         * Set a field to the concatenation of two string values. Result data is copied
         * into the internal buffer.
         *
         * @param elementNum index within column vector to set
         * @param leftSourceBuf container of left argument
         * @param leftStart start of left argument
         * @param leftLen length of left argument
         * @param rightSourceBuf container of right argument
         * @param rightStart start of right argument
         * @param rightLen length of right arugment
         */
        public void setConcat(int elementNum, byte[] leftSourceBuf, int leftStart, int leftLen,
            byte[] rightSourceBuf, int rightStart, int rightLen)
        {
            int newLen = leftLen + rightLen;
            if ((nextFree + newLen) > buffer.Length)
            {
                increaseBufferSpace(newLen);
            }
            vector[elementNum] = buffer;
            this.start[elementNum] = nextFree;
            this.length[elementNum] = newLen;

            Array.Copy(leftSourceBuf, leftStart, buffer, nextFree, leftLen);
            nextFree += leftLen;
            Array.Copy(rightSourceBuf, rightStart, buffer, nextFree, rightLen);
            nextFree += rightLen;
        }

        /**
         * Increase buffer space enough to accommodate next element.
         * This uses an exponential increase mechanism to rapidly
         * increase buffer size to enough to hold all data.
         * As batches get re-loaded, buffer space allocated will quickly
         * stabilize.
         *
         * @param nextElemLength size of next element to be added
         */
        public void increaseBufferSpace(int nextElemLength)
        {

            // Keep doubling buffer size until there will be enough space for next element.
            int newLength = 2 * buffer.Length;
            while ((nextFree + nextElemLength) > newLength)
            {
                newLength *= 2;
            }

            // Allocate new buffer, copy data to it, and set buffer to new buffer.
            byte[] newBuffer = new byte[newLength];
            Array.Copy(buffer, 0, newBuffer, 0, nextFree);
            buffer = newBuffer;
        }

        /** Copy the current object contents into the output. Only copy selected entries,
          * as indicated by selectedInUse and the sel array.
          */
        public void copySelected(
            bool selectedInUse, int[] sel, int size, BytesColumnVector output)
        {

            // Output has nulls if and only if input has nulls.
            output.noNulls = noNulls;
            output.isRepeating = false;

            // Handle repeating case
            if (isRepeating)
            {
                output.setVal(0, vector[0], start[0], length[0]);
                output.isNull[0] = isNull[0];
                output.isRepeating = true;
                return;
            }

            // Handle normal case

            // Copy data values over
            if (selectedInUse)
            {
                for (int j = 0; j < size; j++)
                {
                    int i = sel[j];
                    output.setVal(i, vector[i], start[i], length[i]);
                }
            }
            else
            {
                for (int i = 0; i < size; i++)
                {
                    output.setVal(i, vector[i], start[i], length[i]);
                }
            }

            // Copy nulls over if needed
            if (!noNulls)
            {
                if (selectedInUse)
                {
                    for (int j = 0; j < size; j++)
                    {
                        int i = sel[j];
                        output.isNull[i] = isNull[i];
                    }
                }
                else
                {
                    Array.Copy(isNull, 0, output.isNull, 0, size);
                }
            }
        }

        /** Simplify vector by brute-force flattening noNulls and isRepeating
          * This can be used to reduce combinatorial explosion of code paths in VectorExpressions
          * with many arguments, at the expense of loss of some performance.
          */
        public override void flatten(bool selectedInUse, int[] sel, int size)
        {
            flattenPush();
            if (isRepeating)
            {
                isRepeating = false;

                // setRef is used below and this is safe, because the reference
                // is to data owned by this column vector. If this column vector
                // gets re-used, the whole thing is re-used together so there
                // is no danger of a dangling reference.

                // Only copy data values if entry is not null. The string value
                // at position 0 is undefined if the position 0 value is null.
                if (noNulls || !isNull[0])
                {

                    // loops start at position 1 because position 0 is already set
                    if (selectedInUse)
                    {
                        for (int j = 1; j < size; j++)
                        {
                            int i = sel[j];
                            this.setRef(i, vector[0], start[0], length[0]);
                        }
                    }
                    else
                    {
                        for (int i = 1; i < size; i++)
                        {
                            this.setRef(i, vector[0], start[0], length[0]);
                        }
                    }
                }
                flattenRepeatingNulls(selectedInUse, sel, size);
            }
            flattenNoNulls(selectedInUse, sel, size);
        }

        // Fill the all the vector entries with provided value
        public void fill(byte[] value)
        {
            noNulls = true;
            isRepeating = true;
            setRef(0, value, 0, value.Length);
        }

        // Fill the column vector with nulls
        public void fillWithNulls()
        {
            noNulls = false;
            isRepeating = true;
            vector[0] = null;
            isNull[0] = true;
        }

        public override void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector)
        {
            if (inputVector.isRepeating)
            {
                inputElementNum = 0;
            }
            if (inputVector.noNulls || !inputVector.isNull[inputElementNum])
            {
                isNull[outElementNum] = false;
                BytesColumnVector @in = (BytesColumnVector)inputVector;
                setVal(outElementNum, @in.vector[inputElementNum],
                    @in.start[inputElementNum], @in.length[inputElementNum]);
            }
            else
            {
                isNull[outElementNum] = true;
                noNulls = false;
            }
        }

        public override void init()
        {
            initBuffer(0);
        }

        public override void stringifyValue(StringBuilder buffer, int row)
        {
            if (isRepeating)
            {
                row = 0;
            }
            if (noNulls || !isNull[row])
            {
                buffer.Append('"');
                buffer.Append(Encoding.UTF8.GetString(this.buffer, start[row], length[row]));
                buffer.Append('"');
            }
            else
            {
                buffer.Append("null");
            }
        }

        public override void ensureSize(int size, bool preserveData)
        {
            if (size > vector.Length)
            {
                base.ensureSize(size, preserveData);
                int[] oldStart = start;
                start = new int[size];
                int[] oldLength = length;
                length = new int[size];
                byte[][] oldVector = vector;
                vector = new byte[size][];
                if (preserveData)
                {
                    if (isRepeating)
                    {
                        vector[0] = oldVector[0];
                        start[0] = oldStart[0];
                        length[0] = oldLength[0];
                    }
                    else
                    {
                        Array.Copy(oldVector, 0, vector, 0, oldVector.Length);
                        Array.Copy(oldStart, 0, start, 0, oldStart.Length);
                        Array.Copy(oldLength, 0, length, 0, oldLength.Length);
                    }
                }
            }
        }
    }
}
