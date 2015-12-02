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

namespace OrcSharp.Types
{
    using System.Text;

    /**
     * The representation of a vectorized column of map objects.
     *
     * Each map is composed of a range of elements in the underlying child
     * ColumnVector. The range for map i is
     * offsets[i]..offsets[i]+lengths[i]-1 inclusive.
     */
    public class MapColumnVector : MultiValuedColumnVector
    {
        public ColumnVector keys;
        public ColumnVector values;

        /**
         * Constructor for MapColumnVector
         *
         * @param len Vector length
         * @param keys The keys column vector
         * @param values The values column vector
         */
        public MapColumnVector(
            int len = VectorizedRowBatch.DEFAULT_SIZE,
            ColumnVector keys = null,
            ColumnVector values = null)
            : base(len)
        {
            this.keys = keys;
            this.values = values;
        }

        protected override void childFlatten(bool useSelected, int[] selected, int size)
        {
            keys.flatten(useSelected, selected, size);
            values.flatten(useSelected, selected, size);
        }

        public override void setElement(int outElementNum, int inputElementNum,
                               ColumnVector inputVector)
        {
            if (inputVector.isRepeating)
            {
                inputElementNum = 0;
            }
            if (!inputVector.noNulls && inputVector.isNull[inputElementNum])
            {
                isNull[outElementNum] = true;
                noNulls = false;
            }
            else
            {
                MapColumnVector input = (MapColumnVector)inputVector;
                isNull[outElementNum] = false;
                int offset = childCount;
                int length = (int)input.lengths[inputElementNum];
                int inputOffset = (int)input.offsets[inputElementNum];
                offsets[outElementNum] = offset;
                childCount += length;
                lengths[outElementNum] = length;
                keys.ensureSize(childCount, true);
                values.ensureSize(childCount, true);
                for (int i = 0; i < length; ++i)
                {
                    keys.setElement(i + offset, inputOffset + i, input.keys);
                    values.setElement(i + offset, inputOffset + i, input.values);
                }
            }
        }

        public override void stringifyValue(StringBuilder buffer, int row)
        {
            if (isRepeating)
            {
                row = 0;
            }
            if (noNulls || !isNull[row])
            {
                buffer.Append('[');
                bool isFirst = true;
                for (long i = offsets[row]; i < offsets[row] + lengths[row]; ++i)
                {
                    if (isFirst)
                    {
                        isFirst = false;
                    }
                    else
                    {
                        buffer.Append(", ");
                    }
                    buffer.Append("{\"key\": ");
                    keys.stringifyValue(buffer, (int)i);
                    buffer.Append(", \"value\": ");
                    values.stringifyValue(buffer, (int)i);
                    buffer.Append('}');
                }
                buffer.Append(']');
            }
            else
            {
                buffer.Append("null");
            }
        }

        public override void init()
        {
            base.init();
            keys.init();
            values.init();
        }

        public override void reset()
        {
            base.reset();
            keys.reset();
            values.reset();
        }

        public override void unFlatten()
        {
            base.unFlatten();
            if (!isRepeating || noNulls || !isNull[0])
            {
                keys.unFlatten();
                values.unFlatten();
            }
        }
    }
}
