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
     * The representation of a vectorized column of struct objects.
     *
     * Each field is represented by a separate inner ColumnVector. Since this
     * ColumnVector doesn't own any per row data other that the isNull flag, the
     * isRepeating only covers the isNull array.
     */
    public class StructColumnVector : ColumnVector
    {

        public ColumnVector[] fields;

        /**
         * Constructor for StructColumnVector
         *
         * @param len Vector length
         * @param fields the field column vectors
         */
        public StructColumnVector(int len = VectorizedRowBatch.DEFAULT_SIZE, params ColumnVector[] fields)
            : base(len)
        {
            this.fields = fields;
        }

        public override void flatten(bool selectedInUse, int[] sel, int size)
        {
            flattenPush();
            for (int i = 0; i < fields.Length; ++i)
            {
                fields[i].flatten(selectedInUse, sel, size);
            }
            flattenNoNulls(selectedInUse, sel, size);
        }

        public override void setElement(int outElementNum, int inputElementNum,
                               ColumnVector inputVector)
        {
            if (inputVector.isRepeating)
            {
                inputElementNum = 0;
            }
            if (inputVector.noNulls || !inputVector.isNull[inputElementNum])
            {
                isNull[outElementNum] = false;
                ColumnVector[] inputFields = ((StructColumnVector)inputVector).fields;
                for (int i = 0; i < inputFields.Length; ++i)
                {
                    fields[i].setElement(outElementNum, inputElementNum, inputFields[i]);
                }
            }
            else
            {
                noNulls = false;
                isNull[outElementNum] = true;
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
                for (int i = 0; i < fields.Length; ++i)
                {
                    if (i != 0)
                    {
                        buffer.Append(", ");
                    }
                    fields[i].stringifyValue(buffer, row);
                }
                buffer.Append(']');
            }
            else
            {
                buffer.Append("null");
            }
        }

        public override void ensureSize(int size, bool preserveData)
        {
            base.ensureSize(size, preserveData);
            for (int i = 0; i < fields.Length; ++i)
            {
                fields[i].ensureSize(size, preserveData);
            }
        }

        public override void reset()
        {
            base.reset();
            for (int i = 0; i < fields.Length; ++i)
            {
                fields[i].reset();
            }
        }

        public override void init()
        {
            base.init();
            for (int i = 0; i < fields.Length; ++i)
            {
                fields[i].init();
            }
        }

        public override void unFlatten()
        {
            base.unFlatten();
            for (int i = 0; i < fields.Length; ++i)
            {
                fields[i].unFlatten();
            }
        }

        public override void setRepeating(bool isRepeating)
        {
            base.setRepeating(isRepeating);
            for (int i = 0; i < fields.Length; ++i)
            {
                fields[i].setRepeating(isRepeating);
            }
        }
    }
}
